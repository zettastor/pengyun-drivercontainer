/*
 * Copyright (c) 2022. PengYunNetWork
 *
 * This program is free software: you can use, redistribute, and/or modify it
 * under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 *  You should have received a copy of the GNU Affero General Public License along with
 *  this program. If not, see <http://www.gnu.org/licenses/>.
 */

package py.drivercontainer.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import py.app.context.AppContext;
import py.common.RequestIdBuilder;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.DriverUpgradeProcessor;
import py.drivercontainer.driver.store.DriverStore;
import py.drivercontainer.driver.store.DriverStoreImpl;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.drivercontainer.driver.store.DriverStoreManagerImpl;
import py.drivercontainer.driver.version.Version;
import py.drivercontainer.driver.version.VersionException;
import py.drivercontainer.driver.version.VersionManager;
import py.drivercontainer.driver.version.file.VersionImpl;
import py.drivercontainer.driver.version.file.VersionManagerImpl;
import py.drivercontainer.utils.DriverContainerUtils;
import py.drivercontainer.worker.DriverScanWorker;
import py.drivercontainer.worker.LaunchDriverWorker;
import py.drivercontainer.worker.LaunchDriverWorkerFactory;
import py.drivercontainer.worker.RemoveDriverWorker;
import py.drivercontainer.worker.RemoveDriverWorkerFactory;
import py.icshare.DriverKey;
import py.instance.InstanceId;
import py.test.TestBase;

//import py.coordinator.iscsi.IETManagerConfiguration;

/**
 * A class includes some tests for relaunching driver in driver container bootstrap.
 *
 */
public class BootstrapTest extends TestBase {

  @Mock
  DriverScanWorker driverScanWorker;
  @Mock
  private AppContext appContext;
  @Mock
  private ExecutorService driverExecutor;
  @Mock
  private Future future;
  @Mock
  private LaunchDriverWorkerFactory launchDriverWorkerFactory;
  @Mock
  private LaunchDriverWorker launchDriverWorker;
  @Mock
  private RemoveDriverWorkerFactory removeDriverWorkerFactory;
  @Mock
  private RemoveDriverWorker removeDriverWorker;
  @Mock
  private DriverContainerConfiguration driverContainerConfig;
  @Mock
  private PortContainerFactory portContainerFactory;
  @Mock
  private PortContainer portContainer;
  private ExecutorService cmdThreadPool = Executors.newFixedThreadPool(2, new ThreadFactory() {

    @Override
    public Thread newThread(Runnable r) {
      return new Thread(r, "OS CMD Consumer");
    }
  });
  private BootStrap bootstrap = new BootStrapRewrite();
  private int fakePort = 10000;
  private DriverStoreManagerImpl driverStoreManager;
  @Mock
  private DriverUpgradeProcessor driverUpgradeProcessor;
  //    private IETManagerConfiguration ietManagerConfiguration;
  private VersionManager versionManager;
  private String driverStoreDirectory = "/tmp/BootStrapTest";
  private String loadDirectory = "/tmp/loadTest";
  private Version nbdCurVer = VersionImpl.get("2.3.0-internal-20170918000011");
  private Version nbdLatVer = VersionImpl.get("2.3.0-internal-20170925000022");

  @Override
  @Before
  public void init() throws Exception {
    super.init();
    DriverContainerUtils.osCMDThreadPool = cmdThreadPool;
    when(portContainerFactory.getPortContainer(Mockito.any(DriverType.class)))
        .thenReturn(portContainer);
    File file = new File(driverStoreDirectory);
    file.mkdirs();
    File file1 = new File(loadDirectory);
    file1.mkdirs();
    driverStoreManager = new DriverStoreManagerImpl();
    bootstrap.setAppContext(appContext);
    bootstrap.setPortContainerFactory(portContainerFactory);
    bootstrap.setDriverContainerConfig(driverContainerConfig);
    bootstrap.setDriverStoreManager(driverStoreManager);
    bootstrap.setDriverUpgradeProcessor(driverUpgradeProcessor);
    bootstrap.setDriverScanWorker(driverScanWorker);
    versionManager = new VersionManagerImpl(driverStoreDirectory);

    versionManager.setCurrentVersion(DriverType.NBD, nbdCurVer);
    versionManager.setLatestVersion(DriverType.NBD, nbdCurVer);
    bootstrap.setVersionManager(versionManager);

    when(portContainerFactory.getPortContainer(any(DriverType.class))).thenReturn(portContainer);
    when(launchDriverWorkerFactory.createWorker()).thenReturn(launchDriverWorker);
    when(removeDriverWorkerFactory.createWorker(any(DriverKey.class)))
        .thenReturn(removeDriverWorker);
    when(appContext.getInstanceId()).thenReturn(new InstanceId(1));
    when(driverExecutor.submit(any(RemoveDriverWorker.class))).thenReturn(future);
    when(future.get()).thenReturn(null);
    when(driverContainerConfig.getDriverOperationTimeout()).thenReturn(10000L);

    bootstrap.setStartLioServiceCommand("ls /tmp");
    bootstrap.setAppContext(appContext);
    bootstrap.setPortContainerFactory(portContainerFactory);
    bootstrap.setDriverContainerConfig(driverContainerConfig);
  }

  /**
   * Test the case in which driver has launching before. After bootstrap, a woker is created to
   * launch the driver again.
   */
  @Test
  public void launchDriverWithLaunchingStatus() throws VersionException, IOException {

    DriverStore nbdCurStore1 = new DriverStoreImpl(Paths.get(loadDirectory), nbdCurVer);
    driverStoreManager.put(nbdCurVer, nbdCurStore1);
    DriverKey driverKey = new DriverKey(1, 3, 3, DriverType.NBD);
    DriverStore nbdCurStore = new DriverStoreImpl(Paths.get(loadDirectory), nbdCurVer);
    nbdCurStore.save(buildDriver(driverKey, DriverStatus.LAUNCHING));
    Assert.assertTrue(bootstrap.bootstrap());
    Assert.assertTrue(
        driverStoreManager.get(nbdCurVer).get(driverKey).getDriverStatus() == DriverStatus.ERROR);
  }

  /**
   * Firstly,save driver to loadDirectory and create driverStorageManage with null value,and then
   * test load driver in loadDirectory to driverStorageManage.
   */
  @Test
  public void loadDriverTest() throws IOException, VersionException {

    //change latest version different to current version,means driver is upgrading
    versionManager.setLatestVersion(DriverType.NBD, nbdLatVer);
    versionManager.setOnMigration(DriverType.NBD, true);

    final DriverStoreManager driverStoreManager = new DriverStoreManagerImpl();

    DriverStore nbdCurStore = new DriverStoreImpl(Paths.get(loadDirectory), nbdCurVer);
    DriverStore nbdLatStore = new DriverStoreImpl(Paths.get(loadDirectory), nbdLatVer);

    DriverKey driverKey1 = new DriverKey(1, 1, 1, DriverType.NBD);
    DriverKey driverKey2 = new DriverKey(1, 2, 2, DriverType.NBD);
    nbdCurStore.save(buildDriver(driverKey1, DriverStatus.LAUNCHING));
    nbdCurStore.save(buildDriver(driverKey2, DriverStatus.LAUNCHED));

    nbdLatStore.save(buildDriver(driverKey1, DriverStatus.LAUNCHING));

    //initial driverstore
    DriverStore nbdCurStore1 = new DriverStoreImpl(Paths.get(loadDirectory), nbdCurVer);
    DriverStore nbdLatStore1 = new DriverStoreImpl(Paths.get(loadDirectory), nbdLatVer);

    driverStoreManager.put(nbdCurVer, nbdCurStore1);
    driverStoreManager.put(nbdLatVer, nbdLatStore1);

    // before load from file ,driverStoreManager has no driver
    for (Version version : driverStoreManager.keySet()) {
      Assert.assertTrue(driverStoreManager.get(version).list().size() == 0);
    }

    bootstrap.setDriverStoreManager(driverStoreManager);

    DriverContainerUtils.init();
    bootstrap.bootstrap();
    Assert.assertTrue(driverStoreManager.get(nbdCurVer).list().size() == 2);
    Assert.assertTrue(
        driverStoreManager.get(nbdCurVer).get(driverKey1).getDriverStatus() == DriverStatus.ERROR);
    Assert.assertTrue(
        driverStoreManager.get(nbdLatVer).get(driverKey1).getDriverStatus()
            == DriverStatus.RECOVERING);
  }

  private DriverMetadata buildDriver(DriverKey driverKey, DriverStatus driverStatus) {
    DriverMetadata driver = new DriverMetadata();
    driver.setDriverContainerId(driverKey.getDriverContainerId());
    driver.setVolumeId(driverKey.getVolumeId());
    driver.setDriverType(driverKey.getDriverType());
    driver.setSnapshotId(driverKey.getSnapshotId());
    driver.setDriverStatus(driverStatus);
    driver.setProcessId(Integer.MAX_VALUE);
    return driver;
  }

  private DriverMetadata buildDriverWithStatus(DriverStatus status) {
    DriverMetadata driver = new DriverMetadata();
    driver.setVolumeId(RequestIdBuilder.get());
    driver.setDriverType(DriverType.NBD);
    driver.setDriverStatus(status);
    driver.setProcessId(Integer.MAX_VALUE);
    return driver;
  }

  private boolean checkPort(int port, boolean close) {
    try {
      ServerSocket server = new ServerSocket(port);
      System.out.println("The port is available.");
      if (close) {
        server.close();
      }
      return true;
    } catch (IOException e) {
      System.out.println("The port is occupied.");
      return false;
    }
  }

  @After
  public void clean() {
    FileUtils.deleteQuietly(Paths.get("/tmp/BootStrapTest").toFile());
    FileUtils.deleteQuietly(Paths.get("/tmp/loadTest").toFile());
  }

  class BootStrapRewrite extends BootStrap {

    public void syncTargetcliToConfigFile() {
      return;
    }

    public void setGlobalAutoAddDefaultPortal() {
      return;
    }
  }
}
