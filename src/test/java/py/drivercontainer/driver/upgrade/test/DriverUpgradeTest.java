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

package py.drivercontainer.driver.upgrade.test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Paths;
import java.util.concurrent.Future;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import py.app.context.AppContext;
import py.driver.DriverAction;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.DriverUpgradeProcessor;
import py.drivercontainer.DriverVersion;
import py.drivercontainer.driver.store.DriverStore;
import py.drivercontainer.driver.store.DriverStoreImpl;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.drivercontainer.driver.store.DriverStoreManagerImpl;
import py.drivercontainer.driver.upgrade.UpgradeDriverPollerImpl;
import py.drivercontainer.driver.version.Version;
import py.drivercontainer.driver.version.VersionManager;
import py.drivercontainer.driver.version.file.VersionImpl;
import py.drivercontainer.driver.version.file.VersionManagerImpl;
import py.drivercontainer.service.PortContainer;
import py.drivercontainer.service.PortContainerFactory;
import py.drivercontainer.service.taskqueue.TaskExecutor;
import py.drivercontainer.utils.DriverContainerUtils;
import py.drivercontainer.worker.LaunchDriverWorker;
import py.drivercontainer.worker.LaunchDriverWorkerFactory;
import py.drivercontainer.worker.RemoveDriverWorker;
import py.drivercontainer.worker.RemoveDriverWorkerFactory;
import py.drivercontainer.worker.SubmitUpgradeRequestWorker;
import py.icshare.DriverKey;
import py.processmanager.Pmdb;
import py.processmanager.utils.PmUtils;
import py.test.TestBase;

public class DriverUpgradeTest extends TestBase {

  @Mock
  private static Future future;
  DriverStoreManager driverStoreManager = new DriverStoreManagerImpl();
  private VersionManager versionManager;

  private DriverUpgradeProcessor driverUpgradeProcessor;
  @Mock
  private LaunchDriverWorkerFactory launchDriverWorkerFactory;
  @Mock
  private LaunchDriverWorker launchDriverWorker;
  @Mock
  private RemoveDriverWorkerFactory removeDriverWorkerFactory;
  @Mock
  private RemoveDriverWorker removeDriverWorker;
  @Mock
  private TaskExecutor taskExecutor;
  private UpgradeDriverPollerImpl upgradeDriverPoller;
  @Mock
  private PortContainerFactory portContainerFactory;
  @Mock
  private PortContainer portContainer;
  @Mock
  private AppContext appContext;
  private SubmitUpgradeRequestWorker submitUpgradeRequestWorker = new SubmitUpgradeRequestWorker();
  @Mock
  private DriverContainerConfiguration dcConfig;
  private String driverStoreDirectory = "/tmp/updradeTest";
  private String nbdCurVer = "2.3.0-internal-20170918000011";
  Version nbdCurrentVersion = VersionImpl.get(nbdCurVer);
  private String nbdLatVer = "2.3.0-internal-20170918000022";
  Version nbdLatestVersion = VersionImpl.get(nbdLatVer);


  /**
   * xx.
   */
  @Before
  public void init() throws Exception {
    File file = new File(driverStoreDirectory);
    file.mkdirs();
    DriverContainerUtils.init();
    driverUpgradeProcessor = new FakeDriverUpgradeProcessor();
    versionManager = new VersionManagerImpl(driverStoreDirectory);

    // When driver is not upgrade,latest version is same as current version
    versionManager.setCurrentVersion(DriverType.NBD, nbdCurrentVersion);
    versionManager.setLatestVersion(DriverType.NBD, nbdCurrentVersion);

    DriverVersion.currentVersion.put(DriverType.NBD, nbdCurrentVersion);
    DriverVersion.latestVersion.put(DriverType.NBD, nbdCurrentVersion);
    DriverVersion.isOnMigration.put(DriverType.NBD, false);

    driverUpgradeProcessor.setVersionManager(versionManager);

    driverUpgradeProcessor.setLaunchDriverWorkerFactory(launchDriverWorkerFactory);
    driverUpgradeProcessor.setRemoveDriverWorkerFactory(removeDriverWorkerFactory);
    driverUpgradeProcessor.setTaskExecutor(taskExecutor);
    driverUpgradeProcessor.setPortContainerFactory(portContainerFactory);
    driverUpgradeProcessor.setDcConfig(dcConfig);

    when(launchDriverWorkerFactory.createWorker()).thenReturn(launchDriverWorker);
    when(removeDriverWorkerFactory.createWorker(any(DriverKey.class)))
        .thenReturn(removeDriverWorker);
    when(taskExecutor.submit(any(DriverKey.class), any(DriverAction.class), any(Runnable.class),
        any(Version.class))).thenReturn(future);
    when(future.isDone()).thenReturn(true);
    when(portContainerFactory.getPortContainer(any(DriverType.class))).thenReturn(portContainer);
    when(portContainer.getAvailablePort()).thenReturn(1234);
    when(dcConfig.getDriverUpgradeLimit()).thenReturn(5);
    when(dcConfig.getDriverAbsoluteRootPath()).thenReturn(driverStoreDirectory);
  }

  /**
   * Check whether current version turn to latest version and driver port turn to 1234  after
   * upgrade.
   */
  @Test
  public void upgradeDriverTest() throws Exception {
    //Initial driverStoreManager,current store and latest store contents are same
    final DriverKey iscsiDriverKey = new DriverKey(0, 0, 0, DriverType.ISCSI);
    DriverKey nbdDriverKey = new DriverKey(0, 0, 0, DriverType.NBD);

    DriverStore nbdCurrentDriverStore = createDriverStore(DriverType.ISCSI, 2, nbdCurrentVersion);
    DriverMetadata ndbDriver = buildDriver(nbdDriverKey, 0);
    nbdCurrentDriverStore.save(ndbDriver);

    DriverStore nbdLatestDriverStore = new DriverStoreImpl(
        Paths.get(driverStoreDirectory, Pmdb.COORDINATOR_PIDS_DIR_NAME), nbdLatestVersion);
    driverStoreManager.put(nbdCurrentVersion, nbdCurrentDriverStore);
    driverStoreManager.put(nbdLatestVersion, nbdLatestDriverStore);

    //Before upgrade ,iscsiDriver port is 0
    Assert.assertTrue(driverStoreManager.get(nbdCurrentVersion).get(iscsiDriverKey).getPort() == 0);

    //Going to upgrade,set latest version different to current version
    versionManager.setLatestVersion(DriverType.NBD, nbdLatestVersion);
    DriverVersion.latestVersion.put(DriverType.NBD, nbdLatestVersion);

    upgradeDriverPoller = new UpgradeDriverPollerImpl(dcConfig, versionManager, driverStoreManager);
    driverUpgradeProcessor.setUpgradeDriverPoller(upgradeDriverPoller);
    driverUpgradeProcessor.setDriverStoreManager(driverStoreManager);
    driverUpgradeProcessor.setVersionManager(versionManager);

    submitUpgradeRequestWorker.setDriverUpgradeProcessor(driverUpgradeProcessor);
    versionManager.setOnMigration(DriverType.NBD, true);
    DriverVersion.isOnMigration.put(DriverType.NBD, true);

    driverUpgradeProcessor.submit(DriverType.NBD);

    Assert.assertTrue(versionManager.isOnMigration(DriverType.NBD));

    ServerSocket ss = null;
    try {
      ss = new ServerSocket(1111);
      Thread thread = new Thread(driverUpgradeProcessor);
      thread.start();
      driverUpgradeProcessor.interrupt();
      thread.join();

      Assert.assertFalse(versionManager.isOnMigration(DriverType.NBD));
      Assert.assertTrue(driverStoreManager.get(versionManager.getCurrentVersion(DriverType.NBD))
          .get(iscsiDriverKey).getPort() == 1111);
    } catch (IOException e) {
      logger.warn("catch an exception when bind a port", e);
    } finally {
      if (ss != null) {
        try {
          ss.close();
        } catch (IOException e) {
          logger.warn("catch an exception when unbind a port", e);
        }
      }
    }


  }


  /**
   * xx.
   */
  public DriverStore createDriverStore(DriverType driverType, int driverNumber, Version version) {
    DriverStore driverStore = new DriverStoreImpl(
        Paths.get(driverStoreDirectory, Pmdb.COORDINATOR_PIDS_DIR_NAME), version);
    for (int i = 0; i < driverNumber; i++) {
      DriverKey driverKey = new DriverKey(0, i, 0, driverType);
      driverStore.save(buildDriver(driverKey, 0));
    }

    return driverStore;

  }

  private DriverMetadata buildDriver(DriverKey driverKey, int port) {
    DriverMetadata driver = new DriverMetadata();
    driver.setDriverContainerId(driverKey.getDriverContainerId());
    driver.setVolumeId(driverKey.getVolumeId());
    driver.setDriverType(driverKey.getDriverType());
    driver.setSnapshotId(driverKey.getSnapshotId());
    driver.setDriverStatus(DriverStatus.LAUNCHED);
    driver.setPort(port);
    return driver;
  }

  @After
  public void clean() {
    FileUtils.deleteQuietly(Paths.get(driverStoreDirectory).toFile());
  }

  public class FakeDriverUpgradeProcessor extends DriverUpgradeProcessor {

    public Future<?> getRemoveWorkFuture(DriverKey driverKey, Version version) {
      driverStoreManager.get(version).remove(driverKey);
      return DriverUpgradeTest.future;
    }


    /**
     * xx.
     */
    public Future<?> getLaunchWorkFuture(DriverMetadata driver, Version version) {
      driver.setProcessId(PmUtils.getCurrentProcessPid());
      driver.setDriverStatus(DriverStatus.LAUNCHED);
      driver.setPort(1111);
      logger.warn("driver store session is:{}", driverStoreManager.get(version).getVersion());
      driverStoreManager.get(version).save(driver);
      return DriverUpgradeTest.future;
    }
  }
}
