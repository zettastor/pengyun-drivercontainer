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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Paths;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import py.app.NetworkConfiguration;
import py.app.context.AppContext;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.driver.DriverAction;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.DriverVersion;
import py.drivercontainer.JvmConfigurationForDriver;
import py.drivercontainer.JvmConfigurationManager;
import py.drivercontainer.driver.store.DriverStore;
import py.drivercontainer.driver.store.DriverStoreImpl;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.drivercontainer.driver.store.DriverStoreManagerImpl;
import py.drivercontainer.driver.version.Version;
import py.drivercontainer.driver.version.VersionManager;
import py.drivercontainer.driver.version.file.VersionImpl;
import py.drivercontainer.driver.version.file.VersionManagerImpl;
import py.drivercontainer.service.taskqueue.TaskExecutor;
import py.drivercontainer.worker.LaunchDriverWorker;
import py.drivercontainer.worker.LaunchDriverWorkerFactory;
import py.icshare.DriverKey;
import py.instance.InstanceId;
import py.monitor.jmx.server.SigarJniLibraryCopier;
import py.test.TestBase;
import py.thrift.drivercontainer.service.DriverContainer;
import py.thrift.share.DriverIsUpgradingExceptionThrift;
import py.thrift.share.DriverTypeThrift;
import py.thrift.share.ExistsDriverExceptionThrift;
import py.thrift.share.LaunchDriverRequestThrift;
import py.thrift.share.SystemMemoryIsNotEnoughThrift;


/**
 * A class including some test for interface launchDriver in {@link DriverContainer.Iface}.
 *
 */
public class LaunchDriverTest extends TestBase {

  @Mock
  private DriverStore nbdDriverStore;
  private DriverStore driverStore;

  @Mock
  private LaunchDriverWorkerFactory launchDriverWorkerFactory;
  @Mock
  private LaunchDriverWorker launchDriverWorker;

  @Mock
  private TaskExecutor taskExecutor;

  @Mock
  private PortContainerFactory portContainerFactory;

  @Mock
  private PortContainer portContainer;

  private NetworkConfiguration networkConfiguration;

  private DriverContainerImpl dcImpl;

  @Mock
  private InstanceId instanceId;

  @Mock
  private AppContext appContext;

  private Long driverContainerId = RequestIdBuilder.get();

  private VersionManager versionManager;
  private String driverStoreDirectory = "/tmp/LaunchDriverTest";
  private String launchUpgradingDriverDir = "/tmp/launchUpgradingDir";
  private DriverStoreManager driverStoreManager = new DriverStoreManagerImpl();

  @Override
  @Before
  public void init() throws Exception {
    super.init();
    final String subnet = "10.0.1.0/8";
    File file = new File(driverStoreDirectory);
    file.mkdirs();
    File file1 = new File(launchUpgradingDriverDir);
    file1.mkdirs();
    versionManager = new VersionManagerImpl(driverStoreDirectory);

    Version version = VersionImpl.get("2.3.0-internal-20170918000011");
    //If current version different from latest version,means driver is upgrading,can not launch
    // driver.Set current version and latest version are same
    versionManager.setCurrentVersion(DriverType.NBD, version);
    versionManager.setLatestVersion(DriverType.NBD, version);
    DriverVersion.isOnMigration.put(DriverType.NBD, false);
    DriverVersion.currentVersion.put(DriverType.NBD, version);
    DriverVersion.latestVersion.put(DriverType.NBD, version);

    driverStore = new DriverStoreImpl(Paths.get(driverStoreDirectory), version);
    driverStoreManager.put(version, driverStore);

    networkConfiguration = new NetworkConfiguration();
    networkConfiguration.setControlFlowSubnet(subnet);
    networkConfiguration.setDataFlowSubnet(subnet);
    networkConfiguration.setMonitorFlowSubnet(subnet);
    networkConfiguration.setOutwardFlowSubnet(subnet);
    networkConfiguration.setEnableDataDepartFromControl(false);
    dcImpl = new DriverContainerImpl(appContext);
    dcImpl.setDriverStoreManager(driverStoreManager);
    dcImpl.setVersionManager(versionManager);
    dcImpl.setTaskExecutor(taskExecutor);
    dcImpl.setLaunchDriverWorkerFactory(launchDriverWorkerFactory);
    dcImpl.setNetworkConfiguration(networkConfiguration);
    dcImpl.setPortContainerFactory(portContainerFactory);
    SigarJniLibraryCopier.getInstance();
    InstanceId instanceId = new InstanceId(driverContainerId);
    when(appContext.getInstanceId()).thenReturn(instanceId);
  }

  /**
   * Test the case in which request to launch nbd driver is refused due to nbd driver binding to
   * same volume already exists.
   */
  @Test
  public void launchNbdDriverAlreadyExists() throws Exception {
    driverStoreManager.put(VersionImpl.get("2.3.0-internal-20170918000011"), nbdDriverStore);
    when(nbdDriverStore.get(any(DriverKey.class)))
        .thenReturn(buildDriverWithStatus(DriverStatus.LAUNCHED));
    when(appContext.getInstanceId()).thenReturn(instanceId);
    boolean exceptionCatched = false;
    try {
      LaunchDriverRequestThrift request = new LaunchDriverRequestThrift();
      request.setVolumeId(0L);
      request.setDriverType(DriverTypeThrift.NBD);
      dcImpl.launchDriver(request);
    } catch (ExistsDriverExceptionThrift e) {
      exceptionCatched = true;
    }

    Assert.assertTrue(exceptionCatched);
  }

  /**
   * Current version different from latest version means driver is upgrading,in this case,can not
   * launch driver.
   */
  @Test
  public void launchUpgradingDriverFailedTest() throws Exception {
    VersionManager upVersionManager = new VersionManagerImpl(launchUpgradingDriverDir);
    Version currentVer = VersionImpl.get("2.3.0-internal-20170918000011");
    Version latestVer = VersionImpl.get("2.3.0-internal-20170918000022");
    upVersionManager.setLatestVersion(DriverType.NBD, currentVer);
    upVersionManager.setCurrentVersion(DriverType.NBD, latestVer);
    DriverVersion.isOnMigration.put(DriverType.NBD, true);
    DriverVersion.currentVersion.put(DriverType.NBD, currentVer);
    DriverVersion.latestVersion.put(DriverType.NBD, latestVer);

    dcImpl.setVersionManager(upVersionManager);
    boolean exceptionCatched = false;
    try {
      LaunchDriverRequestThrift request = new LaunchDriverRequestThrift();
      request.setVolumeId(0L);
      request.setDriverType(DriverTypeThrift.NBD);
      dcImpl.launchDriver(request);
    } catch (DriverIsUpgradingExceptionThrift e) {
      exceptionCatched = true;
    }

    Assert.assertTrue(exceptionCatched);
  }

  /**
   * Test the case in which request to launch nbd driver is refused due to no enough memory is
   * system exists.
   */
  @Test
  public void launchNbdDriverWithLessMem() throws Exception {
    boolean exceptionCatched = false;

    JvmConfigurationForDriver jvmConfigurationForDriver = new JvmConfigurationForDriver();
    jvmConfigurationForDriver.setMinMemPoolSize("1000000G");

    JvmConfigurationManager manager = new JvmConfigurationManager();
    manager.setCoordinatorJvmConfig(jvmConfigurationForDriver);

    dcImpl.setJvmConfigManager(manager);

    DriverContainerConfiguration driverConfig = new DriverContainerConfiguration();
    driverConfig.setSystemMemoryForceReserved("100000G");
    dcImpl.setDriverContainerConfig(driverConfig);

    try {
      LaunchDriverRequestThrift request = new LaunchDriverRequestThrift();
      request.setVolumeId(0L);
      request.setDriverType(DriverTypeThrift.NBD);
      dcImpl.launchDriver(request);
    } catch (SystemMemoryIsNotEnoughThrift e) {
      exceptionCatched = true;
    }

    Assert.assertTrue(exceptionCatched);
  }

  /**
   * Test the case in which successfully launch nbd driver.
   */
  @Test
  public void launchNbdDriver() throws Exception {
    when(launchDriverWorkerFactory.createWorker()).thenReturn(launchDriverWorker);
    when(appContext.getMainEndPoint()).thenReturn(new EndPoint());
    when(appContext.getInstanceId()).thenReturn(new InstanceId(0L));
    when(portContainerFactory.getPortContainer(any(DriverType.class))).thenReturn(portContainer);
    when(portContainer.getAvailablePort()).thenReturn(1234);

    JvmConfigurationForDriver jvmConfigurationForDriver = new JvmConfigurationForDriver();
    jvmConfigurationForDriver.setMinMemPoolSize("128M");

    JvmConfigurationManager manager = new JvmConfigurationManager();
    manager.setCoordinatorJvmConfig(jvmConfigurationForDriver);

    dcImpl.setJvmConfigManager(manager);

    DriverContainerConfiguration driverConfig = new DriverContainerConfiguration();
    driverConfig.setSystemMemoryForceReserved("10M");

    dcImpl.setDriverContainerConfig(driverConfig);

    LaunchDriverRequestThrift request = new LaunchDriverRequestThrift();
    request.setVolumeId(0L);
    request.setDriverType(DriverTypeThrift.NBD);
    dcImpl.launchDriver(request);
    Mockito.verify(taskExecutor, Mockito.times(1))
        .submit(any(DriverKey.class), any(DriverAction.class),
            any(Runnable.class), any(Version.class));
  }

  private DriverMetadata buildDriverWithStatus(DriverStatus status) {
    DriverMetadata driver = new DriverMetadata();
    driver.setVolumeId(RequestIdBuilder.get());
    driver.setDriverType(DriverType.NBD);
    driver.setDriverStatus(status);
    driver.setProcessId(Integer.MAX_VALUE);
    return driver;
  }

  @After
  public void clean() {
    FileUtils.deleteQuietly(Paths.get("/tmp/LaunchDriverTest").toFile());
  }
}
