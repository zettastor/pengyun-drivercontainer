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

package py.drivercontainer.worker;

import java.nio.file.Paths;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.JvmConfigurationForDriver;
import py.drivercontainer.driver.LaunchDriverParameters;
import py.drivercontainer.driver.store.DriverStore;
import py.drivercontainer.driver.store.DriverStoreImpl;
import py.drivercontainer.driver.version.Version;
import py.drivercontainer.driver.version.file.VersionImpl;
import py.drivercontainer.exception.NoAvailablePortException;
import py.drivercontainer.service.PortContainerFactory;
import py.drivercontainer.utils.IscsiProcessor;
import py.icshare.DriverKey;
import py.test.TestBase;

public class LaunchCoordinatorAndIscsiTargetTest extends TestBase {

  private static final Logger logger = LoggerFactory
      .getLogger(LaunchCoordinatorAndIscsiTargetTest.class);
  private LaunchDriverParameters launchParameters;
  @Mock
  private JvmConfigurationForDriver jvmConfig;
  @Mock
  private DriverContainerConfiguration driverContainerConfig;
  @Mock
  private PortContainerFactory portContainerFactory;
  private DriverStore driverStore;
  private long timeout;


  /**
   * xx.
   */
  @Before
  public void init() throws Exception {
    super.init();
    super.setLogLevel(Level.ALL);
    timeout = 5000;
    Version version = VersionImpl.get("2.3.0-internal-20170918000011");
    driverStore = new DriverStoreImpl(Paths.get("/tmp", "spid_coor"), version);
  }

  /*
   * Firstly,set createIscsiTargetNameFlag and coordinatorIsAlive false,iscsi driver turn to
   * SLAUNCHED because it just start up coordinator process and not create iscsi target. Then set
   *  createIscsiTargetNameFlag and coordinatorIsAlive true,after create iscsi target,the driver
   * turn to LAUNCHED.
   */
  @Test
  public void launchIscsiDriverTest() throws NoAvailablePortException {
    launchParameters = buildLaunchParameters(DriverType.ISCSI, 1000000L);
    LaunchDriverWorkerT worker = new LaunchDriverWorkerT(launchParameters, jvmConfig,
        driverContainerConfig,
        timeout);
    worker.setDriverStore(driverStore);
    worker.setPortContainerFactory(portContainerFactory);
    worker.init();
    worker.setCreateIscsiTargetNameFlag(false);
    worker.setCoordinatorIsAlive(false);

    IscsiProcessorT iscsiProcessorT = new IscsiProcessorT();
    worker.setIscsiProcessor(iscsiProcessorT);

    driverStore.save(launchParameters.buildDriver());
    DriverKey driverKey = new DriverKey(launchParameters.getDriverContainerInstanceId(),
        launchParameters.getVolumeId(), launchParameters.getSnapshotId(),
        launchParameters.getDriverType());

    DriverMetadata driver = driverStore.get(driverKey);

    Assert.assertEquals(DriverStatus.LAUNCHING, driver.getDriverStatus());
    worker.doWork();
    // createIscsiTargetNameFlag is false,not create iscsi target,so driver status is slaunched
    Assert.assertEquals(DriverStatus.LAUNCHED, driver.getDriverStatus());

    driver.setDriverStatus(DriverStatus.LAUNCHING);
    worker.setCreateIscsiTargetNameFlag(true);
    worker.setCoordinatorIsAlive(true);
    // coordinator is alive and iscsi target has been created ,so driver status is launched
    worker.doWork();
    Assert.assertEquals("/tmp/dev0", driver.getNbdDevice());
    Assert.assertEquals(DriverStatus.LAUNCHED, driver.getDriverStatus());

  }

  @Test
  public void launchPydDriverTest() throws NoAvailablePortException {
    launchParameters = buildLaunchParameters(DriverType.NBD, 2000000L);
    LaunchDriverWorkerT worker = new LaunchDriverWorkerT(launchParameters, jvmConfig,
        driverContainerConfig,
        timeout);
    worker.setDriverStore(driverStore);
    worker.setPortContainerFactory(portContainerFactory);
    worker.init();
    worker.setCreateIscsiTargetNameFlag(false);
    worker.setCoordinatorIsAlive(false);
    driverStore.save(launchParameters.buildDriver());

    IscsiProcessorT iscsiProcessorT = new IscsiProcessorT();
    worker.setIscsiProcessor(iscsiProcessorT);

    DriverKey driverKey = new DriverKey(launchParameters.getDriverContainerInstanceId(),
        launchParameters.getVolumeId(), launchParameters.getSnapshotId(),
        launchParameters.getDriverType());

    DriverMetadata driver = driverStore.get(driverKey);

    Assert.assertEquals(DriverStatus.LAUNCHING, driver.getDriverStatus());
    worker.doWork();
    Assert.assertEquals(DriverStatus.LAUNCHED, driver.getDriverStatus());
  }


  /**
   * xx.
   */
  public LaunchDriverParameters buildLaunchParameters(DriverType driverType, long volumeId) {
    launchParameters = new LaunchDriverParameters();
    launchParameters.setDriverContainerInstanceId(1);
    launchParameters.setAccountId(1);
    launchParameters.setVolumeId(volumeId);
    launchParameters.setSnapshotId(0);
    launchParameters.setDriverType(driverType);
    launchParameters.setHostName("127.0.0.1");
    launchParameters.setPort(1234);
    launchParameters.setDriverName("driver");
    return launchParameters;
  }
  /*
   * Set createIscsiTargetNameFlag and coordinatorIsAlive false,and then launch a PYD driver,as pyd
   *  should no create iscsi target,so with false flag ,it can be turn to LAUNCHED.
   */

  @After
  public void clean() throws Exception {
    FileUtils.deleteQuietly(Paths.get("/tmp/spid_coor").toFile());
  }

  public class IscsiProcessorT extends IscsiProcessor {

    @Override
    public String createIscsiTarget(String pydDevice, long volId) {
      return "/tmp/dev0";
    }
  }

  public class LaunchDriverWorkerT extends LaunchDriverWorker {

    public LaunchDriverWorkerT(LaunchDriverParameters launchDriverParameters,
        JvmConfigurationForDriver jvmConfig,
        DriverContainerConfiguration driverContainerConfig, long timeout) {
      super(launchDriverParameters, jvmConfig, driverContainerConfig);
    }

    @Override
    protected Process buildJvmProcess(long volId) throws Exception {
      Thread.sleep(1000);
      return null;
    }

    @Override
    protected void scanProcessId(Process process, DriverMetadata driver) {
      driver.setProcessId(1);
    }

    @Override
    void setChapAndAclForRebootCase(DriverMetadata driver) {
      return;
    }
  }
}
