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
import java.nio.file.Paths;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
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
import py.drivercontainer.driver.workspace.DriverWorkspaceImpl;
import py.drivercontainer.driver.workspace.DriverWorkspaceManager;
import py.drivercontainer.worker.LaunchDriverWorker;
import py.icshare.DriverKey;
import py.test.TestBase;

public class LauncherDriverSetErrorTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(LauncherDriverSetErrorTest.class);
  @Mock
  DriverContainerConfiguration driverContainerConfig;
  private LaunchDriverWorker launchDriverWorker;
  private LaunchDriverParameters launchDriverParameters;
  private DriverStore driverStore;
  private JvmConfigurationForDriver driverJvmConfig;
  @Mock
  private DriverWorkspaceManager driverWorkspaceManager;


  /**
   * xx.
   */
  @Before
  public void init() throws IOException, InterruptedException {
    driverJvmConfig = new JvmConfigurationForDriver();
    launchDriverParameters = new LaunchDriverParameters();
    launchDriverParameters.setDriverType(DriverType.NBD);
    launchDriverParameters.setVolumeId(1L);
    launchDriverParameters.setSnapshotId(1);
    launchDriverParameters.setPort(1);

    launchDriverWorker = new LaunchDriverWorker(launchDriverParameters, driverJvmConfig,
        driverContainerConfig);
    DriverMetadata driver = new DriverMetadata();
    driver.setDriverType(DriverType.NBD);
    driver.setVolumeId(1L);
    driver.setSnapshotId(1);
    driver.setDriverStatus(DriverStatus.LAUNCHING);
    driver.setProcessId(2);
    Version version = VersionImpl.get("2.3.0-internal-20170918000011");
    driverStore = new DriverStoreImpl(Paths.get("/tmp/driverStore"), version);
    driverStore.save(driver);
    DriverKey driverKey = new DriverKey(driver.getDriverContainerId(), driver.getVolumeId(),
        driver.getSnapshotId(), driver.getDriverType());

    launchDriverWorker.setDriverStore(driverStore);

    launchDriverWorker.setDriverWorkspaceManager(driverWorkspaceManager);
    DriverWorkspaceImpl workspace = new DriverWorkspaceImpl("/tmp/driverStore/nbd", version,
        driverKey);
    if (!workspace.getDir().exists()) {
      new File(workspace.getPath()).mkdirs();
    }
    when(driverWorkspaceManager.createWorkspace(any(Version.class), any(DriverKey.class)))
        .thenReturn(workspace);
    when(driverWorkspaceManager.getWorkspace(any(Version.class), any(DriverKey.class)))
        .thenReturn(workspace);

  }

  @Test
  public void launchExceptionTest() {
    launchDriverWorker.doWork();
    DriverMetadata driver = driverStore
        .get(new DriverKey(launchDriverParameters.getDriverContainerInstanceId(),
            launchDriverParameters.getVolumeId(), launchDriverParameters.getSnapshotId(),
            launchDriverParameters.getDriverType()));
    Assert.assertEquals(driver.getDriverStatus().name(), "ERROR");

  }

  @After
  public void clean() {
    FileUtils.deleteQuietly(Paths.get("/tmp/driverStore").toFile());
  }
}
