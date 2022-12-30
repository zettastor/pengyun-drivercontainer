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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import java.nio.file.Paths;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import py.common.struct.EndPoint;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.client.CoordinatorClientFactory;
import py.drivercontainer.driver.store.DriverStore;
import py.drivercontainer.driver.store.DriverStoreImpl;
import py.drivercontainer.driver.version.Version;
import py.drivercontainer.driver.version.file.VersionImpl;
import py.icshare.DriverKey;
import py.test.TestBase;
import py.thrift.coordinator.service.Coordinator;

/**
 * The class use to test change volumeId for migrating volume on line.
 */
public class ChangeDriverVolumeWorkTest extends TestBase {

  @Mock
  private DriverContainerConfiguration dcConfig;

  @Mock
  private CoordinatorClientFactory coordinatorClientFactory;

  private ChangeDriverVolumeTask task;

  private DriverStore driverStore;

  @Mock
  private CoordinatorClientFactory.CoordinatorClientWrapper coordinatorClientWrapper;

  @Mock
  private Coordinator.Iface coordinator;

  private long oldVolumeId = 123;

  private long newVolumeId = 456;
  private String driverPath;


  /**
   * xx.
   */
  @Before
  public void init() throws Exception {

    task = new ChangeDriverVolumeTask();
    task.setDcConfig(dcConfig);
    task.setCoordinatorClientFactory(coordinatorClientFactory);
    Version version = VersionImpl.get("2.4.0-internal-20170918000011");
    driverPath = "/tmp/ChangeDriverVolumeWorkTest/SPid_coordinator";
    driverStore = new DriverStoreImpl(Paths.get(driverPath), version);
    when(dcConfig.getBuildCoordinatorClientTimeout()).thenReturn(1000);
    when(coordinatorClientFactory.build(any(EndPoint.class), anyLong()))
        .thenReturn(coordinatorClientWrapper);
    when(coordinatorClientWrapper.getClient()).thenReturn(coordinator);


  }

  @Test
  public void testChangeDriverVolume() {
    DriverKey oldDriverKey = new DriverKey(123, oldVolumeId, 0, DriverType.ISCSI);
    final DriverKey newDriverkey = new DriverKey(123, newVolumeId, 0, DriverType.ISCSI);
    driverStore.save(buildDriver(oldDriverKey));
    task.setDriverStore(driverStore);
    task.setDriverKey(oldDriverKey);
    Assert.assertTrue(driverStore.get(oldDriverKey).getVolumeId() == oldVolumeId);
    task.run();
    Assert.assertTrue(driverStore.get(newDriverkey).getVolumeId() == newVolumeId);
    Assert.assertTrue(driverStore.get(oldDriverKey) == null);
  }


  /**
   * xx.
   */
  public DriverMetadata buildDriver(DriverKey driverKey) {
    DriverMetadata driver = new DriverMetadata();
    driver.setDriverStatus(DriverStatus.LAUNCHED);
    driver.setDriverContainerId(driverKey.getDriverContainerId());
    driver.setDriverType(driverKey.getDriverType());
    driver.setVolumeId(driverKey.getVolumeId());
    driver.setSnapshotId(driverKey.getSnapshotId());
    driver.setMigratingVolumeId(newVolumeId);
    driver.setHostName("192.168.2.103");
    driver.setCoordinatorPort(2234);
    return driver;
  }


  @After
  public void clean() throws Exception {
    FileUtils.deleteQuietly(Paths.get(driverPath).toFile());
  }
}
