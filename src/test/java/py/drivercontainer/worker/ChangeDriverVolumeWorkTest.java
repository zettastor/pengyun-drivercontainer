/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
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
