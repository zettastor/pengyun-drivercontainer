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

import junit.framework.Assert;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.driver.store.DriverMemoryStoreImpl;
import py.drivercontainer.driver.store.DriverStore;
import py.drivercontainer.utils.DriverContainerUtils;
import py.test.TestBase;

public class AvailableCpuForIscsiTargetTest extends TestBase {

  private static final Logger logger = LoggerFactory
      .getLogger(AvailableCpuForIscsiTargetTest.class);
  private DriverStore driverStore = new DriverMemoryStoreImpl();
  private DriverContainerConfiguration driverContainerConfig = new DriverContainerConfiguration();

  @Mock
  private AppContext appContext;

  private DriverContainerImpl dcImpl;


  /**
   * xx.
   */
  @Before
  public void init() throws Exception {
    super.init();
    super.setLogLevel(Level.ALL);
    dcImpl = new DriverContainerImpl(appContext);
  }

  @After
  public void cleanup() throws Exception {
    driverStore.clearMomory();
  }

  @Test
  public void testAvailableCpuForIscsiTarget() throws Exception {
    driverStore.save(buildNbdDriver(02, 0));
    driverStore.save(buildNbdDriver(03, 1));
    driverStore.save(buildNbdDriver(04, 0));
    driverStore.save(buildNbdDriver(05, 1));

    driverContainerConfig.setSystemCpuNumReserved(0);
    driverContainerConfig.setSystemCpuPercentReserved(99);
    dcImpl.setDriverContainerConfig(driverContainerConfig);
    boolean res = true;
    res = dcImpl.availableCpuForIscsiTarget(driverStore);
    logger.debug("availableCpuForIscsiTarget res {} ", res);
    Assert.assertTrue(res == true);

    driverContainerConfig.setSystemCpuPercentReserved(0);
    dcImpl.setDriverContainerConfig(driverContainerConfig);
    res = dcImpl.availableCpuForIscsiTarget(driverStore);
    logger.debug("availableCpuForIscsiTarget res {} ", res);
    Assert.assertTrue(res == true);

    driverStore.save(buildIscsiDriver(02, 0));
    driverStore.save(buildIscsiDriver(03, 1));
    res = dcImpl.availableCpuForIscsiTarget(driverStore);
    logger.debug("availableCpuForIscsiTarget res {} ", res);
    Assert.assertTrue(res == true);

  }

  @Test
  public void testNoLimit() throws Exception {
    driverStore.save(buildNbdDriver(02, 0));
    driverStore.save(buildNbdDriver(03, 1));
    driverStore.save(buildNbdDriver(04, 0));
    driverStore.save(buildNbdDriver(05, 1));
    int cpuTotalNum = DriverContainerUtils.getSysCpuNum();
    driverContainerConfig.setSystemCpuNumReserved(0);
    driverContainerConfig.setSystemCpuPercentReserved(0);
    for (int i = 0; i < cpuTotalNum; i++) {
      driverStore.save(buildIscsiDriver(i + 5, 0));
    }
    driverStore.save(buildIscsiDriver(1000, 0));
    dcImpl.setDriverContainerConfig(driverContainerConfig);
    boolean res = dcImpl.availableCpuForIscsiTarget(driverStore);
    logger.debug("availableCpuForIscsiTarget res {} ", res);
    Assert.assertTrue(res == true);

  }


  /**
   * xx.
   */
  public DriverMetadata buildIscsiDriver(long volumeId, int snapshotId) {
    DriverMetadata driver = new DriverMetadata();
    driver.setDriverContainerId(0L);
    driver.setDriverType(DriverType.ISCSI);
    driver.setVolumeId(volumeId);
    driver.setSnapshotId(snapshotId);
    driver.setDriverStatus(DriverStatus.LAUNCHED);
    driver.setHostName("10.0.1.103");
    driver.setCoordinatorPort(1234);
    return driver;
  }


  /**
   * xx.
   */
  public DriverMetadata buildNbdDriver(long volumeId, int snapshotId) {
    DriverMetadata driver = new DriverMetadata();
    driver.setDriverContainerId(0L);
    driver.setDriverType(DriverType.NBD);
    driver.setVolumeId(volumeId);
    driver.setSnapshotId(snapshotId);
    driver.setDriverStatus(DriverStatus.LAUNCHED);
    driver.setHostName("10.0.1.103");
    driver.setCoordinatorPort(1234);
    return driver;
  }

}
