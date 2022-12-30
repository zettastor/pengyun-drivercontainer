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

import java.util.ArrayList;
import java.util.List;
import junit.framework.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.driver.store.DriverMemoryStoreImpl;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.drivercontainer.driver.store.DriverStoreManagerImpl;
import py.drivercontainer.driver.upgrade.UpgradeDriverPoller;
import py.drivercontainer.driver.upgrade.UpgradeDriverPollerImpl;
import py.drivercontainer.driver.version.Version;
import py.drivercontainer.driver.version.VersionManager;
import py.drivercontainer.driver.version.file.VersionImpl;
import py.icshare.DriverKey;
import py.test.TestBase;

/**
 * A class contains some tests for {@link UpgradeDriverPollerImpl}.
 *
 */
public class UpgradeDriverPollerImplTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(UpgradeDriverPollerImplTest.class);

  @Override
  public void init() throws Exception {
    super.init();
  }

  /**
   * In this test plan, it polls driver out one per time at most limit times. After that, expect
   * that any polling action on the poller will get null value.
   */
  @Test
  public void testPollDriversExceedingLimit() throws Exception {
    final int limit = 10;
    final Version curVer = VersionImpl.get("2.3.0-internal-20170919144444");
    final Version latestVer = VersionImpl.get("2.3.0-internal-20170919144445");
    final int nDrivers = 20;

    DriverContainerConfiguration dcConfig;
    VersionManager versionManager;
    final DriverStoreManager driverStoreManager;
    final UpgradeDriverPoller poller;

    dcConfig = Mockito.mock(DriverContainerConfiguration.class);
    Mockito.when(dcConfig.getDriverUpgradeLimit()).thenReturn(limit);

    versionManager = Mockito.mock(VersionManager.class);
    Mockito.when(versionManager.getCurrentVersion(Mockito.any(DriverType.class)))
        .thenReturn(curVer);
    Mockito.when(versionManager.getLatestVersion(Mockito.any(DriverType.class)))
        .thenReturn(latestVer);

    driverStoreManager = new DriverStoreManagerImpl();
    driverStoreManager.put(curVer, new DriverMemoryStoreImpl());
    driverStoreManager.put(latestVer, new DriverMemoryStoreImpl());

    for (int i = 0; i < nDrivers; i++) {
      DriverMetadata driver = new DriverMetadata();
      driver.setDriverContainerId(0);
      driver.setVolumeId(RequestIdBuilder.get());
      driver.setSnapshotId(0);
      driver.setDriverType(DriverType.NBD);
      driver.setDriverStatus(DriverStatus.LAUNCHED);

      driverStoreManager.get(curVer).save(driver);
    }

    List<DriverMetadata> migratingDrivers = new ArrayList<>();

    poller = new UpgradeDriverPollerImpl(dcConfig, versionManager, driverStoreManager);
    for (int i = 0; i < limit; i++) {
      DriverMetadata driver = poller.poll(DriverType.NBD);
      DriverMetadata newDriver = new DriverMetadata();

      newDriver.setDriverContainerId(driver.getDriverContainerId());
      newDriver.setVolumeId(driver.getVolumeId());
      newDriver.setSnapshotId(driver.getSnapshotId());
      newDriver.setDriverType(driver.getDriverType());
      newDriver.setDriverStatus(DriverStatus.LAUNCHING);

      driverStoreManager.get(latestVer).save(newDriver);

      migratingDrivers.add(driver);
    }

    Assert.assertNull(poller.poll(DriverType.NBD));
    Assert.assertTrue(poller.hasMore(DriverType.NBD));

    for (DriverMetadata driver : migratingDrivers) {
      DriverKey key = new DriverKey(driver.getDriverContainerId(), driver.getVolumeId(),
          driver.getSnapshotId(),
          driver.getDriverType());
      driverStoreManager.get(curVer).remove(key);
    }

    for (int i = 0; i < nDrivers - limit; i++) {
      DriverMetadata driver = poller.poll(DriverType.NBD);
      Assert.assertNotNull(driver);
      driverStoreManager.get(curVer)
          .remove(new DriverKey(driver.getDriverContainerId(), driver.getVolumeId(),
              driver.getSnapshotId(), driver.getDriverType()));
    }

    Assert.assertFalse(poller.hasMore(DriverType.NBD));
  }

  @Test
  public void testDrainTo() throws Exception {
    final int limit = 10;
    final Version curVer = VersionImpl.get("2.3.0-internal-20170919144444");
    final Version latestVer = VersionImpl.get("2.3.0-internal-20170919144445");
    final int nDrivers = 20;

    DriverContainerConfiguration dcConfig;
    VersionManager versionManager;
    final DriverStoreManager driverStoreManager;
    final UpgradeDriverPoller poller;

    dcConfig = Mockito.mock(DriverContainerConfiguration.class);
    Mockito.when(dcConfig.getDriverUpgradeLimit()).thenReturn(limit);

    versionManager = Mockito.mock(VersionManager.class);
    Mockito.when(versionManager.getCurrentVersion(Mockito.any(DriverType.class)))
        .thenReturn(curVer);
    Mockito.when(versionManager.getLatestVersion(Mockito.any(DriverType.class)))
        .thenReturn(latestVer);

    driverStoreManager = new DriverStoreManagerImpl();
    driverStoreManager.put(curVer, new DriverMemoryStoreImpl());
    driverStoreManager.put(latestVer, new DriverMemoryStoreImpl());

    for (int i = 0; i < nDrivers; i++) {
      DriverMetadata driver = new DriverMetadata();
      driver.setDriverContainerId(0);
      driver.setVolumeId(RequestIdBuilder.get());
      driver.setSnapshotId(0);
      driver.setDriverType(DriverType.NBD);
      driver.setDriverStatus(DriverStatus.LAUNCHED);

      driverStoreManager.get(curVer).save(driver);
    }
    poller = new UpgradeDriverPollerImpl(dcConfig, versionManager, driverStoreManager);

    List<DriverMetadata> migratingDrivers = new ArrayList<>();

    poller.drainTo(DriverType.NBD, migratingDrivers);
    Assert.assertEquals(limit, migratingDrivers.size());

    for (DriverMetadata driver : migratingDrivers) {
      DriverMetadata newDriver = new DriverMetadata();

      newDriver.setDriverContainerId(driver.getDriverContainerId());
      newDriver.setVolumeId(driver.getVolumeId());
      newDriver.setSnapshotId(driver.getSnapshotId());
      newDriver.setDriverType(driver.getDriverType());
      newDriver.setDriverStatus(DriverStatus.LAUNCHING);

      driverStoreManager.get(latestVer).save(newDriver);
    }

    poller.drainTo(DriverType.NBD, migratingDrivers);
    Assert.assertEquals(limit, migratingDrivers.size());

    for (DriverMetadata driver : migratingDrivers) {
      driverStoreManager.get(curVer)
          .remove(new DriverKey(driver.getDriverContainerId(), driver.getVolumeId(),
              driver.getSnapshotId(), driver.getDriverType()));
    }

    int remaining = nDrivers - migratingDrivers.size();
    while (remaining > 0) {
      migratingDrivers.clear();
      poller.drainTo(DriverType.NBD, migratingDrivers);
      Assert.assertTrue(migratingDrivers.size() == Math.min(remaining, limit));
      remaining -= migratingDrivers.size();
      for (DriverMetadata driver : migratingDrivers) {
        driverStoreManager.get(curVer)
            .remove(new DriverKey(driver.getDriverContainerId(), driver.getVolumeId(),
                driver.getSnapshotId(), driver.getDriverType()));
      }
    }

    Assert.assertFalse(poller.hasMore(DriverType.NBD));
  }
}
