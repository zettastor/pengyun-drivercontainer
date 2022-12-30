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

package py.drivercontainer.driver.upgrade;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.driver.DriverMetadata;
import py.driver.DriverType;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.driver.store.DriverStore;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.drivercontainer.driver.version.Version;
import py.drivercontainer.driver.version.VersionException;
import py.drivercontainer.driver.version.VersionManager;
import py.icshare.DriverKey;


/**
 * An implementation controls window for driver upgrade. That is number of driver upgrade
 * simultaneously is under a specific limit. Only when completing one of drivers being upgraded,
 * another driver is allowed to be upgraded if window is full.
 *
 */
public class UpgradeDriverPollerImpl implements UpgradeDriverPoller {

  private static final Logger logger = LoggerFactory.getLogger(UpgradeDriverPollerImpl.class);

  private DriverContainerConfiguration dcConfig;

  private VersionManager versionManager;

  private DriverStoreManager driverStoreManager;


  /**
   * xx.
   */
  public UpgradeDriverPollerImpl(DriverContainerConfiguration dcConfig,
      VersionManager versionManager,
      DriverStoreManager driverStoreManager) {
    this.dcConfig = dcConfig;
    this.versionManager = versionManager;
    this.driverStoreManager = driverStoreManager;
  }


  @Override
  public int limit(DriverType type) {
    return dcConfig.getDriverUpgradeLimit();

  }


  @Override
  public boolean hasMore(DriverType type) {
    Version curVer;
    Version latestVer;

    try {
      curVer = versionManager.getCurrentVersion(type);
      latestVer = versionManager.getLatestVersion(type);
    } catch (VersionException e) {
      logger.error("Unable to get current or latest version", e);
      throw new RuntimeException(e);
    }

    if (latestVer.equals(curVer)) {
      logger.info("Same current version and latest version, may not on migration.");
      return false;
    }
    List<DriverMetadata> driverMetadataList = new ArrayList<>();
    DriverStore driverStore = driverStoreManager.get(curVer);
    if (driverStore == null) {
      logger.warn("DriverStore is null,there has no driver for type:{}", type);
      return false;
    }
    for (DriverMetadata driver : driverStore.list()) {
      if (type.equals(DriverType.NBD)) {
        if (driver.getDriverType().equals(DriverType.NBD) || driver.getDriverType()
            .equals(DriverType.ISCSI)) {
          driverMetadataList.add(driver);
        }
      } else {
        if (driver.getDriverType().equals(type)) {
          driverMetadataList.add(driver);
        }
      }
    }
    return driverMetadataList.size() > 0;
  }

  @Override
  public DriverMetadata poll(DriverType type) {
    final int nDrivers = 1;
    final int onlyDriverIndex = 0;

    List<DriverMetadata> drivers;

    drivers = poll(type, nDrivers);
    if (drivers.isEmpty()) {
      return null;
    } else {
      return drivers.get(onlyDriverIndex);
    }
  }

  List<DriverMetadata> poll(DriverType type, int limit) {
    Version curVer;
    Version latestVer;
    final DriverStore curVerStore;
    final DriverStore latestVerStore;
    Set<DriverKey> migratingDrivers = new HashSet<DriverKey>();
    List<DriverMetadata> targetDrivers = new ArrayList<DriverMetadata>(limit(type));

    try {
      curVer = versionManager.getCurrentVersion(type);
      latestVer = versionManager.getLatestVersion(type);
    } catch (VersionException e) {
      logger.error("Unable to get current or latest version", e);
      throw new RuntimeException(e);
    }

    if (latestVer.equals(curVer)) {
      logger.info("Same current version and latest version, may not on migration.");
      return targetDrivers;
    }

    curVerStore = driverStoreManager.get(curVer);
    latestVerStore = driverStoreManager.get(latestVer);
    List<DriverMetadata> currentDriverMetadataList = curVerStore.list();
    for (DriverMetadata driver : currentDriverMetadataList) {
      if (type.equals(DriverType.NBD)) {
        if (latestVerStore.get(new DriverKey(driver.getDriverContainerId(), driver.getVolumeId(),
            driver.getSnapshotId(), driver.getDriverType())) != null
            && (driver.getDriverType().equals(DriverType.NBD)
            || driver.getDriverType().equals(DriverType.ISCSI))) {
          migratingDrivers.add(new DriverKey(driver.getDriverContainerId(), driver.getVolumeId(),
              driver.getSnapshotId(), driver.getDriverType()));
          continue;
        }
        if (targetDrivers.size() < limit && (driver.getDriverType().equals(DriverType.NBD) || driver
            .getDriverType().equals(DriverType.ISCSI))) {
          switch (driver.getDriverStatus()) {
            case LAUNCHED:
            case ERROR:
              targetDrivers.add(driver);
              break;
            default:
              break;
          }
        }
      } else {
        if (latestVerStore.get(new DriverKey(driver.getDriverContainerId(), driver.getVolumeId(),
            driver.getSnapshotId(), driver.getDriverType())) != null && driver.getDriverType()
            .equals(type)) {
          migratingDrivers.add(new DriverKey(driver.getDriverContainerId(), driver.getVolumeId(),
              driver.getSnapshotId(), driver.getDriverType()));
          continue;
        }
        if (targetDrivers.size() < limit && driver.getDriverType().equals(type)) {
          switch (driver.getDriverStatus()) {
            case LAUNCHED:
            case ERROR:
              targetDrivers.add(driver);
              break;
            default:
              break;
          }
        }
      }
    }
    int remaining = limit(type) - migratingDrivers.size();
    if (remaining < 0) {
      String errMsg = "Number of migrating drivers exceeds limit. Number of migrating drivers: "
          + migratingDrivers.size() + ", limit: " + limit(type);
      logger.error("{}", errMsg);
      throw new IllegalStateException(errMsg);
    }

    remaining = Math.min(remaining, targetDrivers.size());
    return targetDrivers.subList(0, remaining);
  }

  @Override
  public void drainTo(DriverType type, Collection<DriverMetadata> drivers) {
    List<DriverMetadata> result;
    result = poll(type, limit(type));
    drivers.addAll(result);
  }

  public DriverContainerConfiguration getDcConfig() {
    return dcConfig;
  }

  public void setDcConfig(DriverContainerConfiguration dcConfig) {
    this.dcConfig = dcConfig;
  }

  @Override
  public VersionManager getVersionManager() {
    return versionManager;
  }

  public void setVersionManager(VersionManager versionManager) {
    this.versionManager = versionManager;
  }

  @Override
  public DriverStoreManager getDriverStoreManager() {
    return driverStoreManager;
  }

  public void setDriverStoreManager(DriverStoreManager driverStoreManager) {
    this.driverStoreManager = driverStoreManager;

  }
}
