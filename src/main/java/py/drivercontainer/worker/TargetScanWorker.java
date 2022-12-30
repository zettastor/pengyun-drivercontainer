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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.coordinator.DriverFailureSignal;
import py.coordinator.IscsiTargetNameManagerFactory;
import py.coordinator.lio.LioNameBuilder;
import py.driver.DriverAction;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.drivercontainer.DriverVersion;
import py.drivercontainer.driver.version.Version;
import py.drivercontainer.driver.version.VersionManager;
import py.drivercontainer.utils.DriverContainerUtils;

public class TargetScanWorker extends DriverScanWorker {

  private static final Logger logger = LoggerFactory.getLogger(TargetScanWorker.class);
  private LioNameBuilder lioNameBuilder;
  /*
   * If driver is Iscsi,should set createIscsiTargetFlag true to create iscsiTarget and set
   * coordinatorIsAlive true to avoid launch coordinator again.
   */
  private IscsiTargetNameManagerFactory iscsiTargetNameManagerFactory;
  private VersionManager versionManager;


  @Override
  public void doWork() throws Exception {
    logger.info("It is time to scan all targets for currentDriverStore...");

    //If driverType is iscsi ,use NBD version
    Version currentVersion = DriverVersion.currentVersion.get(DriverType.NBD);
    if (currentVersion == null) {
      return;
    }
    if (driverStoreManager.get(currentVersion) == null) {
      return;
    }
    Version latestVersion = DriverVersion.latestVersion.get(DriverType.NBD);
    Set<Version> versions = new HashSet<>();
    versions.add(currentVersion);
    versions.add(latestVersion);
    for (Version version : versions) {
      //logger.debug("version in targetScan is :{}", version);
      Set<DriverMetadata> drivers = new HashSet<>();
      if (driverStoreManager.get(version) == null) {
        continue;
      }
      List<DriverMetadata> driverMetadataList = driverStoreManager.get(version).list();
      for (DriverMetadata driver : driverMetadataList) {
        if (driver.getDriverType() != DriverType.ISCSI) {
          continue;
        }
        switch (driver.getDriverStatus()) {
          case LAUNCHING:
          case ERROR:
            continue;
          default:
            // do nothing
        }

        if (driver.hasConflictActionWith(DriverAction.CHECK_TARGET)
            || !DriverContainerUtils
            .processFound(driver.getProcessId(), driver.getCoordinatorPort())) {
          continue;
        }

        if (driver.getDriverStatus() != DriverStatus.REMOVING
            && !DriverContainerUtils
            .processFound(driver.getProcessId(), driver.getCoordinatorPort())) {
          continue;
        }

        if (!driver.addActionWithoutConflict(DriverAction.CHECK_TARGET)) {
          continue;
        }
        drivers.add(driver);
      }
      try {
        scan(drivers, version);
      } finally {
        for (DriverMetadata driver : drivers) {
          driver.removeAction(DriverAction.CHECK_TARGET);
        }
      }
    }
  }

  public LioNameBuilder getLioNameBuilder() {
    return lioNameBuilder;
  }

  public void setLioNameBuilder(LioNameBuilder lioNameBuilder) {
    this.lioNameBuilder = lioNameBuilder;
  }


  public IscsiTargetNameManagerFactory getIscsiTargetNameManagerFactory() {
    return iscsiTargetNameManagerFactory;
  }

  public void setIscsiTargetNameManagerFactory(
      IscsiTargetNameManagerFactory iscsiTargetNameManagerFactory) {
    this.iscsiTargetNameManagerFactory = iscsiTargetNameManagerFactory;
  }

  void scan(Set<DriverMetadata> drivers, Version version) throws Exception {
    //logger.debug("Scanning drivers {} ...", drivers);
    String targetName = null;
    boolean refreshDevices = true;
    for (DriverMetadata driver : drivers) {
      long volumeId = driver.getVolumeId(true);
      targetName = lioNameBuilder.buildLioWwn(volumeId, driver.getSnapshotId());
      DriverFailureSignal driverFailureSignal;
      try {
        switch (driver.getDriverStatus()) {
          case REMOVING:
            logger.info("going to remover driver :{}", driver);
            scheduleRemovingRecover(driver, version);
            break;
          case LAUNCHED:
            driverFailureSignal = iscsiTargetNameManagerFactory.getIscsiTargetManager()
                .iscsiTargetIsAvailable(targetName, driver.getNbdDevice(), volumeId,
                    driver.getSnapshotId(),
                    refreshDevices);
            if (driverFailureSignal != null) {
              if (driverFailureSignal.equals(DriverFailureSignal.LUNMISSMATHCHED)) {
                refreshDevices = false;
              }
              scheduleLaunchingRecover(driver, version, driverFailureSignal);
            }
            break;
          case MIGRATING:
            driverFailureSignal = iscsiTargetNameManagerFactory.getIscsiTargetManager()
                .iscsiTargetIsAvailable(targetName, driver.getNbdDevice(), volumeId,
                    driver.getSnapshotId(),
                    refreshDevices);
            if (driverFailureSignal != null) {
              if (driverFailureSignal.equals(DriverFailureSignal.LUNMISSMATHCHED)) {
                logger
                    .error("When volume for driver migrate on line , not allow change volume size");
                break;
              }
              scheduleLaunchingRecover(driver, version, driverFailureSignal);
            }
            break;
          case RECOVERING:
            driverFailureSignal = iscsiTargetNameManagerFactory.getIscsiTargetManager()
                .iscsiTargetIsAvailable(targetName, driver.getNbdDevice(), volumeId,
                    driver.getSnapshotId(),
                    refreshDevices);
            if (driverFailureSignal != null) {
              scheduleLaunchingRecover(driver, version, driverFailureSignal);
            } else {
              driver.setDriverStatus(DriverStatus.LAUNCHED);
              driverStoreManager.get(version).save(driver);

            }
            break;
          default:
            String errMsg =
                "Unsupported driver status " + driver.getDriverStatus().name() + " in scan.";
            logger.error("{}", errMsg);
            throw new IllegalStateException(errMsg);
        }
      } finally {
        driver.removeAction(DriverAction.CHECK_TARGET);
      }
    }
  }

  public VersionManager getVersionManager() {
    return versionManager;
  }

  public void setVersionManager(VersionManager versionManager) {
    this.versionManager = versionManager;
  }
}
