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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.struct.EndPoint;
import py.coordinator.DriverFailureSignal;
import py.driver.DriverAction;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.drivercontainer.DriverVersion;
import py.drivercontainer.driver.version.Version;
import py.drivercontainer.driver.version.VersionManager;
import py.drivercontainer.service.PortContainerFactory;
import py.drivercontainer.utils.DriverContainerUtils;

/**
 * A worker to scan status of all driver servers (e.g. coordinator, fsd ...). And it generates a
 * task to recover the server which is sick or remove it if status is in {@link
 * DriverStatus#REMOVING}. Actually the worker is daemon thread for driver servers.
 *
 */
public class ServerScanWorker extends DriverScanWorker {

  private static final Logger logger = LoggerFactory.getLogger(ServerScanWorker.class);
  private EndPoint localDihEndPoint;
  private PortContainerFactory portContainerFactory;
  private VersionManager versionManager;

  /**
   * Current version equals latest version means driver is not in upgrading,just check
   * currentDriverStore,and when driver is upgrading ,should check currentDriverStore and
   * latestDriverStore.
   */
  @Override
  public void doWork() throws Exception {
    logger.info("It is time to scan all driver servers ...");
    // Now,only support NBD driverType to report
    List<DriverType> driverTypeList = new ArrayList<>();
    driverTypeList.add(DriverType.NBD);

    Set<Version> versions = new HashSet<>();
    for (DriverType type : driverTypeList) {
      Version currentVersion = DriverVersion.currentVersion.get(type);
      if (currentVersion == null) {
        continue;
      }
      if (driverStoreManager.get(currentVersion) == null) {
        logger.debug("No driver with type {} is launched", type);
        continue;
      }
      versions.add(currentVersion);
      versions.add(DriverVersion.latestVersion.get(type));
    }

    for (Version version : versions) {
      Set<DriverMetadata> drivers = new HashSet<>();
      //logger.debug("version in server scan is :{}", version);
      if (driverStoreManager.get(version) == null) {
        continue;
      }
      List<DriverMetadata> driverMetadataList = driverStoreManager.get(version).list();
      for (DriverMetadata driver : driverMetadataList) {
        switch (driver.getDriverStatus()) {
          case ERROR:
          case LAUNCHING:
            continue;
          default:
            // do nothing
        }

        if (!driver.addActionWithoutConflict(DriverAction.CHECK_SERVER)) {
          continue;
        }
        drivers.add(driver);
      }
      try {
        scan(drivers, version);
      } finally {
        for (DriverMetadata driver : drivers) {
          driver.removeAction(DriverAction.CHECK_SERVER);
        }
      }
    }
  }

  public PortContainerFactory getPortContainerFactory() {
    return portContainerFactory;
  }

  public void setPortContainerFactory(PortContainerFactory portContainerFactory) {
    this.portContainerFactory = portContainerFactory;
  }

  public EndPoint getLocalDihEndPoint() {
    return localDihEndPoint;
  }

  public void setLocalDihEndPoint(EndPoint localDihEndPoint) {
    this.localDihEndPoint = localDihEndPoint;
  }

  public VersionManager getVersionManager() {
    return versionManager;
  }

  public void setVersionManager(VersionManager versionManager) {
    this.versionManager = versionManager;
  }

  void scan(Set<DriverMetadata> drivers, Version version) {
    DriverStatus driverStatus;
    for (DriverMetadata driver : drivers) {
      driverStatus = driver.getDriverStatus();
      try {
        switch (driverStatus) {
          case MIGRATING:
          case LAUNCHED:
            if (!DriverContainerUtils
                .processFound(driver.getProcessId(), driver.getCoordinatorPort())) {
              logger.warn("Driver {} is sick due to no available server.", driver);
              scheduleLaunchingRecover(driver, version, DriverFailureSignal.SERVER);
            }
            break;
          case RECOVERING:
            if (!DriverContainerUtils
                .processFound(driver.getProcessId(), driver.getCoordinatorPort())) {
              logger.warn("Driver {} is sick due to no available server.", driver);
              scheduleLaunchingRecover(driver, version, DriverFailureSignal.SERVER);
            } else {
              // if DriverAction.CREATE_TARGET
              Set<DriverAction> actions = driver.listAndCopyActions();
              Set<DriverAction> addingActions = driver.listAndCopyAddingActions();
              Validate.isTrue(!actions.contains(DriverAction.START_SERVER));
              Validate.isTrue(!addingActions.contains(DriverAction.START_SERVER));
              if (actions.contains(DriverAction.CREATE_TARGET) || addingActions
                  .contains(DriverAction.CREATE_TARGET)) {
                break;
              }
              driver.setDriverStatus(DriverStatus.LAUNCHED);
              driverStoreManager.get(version).save(driver);
            }
            break;
          case REMOVING:
            scheduleRemovingRecover(driver, version);
            break;
          default:
            String errMsg = "Unsupported driver status " + driverStatus.name() + " in scan.";
            logger.error("{}", errMsg);
            throw new IllegalStateException(errMsg);
        }
      } catch (Exception e) {
        logger.warn("Something wrong when scanning server status. Driver info: {}", driver, e);
      } finally {
        driver.removeAction(DriverAction.CHECK_SERVER);
      }
    }
  }


}
