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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.driver.DriverAction;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.DriverVersion;
import py.drivercontainer.client.CoordinatorClientFactory;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.drivercontainer.driver.version.Version;
import py.drivercontainer.service.taskqueue.TaskExecutor;
import py.icshare.DriverKey;
import py.periodic.Worker;

/**
 * The work use to check driver change volumeId finished or not.
 */
public class MigratingScanWorker implements Worker {

  private static final Logger logger = LoggerFactory.getLogger(MigratingScanWorker.class);
  private DriverContainerConfiguration dcConfig;
  private DriverStoreManager driverStoreManager;
  private TaskExecutor taskExecutor;
  private CoordinatorClientFactory coordinatorClientFactory;

  @Override
  public void doWork() {
    logger.info("Going to check driver migrating status...");
    // only support NBD and FSD driverType
    List<DriverType> driverTypeList = new ArrayList<>();
    driverTypeList.add(DriverType.NBD);
    driverTypeList.add(DriverType.FSD);

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
      logger.debug("version in change volume task is :{}", version);
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
        if (!driver.addActionWithoutConflict(DriverAction.CHECK_MIGRATING)) {
          continue;
        }
        drivers.add(driver);
      }
      try {
        scan(drivers, version);
      } finally {
        for (DriverMetadata driver : drivers) {
          driver.removeAction(DriverAction.CHECK_MIGRATING);
        }
      }
    }
  }


  /**
   * xx.
   */
  public void scan(Set<DriverMetadata> drivers, Version version) {
    for (DriverMetadata driver : drivers) {
      DriverStatus driverStatus = driver.getDriverStatus();
      switch (driverStatus) {
        case MIGRATING:
          DriverKey driverKey = new DriverKey(driver.getDriverContainerId(), driver.getVolumeId(),
              driver.getSnapshotId(), driver.getDriverType());
          logger.warn("Going to change volumeId for migrating status driver :{}", driverKey);
          ChangeDriverVolumeTask task = new ChangeDriverVolumeTask();
          task.setDriverKey(driverKey);
          task.setDriverStore(driverStoreManager.get(version));
          task.setDcConfig(dcConfig);
          task.setCoordinatorClientFactory(coordinatorClientFactory);
          taskExecutor.submit(driverKey, DriverAction.CHANGE_VOLUME, task, version);
          break;
        default:
          //logger.debug("Not deal with :{} status driver", driverStatus);
      }
    }
  }

  public DriverStoreManager getDriverStoreManager() {
    return driverStoreManager;
  }

  public void setDriverStoreManager(DriverStoreManager driverStoreManager) {
    this.driverStoreManager = driverStoreManager;
  }

  public DriverContainerConfiguration getDcConfig() {
    return dcConfig;
  }

  public void setDcConfig(DriverContainerConfiguration dcConfig) {
    this.dcConfig = dcConfig;
  }

  public TaskExecutor getTaskExecutor() {
    return taskExecutor;
  }

  public void setTaskExecutor(TaskExecutor taskExecutor) {
    this.taskExecutor = taskExecutor;
  }

  public CoordinatorClientFactory getCoordinatorClientFactory() {
    return coordinatorClientFactory;
  }

  public void setCoordinatorClientFactory(CoordinatorClientFactory coordinatorClientFactory) {
    this.coordinatorClientFactory = coordinatorClientFactory;
  }
}
