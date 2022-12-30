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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.common.OsCmdExecutor;
import py.coordinator.DriverFailureSignal;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.DriverUpgradeProcessor;
import py.drivercontainer.driver.store.DriverFileStore;
import py.drivercontainer.driver.store.DriverStore;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.drivercontainer.driver.version.Version;
import py.drivercontainer.driver.version.VersionException;
import py.drivercontainer.driver.version.VersionManager;
import py.drivercontainer.utils.DriverContainerUtils;
import py.drivercontainer.worker.DriverScanWorker;
import py.icshare.DriverKey;

/**
 * this class will be used to do something before driver container started,such as find out the
 * ghost driver and reuse it,and so on.
 *
 */
public class BootStrap {

  private static final Logger logger = LoggerFactory.getLogger(BootStrap.class);
  // versionLoadedSet use to save version which has been loaded driver
  // from file to memory,avoid to load it again
  Set<Version> versionLoadedSet = new HashSet<>();
  private AppContext appContext;
  private VersionManager versionManager;
  private PortContainerFactory portContainerFactory;
  private DriverContainerConfiguration driverContainerConfig;
  private String startLioServiceCommand;
  private String stopLioServiceCommand;
  private String lioSaveConfigCommand;
  private String lioAutoAddDefaultPortalCommand;
  private String defaultSaveConfigFilePath;
  private DriverUpgradeProcessor driverUpgradeProcessor;
  private DriverStoreManager driverStoreManager;
  private DriverScanWorker driverScanWorker;


  /**
   * xx.
   */
  public boolean bootstrap() {
    logger.info("Going to bootstrap driver container ...");
    setGlobalAutoAddDefaultPortal();
    /* synchronize targetcli data into saveconfig.json file to keep in accordance */
    syncTargetcliToConfigFile();

    if (driverContainerConfig.isCheckIscsiServiceFlag()) {
      logger.warn("Going to check iscsi (lio or iet) service status");
      OsCmdExecutor.OsCmdOutputLogger startLioServiceConsumer =
          new OsCmdExecutor.OsCmdOutputLogger(logger, startLioServiceCommand);
      try {
        OsCmdExecutor.exec(startLioServiceCommand, DriverContainerUtils.osCMDThreadPool,
            startLioServiceConsumer, startLioServiceConsumer);
      } catch (Exception e) {
        logger.error("Catch an Exception when exec oscmd", e);
        return false;
      }
    }
    // load driver metadatas by driverType and version from file store
    long driverContainerId = appContext.getInstanceId().getId();
    Version currentVersion = null;
    Version latestVersion = null;
    boolean migration;

    // If driver is upgrading before stop drivercontainer,
    // when reboot it should recover the before environment,
    // check latestVersion and currentVersion for driverType.
    // Now,just support NBD and FSD to upgrade.
    List<DriverType> driverTypeList = new ArrayList<>();
    driverTypeList.add(DriverType.NBD);
    for (DriverType driverType : driverTypeList) {
      try {
        try {
          versionManager.lockVersion(driverType);
        } catch (IllegalStateException e) {
          logger.warn("Driver type :{} has been locked ", driverType);
        }
        currentVersion = versionManager.getCurrentVersion(driverType);
        latestVersion = versionManager.getLatestVersion(driverType);
        migration = versionManager.isOnMigration(driverType);
        versionManager.unlockVersion(driverType);
        if (currentVersion == null) {
          logger.warn("No driver with type {} is launched", driverType);
          continue;
        }
      } catch (VersionException e) {
        logger.warn("Catch an exception when get driver version ", e);
        return false;
      }

      if (!migration) {
        //Driver has not been upgrade,only need to load currentDriverStore from file to memory
        if (!versionHasBeenLoaded(currentVersion)) {
          ((DriverFileStore) driverStoreManager.get(currentVersion)).load(driverContainerId);
          versionLoadedSet.add(currentVersion);
          turnToErrorForInterruptedDriver(driverStoreManager.get(currentVersion));
        }
      } else if (!currentVersion.equals(latestVersion) && migration) {
        // DriverType which upgrading process has not been finished before stop drivercontainer,it
        // should be reSubmit
        // to DriverUpgradeProcess and resolve the remain drivers.
        logger.warn("going to submit driverType:{}", driverType);
        try {
          driverUpgradeProcessor.submit(driverType);
        } catch (InterruptedException e) {
          logger.error("Catch an exception when submit driver type to upgrade", e);
          return false;
        }

        DriverStore currentDriverStore = driverStoreManager.get(currentVersion);
        if (!versionHasBeenLoaded(currentVersion)) {
          ((DriverFileStore) driverStoreManager.get(currentVersion)).load(driverContainerId);
          versionLoadedSet.add(currentVersion);
          turnToErrorForInterruptedDriver(driverStoreManager.get(currentVersion));
        }

        if (!versionHasBeenLoaded(latestVersion)) {
          ((DriverFileStore) driverStoreManager.get(latestVersion)).load(driverContainerId);
          versionLoadedSet.add(latestVersion);
          DriverStore latestDriverStrore = driverStoreManager.get(latestVersion);
          List<DriverMetadata> createNewFutureDrivers = new ArrayList<>();
          for (DriverMetadata driver : latestDriverStrore.list()) {
            DriverKey driverKey = new DriverKey(driver.getDriverContainerId(), driver.getVolumeId(),
                driver.getSnapshotId(), driver.getDriverType());
            /*
             * If latest driver is launching,the old one in currentDriverStore is launched ,
             * we should create a new future save to driverTaskMap,DriverUpgradeProcessor will
             * remove the old one after the new one process up.And if latest driver is
             * launched or recovering,the old one in  currentDriverStore may has been created
             * remove work ,so if the old drivre status is removing ,after  reboot
             * drivercontainer there will have serverScan work to deal with it
             */
            if (driver.getDriverStatus() == DriverStatus.LAUNCHING) {
              createNewFutureDrivers.add(driver);
            } else if (driver.getDriverStatus() == DriverStatus.LAUNCHED
                || driver.getDriverStatus() == DriverStatus.RECOVERING) {
              DriverMetadata currentDriver = currentDriverStore.get(driverKey);
              if (currentDriver != null
                  && currentDriver.getDriverStatus() != DriverStatus.REMOVING) {
                createNewFutureDrivers.add(driver);
              }
            }
          }
          try {
            recoverUpgradeInterruptedDrivers(createNewFutureDrivers, latestVersion, currentVersion);
          } catch (IOException e) {
            logger.warn("catch an exception when judge driver process exist or not", e);
            return false;
          }
        }

      }
    }

    return true;
  }


  /**
   * xx.
   */
  public boolean versionHasBeenLoaded(Version version) {
    if (versionLoadedSet.contains(version)) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * Driver which has not been launched successfully before stop drivercontainer,when reboot
   * drivercontainer,set the driver status as ERROR.
   */
  public void turnToErrorForInterruptedDriver(DriverStore driverStore) {
    DriverStatus driverStatus;
    List<DriverMetadata> drivers = driverStore.list();
    logger.warn("Drivers :{} for checking status", drivers);
    for (DriverMetadata driver : drivers) {
      // remove port used by driver to avoid using by other new driver
      PortContainer portContainer = portContainerFactory.getPortContainer(driver.getDriverType());
      portContainer.removeUnavailablePort(driver.getPort());
      driverStatus = driver.getDriverStatus();

      if (driverStatus == DriverStatus.LAUNCHING) {
        logger.warn("Failed to launch driver {}: service driver-container was interrupted before.",
            driver);
        // It is designed to set driver 'ERROR' status if any exception occurred during process
        // launching driver request. And interruption of driver-container service is one of these
        // exceptions. Note that it is not necessary to synchronize driver-store due to only
        // bootstrap thread accessing it.
        driver.setDriverStatus(DriverStatus.ERROR);
        driverStore.save(driver);
      }
    }

  }

  /**
   * If driver upgrade process has not been finished before stop drivercontainer,when reboot
   * drivercontainer,it should launch latest driver aand remove current driver according to
   * different driver status.
   */
  public void recoverUpgradeInterruptedDrivers(List<DriverMetadata> drivers, Version latestVersion,
      Version currentVersion) throws IOException {
    DriverStatus driverStatus;
    logger.warn("Recover upgrading drivers is :{}", drivers);
    for (DriverMetadata driver : drivers) {
      driverStatus = driver.getDriverStatus();
      switch (driverStatus) {
        case LAUNCHING:
          driver.setDriverStatus(DriverStatus.RECOVERING);
          driverStoreManager.get(latestVersion).save(driver);
          break;
        case LAUNCHED:
        case RECOVERING:
          driver.setDriverStatus(DriverStatus.RECOVERING);
          driverStoreManager.get(latestVersion).save(driver);
          DriverKey driverKey = new DriverKey(driver.getDriverContainerId(), driver.getVolumeId(),
              driver.getSnapshotId(), driver.getDriverType());
          if (!DriverContainerUtils
              .processFound(driver.getProcessId(), driver.getCoordinatorPort())) {
            logger.warn("create future to launch new driver:{} and remove old driver:{}", driver,
                driverStoreManager.get(currentVersion).get(driverKey));
            Future<?> future = driverScanWorker
                .scheduleLaunchingRecover(driver, latestVersion, DriverFailureSignal.SERVER);
            if (future != null) {
              driverUpgradeProcessor.getDriverTaskMap().put(driverKey, future);
            }
          } else {
            //After reboot driverContainer,if the latest driver process still alive,we will not
            // create launch work for it again, and DriverUpgradeProcessor will not deal with the
            // old driver in currentDriverStore,so we create a remove work for the old driver.
            DriverMetadata oldDriver = driverStoreManager.get(currentVersion).get(driverKey);
            logger.warn("going to remove driver:{}", oldDriver);
            driverScanWorker.scheduleRemovingRecover(oldDriver, currentVersion);
          }
          break;
        default:
          break;
      }
    }

  }


  /**
   * xx.
   */
  public void syncTargetcliToConfigFile() {
    String syncTargetcliCmd = String.format(lioSaveConfigCommand, defaultSaveConfigFilePath);
    OsCmdExecutor.OsCmdOutputLogger lioSaveConfigConsumer = new OsCmdExecutor.OsCmdOutputLogger(
        logger, syncTargetcliCmd);
    try {
      OsCmdExecutor
          .exec(syncTargetcliCmd, DriverContainerUtils.osCMDThreadPool, lioSaveConfigConsumer,
              lioSaveConfigConsumer);
      logger.debug("cmd {} executed", syncTargetcliCmd);
    } catch (Exception e) {
      //don't process exception temporarily for special test environment, ex: domestic environment
      // can't load ISCSI kernel module
      logger.warn("Catch an Exception when exec oscmd {}, exception {}", syncTargetcliCmd, e);
    }
  }

  public void setGlobalAutoAddDefaultPortal() {
    DriverContainerUtils.setGlobalAutoAddDefaultPortalFalse(getLioAutoAddDefaultPortalCommand());
  }

  public AppContext getAppContext() {
    return appContext;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }


  public DriverContainerConfiguration getDriverContainerConfig() {
    return driverContainerConfig;
  }

  public void setDriverContainerConfig(DriverContainerConfiguration driverContainerConfig) {
    this.driverContainerConfig = driverContainerConfig;
  }

  public PortContainerFactory getPortContainerFactory() {
    return portContainerFactory;
  }

  public void setPortContainerFactory(PortContainerFactory portContainerFactory) {
    this.portContainerFactory = portContainerFactory;
  }

  public void setStopLioServiceCommand(String stopLioServiceCommand) {
    this.stopLioServiceCommand = stopLioServiceCommand;
  }

  public void setLioSaveConfigCommand(String lioSaveConfigCommand) {
    this.lioSaveConfigCommand = lioSaveConfigCommand;
  }

  public String getLioAutoAddDefaultPortalCommand() {
    return lioAutoAddDefaultPortalCommand;
  }

  public void setLioAutoAddDefaultPortalCommand(String lioAutoAddDefaultPortalCommand) {
    this.lioAutoAddDefaultPortalCommand = lioAutoAddDefaultPortalCommand;
  }

  public String getDefaultSaveConfigFilePath() {
    return defaultSaveConfigFilePath;
  }

  public void setDefaultSaveConfigFilePath(String defaultSaveConfigFilePath) {
    this.defaultSaveConfigFilePath = defaultSaveConfigFilePath;
  }

  public String getStartLioServiceCommand() {
    return startLioServiceCommand;
  }

  public void setStartLioServiceCommand(String startLioServiceCommand) {
    this.startLioServiceCommand = startLioServiceCommand;
  }

  public DriverStoreManager getDriverStoreManager() {
    return driverStoreManager;
  }

  public void setDriverStoreManager(DriverStoreManager driverStoreManager) {
    this.driverStoreManager = driverStoreManager;
  }

  public VersionManager getVersionManager() {
    return versionManager;
  }

  public void setVersionManager(VersionManager versionManager) {
    this.versionManager = versionManager;
  }

  public DriverUpgradeProcessor getDriverUpgradeProcessor() {
    return driverUpgradeProcessor;
  }

  public void setDriverUpgradeProcessor(DriverUpgradeProcessor driverUpgradeProcessor) {
    this.driverUpgradeProcessor = driverUpgradeProcessor;
  }

  public DriverScanWorker getDriverScanWorker() {
    return driverScanWorker;
  }

  public void setDriverScanWorker(DriverScanWorker driverScanWorker) {
    this.driverScanWorker = driverScanWorker;
  }
}

