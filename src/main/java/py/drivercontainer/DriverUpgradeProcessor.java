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

package py.drivercontainer;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.driver.DriverAction;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.drivercontainer.driver.LaunchDriverParameters;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.drivercontainer.driver.upgrade.UpgradeDriverPoller;
import py.drivercontainer.driver.version.Version;
import py.drivercontainer.driver.version.VersionException;
import py.drivercontainer.driver.version.VersionManager;
import py.drivercontainer.exception.NoAvailablePortException;
import py.drivercontainer.service.PortContainerFactory;
import py.drivercontainer.service.taskqueue.TaskExecutor;
import py.drivercontainer.utils.DriverContainerUtils;
import py.drivercontainer.worker.LaunchDriverWorker;
import py.drivercontainer.worker.LaunchDriverWorkerFactory;
import py.drivercontainer.worker.RemoveDriverWorker;
import py.drivercontainer.worker.RemoveDriverWorkerFactory;
import py.icshare.DriverKey;

/**
 * The class use to upgrade drivers by driverType , only when all drivers for the driverType upgrade
 * successfully,and then upgrade drivers for another driverType.
 */
public class DriverUpgradeProcessor implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(DriverUpgradeProcessor.class);
  private UpgradeDriverPoller upgradeDriverPoller;
  private VersionManager versionManager;
  private DriverContainerConfiguration dcConfig;
  private DriverStoreManager driverStoreManager;
  private LaunchDriverWorkerFactory launchDriverWorkerFactory;
  private RemoveDriverWorkerFactory removeDriverWorkerFactory;
  private PortContainerFactory portContainerFactory;
  private TaskExecutor taskExecutor;
  // Create a launch driver work and get the future ,save driverKey and its future to driverTaskMap
  private Map<DriverKey, Future> driverTaskMap = new ConcurrentHashMap<>();
  private BlockingQueue<DriverType> blockingQueue = new LinkedBlockingDeque<DriverType>();

  public void submit(DriverType driverType) throws InterruptedException {
    blockingQueue.put(driverType);
  }

  @Override
  public void run() {
    DriverType driverType = null;
    Version currentVersion = null;
    Version latestVersion = null;
    while (true) {
      try {
        driverType = blockingQueue.take();
      } catch (InterruptedException e) {
        logger.error("Get driverType is interrupted exceptionally!", e);
      }
      if (driverType == DriverType.BOGUS) {
        // When get BOGUS driverType ,it means stop upgrade driver process
        logger.warn("Interruption signal captured!");
        break;
      }
      logger.info("Driver type from blockingQueue is :{}", driverType);
      // Upgrade driver by driverType,create a new driverStore for latestVersion
      try {
        versionManager.lockVersion(driverType);
        latestVersion = versionManager.getLatestVersion(driverType);
        logger.debug("driverStoreManager:{}", driverStoreManager);
        currentVersion = versionManager.getCurrentVersion(driverType);
        versionManager.unlockVersion(driverType);
      } catch (VersionException e1) {
        logger.warn("catch an exception", e1);
        // When can not get version,set migration false to retry upgrade
        try {
          versionManager.lockVersion(driverType);
          versionManager.setOnMigration(driverType, false);
          versionManager.unlockVersion(driverType);
        } catch (VersionException e) {
          logger.warn("Can not reset migration to false", e);
        }
        logger.error("Catch an exception when get driver version", e1);
        continue;
      }

      /*
       * If currentDriverStore still have driver to upgrade ,get the driver and create launch work
       * for it
       */
      while (upgradeDriverPoller.hasMore(driverType)) {
        DriverMetadata driverMetadata = upgradeDriverPoller.poll(driverType);
        DriverMetadata latestDriver = null;
        logger.warn("poller driver is :{}", driverMetadata);
        if (driverMetadata != null) {
          DriverKey driverKey = new DriverKey(driverMetadata.getDriverContainerId(),
              driverMetadata.getVolumeId(), driverMetadata.getSnapshotId(),
              driverMetadata.getDriverType());
          latestDriver = buildDriver(driverMetadata);

          if (driverMetadata.getDriverStatus() == DriverStatus.ERROR) {
            driverStoreManager.get(latestVersion).save(latestDriver);
            synchronized (driverStoreManager.get(currentVersion)) {
              driverStoreManager.get(currentVersion).remove(driverKey);
            }
          } else {
            latestDriver.setDriverStatus(DriverStatus.LAUNCHING);
            latestDriver.setUpgradePhrase(0);
            driverStoreManager.get(latestVersion).save(latestDriver);
            Future<?> future = getLaunchWorkFuture(latestDriver, latestVersion);
            if (future != null) {
              driverTaskMap.put(driverKey, future);
            }
          }
        }
        Iterator<Map.Entry<DriverKey, Future>> iterator = driverTaskMap.entrySet().iterator();
        while (iterator.hasNext()) {
          Map.Entry<DriverKey, Future> entry = iterator.next();
          DriverMetadata newDriver = driverStoreManager.get(latestVersion).get(entry.getKey());
          logger.warn("new driver is :{}", newDriver);
          if (DriverContainerUtils
              .processFound(newDriver.getProcessId(), newDriver.getCoordinatorPort())
              && entry.getValue().isDone()
              && newDriver.getDriverStatus() == DriverStatus.LAUNCHED) {
            DriverMetadata oldDriver = driverStoreManager.get(currentVersion).get(entry.getKey());
            logger.warn("old driver is :{}", oldDriver);
            if (oldDriver != null) {
              getRemoveWorkFuture(entry.getKey(), currentVersion);
            }
            iterator.remove();
          }
        }

        try {
          // Sleep 1 sec to wait for launchWork finished
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          logger.warn("Wait for driver updrage process failed", e);
        }

      }
      logger.info("There has no {} driver to upgrade", driverType);
      try {
        driverStoreManager.remove(versionManager.getCurrentVersion(driverType));
        versionManager.lockVersion(driverType);
        logger.debug("going to change currentVersion :{} to latestVersion :{}",
            versionManager.getCurrentVersion(driverType),
            versionManager.getLatestVersion(driverType));
        versionManager.setCurrentVersion(driverType, versionManager.getLatestVersion(driverType));
        logger.warn("finished change currentVersion :{}",
            versionManager.getCurrentVersion(driverType));
        DriverVersion.currentVersion.put(driverType, versionManager.getCurrentVersion(driverType));
        versionManager.setOnMigration(driverType, false);
        DriverVersion.isOnMigration.put(driverType, versionManager.isOnMigration(driverType));
        versionManager.unlockVersion(driverType);
      } catch (VersionException e) {
        logger.error("Can not get driver version ,please check it", e);
      }
    }
  }

  // Create a launch driver work for driver

  /**
   * xx.
   */
  public Future<?> getLaunchWorkFuture(DriverMetadata driver, Version version) {
    logger.info("going to launch new upgrade version :{} driver:{}", version, driver);
    LaunchDriverParameters launchParameters = new LaunchDriverParameters();
    launchParameters.setDriverContainerInstanceId(driver.getDriverContainerId());
    launchParameters.setAccountId(driver.getAccountId());
    launchParameters.setVolumeId(driver.getVolumeId());
    launchParameters.setSnapshotId(driver.getSnapshotId());
    launchParameters.setDriverType(driver.getDriverType());
    launchParameters.setDriverIp(driver.getHostName());
    launchParameters.setDriverName(driver.getDriverName());
    launchParameters.setNicName(driver.getNicName());
    launchParameters.setIpv6Addr(driver.getIpv6Addr());
    launchParameters.setPortalType(driver.getPortalType());
    /*
     * ISCSI should driver use LO network to connect with coordinator.
     */
    if (driver.getDriverType() == DriverType.ISCSI && !dcConfig.getDriverServerIp().isEmpty()) {
      launchParameters.setHostName(dcConfig.getDriverServerIp());
    } else {
      launchParameters.setHostName(driver.getHostName());
    }
    // when upgrade driver,port and coordinator should get a new one,and if can not get an available
    // port ,retry to get
    while (true) {
      try {
        launchParameters.setPort(
            portContainerFactory.getPortContainer(launchParameters.getDriverType())
                .getAvailablePort());
        logger.debug("new port is :{}", launchParameters.getPort());
        launchParameters
            .setCoordinatorPort(launchParameters.getPort() + dcConfig.getCoordinatorBasePort());
        break;
      } catch (NoAvailablePortException e) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e1) {
          logger.error("Catch an Exception when sleep", e);
        }
        logger.warn("Catch an exception when get port", e);
        continue;
      }
    }

    launchParameters.setInstanceId(RequestIdBuilder.get());
    launchDriverWorkerFactory.setLaunchDriverParameters(launchParameters);
    launchDriverWorkerFactory.setVersion(version);

    LaunchDriverWorker worker = (LaunchDriverWorker) launchDriverWorkerFactory.createWorker();
    if (worker == null) {
      logger.error("Unable to create a worker to launch driver {}", driver);
      return null;
    }

    worker.setCreateIscsiTargetNameFlag(false);
    worker.setCoordinatorIsAlive(false);

    DriverKey driverKey = new DriverKey(driver.getDriverContainerId(), driver.getVolumeId(),
        driver.getSnapshotId(),
        driver.getDriverType());

    DriverAction futureAction = DriverAction.START_SERVER;
    Future<?> future = taskExecutor.submit(driverKey, futureAction, worker, version);

    return future;
  }


  /**
   * xx.
   */
  public Future<?> getRemoveWorkFuture(DriverKey driverKey, Version version) {
    logger.warn("going to remove old driver version :{} driverkey is:{}", version, driverKey);

    removeDriverWorkerFactory.setUpgrade(true);
    removeDriverWorkerFactory.setVersion(version);
    RemoveDriverWorker removeDriverWorker = removeDriverWorkerFactory.createWorker(driverKey);
    Future<?> future = taskExecutor
        .acceptAndSubmit(driverKey, DriverStatus.REMOVING, DriverAction.REMOVE,
            removeDriverWorker, version);
    return future;
  }


  /**
   * xx.
   */
  public DriverMetadata buildDriver(DriverMetadata driverMetadata) {
    DriverMetadata driver = new DriverMetadata();
    driver.setDriverContainerId(driverMetadata.getDriverContainerId());
    driver.setAccountId(driverMetadata.getAccountId());
    driver.setInstanceId(driverMetadata.getInstanceId());
    driver.setVolumeId(driverMetadata.getVolumeId());
    driver.setSnapshotId(driverMetadata.getSnapshotId());
    driver.setDriverType(driverMetadata.getDriverType());
    driver.setDriverName(driverMetadata.getDriverName());
    driver.setHostName(driverMetadata.getHostName());
    driver.setPort(driverMetadata.getPort());
    driver.setCoordinatorPort(driverMetadata.getCoordinatorPort());
    driver.setDriverStatus(driverMetadata.getDriverStatus());
    driver.setNbdDevice(driverMetadata.getNbdDevice());
    driver.setDriverState(driverMetadata.getDriverState());
    driver.setChapControl(driverMetadata.getChapControl());
    driver.setOldDriverKey(driverMetadata.getOldDriverKey());
    driver.setIpv6Addr(driverMetadata.getIpv6Addr());
    driver.setNicName(driverMetadata.getNicName());
    driver.setPortalType(driverMetadata.getPortalType());
    return driver;
  }


  /**
   * xx.
   */
  public void interrupt() {
    try {
      blockingQueue.offer(DriverType.BOGUS);
    } catch (Exception e) {
      logger.error("blockingQueue error.");
    }
  }

  public VersionManager getVersionManager() {
    return versionManager;
  }

  public void setVersionManager(VersionManager versionManager) {
    this.versionManager = versionManager;
  }

  public void setUpgradeDriverPoller(UpgradeDriverPoller upgradeDriverPoller) {
    this.upgradeDriverPoller = upgradeDriverPoller;
  }

  public DriverContainerConfiguration getDcConfig() {
    return dcConfig;
  }

  public void setDcConfig(DriverContainerConfiguration dcConfig) {
    this.dcConfig = dcConfig;
  }

  public DriverStoreManager getDriverStoreManager() {
    return driverStoreManager;
  }

  public void setDriverStoreManager(DriverStoreManager driverStoreManager) {
    this.driverStoreManager = driverStoreManager;
  }

  public TaskExecutor getTaskExecutor() {
    return taskExecutor;
  }

  public void setTaskExecutor(TaskExecutor taskExecutor) {
    this.taskExecutor = taskExecutor;
  }

  public PortContainerFactory getPortContainerFactory() {
    return portContainerFactory;
  }

  public void setPortContainerFactory(PortContainerFactory portContainerFactory) {
    this.portContainerFactory = portContainerFactory;
  }

  public RemoveDriverWorkerFactory getRemoveDriverWorkerFactory() {
    return removeDriverWorkerFactory;
  }

  public void setRemoveDriverWorkerFactory(RemoveDriverWorkerFactory removeDriverWorkerFactory) {
    this.removeDriverWorkerFactory = removeDriverWorkerFactory;
  }

  public LaunchDriverWorkerFactory getLaunchDriverWorkerFactory() {
    return launchDriverWorkerFactory;
  }

  public void setLaunchDriverWorkerFactory(LaunchDriverWorkerFactory launchDriverWorkerFactory) {
    this.launchDriverWorkerFactory = launchDriverWorkerFactory;
  }

  public Map<DriverKey, Future> getDriverTaskMap() {
    return driverTaskMap;
  }


  public void setDriverTaskMap(Map<DriverKey, Future> driverTaskMap) {
    this.driverTaskMap = driverTaskMap;
  }

  public BlockingQueue<DriverType> getBlockingQueue() {
    return blockingQueue;
  }

  public void setBlockingQueue(BlockingQueue<DriverType> blockingQueue) {
    this.blockingQueue = blockingQueue;
  }
}
