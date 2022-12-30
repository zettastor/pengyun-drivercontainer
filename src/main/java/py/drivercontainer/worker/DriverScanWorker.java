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

import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.coordinator.DriverFailureSignal;
import py.driver.DriverAction;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.driver.LaunchDriverParameters;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.drivercontainer.driver.version.Version;
import py.drivercontainer.service.PortContainerFactory;
import py.drivercontainer.service.taskqueue.TaskExecutor;
import py.icshare.DriverKey;
import py.periodic.Worker;

public abstract class DriverScanWorker implements Worker {

  private static final Logger logger = LoggerFactory.getLogger(DriverScanWorker.class);

  protected AppContext appContext;

  protected DriverContainerConfiguration dcConfig;

  protected DriverStoreManager driverStoreManager;

  protected TaskExecutor taskExecutor;

  protected LaunchDriverWorkerFactory launchDriverWorkerFactory;

  protected RemoveDriverWorkerFactory removeDriverWorkerFactory;

  protected PortContainerFactory portContainerFactory;

  public static Logger getLogger() {
    return logger;
  }

  public AppContext getAppContext() {
    return appContext;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
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

  public LaunchDriverWorkerFactory getLaunchDriverWorkerFactory() {
    return launchDriverWorkerFactory;
  }

  public void setLaunchDriverWorkerFactory(LaunchDriverWorkerFactory launchDriverWorkerFactory) {
    this.launchDriverWorkerFactory = launchDriverWorkerFactory;
  }

  public RemoveDriverWorkerFactory getRemoveDriverWorkerFactory() {
    return removeDriverWorkerFactory;
  }

  public void setRemoveDriverWorkerFactory(RemoveDriverWorkerFactory removeDriverWorkerFactory) {
    this.removeDriverWorkerFactory = removeDriverWorkerFactory;
  }

  public PortContainerFactory getPortContainerFactory() {
    return portContainerFactory;
  }

  public void setPortContainerFactory(PortContainerFactory portContainerFactory) {
    this.portContainerFactory = portContainerFactory;
  }

  /**
   * Schedule a task to recover the given driver.
   *
   * @param driver  recovering target driver meta-data
   * @param version version of the recovering driver
   * @return future of task to recover the driver
   */
  public Future<?> scheduleLaunchingRecover(DriverMetadata driver, Version version,
      DriverFailureSignal driverFailureSignal) {
    DriverAction futureAction;
    //isTarget:check if the driver is ISCSI target.
    boolean isTarget = false;
    if (driverFailureSignal.equals(DriverFailureSignal.SERVER)) {
      futureAction = DriverAction.START_SERVER;
    } else {
      futureAction = DriverAction.CREATE_TARGET;
      isTarget = true;
    }

    LaunchDriverParameters launchParameters = new LaunchDriverParameters();
    launchParameters.setDriverContainerInstanceId(appContext.getInstanceId().getId());
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
    launchParameters.setPort(driver.getPort());
    launchParameters.setCoordinatorPort(driver.getCoordinatorPort());

    launchParameters.setInstanceId(driver.getInstanceId());
    final Future<?> future;

    final DriverKey driverKey = new DriverKey(driver.getDriverContainerId(), driver.getVolumeId(),
        driver.getSnapshotId(),
        driver.getDriverType());

    logger.debug("launchParameters volid :{}", launchParameters.getVolumeId());
    launchDriverWorkerFactory.setLaunchDriverParameters(launchParameters);
    launchDriverWorkerFactory.setVersion(version);

    LaunchDriverWorker worker = (LaunchDriverWorker) launchDriverWorkerFactory.createWorker();
    if (worker == null) {
      logger.error("Unable to create a worker to launch driver {}", driver);
      return null;
    }
    worker.setCreateIscsiTargetNameFlag(isTarget);
    worker.setCoordinatorIsAlive(isTarget);
    future = taskExecutor.acceptAndSubmit(driverKey, DriverStatus.RECOVERING, futureAction, worker,
        version);
    return future;
  }

  /**
   * Schedule a task to continue removing the given driver.
   *
   * @return future of task to continue removing the given driver
   */
  public Future<?> scheduleRemovingRecover(DriverMetadata driver, Version version) {
    DriverKey driverKey = new DriverKey(driver.getDriverContainerId(), driver.getVolumeId(),
        driver.getSnapshotId(),
        driver.getDriverType());
    removeDriverWorkerFactory.setVersion(version);
    removeDriverWorkerFactory.setUpgrade(false);
    RemoveDriverWorker removeDriverWorker = removeDriverWorkerFactory.createWorker(driverKey);
    Future<?> future = taskExecutor
        .acceptAndSubmit(driverKey, DriverStatus.REMOVING, DriverAction.REMOVE,
            removeDriverWorker, version);

    return future;
  }

}
