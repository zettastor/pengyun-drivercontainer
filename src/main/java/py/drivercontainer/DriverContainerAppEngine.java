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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.thrift.ThriftAppEngine;
import py.drivercontainer.driver.upgrade.QueryServer;
import py.drivercontainer.driver.version.VersionException;
import py.drivercontainer.driver.version.VersionManager;
import py.drivercontainer.service.BootStrap;
import py.drivercontainer.service.DriverContainerImpl;
import py.drivercontainer.utils.DriverContainerUtils;
import py.periodic.PeriodicWorkExecutor;

public class DriverContainerAppEngine extends ThriftAppEngine {

  private static final Logger logger = LoggerFactory.getLogger(DriverContainerAppEngine.class);

  private DriverContainerConfiguration dcConfig;

  private VersionManager versionManager;

  private DriverContainerImpl driverContainerImpl;

  private BootStrap bootstrap;

  private PeriodicWorkExecutor serverScannerExecutor;

  private PeriodicWorkExecutor targetScannerExecutor;

  private PeriodicWorkExecutor reporterExecutor;

  private PeriodicWorkExecutor accessRulesPollerExecutor;

  private PeriodicWorkExecutor accessRulesUpdaterExecutor;

  private PeriodicWorkExecutor taskExecutorRunner;

  private PeriodicWorkExecutor submitUpgradeRequestRunner;

  private QueryServer queryServer;

  private DriverUpgradeProcessor driverUpgradeProcessor;

  private PeriodicWorkExecutor getIoLimitationExecutor;

  private PeriodicWorkExecutor migratingScanExecutor;

  private PeriodicWorkExecutor scsiDeviceScanExecutor;

  private boolean scsiClientMode;

  private boolean normalMode;


  /**
   * xx.
   */
  public DriverContainerAppEngine(DriverContainerImpl driverContainerImpl) {
    super(driverContainerImpl);
    this.driverContainerImpl = driverContainerImpl;
    driverContainerImpl.setDriverContainerAppEngine(this);
  }

  @Override
  public void start() throws Exception {
    initScsiClientMode();
    initNormalMode();

    logger.warn("\n\n\n### dc launch start ###");
    logger.warn("launch mode {} [1:mixed 2:scsi client 3:no scsi client].",
        dcConfig.getDrivercontainerRole());
    logger.warn("code data 2019-07-18.\n\n");
    DriverContainerUtils.init();

    if (!bootstrap.bootstrap()) {
      String errMsg = "Something wrong in bootstrap.";
      logger.error("{}", errMsg);
      throw new RuntimeException(errMsg);
    }

    logger.info("Starting submit upgrade request executor ...");
    submitUpgradeRequestRunner.start();

    if (isNormalMode()) {
      logger.info("Starting server scanner ...");
      serverScannerExecutor.start();

      logger.info("Starting target scanner ...");
      targetScannerExecutor.start();

      logger.info("Starting driver reporter ...");
      reporterExecutor.start();

      logger.info("Starting migrating scanner ...");
      migratingScanExecutor.start();

      logger.info("Starting access rules poller ...");
      accessRulesPollerExecutor.start();

      logger.info("Starting access rules updater ...");
      accessRulesUpdaterExecutor.start();

      logger.info("Starting io limitation executor ...");
      getIoLimitationExecutor.start();

      logger.info("Starting queryServer ...");
      // begin queryServer thread, which can start queryServer in run
      Thread thread = new Thread(queryServer);
      thread.start();
    }

    logger.info("Starting task executor ...");
    taskExecutorRunner.start();

    logger.info("Starting upgrade driver processor ...");
    Thread upgradeThread = new Thread(driverUpgradeProcessor);
    upgradeThread.start();

    if (isScsiClientMode()) {
      logger.warn("Starting check scsi thread ...");
      scsiDeviceScanExecutor.start();
    }

    super.start();
    logger.warn("\n\n\n@@@ dc launch end @@@\n\n");
  }

  @Override
  public void stop() {
    int step = 0;

    logger.warn(">>>><step{}> shutdown driver-container service.", ++step);
    super.stop();

    if (isNormalMode()) {
      logger.warn(">>>><step{}> stop server scanner.", ++step);
      try {
        serverScannerExecutor.stop();
      } catch (Exception e) {
        logger.error("Caught an exception", e);
      }

      logger.warn(">>>><step{}> stop target scanner.", ++step);
      try {
        targetScannerExecutor.stop();
      } catch (Exception e) {
        logger.error("Caught an exception", e);
      }

      logger.warn(">>>><step{}> stop driver reporter.", ++step);
      try {
        reporterExecutor.stop();
      } catch (Exception e) {
        logger.error("Caught an exception", e);
      }

      logger.warn(">>>><step{}> stop access rules poller.", ++step);
      try {
        accessRulesPollerExecutor.stop();
      } catch (Exception e) {
        logger.error("Caught an exception", e);
      }

      logger.warn(">>>><step{}> stop access rules updater.", ++step);
      try {
        accessRulesUpdaterExecutor.stop();
      } catch (Exception e) {
        logger.error("Caught an exception", e);
      }

      logger.warn(">>>><step{}> stop queryServer.", ++step);
      queryServer.close();

      logger.warn(">>>><step{}> stop io limitation executor.", ++step);
      try {
        getIoLimitationExecutor.stop();
      } catch (Exception e) {
        logger.error("Caught an exception", e);
      }

      logger.warn(">>>><step{}> stop migrating scanner.", ++step);
      migratingScanExecutor.stop();

      logger.warn(">>>><step{}> stop coordinator client factory.", ++step);
      driverContainerImpl.getChangeDriverVolumeTask().getCoordinatorClientFactory().close();
    }

    logger.warn(">>>><step{}> stop submit upgrade request updater.", ++step);
    try {
      submitUpgradeRequestRunner.stop();
    } catch (Exception e) {
      logger.error("Caught an exception", e);
    }

    logger.warn(">>>><step{}> stop task executor.", ++step);
    try {
      taskExecutorRunner.stop();
    } catch (Exception e) {
      logger.error("Caught an exception", e);
    }

    if (isScsiClientMode()) {
      logger.warn(">>>><step{}> stop io check scsi device executor.", ++step);
      try {
        scsiDeviceScanExecutor.stop();
      } catch (Exception e) {
        logger.error("Caught an exception", e);
      }
    }

    logger.warn(">>>><step{}> stop upgrade driver processor.", ++step);
    driverUpgradeProcessor.interrupt();

    logger.warn(">>>><step{}> destroy driver-container utils.", ++step);
    DriverContainerUtils.destroy();
  }

  private void initScsiClientMode() {
    if (dcConfig.getDrivercontainerRole() == 1 || dcConfig.getDrivercontainerRole() == 2) {
      setScsiClientMode(true);
    } else {
      setScsiClientMode(false);
    }
  }

  private void initNormalMode() {
    if (dcConfig.getDrivercontainerRole() == 1 || dcConfig.getDrivercontainerRole() == 3) {
      setNormalMode(true);
    } else {
      setNormalMode(false);
    }
  }

  public PeriodicWorkExecutor getServerScannerExecutor() {
    return serverScannerExecutor;
  }

  public void setServerScannerExecutor(PeriodicWorkExecutor serverScannerExecutor) {
    this.serverScannerExecutor = serverScannerExecutor;
  }

  public PeriodicWorkExecutor getTargetScannerExecutor() {
    return targetScannerExecutor;
  }

  public void setTargetScannerExecutor(PeriodicWorkExecutor targetScannerExecutor) {
    this.targetScannerExecutor = targetScannerExecutor;
  }

  public PeriodicWorkExecutor getReporterExecutor() {
    return reporterExecutor;
  }

  public void setReporterExecutor(PeriodicWorkExecutor reporterExecutor) {
    this.reporterExecutor = reporterExecutor;
  }

  public PeriodicWorkExecutor getScsiDeviceScanExecutor() {
    return scsiDeviceScanExecutor;
  }

  public void setScsiDeviceScanExecutor(PeriodicWorkExecutor scsiDeviceScanExecutor) {
    this.scsiDeviceScanExecutor = scsiDeviceScanExecutor;
  }

  public PeriodicWorkExecutor getAccessRulesPollerExecutor() {
    return accessRulesPollerExecutor;
  }

  public void setAccessRulesPollerExecutor(PeriodicWorkExecutor accessRulesPollerExecutor) {
    this.accessRulesPollerExecutor = accessRulesPollerExecutor;
  }

  public PeriodicWorkExecutor getAccessRulesUpdaterExecutor() {
    return accessRulesUpdaterExecutor;
  }

  public void setAccessRulesUpdaterExecutor(PeriodicWorkExecutor accessRulesUpdaterExecutor) {
    this.accessRulesUpdaterExecutor = accessRulesUpdaterExecutor;
  }

  public PeriodicWorkExecutor getTaskExecutorRunner() {
    return taskExecutorRunner;
  }

  public void setTaskExecutorRunner(PeriodicWorkExecutor taskExecutorRunner) {
    this.taskExecutorRunner = taskExecutorRunner;
  }

  public PeriodicWorkExecutor getGetIoLimitationExecutor() {
    return getIoLimitationExecutor;
  }

  public void setGetIoLimitationExecutor(PeriodicWorkExecutor getIoLimitationExecutor) {
    this.getIoLimitationExecutor = getIoLimitationExecutor;
  }

  public BootStrap getBootstrap() {
    return bootstrap;
  }

  public void setBootstrap(BootStrap bootstrap) {
    this.bootstrap = bootstrap;
  }

  public PeriodicWorkExecutor getSubmitUpgradeRequestRunner() {
    return submitUpgradeRequestRunner;
  }

  public void setSubmitUpgradeRequestRunner(PeriodicWorkExecutor submitUpgradeRequestRunner) {
    this.submitUpgradeRequestRunner = submitUpgradeRequestRunner;
  }

  public DriverUpgradeProcessor getDriverUpgradeProcessor() {
    return driverUpgradeProcessor;
  }

  public void setDriverUpgradeProcessor(DriverUpgradeProcessor driverUpgradeProcessor) {
    this.driverUpgradeProcessor = driverUpgradeProcessor;
  }

  public QueryServer getQueryServer() {
    return queryServer;
  }

  public void setQueryServer(QueryServer queryServer) {
    this.queryServer = queryServer;
  }

  public PeriodicWorkExecutor getMigratingScanExecutor() {
    return migratingScanExecutor;
  }

  public void setMigratingScanExecutor(PeriodicWorkExecutor migratingScanExecutor) {
    this.migratingScanExecutor = migratingScanExecutor;
  }

  public DriverContainerConfiguration getDcConfig() {
    return dcConfig;
  }

  public void setDcConfig(DriverContainerConfiguration dcConfig) {
    this.dcConfig = dcConfig;
  }

  public VersionManager getVersionManager() {
    return versionManager;
  }

  public void setVersionManager(VersionManager versionManager) {
    this.versionManager = versionManager;
  }

  public boolean isScsiClientMode() {
    return scsiClientMode;
  }

  public void setScsiClientMode(boolean scsiClientMode) {
    this.scsiClientMode = scsiClientMode;
  }

  public boolean isNormalMode() {
    return normalMode;
  }

  public void setNormalMode(boolean normalMode) {
    this.normalMode = normalMode;
  }

  /**
   * Check if requirements (e.g package) for drivers are ready.
   *
   * @throws IllegalStateException if requirements for drivers are not ready.
   */
  private void checkDriverRequirements() throws IllegalStateException {
    // the version info will be set after package of driver having been deployed, or empty version
    // info will be got
  }
}
