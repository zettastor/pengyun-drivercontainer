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

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.driver.DriverType;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.DriverUpgradeProcessor;
import py.drivercontainer.DriverVersion;
import py.drivercontainer.JvmConfiguration;
import py.drivercontainer.JvmConfigurationManager;
import py.drivercontainer.driver.store.DriverStore;
import py.drivercontainer.driver.store.DriverStoreImpl;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.drivercontainer.driver.version.Version;
import py.drivercontainer.driver.version.VersionException;
import py.drivercontainer.utils.DriverContainerUtils;
import py.periodic.Worker;

/**
 * Check latest version and current version,when not equals ,going to upgrade drivers by driverType.
 */
public class SubmitUpgradeRequestWorker implements Worker {

  private static final Logger logger = LoggerFactory.getLogger(SubmitUpgradeRequestWorker.class);
  private DriverUpgradeProcessor driverUpgradeProcessor;
  private DriverStoreManager driverStoreManager;
  private DriverContainerConfiguration dcConfig;
  private JvmConfigurationManager jvmConfigManager;

  @Override
  public void doWork() throws Exception {
    //Now,only support NBD and FSD driverType to upgrade
    List<DriverType> driverTypeList = new ArrayList<>();
    driverTypeList.add(DriverType.NBD);
    for (DriverType driverType : driverTypeList) {
      if (!checkDriverVersion(driverType)) {
        continue;
      }
      Version currentVersion = DriverVersion.currentVersion.get(driverType);
      Version latestVersion = DriverVersion.latestVersion.get(driverType);
      boolean onMigration = DriverVersion.isOnMigration.get(driverType);
      if (currentVersion == null) {
        logger.debug("No driver with type {} is launched", driverType);
        continue;
      }

      //Submit upgrade request by driverType
      if (!currentVersion.equals(latestVersion) && !onMigration) {
        logger.warn("going to submit driverType:{}", driverType);
        int driverNumbers = countDriverNumbers(driverType);
        int minNumber = Math.min(driverNumbers, dcConfig.getDriverUpgradeLimit());
        JvmConfiguration jvmConfig = jvmConfigManager.getJvmConfig(driverType);
        if (DriverContainerUtils.getSysFreeMem()
            - DriverContainerUtils.getBytesSizeFromString(jvmConfig.getMinMemPoolSize()) * minNumber
            > dcConfig.getSystemMemoryForceReserved()) {
          driverUpgradeProcessor.getVersionManager().lockVersion(driverType);
          driverUpgradeProcessor.getVersionManager().setOnMigration(driverType, true);
          driverUpgradeProcessor.getVersionManager().unlockVersion(driverType);
          driverUpgradeProcessor.submit(driverType);
        } else {
          logger.warn(
              "System memory remaining too fewer,please release the memroy to upgrade driver");
        }
      }
    }

  }


  /**
   * xx.
   */
  public int countDriverNumbers(DriverType driverType) {
    int driverNumbers = 0;
    if (driverStoreManager.get(DriverVersion.currentVersion.get(driverType)) != null) {
      driverNumbers += driverStoreManager.get(DriverVersion.currentVersion.get(driverType)).list()
          .size();
    }
    return driverNumbers;
  }

  /**
   * When deploy driverContainer first time ,currentVersion and latestVersion file is null,and when
   * deploy coordinator dd will write latestVersion for NBD driver,and now this method will write
   * currentVersion with value of latestVersion.
   */
  public boolean checkDriverVersion(DriverType driverType) throws VersionException {
    try {
      driverUpgradeProcessor.getVersionManager().lockVersion(driverType);
    } catch (IllegalStateException e) {
      logger.error("Driver type :{} has been locked ", driverType);
      return false;
    }
    Version latestVersionFromFile = driverUpgradeProcessor.getVersionManager()
        .getLatestVersion(driverType);
    if (latestVersionFromFile == null) {
      logger
          .debug("latestVersion is null for {},maybe has not been deploy this service", driverType);
      driverUpgradeProcessor.getVersionManager().unlockVersion(driverType);
      return false;
    }
    Version currentVersionFromFile = driverUpgradeProcessor.getVersionManager()
        .getCurrentVersion(driverType);
    boolean onMigration = driverUpgradeProcessor.getVersionManager().isOnMigration(driverType);
    //Use latestVersion value to initial currentVersion
    String varPath = dcConfig.getDriverAbsoluteRootPath();
    if (currentVersionFromFile == null) {
      currentVersionFromFile = latestVersionFromFile;
      driverUpgradeProcessor.getVersionManager()
          .setCurrentVersion(driverType, currentVersionFromFile);
      //initial current driverStore
      DriverStore currentDriverStore = new DriverStoreImpl(Paths.get(varPath),
          currentVersionFromFile);
      driverStoreManager.put(currentVersionFromFile, currentDriverStore);
    } else if (!currentVersionFromFile.equals(latestVersionFromFile)) {
      //The case means driver will upgrade,and initial latestDriverStore
      if (!driverStoreManager.keySet().contains(latestVersionFromFile)) {
        DriverStore latestDriverStore = new DriverStoreImpl(Paths.get(varPath),
            latestVersionFromFile);
        driverStoreManager.put(latestVersionFromFile, latestDriverStore);
      }
    }
    driverUpgradeProcessor.getVersionManager().unlockVersion(driverType);

    if (DriverVersion.isOnMigration.get(driverType) == null) {
      DriverVersion.isOnMigration.put(driverType, onMigration);
    }
    if (!DriverVersion.isOnMigration.get(driverType).equals(onMigration)) {
      DriverVersion.isOnMigration.put(driverType, onMigration);
    }

    //Save currentVersion ,latestVersion and isOnMigration value to memory , to avoid read version
    // file too frequently.And when version file different from memory,use value of file to change
    // memory value.

    if (!currentVersionFromFile.equals(DriverVersion.currentVersion.get(driverType))) {
      DriverVersion.currentVersion.put(driverType, currentVersionFromFile);
    }

    if (!latestVersionFromFile.equals(DriverVersion.latestVersion.get(driverType))) {
      DriverVersion.latestVersion.put(driverType, latestVersionFromFile);
    }
    return true;
  }

  public DriverUpgradeProcessor getDriverUpgradeProcessor() {
    return driverUpgradeProcessor;
  }

  public void setDriverUpgradeProcessor(DriverUpgradeProcessor driverUpgradeProcessor) {
    this.driverUpgradeProcessor = driverUpgradeProcessor;
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

  public JvmConfigurationManager getJvmConfigManager() {
    return jvmConfigManager;
  }

  public void setJvmConfigManager(JvmConfigurationManager jvmConfigManager) {
    this.jvmConfigManager = jvmConfigManager;
  }
}
