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

import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.DriverUpgradeProcessor;
import py.drivercontainer.JvmConfigurationManager;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.periodic.Worker;
import py.periodic.WorkerFactory;

public class SubmitUpgradeRequestWorkFactory implements WorkerFactory {

  private SubmitUpgradeRequestWorker worker;
  private DriverUpgradeProcessor driverUpgradeProcessor;

  private DriverStoreManager driverStoreManager;
  private DriverContainerConfiguration dcConfig;
  private JvmConfigurationManager jvmConfigManager;

  @Override
  public Worker createWorker() {
    if (worker == null) {
      worker = new SubmitUpgradeRequestWorker();
      worker.setDriverUpgradeProcessor(driverUpgradeProcessor);
      worker.setDcConfig(dcConfig);
      worker.setDriverStoreManager(driverStoreManager);
      worker.setJvmConfigManager(jvmConfigManager);
    }
    return worker;
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
