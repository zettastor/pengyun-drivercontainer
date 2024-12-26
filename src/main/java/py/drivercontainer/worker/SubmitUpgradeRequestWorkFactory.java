/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
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
