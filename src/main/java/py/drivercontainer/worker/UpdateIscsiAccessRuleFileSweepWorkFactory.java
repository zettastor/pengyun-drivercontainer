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

import py.coordinator.IscsiTargetNameManagerFactory;
import py.coordinator.lio.LioNameBuilder;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.drivercontainer.driver.version.VersionManager;
import py.infocenter.client.InformationCenterClientFactory;
import py.periodic.Worker;
import py.periodic.WorkerFactory;

public class UpdateIscsiAccessRuleFileSweepWorkFactory implements WorkerFactory {

  private UpdateIscsiAccessRuleFileSweepWork worker;
  private InformationCenterClientFactory infoCenterClientFactory;
  //    private InitiatorsAllowFileMapper initiatorsAllowFileMapper;
  private DriverContainerConfiguration driverContainerConfig;
  private String filePath;
  private IscsiTargetNameManagerFactory iscsiTargetNameManagerFactory;
  private LioNameBuilder lioNameBuilder;
  private DriverStoreManager driverStoreManager;
  private VersionManager versionManager;

  @Override
  public Worker createWorker() {
    if (worker == null) {
      worker = new UpdateIscsiAccessRuleFileSweepWork();
      worker.setInfoCenterClientFactory(infoCenterClientFactory);
      worker.setDriverContainerConfig(driverContainerConfig);
      worker.setFilePath(filePath);
      worker.setIscsiTargetManager(iscsiTargetNameManagerFactory.getIscsiTargetManager());
      worker.setLioNameBuilder(lioNameBuilder);
      worker.setDriverStoreManager(driverStoreManager);
      worker.setVersionManager(versionManager);
      worker.retrieveIscsiAccessRule();
    }
    return worker;
  }

  public DriverStoreManager getDriverStoreManager() {
    return driverStoreManager;
  }

  public void setDriverStoreManager(DriverStoreManager driverStoreManager) {
    this.driverStoreManager = driverStoreManager;
  }

  public InformationCenterClientFactory getInfoCenterClientFactory() {
    return infoCenterClientFactory;
  }

  public void setInfoCenterClientFactory(InformationCenterClientFactory infoCenterClientFactory) {
    this.infoCenterClientFactory = infoCenterClientFactory;
  }


  public DriverContainerConfiguration getDriverContainerConfig() {
    return driverContainerConfig;
  }

  public void setDriverContainerConfig(DriverContainerConfiguration driverContainerConfig) {
    this.driverContainerConfig = driverContainerConfig;
  }

  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public IscsiTargetNameManagerFactory getIscsiTargetNameManagerFactory() {
    return iscsiTargetNameManagerFactory;
  }

  public void setIscsiTargetNameManagerFactory(
      IscsiTargetNameManagerFactory iscsiTargetNameManagerFactory) {
    this.iscsiTargetNameManagerFactory = iscsiTargetNameManagerFactory;
  }

  public LioNameBuilder getLioNameBuilder() {
    return lioNameBuilder;
  }

  public void setLioNameBuilder(LioNameBuilder lioNameBuilder) {
    this.lioNameBuilder = lioNameBuilder;
  }

  public VersionManager getVersionManager() {
    return versionManager;
  }

  public void setVersionManager(VersionManager versionManager) {
    this.versionManager = versionManager;
  }
}
