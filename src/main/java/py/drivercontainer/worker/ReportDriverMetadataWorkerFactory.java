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

import py.app.context.AppContext;
import py.coordinator.IscsiTargetNameManagerFactory;
import py.coordinator.lio.LioNameBuilder;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.drivercontainer.driver.version.VersionManager;
import py.infocenter.client.InformationCenterClientFactory;
import py.periodic.Worker;
import py.periodic.WorkerFactory;

public class ReportDriverMetadataWorkerFactory implements WorkerFactory {

  private static ReportDriverMetadataWorker worker;

  private InformationCenterClientFactory inforCenterClientFactory;
  private DriverContainerConfiguration driverContainerConfiguration;

  private IscsiTargetNameManagerFactory iscsiTargetNameManagerFactory;

  private LioNameBuilder lioNameBuilder;

  private DriverStoreManager driverStoreManager;

  private int iscsiDriverPort;
  private int reportDriverClientSessionTryTimes;

  private VersionManager versionManager;
  private AppContext appContext;

  public static ReportDriverMetadataWorker getWorker() {
    return worker;
  }

  public static void setWorker(ReportDriverMetadataWorker worker) {
    ReportDriverMetadataWorkerFactory.worker = worker;
  }

  @Override
  public synchronized Worker createWorker() {
    if (worker == null) {
      worker = new ReportDriverMetadataWorker();
      worker.setDriverStoreManager(driverStoreManager);
      worker.setVersionManager(versionManager);
      worker.setInformationCenterClientFactory(inforCenterClientFactory);
      worker.setIscsiDriverPort(iscsiDriverPort);
      worker.setDriverContainerConfiguration(driverContainerConfiguration);
      worker.setIscsiTargetManager(iscsiTargetNameManagerFactory.getIscsiTargetManager());
      worker.setLioNameBuilder(lioNameBuilder);
      worker.setAppContext(appContext);
      worker.setReportDriverClientSessionTryTimes(reportDriverClientSessionTryTimes);
    }
    return worker;
  }

  public VersionManager getVersionManager() {
    return versionManager;
  }

  public void setVersionManager(VersionManager versionManager) {
    this.versionManager = versionManager;
  }

  public InformationCenterClientFactory getInforCenterClientFactory() {
    return inforCenterClientFactory;
  }

  public void setInforCenterClientFactory(InformationCenterClientFactory inforCenterClientFactory) {
    this.inforCenterClientFactory = inforCenterClientFactory;
  }

  public DriverStoreManager getDriverStoreManager() {
    return driverStoreManager;
  }

  public void setDriverStoreManager(DriverStoreManager driverStoreManager) {
    this.driverStoreManager = driverStoreManager;
  }

  public int getIscsiDriverPort() {
    return iscsiDriverPort;
  }

  public void setIscsiDriverPort(int iscsiDriverPort) {
    this.iscsiDriverPort = iscsiDriverPort;
  }


  public DriverContainerConfiguration getDriverContainerConfiguration() {
    return driverContainerConfiguration;
  }

  public void setDriverContainerConfiguration(
      DriverContainerConfiguration driverContainerConfiguration) {
    this.driverContainerConfiguration = driverContainerConfiguration;
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

  public AppContext getAppContext() {
    return appContext;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }

  public int getReportDriverClientSessionTryTimes() {
    return reportDriverClientSessionTryTimes;
  }

  public void setReportDriverClientSessionTryTimes(int reportDriverClientSessionTryTimes) {
    this.reportDriverClientSessionTryTimes = reportDriverClientSessionTryTimes;
  }
}
