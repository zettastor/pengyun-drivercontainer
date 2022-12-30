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
