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
