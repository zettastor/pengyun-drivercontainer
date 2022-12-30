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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.coordinator.IscsiTargetNameManagerFactory;
import py.coordinator.lio.LioNameBuilder;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.client.CoordinatorClientFactory;
import py.drivercontainer.delegate.DihDelegate;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.drivercontainer.driver.version.Version;
import py.drivercontainer.service.PortContainerFactory;
import py.icshare.DriverKey;
import py.infocenter.client.InformationCenterClientFactory;

public class RemoveDriverWorkerFactory {

  private static final Logger logger = LoggerFactory.getLogger(RemoveDriverWorkerFactory.class);
  private AppContext appContext;

  private DriverContainerConfiguration driverContainerConfig;

  private DriverStoreManager driverStoreManager;

  private PortContainerFactory portContainerFactory;

  private IscsiTargetNameManagerFactory iscsiTargetNameManagerFactory;

  private LioNameBuilder lioNameBuilder;

  private DihDelegate dihDelegate;

  private Version version;

  private boolean upgrade;

  private InformationCenterClientFactory infoCenterClientFactory;

  private CoordinatorClientFactory coordinatorClientFactory;


  /**
   * xx.
   */
  public RemoveDriverWorker createWorker(DriverKey driverKey) {
    RemoveDriverWorker removeDriverWorker = new RemoveDriverWorker(driverKey);
    removeDriverWorker.setIscsiTargetManager(iscsiTargetNameManagerFactory.getIscsiTargetManager());
    removeDriverWorker.setDriverStore(driverStoreManager.get(version));
    removeDriverWorker.setDriverContainerConfig(driverContainerConfig);
    removeDriverWorker.setPortContainerFactory(portContainerFactory);
    removeDriverWorker.setLioNameBuilder(lioNameBuilder);
    removeDriverWorker.setDihDelegate(dihDelegate);
    removeDriverWorker.setUpgrade(upgrade);
    removeDriverWorker.setInfoCenterClientFactory(infoCenterClientFactory);
    removeDriverWorker.setCoordinatorClientFactory(coordinatorClientFactory);
    return removeDriverWorker;
  }

  public DriverContainerConfiguration getDriverContainerConfig() {
    return driverContainerConfig;
  }

  public void setDriverContainerConfig(DriverContainerConfiguration driverContainerConfig) {
    this.driverContainerConfig = driverContainerConfig;
  }

  public AppContext getAppContext() {
    return appContext;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }

  public PortContainerFactory getPortContainerFactory() {
    return portContainerFactory;
  }

  public void setPortContainerFactory(PortContainerFactory portContainerFactory) {
    this.portContainerFactory = portContainerFactory;
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

  public DihDelegate getDihDelegate() {
    return dihDelegate;
  }

  public void setDihDelegate(DihDelegate dihDelegate) {
    this.dihDelegate = dihDelegate;
  }

  public boolean isUpgrade() {
    return upgrade;
  }

  public void setUpgrade(boolean upgrade) {
    this.upgrade = upgrade;
  }

  public Version getVersion() {
    return version;
  }

  public void setVersion(Version version) {
    this.version = version;
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

  public CoordinatorClientFactory getCoordinatorClientFactory() {
    return coordinatorClientFactory;
  }

  public void setCoordinatorClientFactory(CoordinatorClientFactory coordinatorClientFactory) {
    this.coordinatorClientFactory = coordinatorClientFactory;
  }
}
