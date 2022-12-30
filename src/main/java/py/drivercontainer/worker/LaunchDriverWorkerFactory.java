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
import py.coordinator.IscsiTargetNameManagerFactory;
import py.coordinator.lio.LioNameBuilder;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.JvmConfigurationManager;
import py.drivercontainer.client.CoordinatorClientFactory;
import py.drivercontainer.driver.LaunchDriverParameters;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.drivercontainer.driver.version.Version;
import py.drivercontainer.service.PortContainerFactory;
import py.drivercontainer.utils.IscsiProcessor;
import py.infocenter.client.InformationCenterClientFactory;
import py.periodic.Worker;
import py.periodic.WorkerFactory;

public class LaunchDriverWorkerFactory implements WorkerFactory {

  private static final Logger logger = LoggerFactory.getLogger(LaunchDriverWorkerFactory.class);
  private InformationCenterClientFactory informationCenterClientFactory;

  private LaunchDriverParameters launchDriverParameters;

  private PortContainerFactory portContainerFactory;

  private JvmConfigurationManager jvmConfigManager;

  private IscsiTargetNameManagerFactory iscsiTargetNameManagerFactory;

  private DriverContainerConfiguration driverContainerConfig;

  private LioNameBuilder lioNameBuilder;

  private DriverStoreManager driverStoreManager;

  private Version version;

  private CoordinatorClientFactory coordinatorClientFactory;

  @Override
  public Worker createWorker() {
    try {
      IscsiProcessor iscsiProcessor = new IscsiProcessor();
      iscsiProcessor.setDriverContainerConfig(driverContainerConfig);
      iscsiProcessor.setIscsiTargetManager(iscsiTargetNameManagerFactory.getIscsiTargetManager());
      iscsiProcessor.setInformationCenterFactory(informationCenterClientFactory);
      iscsiProcessor.setTwoIscsiTargetSwitch(driverContainerConfig.getCreateTwoIscsiTargetSwitch());
      iscsiProcessor.setLaunchDriverParameters(launchDriverParameters);
      iscsiProcessor.setTimeout(driverContainerConfig.getDriverOperationTimeout());
      iscsiProcessor.setLioNameBuilder(lioNameBuilder);
      iscsiProcessor.setDriverContainerConfiguration(driverContainerConfig);
      LaunchDriverWorker launchDriverWorker;

      launchDriverWorker = new LaunchDriverWorker(launchDriverParameters,
          jvmConfigManager.getJvmConfig(launchDriverParameters.getDriverType()),
          driverContainerConfig);
      launchDriverWorker.setPortContainerFactory(portContainerFactory);
      launchDriverWorker.setIscsiProcessor(iscsiProcessor);
      launchDriverWorker.setDriverStore(driverStoreManager.get(version));
      launchDriverWorker.setCoordinatorClientFactory(coordinatorClientFactory);
      launchDriverWorker.init();

      return launchDriverWorker;
    } catch (IllegalStateException e) {
      logger.error("Caugth an exception when create worker to launch driver", e);
      return null;
    }
  }

  public LaunchDriverParameters getLaunchDriverParameters() {
    return launchDriverParameters;
  }

  public void setLaunchDriverParameters(LaunchDriverParameters launchDriverParameters) {
    this.launchDriverParameters = launchDriverParameters;
  }

  public InformationCenterClientFactory getInformationCenterClientFactory() {
    return informationCenterClientFactory;
  }

  public void setInformationCenterClientFactory(
      InformationCenterClientFactory informationCenterClientFactory) {
    this.informationCenterClientFactory = informationCenterClientFactory;
  }

  public DriverStoreManager getDriverStoreManager() {
    return driverStoreManager;
  }

  public void setDriverStoreManager(DriverStoreManager driverStoreManager) {
    this.driverStoreManager = driverStoreManager;
  }

  public PortContainerFactory getPortContainerFactory() {
    return portContainerFactory;
  }

  public void setPortContainerFactory(PortContainerFactory portContainerFactory) {
    this.portContainerFactory = portContainerFactory;
  }

  public JvmConfigurationManager getJvmConfigManager() {
    return jvmConfigManager;
  }

  public void setJvmConfigManager(JvmConfigurationManager jvmConfigManager) {
    this.jvmConfigManager = jvmConfigManager;
  }


  public DriverContainerConfiguration getDriverContainerConfig() {
    return driverContainerConfig;
  }

  public void setDriverContainerConfig(DriverContainerConfiguration driverContainerConfig) {
    this.driverContainerConfig = driverContainerConfig;
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

  public Version getVersion() {
    return version;
  }

  public void setVersion(Version version) {
    this.version = version;
  }

  public CoordinatorClientFactory getCoordinatorClientFactory() {
    return coordinatorClientFactory;
  }

  public void setCoordinatorClientFactory(CoordinatorClientFactory coordinatorClientFactory) {
    this.coordinatorClientFactory = coordinatorClientFactory;
  }
}
