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

package py.drivercontainer.utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.IscsiTargetNameBuilder;
import py.coordinator.IscsiTargetManager;
import py.coordinator.lio.LioNameBuilder;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.driver.LaunchDriverParameters;
import py.infocenter.client.InformationCenterClientFactory;

public class IscsiProcessor {

  private static final Logger logger = LoggerFactory.getLogger(IscsiProcessor.class);
  boolean twoIscsiTargetSwitch;
  private long timeout;
  private InformationCenterClientFactory informationCenterClientFactory;
  private LaunchDriverParameters launchDriverParameters;
  private DriverContainerConfiguration driverContainerConfig;
  private IscsiTargetManager iscsiTargetManager;
  private LioNameBuilder lioNameBuilder;
  private DriverContainerConfiguration driverContainerConfiguration;

  /**
   * create iscsi target.
   *
   * @return boolen:false faile to create,true sucess to create;
   */
  public String createIscsiTarget(String pydDevice) throws Exception {
    String nbdDevice = createIscsiTarget(pydDevice, launchDriverParameters.getVolumeId());
    return nbdDevice;
  }

  //If volumeId has been changed for volume migrate on line ,driver on the volume will still use
  // the old volumeId to create targetName. So in this case , volId will be setted as old volumeId.
  // But volumeId from launchDriverParameters is new volumeId always.

  /**
   * xx.
   */
  public String createIscsiTarget(String pydDevice, long volId) throws Exception {
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
    logger.warn("5.1 current time : {}", df.format(new Date()));
    String targetName = null;
    targetName = lioNameBuilder.buildLioWwn(volId, launchDriverParameters.getSnapshotId());
    logger.warn("Create an iscsi target {} then,pydDevice:{}", targetName, pydDevice);

    String nbdDevice = null;
    nbdDevice = iscsiTargetManager
        .createTarget(targetName, launchDriverParameters.getIscsiTargetNameIp(),
            launchDriverParameters.getHostName(), pydDevice, volId,
            launchDriverParameters.getSnapshotId());
    logger.warn("5.4 current time : {}", df.format(new Date()));
    if (nbdDevice == null) {
      logger.error("Something wrong when create iscsi target");
      return null;
    }
    logger.debug("iscsi target service create nbddev:{}", nbdDevice);

    // create second target if need
    if (twoIscsiTargetSwitch) {
      String secondTargetName = IscsiTargetNameBuilder
          .buildSecondIqnName(volId, launchDriverParameters.getSnapshotId());
      logger.warn("going to create second target:{}", secondTargetName);
      nbdDevice = iscsiTargetManager
          .createTarget(secondTargetName, launchDriverParameters.getIscsiTargetNameIp(),
              launchDriverParameters.getHostName(), pydDevice, volId,
              launchDriverParameters.getSnapshotId());
      if (nbdDevice == null) {
        logger.error("Something wrong when create iscsi target");
        return null;
      }
    }
    logger.warn("5.5 current time : {}", df.format(new Date()));
    logger.warn("nbdDevice in iscsiProcessor:{}", nbdDevice);
    return nbdDevice;
  }

  /**
   * xx.
   */
  public void setInformationCenterFactory(
      InformationCenterClientFactory informationCenterClientFactory) {
    this.informationCenterClientFactory = informationCenterClientFactory;
  }

  /**
   * xx.
   */
  public void setTwoIscsiTargetSwitch(boolean twoIscsiTargetSwitch) {
    this.twoIscsiTargetSwitch = twoIscsiTargetSwitch;
  }

  /**
   * xx.
   */
  public void setTimeout(long timeout) {
    this.timeout = timeout;
  }

  public void setLaunchDriverParameters(LaunchDriverParameters launchDriverParameters) {
    this.launchDriverParameters = launchDriverParameters;
  }

  public DriverContainerConfiguration getDriverContainerConfig() {
    return driverContainerConfig;
  }

  public void setDriverContainerConfig(DriverContainerConfiguration driverContainerConfig) {
    this.driverContainerConfig = driverContainerConfig;
  }

  public IscsiTargetManager getIscsiTargetManager() {
    return iscsiTargetManager;
  }

  public void setIscsiTargetManager(IscsiTargetManager iscsiTargetManager) {
    this.iscsiTargetManager = iscsiTargetManager;
  }

  public LioNameBuilder getLioNameBuilder() {
    return lioNameBuilder;
  }

  public void setLioNameBuilder(LioNameBuilder lioNameBuilder) {
    this.lioNameBuilder = lioNameBuilder;
  }

  public DriverContainerConfiguration getDriverContainerConfiguration() {
    return driverContainerConfiguration;
  }

  public void setDriverContainerConfiguration(
      DriverContainerConfiguration driverContainerConfiguration) {
    this.driverContainerConfiguration = driverContainerConfiguration;
  }
}
