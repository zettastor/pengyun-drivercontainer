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
import py.driver.DriverMetadata;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.client.CoordinatorClientFactory;
import py.drivercontainer.driver.store.DriverStore;
import py.icshare.DriverKey;

/**
 * When migrate volume on line , the class going to notify coordinator ,and change volumeId for
 * driverMetadata.
 */
public class ChangeDriverVolumeTask implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(ChangeDriverVolumeTask.class);

  private DriverStore driverStore;

  private DriverKey driverKey;

  private CoordinatorClientFactory coordinatorClientFactory;

  private DriverContainerConfiguration dcConfig;


  @Override
  public void run() {
    DriverMetadata driver = driverStore.get(driverKey);
    logger.info("i am work");
  }

  public DriverContainerConfiguration getDcConfig() {
    return dcConfig;
  }

  public void setDcConfig(DriverContainerConfiguration dcConfig) {
    this.dcConfig = dcConfig;
  }

  public CoordinatorClientFactory getCoordinatorClientFactory() {
    return coordinatorClientFactory;
  }

  public void setCoordinatorClientFactory(CoordinatorClientFactory coordinatorClientFactory) {
    this.coordinatorClientFactory = coordinatorClientFactory;
  }

  public DriverKey getDriverKey() {
    return driverKey;
  }

  public void setDriverKey(DriverKey driverKey) {
    this.driverKey = driverKey;
  }

  public DriverStore getDriverStore() {
    return driverStore;
  }

  public void setDriverStore(DriverStore driverStore) {
    this.driverStore = driverStore;
  }
}
