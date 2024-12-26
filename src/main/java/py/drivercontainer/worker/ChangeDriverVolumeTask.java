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
