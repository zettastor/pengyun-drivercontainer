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

package py.drivercontainer.driver.store;

import java.io.IOException;
import py.driver.DriverMetadata;
import py.drivercontainer.driver.store.exception.DuplicatedDriverException;
import py.icshare.DriverKey;

public interface DriverFileStore extends DriverStore {

  /**
   * Add a completely new driver to the store. Any {@link IOException} occurred when persisting
   * driver in this method will be thrown out immediately.
   *
   * @param driver a completely new driver
   * @throws IOException               if something wrong happened to persist the given driver.
   * @throws DuplicatedDriverException if the store already exists the driver with same key as the
   *                                   given one.
   */
  public void addOrFailImmediately(DriverMetadata driver)
      throws IOException, DuplicatedDriverException;

  /**
   * Save the given driver to the store and block until successfully persisting driver.
   *
   * @param driver a completely new driver or driver with new features
   * @throws DuplicatedDriverException if the store already exists the driver with same key as the
   *                                   given one.
   */
  public void saveOrBlockOnFailure(DriverMetadata driver) throws DuplicatedDriverException;

  /**
   * Delete the driver with the given key and block until successfully applying modification to
   * disk.
   *
   * @param driverKey key of target driver
   */
  public void deleteOrBlockOnFailure(DriverKey driverKey);

  /**
   * xx.
   */
  public boolean load(Long driverContainerId);

  public boolean load(DriverKey targetInfo);

  /**
   * xx.
   */
  public boolean flush();

  public boolean flush(DriverKey targetInfo);
}
