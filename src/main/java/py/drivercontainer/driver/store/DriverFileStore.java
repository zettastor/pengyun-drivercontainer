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
