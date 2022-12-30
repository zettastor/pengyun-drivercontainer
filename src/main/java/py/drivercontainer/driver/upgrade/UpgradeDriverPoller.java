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

package py.drivercontainer.driver.upgrade;

import java.util.Collection;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.drivercontainer.driver.version.VersionManager;

/**
 * An interface to control limit number of drivers to be upgraded.
 *
 */
public interface UpgradeDriverPoller {

  /**
   * Limit is the max number of drivers being upgrading simultaneously. Let's suppose the limit
   * value is 5, and in this case, it is allowed to upgrade 5 driver of the given type
   * simultaneously. After one of upgrading drivers having been upgraded, it is then allowed another
   * one driver to be upgraded.
   *
   * <p>And if the limit value is 0, then it is supposed that there is no limit for driver poller.
   *
   * <p>This method gets the limit value of driver poller.
   *
   * @return the limit of driver poller.
   */
  public int limit(DriverType type);

  /**
   * Check if there is more driver with the given type and the given version in the poller.
   *
   * @return true if there is more driver in poller, or false if there is not.
   */
  public boolean hasMore(DriverType type);

  /**
   * Poll driver with the given type and the given version to be upgraded.
   *
   * <p>If number of drivers being upgraded equals to the limit of poller, then null value will be
   * returned.
   *
   * <p>If some drivers are in status with 'ING', e.g {@link DriverStatus#LAUNCHING}, {@link
   * DriverStatus#REMOVING}, then the poller will return null until those drivers changing to final
   * status, e.g {@link DriverStatus#LAUNCHED}, {@link DriverStatus#ERROR}.
   *
   * @return driver with proper status, or null in following case:
   *          <li>number of drivers being upgraded equals to the limit</li>
   *          <li>no more driver in poller</li>
   *          <li>remains only drivers with 'ING' status</li>
   */
  public DriverMetadata poll(DriverType type);

  /**
   * Add at most the limit number of drivers with proper status into to the given collection.
   *
   * @param drivers collection for drivers
   */
  public void drainTo(DriverType type, Collection<DriverMetadata> drivers);

  /**
   * Get instance of {@link VersionManager} in poller.
   *
   * @return instance of {@link VersionManager} in poller.
   */
  public VersionManager getVersionManager();

  /**
   * Get instance of {@link DriverStoreManager} in poller.
   *
   * @return instance of {@link DriverStoreManager} in poller.
   */
  public DriverStoreManager getDriverStoreManager();
}
