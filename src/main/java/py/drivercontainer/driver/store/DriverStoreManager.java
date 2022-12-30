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

import java.util.Map;
import py.drivercontainer.driver.version.Version;

/**
 * One type of driver with different version cannot be in same driver store and two types of driver
 * with same driver version could be in same driver store. That is each version of driver has its
 * own driver store.
 *
 * <p>This class is a table mapping version to driver store.
 *
 */
public interface DriverStoreManager extends Map<Version, DriverStore> {

}
