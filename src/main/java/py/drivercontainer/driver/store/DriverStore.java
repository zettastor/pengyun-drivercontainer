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

import java.util.List;
import py.driver.DriverMetadata;
import py.driver.DriverType;
import py.drivercontainer.driver.version.Version;
import py.icshare.DriverKey;

/**
 * This class provides an entry to access all drivers having some specified version.
 *
 */
public interface DriverStore {

  /**
   * Every driver store has its unique version. And each driver in this store is under that
   * version.
   *
   * @return version of this driver store
   */
  public Version getVersion();

  /**
   * Get driver from driver store.
   *
   * @param key unique key to each driver
   * @return null if driver with the given key does not exist; or meta-data of the driver with the
   *          given key.
   */
  public DriverMetadata get(DriverKey key);

  /**
   * Save driver to driver store.
   *
   * @param driver meta-data to be saved to the store
   * @return true if there is no such driver in store before, or false
   */
  public boolean save(DriverMetadata driver);

  /**
   * List all drivers existing in driver store.
   *
   * @return all drivers existing in driver store.
   */
  public List<DriverMetadata> list();

  /**
   * List all drivers with the given type in this store.
   *
   * @return all drivers with the given type.
   */
  public List<DriverMetadata> list(DriverType type);

  /**
   * Remove driver with specified volume id as key.
   *
   * @param targetInfo unique key to each driver
   * @return true if exists a driver with the given key or false
   */
  public boolean remove(DriverKey targetInfo);

  public void clearMomory();

  /**
   * Save Acl to file.
   */
  public boolean saveAcl(DriverMetadata driver);
}