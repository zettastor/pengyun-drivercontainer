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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import py.driver.DriverMetadata;
import py.driver.DriverType;
import py.drivercontainer.driver.version.Version;
import py.icshare.DriverKey;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * A class implements interface {@link DriverStore} as memory store.
 *
 */
public class DriverMemoryStoreImpl implements DriverStore {

  private Map<DriverKey, DriverMetadata> driverTable = new ConcurrentHashMap<>();

  @Override
  public DriverMetadata get(DriverKey targetInfo) {
    return driverTable.get(targetInfo);
  }

  @Override
  public boolean save(DriverMetadata driver) {
    driverTable.put(
        new DriverKey(driver.getDriverContainerId(), driver.getVolumeId(), driver.getSnapshotId(),
            driver.getDriverType()), driver);
    return true;
  }

  @Override
  public synchronized List<DriverMetadata> list() {
    List<DriverMetadata> driverList = new ArrayList<DriverMetadata>();
    for (Map.Entry<DriverKey, DriverMetadata> entry : driverTable.entrySet()) {
      driverList.add(entry.getValue());
    }
    return driverList;
  }

  @Override
  public List<DriverMetadata> list(DriverType type) {
    List<DriverMetadata> drivers;

    drivers = new ArrayList<>();
    for (Map.Entry<DriverKey, DriverMetadata> entry : driverTable.entrySet()) {
      if (entry.getKey().getDriverType() == type) {
        drivers.add(entry.getValue());
      }
    }
    return drivers;
  }

  @Override
  public boolean remove(DriverKey targetInfo) {
    driverTable.remove(targetInfo);
    return true;
  }

  public void clearMomory() {
    driverTable.clear();
  }

  @Override
  public Version getVersion() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean saveAcl(DriverMetadata driver) {
    throw new NotImplementedException();
  }
}
