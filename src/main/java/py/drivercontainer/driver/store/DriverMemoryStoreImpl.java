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
