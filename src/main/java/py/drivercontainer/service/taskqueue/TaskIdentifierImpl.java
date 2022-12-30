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

package py.drivercontainer.service.taskqueue;

import py.driver.DriverAction;
import py.drivercontainer.driver.version.Version;
import py.icshare.DriverKey;

public class TaskIdentifierImpl implements TaskIdentifier {

  private final DriverKey driverKey;

  private final DriverAction action;

  private final Version version;


  /**
   * xx.
   */
  public TaskIdentifierImpl(DriverKey driverKey, DriverAction action, Version version) {
    super();
    this.driverKey = driverKey;
    this.action = action;
    this.version = version;
  }

  @Override
  public DriverKey getDriverKey() {
    return driverKey;
  }

  @Override
  public DriverAction getAction() {
    return action;
  }

  @Override
  public Version getVersion() {
    return version;
  }

  @Override
  public int hashCode() {
    int result = driverKey != null ? driverKey.hashCode() : 0;
    result = 31 * result + (action != null ? action.hashCode() : 0);
    result = 31 * result + (version != null ? version.hashCode() : 0);
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TaskIdentifierImpl that = (TaskIdentifierImpl) o;

    if (driverKey != null ? !driverKey.equals(that.driverKey) : that.driverKey != null) {
      return false;
    }
    if (action != that.action) {
      return false;
    }
    return version != null ? version.equals(that.version) : that.version == null;
  }

  @Override
  public String toString() {
    return "TaskIdentifierImpl [driverKey=" + driverKey + ", action=" + action + ", version="
        + version + "]";
  }

}
