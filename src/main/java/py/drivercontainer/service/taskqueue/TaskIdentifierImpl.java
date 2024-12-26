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
