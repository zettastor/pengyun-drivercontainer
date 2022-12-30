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

package py.drivercontainer.service;

import java.util.HashMap;
import java.util.Map;
import py.driver.DriverType;

public class PortContainerFactory {

  private Map<DriverType, PortContainer> driverType2PortContainer = new HashMap<>();

  public PortContainerFactory(Map<DriverType, PortContainer> driverType2PortContainer) {
    this.driverType2PortContainer = driverType2PortContainer;
  }


  /**
   * xx.
   */
  public PortContainer getPortContainer(DriverType driverType) {
    switch (driverType) {
      case ISCSI:
        return driverType2PortContainer.get(DriverType.NBD);
      case JSCSI:
        return driverType2PortContainer.get(DriverType.JSCSI);
      case NBD:
        return driverType2PortContainer.get(DriverType.NBD);
      case FSD:
        return driverType2PortContainer.get(DriverType.FSD);
      default:
        return null;
    }
  }
}
