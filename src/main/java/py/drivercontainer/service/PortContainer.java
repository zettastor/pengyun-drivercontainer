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

import py.drivercontainer.exception.NoAvailablePortException;

public interface PortContainer {

  public int getAvailablePort() throws NoAvailablePortException;

  public int getAvailableCoordinatorPort(int port) throws NoAvailablePortException;

  public int getAvailableJmxPort(int port) throws NoAvailablePortException;

  public void addAvailablePort(int port);

  public void removeUnavailablePort(int port);

  public boolean containes(int port);
}
