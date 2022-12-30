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

import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.driver.DriverType;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.JvmConfiguration;
import py.drivercontainer.exception.NoAvailablePortException;
import py.drivercontainer.utils.DriverContainerUtils;

public class PortContainerImpl implements PortContainer {

  private static final Logger logger = LoggerFactory.getLogger(PortContainerImpl.class);

  private DriverType driverType;

  private LinkedBlockingQueue<Integer> portQueue;

  private AppContext appContext;
  private JvmConfiguration driverJvmConfig;
  private DriverContainerConfiguration driverContainerConfig;


  /**
   * xx.
   */
  public PortContainerImpl(AppContext appContext, String driverType,
      JvmConfiguration driverJvmConfig, DriverContainerConfiguration driverContainerConfig) {
    this.appContext = appContext;
    this.driverType = DriverType.valueOf(driverType);
    this.driverJvmConfig = driverJvmConfig;
    this.driverContainerConfig = driverContainerConfig;
  }

  /*
   * get available port if bind port successfully
   *
   * @see py.drivercontainer.service.PortContainer#getAvailablePort()
   */
  @Override
  public synchronized int getAvailablePort() throws NoAvailablePortException {
    String hostname = appContext.getMainEndPoint().getHostName();
    // get an available port from queue
    for (int i = 0; i < portQueue.size(); i++) {
      int availablePort = portQueue.poll();
      //Only when CoordinatorPort and JmxPort are available ,then return availablePort
      if (DriverContainerUtils.isPortAvailable(hostname, availablePort)) {
        if (driverType == DriverType.FSD && DriverContainerUtils
            .isPortAvailable(hostname, getAvailableJmxPort(availablePort))) {
          return availablePort;
        } else if (DriverContainerUtils
            .isPortAvailable(hostname, getAvailableCoordinatorPort(availablePort))
            && DriverContainerUtils.isPortAvailable(hostname, getAvailableJmxPort(availablePort))) {
          return availablePort;
        }
      } else {
        try {
          portQueue.put(availablePort);
        } catch (InterruptedException e) {
          logger.error("can not put port into port queue", e);
        }
      }
    }

    // after loop all port, there is no available port
    throw new NoAvailablePortException();
  }


  public int getAvailableCoordinatorPort(int port) {
    return (port + driverContainerConfig.getCoordinatorBasePort());
  }

  @Override
  public int getAvailableJmxPort(int port) {
    return (port + driverJvmConfig.getJmxBasePort());
  }

  @Override
  public void addAvailablePort(int port) {
    try {
      // if port already exist, no need add it again
      if (portQueue.contains(port)) {
        return;
      }
      portQueue.put(port);
    } catch (InterruptedException e) {
      logger.error("can not put port into port queue", e);
    }
  }

  public DriverType getDriverType() {
    return driverType;
  }

  public void setDriverType(DriverType driverType) {
    this.driverType = driverType;
  }

  public LinkedBlockingQueue<Integer> getPortList() {
    return portQueue;
  }

  public void setPortList(LinkedBlockingQueue<Integer> portList) {
    this.portQueue = portList;
  }

  @Override
  public void removeUnavailablePort(int port) {
    portQueue.remove(port);
  }

  @Override
  public boolean containes(int port) {
    return portQueue.contains(port);
  }

}
