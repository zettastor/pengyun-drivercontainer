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

package py.drivercontainer.worker;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.LinkedBlockingQueue;
import junit.framework.Assert;
import org.apache.log4j.Level;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.common.struct.EndPoint;
import py.driver.DriverType;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.JvmConfigurationForDriver;
import py.drivercontainer.exception.NoAvailablePortException;
import py.drivercontainer.service.PortContainerImpl;
import py.drivercontainer.utils.DriverContainerUtils;
import py.test.TestBase;

public class GetAvailablePortTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(GetAvailablePortTest.class);
  private PortContainerImpl portContainer;
  private JvmConfigurationForDriver jvmConfig;
  private DriverContainerConfiguration driverContainerConfig;

  public void init() {
    super.setLogLevel(Level.ALL);
  }

  @Test
  public void getAvailablePortTestForIscsi()
      throws UnknownHostException, InterruptedException, NoAvailablePortException {
    PortContainerImpl portContainerIscsi = getportContainer(DriverType.ISCSI);
    Socket s = null;
    try {
      s = new Socket();
      s.bind(new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), 2234));
      boolean available = true;
      if (!DriverContainerUtils
          .isPortAvailable(InetAddress.getLocalHost().getHostAddress(), 2234)) {
        available = false;
      }
      Assert.assertEquals(false, available);
      int port = portContainerIscsi.getAvailablePort();
      Assert.assertEquals(1235, port);
    } catch (Throwable e) {
      logger.error("caught exception", e);
    } finally {
      if (s != null) {
        try {
          s.close();
        } catch (IOException e) {
          logger.warn("Caught an exception when close socket on {}", 2234);
        }
      }
    }
  }

  @Test
  public void getAvailablePortTestForFsd()
      throws UnknownHostException, InterruptedException, NoAvailablePortException {
    PortContainerImpl portContainerFsd = getportContainer(DriverType.FSD);
    Socket s = null;
    try {
      s = new Socket();
      s.bind(new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), 51234));
      boolean available = true;
      if (!DriverContainerUtils
          .isPortAvailable(InetAddress.getLocalHost().getHostAddress(), 51234)) {
        available = false;
      }
      Assert.assertEquals(false, available);

      int port = portContainerFsd.getAvailablePort();
      Assert.assertEquals(1235, port);
    } catch (Throwable e) {
      logger.error("caught exception", e);
    } finally {
      if (s != null) {
        try {
          s.close();
        } catch (IOException e) {
          logger.warn("Caught an exception when close socket on {}", 51234);
        }
      }
    }
  }


  /**
   * xx.
   */
  public PortContainerImpl getportContainer(DriverType driverType) throws UnknownHostException {
    final int port1 = 1234;
    final int port2 = 1235;
    final int port3 = 1236;
    final int port4 = 1237;
    final LinkedBlockingQueue<Integer> portQueue = new LinkedBlockingQueue<Integer>();
    AppContext appContext = mock(AppContext.class);
    when(appContext.getMainEndPoint())
        .thenReturn(new EndPoint(InetAddress.getLocalHost().getHostAddress(), 1234));
    jvmConfig = new JvmConfigurationForDriver();
    jvmConfig.setJmxBasePort(50000);
    driverContainerConfig = new DriverContainerConfiguration();
    driverContainerConfig.setCoordinatorBasePort(1000);
    portContainer = new PortContainerImpl(appContext, driverType.name(), jvmConfig,
        driverContainerConfig);
    portContainer.setPortList(portQueue);
    portContainer.addAvailablePort(port1);
    portContainer.addAvailablePort(port2);
    portContainer.addAvailablePort(port3);
    portContainer.addAvailablePort(port4);
    return portContainer;

  }


  /**
   * xx.
   */
  public void occupyPort(int port) {
    logger.warn("going to occupy port:{}", port);
    Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
          serverSocketChannel.configureBlocking(true);
          serverSocketChannel.socket().bind(new InetSocketAddress(port));
          serverSocketChannel.accept();
        } catch (Exception e) {
          logger.warn("***catch an exception ", e);
        }
      }
    };
    thread.start();
  }

}
