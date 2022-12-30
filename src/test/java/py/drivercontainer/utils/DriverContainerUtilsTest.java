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

package py.drivercontainer.utils;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.LinkedBlockingQueue;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import py.app.context.AppContext;
import py.app.thrift.ThriftAppEngine;
import py.client.thrift.GenericThriftClientFactory;
import py.common.struct.EndPoint;
import py.driver.DriverType;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.JvmConfigurationForDriver;
import py.drivercontainer.exception.NoAvailablePortException;
import py.drivercontainer.service.PortContainerImpl;
import py.monitor.jmx.server.SigarJniLibraryCopier;
import py.test.DummyTestServiceConfig;
import py.test.DummyTestServiceEngine;
import py.test.TestBase;
import py.thrift.testing.service.DummyTestService;

public class DriverContainerUtilsTest extends TestBase {

  private JvmConfigurationForDriver jvmConfig;
  private DriverContainerConfiguration driverContainerConfig;

  @Before
  public void init() throws Exception {
    super.init();
    SigarJniLibraryCopier.getInstance();
  }

  @Test
  public void testIsPortAvailable() throws Exception {
    int availablePort = 0;
    int busyPort = 0;
    int count = 0;
    for (int i = 10000; i < 10100; i++) {
      if (DriverContainerUtils.isPortAvailable(InetAddress.getLocalHost().getHostAddress(), i)) {
        switch (count) {
          case 0:
            availablePort = i;
            count++;
            break;
          case 1:
            busyPort = i;
            count++;
            break;
          default:
        }
      }

      if (count == 2) {
        break;
      }
    }

    System.out.printf("availablePort:%d,busyPort:%d,count:%d\n", availablePort, busyPort, count);

    final int busyPort_Another = busyPort;
    Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
          serverSocketChannel.configureBlocking(true);
          serverSocketChannel.socket().bind(new InetSocketAddress(busyPort_Another));
          serverSocketChannel.accept();
        } catch (Exception e) {
          logger.error("caught exception", e);
        }
      }
    };

    thread.start();
    int tryAmount = 5;
    boolean available = true;
    for (int i = 0; i < tryAmount; i++) {
      if (!DriverContainerUtils
          .isPortAvailable(InetAddress.getLocalHost().getHostAddress(), busyPort_Another)) {
        available = false;
        break;
      }
      Thread.sleep(1000);
    }
    Assert.assertEquals(false, available);
    Assert.assertEquals(true, DriverContainerUtils
        .isPortAvailable(InetAddress.getLocalHost().getHostAddress(), availablePort));
  }

  @Test
  public void testPortQueueAction() throws Exception {
    driverContainerConfig = new DriverContainerConfiguration();
    jvmConfig = new JvmConfigurationForDriver();
    int port1 = 11134;
    final int port2 = 11135;
    final int port3 = 11136;
    int getPort = 0;
    int mod = 0;
    LinkedBlockingQueue<Integer> portQueue = new LinkedBlockingQueue<Integer>();
    AppContext appContext = mock(AppContext.class);
    when(appContext.getMainEndPoint())
        .thenReturn(new EndPoint(InetAddress.getLocalHost().getHostAddress(), port1));

    PortContainerImpl portContainer = new PortContainerImpl(appContext, DriverType.ISCSI.name(),
        jvmConfig, driverContainerConfig);

    portContainer.setPortList(portQueue);
    // Inserts the specified port at the tail of the port queue
    portContainer.addAvailablePort(port1);
    portContainer.addAvailablePort(port2);
    portContainer.addAvailablePort(port3);
    // if port has exist,make sure will not add it again
    portContainer.addAvailablePort(port1);
    Assert.assertEquals(3, portContainer.getPortList().size());
    int nsize = 3;
    for (int i = 0; i < 5; i++) {
      // if port queue has no port,should return 0
      if (i > 2) {
        boolean exceptionCatched = false;
        try {
          getPort = portContainer.getAvailablePort();
        } catch (NoAvailablePortException e) {
          exceptionCatched = true;
        }
        Assert.assertTrue(exceptionCatched);
        continue;
      }
      // check port value [1234, 1236]
      try {
        getPort = portContainer.getAvailablePort();
      } catch (NoAvailablePortException e) {
        fail(e.getMessage());
      }
      Assert.assertEquals(port1 + i, getPort);
      nsize--;
      // check the size of the port queue
      Assert.assertEquals(nsize, portContainer.getPortList().size());
    }
    portContainer.addAvailablePort(port1);
    portContainer.addAvailablePort(port3);
    portContainer.addAvailablePort(port2);
    Assert.assertEquals(3, portContainer.getPortList().size());
    for (int k = 0; k < 10; k++) {
      mod = k % 2;
      portContainer.removeUnavailablePort(port2 + mod);
      portContainer.removeUnavailablePort(port2 - mod);
    }
    Assert.assertEquals(0, portContainer.getPortList().size());
  }

  @Test
  public void testGetSysMemInfo() {
    long usedMem = DriverContainerUtils.getSysFreeMem();
    long totalMem = DriverContainerUtils.getSysTotalMem();

    System.out.printf("used mem %d,total mem %d\n", usedMem, totalMem);
  }

  @Test
  public void testGetBytesSizeFromString() {
    String stringSize = "10M";
    long size = DriverContainerUtils.getBytesSizeFromString(stringSize);
    long expectSize = 10 * 1024 * 1024;

    Assert.assertEquals(expectSize, size);

    stringSize = "10G";
    size = DriverContainerUtils.getBytesSizeFromString(stringSize);
    expectSize = 10 * 1024 * 1024 * 1024L;

    Assert.assertEquals(expectSize, size);
  }

  @Test
  public void testPing() {
    DummyTestServiceConfig cfg = new DummyTestServiceConfig();
    cfg.setReceiveMaxFromeSize(1024);
    cfg.setMaxNumThreads(1);
    cfg.setBlocking(false);
    cfg.setNumWorkerThreads(1);
    cfg.setServicePort(6745);

    ThriftAppEngine engine = null;
    try {
      engine = DummyTestServiceEngine.startEngine(cfg);

      EndPoint eps = new EndPoint(InetAddress.getLocalHost().getHostName(), cfg.getServicePort());
      GenericThriftClientFactory<DummyTestService.Iface> genericThriftClientFactory =
          GenericThriftClientFactory.create(DummyTestService.Iface.class, 1);
      try {
        DummyTestService.Iface client = genericThriftClientFactory.generateSyncClient(eps);
        client.pingforcoodinator();
        genericThriftClientFactory.close();

        System.out.print("success ping\n");
      } catch (Exception e) {
        logger.warn("caught an exception when ping {},{}", eps, e);
      }

    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      try {
        DummyTestServiceEngine.stop(engine);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
}
