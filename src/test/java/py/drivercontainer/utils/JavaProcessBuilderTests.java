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

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import py.common.Utils;
import py.driver.DriverMetadata;
import py.driver.DriverType;
import py.drivercontainer.JvmConfigurationForDriver;
import py.drivercontainer.driver.LaunchDriverParameters;
import py.test.TestBase;

public class JavaProcessBuilderTests extends TestBase {

  private static final int LISTENER_PORT = 3261;

  private JvmConfigurationForDriver jvmConfig = new JvmConfigurationForDriver();

  @Override
  @Before
  public void init() throws Exception {
    super.init();
  }

  @Test
  public void testHelloWorldServer() throws Exception {
    final AtomicInteger port = new AtomicInteger(LISTENER_PORT);
    while (!Utils.isPortAvailable(port.get())) {
      port.incrementAndGet();
    }

    jvmConfig.setMainClass("py.drivercontainer.utils.HelloWorldServer");
    final JavaProcessBuilder javaProcessBuilder = new JavaProcessBuilder(jvmConfig) {
      @Override
      Properties loadCoordinatorMetricProps(String workingDir) throws IOException {
        Properties props;

        props = new Properties();
        props.put("metric.enable", "true");
        return props;
      }
    };

    javaProcessBuilder.addArgument(String.valueOf(port));
    final Process process = javaProcessBuilder.startProcess();
    BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));

    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          while (Utils.isPortAvailable(port.get())) {
            Thread.sleep(1000);
          }
          @SuppressWarnings("resource")
          Socket socket = new Socket("localhost", port.get());
          PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
          BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
          System.out.println("CLIENT: Sending 'hello' command.");
          out.println("hello");
          System.out.println("CLIENT: Received '" + in.readLine() + "' from the server.");
          Thread.sleep(2000);
          System.out.println("CLIENT: Sending 'stop' command.");
          out.println("stop");
          System.out.println("CLIENT: Received '" + in.readLine() + "' from the server.");
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    t.start();

    String line;
    while ((line = br.readLine()) != null) {
      System.out.println("SERVER: " + line);
    }
  }

  @Test
  public void startReturnSystemExit() throws Exception {
    jvmConfig.setMainClass("py.drivercontainer.utils.ReturnSystemExit");
    final JavaProcessBuilder javaProcessBuilder = new JavaProcessBuilder(jvmConfig) {
      @Override
      Properties loadCoordinatorMetricProps(String workingDir) throws IOException {
        Properties props;

        props = new Properties();
        props.put("metric.enable", "true");
        return props;
      }
    };
    javaProcessBuilder.addArgument("1");
    final Process process = javaProcessBuilder.startProcess();
    BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
    int readResult = br.read();
    assertEquals(1 + '0', readResult);
  }

  @Test
  public void isPortAvailable() {
    try {
      int port = 1234;
      // bindPort("0.0.0.0", 9000);
      Socket s = null;
      try {
        s = new Socket();
        s.bind(new InetSocketAddress("0.0.0.0", port));
      } finally {
        if (s != null) {
          s.close();
        }
      }

      // bindPort(InetAddress.getLocalHost().getHostAddress(), 9000);
      s = null;
      try {
        s = new Socket();
        s.bind(new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), port));
        System.out.println("host name is  " + InetAddress.getLocalHost().getHostAddress());
      } finally {
        if (s != null) {
          s.close();
        }
      }

    } catch (Throwable t) {
      System.out.println("port is most likely bound");

    }
  }

  @Test
  public void isProcessExist() {
    int pid = 2371;
    boolean b = DriverContainerUtils.processExist(pid);
    if (b) {
      System.out.println("pid is exist");
    } else {
      System.out.println("pid is not exist");
    }
  }

  @Test
  public void killProcess() {
    int pid = 2181;
    try {
      DriverContainerUtils.killProcessByPid(pid);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Test
  public void pidFileTest() throws Exception {
    Long volumeId = 100010301310L;
    String strHostName = "localhost";
    String strPort = "1234";
    LaunchDriverParameters launchDriverParameters = new LaunchDriverParameters();
    launchDriverParameters.setVolumeId(volumeId);
    launchDriverParameters.setDriverName("driver");
    launchDriverParameters.setHostName(strHostName);
    launchDriverParameters.setPort(Integer.valueOf(strPort));
    launchDriverParameters.setDriverType(DriverType.NBD);
    DriverMetadata driver = launchDriverParameters.buildDriver();
    driver.saveToFile(Paths.get("/tmp/", Long.toString(volumeId)));

    DriverMetadata driverFromFile = DriverMetadata
        .buildFromFile(Paths.get("/tmp/", Long.toString(volumeId)));
    Assert.assertTrue(driverFromFile.getProcessId() == driver.getProcessId());
  }

  @Test
  public void systemNanoTimeTest() throws InterruptedException {
    long nanoStartTime = System.nanoTime();
    Thread.sleep(1000, 100000);
    long endTime = System.nanoTime() - nanoStartTime;
    System.out.println("sleep time " + endTime);
  }
}
