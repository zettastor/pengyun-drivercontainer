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

package py.drivercontainer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import py.drivercontainer.service.DriverContainerImpl;
import py.drivercontainer.worker.ServerScanWorker;
import py.drivercontainer.worker.TargetScanWorker;

/**
 * launcher for testing purpose.
 *
 */
public class Launcher extends py.app.Launcher {

  private static final Logger logger = LoggerFactory.getLogger(Launcher.class);

  public Launcher(String beansHolder, String serviceRunningPath) {
    super(beansHolder, serviceRunningPath);
  }


  /**
   * xx.
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      String usage = String
          .format("Usage: %n\t%s beans-holder service-running-path", Launcher.class.getName());
      System.out.println(usage);
      System.exit(1);
    }

    Launcher launcher = new Launcher(DriverContainerAppBeans.class.getName() + ".class", args[0]);
    launcher.launch();
  }

  @Override
  public void startAppEngine(ApplicationContext appContext) {
    try {
      final DriverContainerAppEngine engine = appContext.getBean(DriverContainerAppEngine.class);
      DriverContainerImpl dcImpl = appContext.getBean(DriverContainerImpl.class);
      ServerScanWorker serverScanWorker = new ServerScanWorker();
      TargetScanWorker targetScanWorker = new TargetScanWorker();
      logger.info("Driver Container App Engine  Max Network Frame Size is: {}",
          engine.getMaxNetworkFrameSize());
      engine.start();
    } catch (Exception e) {
      logger.error("Caught an exception when start dih service", e);
      System.exit(1);
    }
  }

}
