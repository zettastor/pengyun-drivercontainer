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
