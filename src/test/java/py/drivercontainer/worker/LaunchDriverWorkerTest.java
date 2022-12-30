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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import junit.framework.Assert;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import py.common.RequestIdBuilder;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.JvmConfigurationForDriver;
import py.drivercontainer.driver.LaunchDriverParameters;
import py.drivercontainer.driver.store.DriverStore;
import py.drivercontainer.driver.store.DriverStoreImpl;
import py.drivercontainer.driver.version.Version;
import py.drivercontainer.driver.version.file.VersionImpl;
import py.drivercontainer.service.PortContainer;
import py.drivercontainer.service.PortContainerFactory;
import py.drivercontainer.utils.IscsiProcessor;
import py.icshare.DriverKey;
import py.iet.file.mapper.InitiatorsAllowFileMapper;
import py.infocenter.client.InformationCenterClientFactory;
import py.test.TestBase;

/**
 * A class including some tests for {@link LaunchDriverWorker}.
 */
public class LaunchDriverWorkerTest extends TestBase {

  public static final String FILE_SEPARATOR = File.separator;
  ExecutorService driverExecutor;
  @Mock
  private InformationCenterClientFactory informationCenterClientFactory;
  @Mock
  private InitiatorsAllowFileMapper initiatorsAllowFileMapper;
  private LaunchDriverParameters launchDriverParameters;
  private DriverStore driverStore;
  @Mock
  private PortContainerFactory portContainerFactory;
  @Mock
  private PortContainer portContainer;
  @Mock
  private JvmConfigurationForDriver jvmConfig;
  @Mock
  private DriverContainerConfiguration driverContainerConfig;
  private long timeout;
  private LaunchDriverWorker launchDriverWorker;

  @Override
  @Before
  public void init() throws Exception {
    super.init();
    super.setLogLevel(Level.ALL);
    driverExecutor = Executors.newFixedThreadPool(10);
    Version version = VersionImpl.get("2.3.0-internal-20170918000011");
    driverStore = new DriverStoreImpl(Paths.get("/tmp", "SPid_coordinator"), version);
  }

  @Test
  public void launchNbd() throws Exception {
    launchDriverParameters = buildDriverConfig(DriverType.NBD);
    timeout = 5000;
    when(portContainerFactory.getPortContainer(any(DriverType.class))).thenReturn(portContainer);

    launchDriverWorker = new LaunchDriverWorker(launchDriverParameters, jvmConfig,
        driverContainerConfig);
    launchDriverWorker.setDriverStore(driverStore);
    launchDriverWorker.setPortContainerFactory(portContainerFactory);
    launchDriverWorker.init();
    driverStore.save(launchDriverParameters.buildDriver());

    // check driver status after initialize the worker
    DriverKey driverKey = new DriverKey(launchDriverParameters.getDriverContainerInstanceId(),
        launchDriverParameters.getVolumeId(), launchDriverParameters.getSnapshotId(),
        launchDriverParameters.getDriverType());
    DriverMetadata driver = driverStore.get(driverKey);
    Assert.assertEquals(DriverStatus.LAUNCHING, driver.getDriverStatus());

    // check driver status after timeout to launch driver
    launchDriverWorker.doWork();
    driver = driverStore.get(driverKey);
    Assert.assertEquals(DriverStatus.ERROR, driver.getDriverStatus());
  }

  @Test
  public void launchFsd() throws Exception {
    launchDriverParameters = buildDriverConfig(DriverType.FSD);
    timeout = 5000;

    when(portContainerFactory.getPortContainer(any(DriverType.class))).thenReturn(portContainer);

    launchDriverWorker = new LaunchDriverWorker(launchDriverParameters, jvmConfig,
        driverContainerConfig);
    launchDriverWorker.setDriverStore(driverStore);
    launchDriverWorker.setPortContainerFactory(portContainerFactory);
    launchDriverWorker.init();
    driverStore.save(launchDriverParameters.buildDriver());

    // check driver status after initialize the worker
    DriverKey driverKey = new DriverKey(launchDriverParameters.getDriverContainerInstanceId(),
        launchDriverParameters.getVolumeId(), launchDriverParameters.getSnapshotId(),
        launchDriverParameters.getDriverType());
    DriverMetadata driver = driverStore.get(driverKey);
    Assert.assertEquals(DriverStatus.LAUNCHING, driver.getDriverStatus());

    // check driver status after timeout to launch driver
    launchDriverWorker.doWork();
    driver = driverStore.get(driverKey);
    Assert.assertEquals(DriverStatus.ERROR, driver.getDriverStatus());
  }

  // @Test

  /**
   * xx.
   */
  public void launchDriverWorkBlocked() throws Exception {
    launchDriverParameters = buildDriverConfig(DriverType.ISCSI);
    timeout = 5000;
    when(portContainerFactory.getPortContainer(any(DriverType.class))).thenReturn(portContainer);

    IscsiProcessorT iscsiProcessorT = new IscsiProcessorT();
    iscsiProcessorT.setDriverStore(driverStore);
    iscsiProcessorT.setLaunchDriverParameters(launchDriverParameters);

    logger.debug("start worker1");
    LaunchDriverWorkerT launchDriverWorkerT1 = null;
    launchDriverWorkerT1 = new LaunchDriverWorkerT(launchDriverParameters, jvmConfig,
        driverContainerConfig,
        timeout);
    launchDriverWorkerT1.setDriverStore(driverStore);
    launchDriverWorkerT1.setPortContainerFactory(portContainerFactory);
    launchDriverWorkerT1.setIscsiProcessor(iscsiProcessorT);
    launchDriverWorkerT1.init();
    driverStore.save(launchDriverParameters.buildDriver());
    launchDriverWorkerT1.setCreateIscsiTargetNameFlag(true);
    launchDriverWorkerT1.setCoordinatorIsAlive(false);
    driverExecutor.execute(launchDriverWorkerT1);

    Thread.sleep(10000);

    logger.debug("start worker2");
    LaunchDriverWorkerT launchDriverWorkerT2 = null;
    launchDriverWorkerT2 = new LaunchDriverWorkerT(launchDriverParameters, jvmConfig,
        driverContainerConfig,
        timeout);
    launchDriverWorkerT2.setDriverStore(driverStore);
    launchDriverWorkerT2.setPortContainerFactory(portContainerFactory);
    launchDriverWorkerT2.setIscsiProcessor(iscsiProcessorT);
    launchDriverWorkerT2.init();
    launchDriverWorkerT2.setCreateIscsiTargetNameFlag(true);
    launchDriverWorkerT1.setCoordinatorIsAlive(false);
    driverExecutor.execute(launchDriverWorkerT2);

    Thread.sleep(5000);

    DriverKey driverKey = new DriverKey(launchDriverParameters.getDriverContainerInstanceId(),
        launchDriverParameters.getVolumeId(), launchDriverParameters.getSnapshotId(),
        launchDriverParameters.getDriverType());
    DriverMetadata driver = driverStore.get(driverKey);
    Assert.assertEquals(DriverStatus.LAUNCHED, driver.getDriverStatus());
  }

  private LaunchDriverParameters buildDriverConfig(DriverType type) {
    launchDriverParameters = new LaunchDriverParameters();
    launchDriverParameters.setDriverContainerInstanceId(RequestIdBuilder.get());
    launchDriverParameters.setAccountId(RequestIdBuilder.get());
    launchDriverParameters.setVolumeId(RequestIdBuilder.get());
    launchDriverParameters.setDriverType(type);
    launchDriverParameters.setHostName("10.0.1.1");
    launchDriverParameters.setPort(0);
    launchDriverParameters.setDriverName("driver");

    return launchDriverParameters;
  }


  /**
   * xx.
   */
  @After
  public void clearVar() throws IOException {
    File dir = new File(".");
    String varPath = null;
    varPath = dir.getCanonicalPath();
    varPath += (FILE_SEPARATOR + "var");
    String command = String.format("rm -rf %s ", varPath);
    Process process;
    try {
      process = Runtime.getRuntime().exec(command);
      process.waitFor();
    } catch (IOException e) {
      logger.error("caught exception", e);
    } catch (InterruptedException e) {
      logger.error("caught exception", e);
    }

  }

  public class IscsiProcessorT extends IscsiProcessor {

    private DriverStore driverStore;

    @Override
    public String createIscsiTarget(String pydDevice, long volId) {
      DriverKey driverKey = new DriverKey(launchDriverParameters.getDriverContainerInstanceId(),
          launchDriverParameters.getVolumeId(), launchDriverParameters.getSnapshotId(),
          launchDriverParameters.getDriverType());
      DriverMetadata driver = driverStore.get(driverKey);
      int turns = 1;
      while (true) {
        synchronized (driverStore) {
          if (turns == 1) {
            if (driver.getDriverStatus() == DriverStatus.SLAUNCHED) {
              logger.debug("i am blocked,work2 not start");
            } else {
              if (driver.getDriverStatus() == DriverStatus.LAUNCHING) {
                ;
              }
              turns++;
              logger.debug("i am blocked,but work2 has start");
            }
          } else {
            if (driver.getDriverStatus() == DriverStatus.SLAUNCHED) {
              logger.debug("i am free,work2 sucess start driver");
              break;
            }
          }
        }

        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
      return "/tmp/dev0";
    }

    void setDriverStore(DriverStore driverStore) {
      this.driverStore = driverStore;
    }
  }

  public class LaunchDriverWorkerT extends LaunchDriverWorker {

    public LaunchDriverWorkerT(LaunchDriverParameters launchDriverParameters,
        JvmConfigurationForDriver jvmConfig,
        DriverContainerConfiguration driverContainerConfig, long timeout) {
      super(launchDriverParameters, jvmConfig, driverContainerConfig);
    }

    @Override
    protected Process buildJvmProcess(long volId) throws Exception {
      Thread.sleep(3000);
      return null;
    }

    @Override
    protected void scanProcessId(Process process, DriverMetadata driver) {
      driver.setProcessId(1);
    }
  }

  // @Test
  // public void testLink() throws IOException, InterruptedException {
  // String configPath = "/root/eclipse.desktop";
  // String workingPath = "/root/Documents/";
  // String command = String.format("ln -s %s %s", configPath, workingPath + "eclipse.desktop");
  // Process process = Runtime.getRuntime().exec(command);
  // process.waitFor();
  //
  // File workdir = new File(workingPath +"config");
  // if (!workdir.exists()) {
  // workdir.mkdir();
  // }
  // }

}
