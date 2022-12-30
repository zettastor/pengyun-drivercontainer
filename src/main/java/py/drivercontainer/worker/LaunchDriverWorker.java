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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.coordinator.lio.LioNameBuilder;
import py.coordinator.lio.LioNameBuilderImpl;
import py.driver.DriverAction;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.JvmConfiguration;
import py.drivercontainer.client.CoordinatorClientFactory;
import py.drivercontainer.driver.LaunchDriverParameters;
import py.drivercontainer.driver.store.DriverStore;
import py.drivercontainer.driver.workspace.DriverWorkspaceManager;
import py.drivercontainer.driver.workspace.DriverWorkspaceManagerImpl;
import py.drivercontainer.service.PortContainerFactory;
import py.drivercontainer.utils.DriverContainerUtils;
import py.drivercontainer.utils.IscsiProcessor;
import py.drivercontainer.utils.JavaProcessBuilder;
import py.icshare.DriverKey;
import py.instance.InstanceId;
import py.periodic.Worker;
import py.thrift.coordinator.service.Coordinator;
import py.thrift.coordinator.service.GetStartupStatusRequest;
import py.thrift.coordinator.service.GetStartupStatusResponse;

/**
 * A class as worker to launch driver including launching driver process and configure driver. The
 * worker will wait until the driver being successfully launched or timeout.
 *
 */
public class LaunchDriverWorker implements Runnable, Worker {

  public static final long randomNumberMarker = 4252063745L;

  public static final String LAUNCH_SUCCEEDED = String.format("[%s]success", randomNumberMarker);

  private static final Logger logger = LoggerFactory.getLogger(LaunchDriverWorker.class);
  /*
   * When more than one coordinator process is not alive,restart drivercontainer will be stuck
   * because session file can not be read.If all coordinator process trun to alive,session file can
   *  be read. When all coordinator turn to alive, createIscsiTargetNameFlag will be set true to
   * create iscsi target name.
   */
  public boolean createIscsiTargetNameFlag = false;
  public boolean coordinatorIsAlive = false;
  private LaunchDriverParameters launchDriverParameters;
  private DriverStore driverStore;
  private PortContainerFactory portContainerFactory;
  private JvmConfiguration driverJvmConfig;
  private DriverContainerConfiguration driverContainerConfiguration;
  private IscsiProcessor iscsiProcessor;
  private DriverWorkspaceManager driverWorkspaceManager;
  private LioNameBuilder lioNameBuilder = new LioNameBuilderImpl();
  private CoordinatorClientFactory coordinatorClientFactory;


  /**
   * xx.
   */
  public LaunchDriverWorker(LaunchDriverParameters launchDriverParameters,
      JvmConfiguration jvmConfig,
      DriverContainerConfiguration driverContainerConfig) {
    this.launchDriverParameters = launchDriverParameters;
    this.driverJvmConfig = jvmConfig;
    this.driverContainerConfiguration = driverContainerConfig;
    this.driverWorkspaceManager = new DriverWorkspaceManagerImpl(
        new InstanceId(launchDriverParameters.getDriverContainerInstanceId()),
        driverContainerConfiguration.getDriverAbsoluteRootPath(),
        driverContainerConfiguration.getLibraryRootPath());
  }

  // copy from class DriverLauncher

  /**
   * xx.
   */
  public static int decodeProcessid(String string) {
    if (!string.contains(LAUNCH_SUCCEEDED)) {
      return 0;
    }
    return Integer.parseInt(string.split("-")[1]);
  }


  /**
   * xx.
   */
  public void init() {
    logger.debug("Port of driver {} to launch is {}", launchDriverParameters.getDriverType().name(),
        launchDriverParameters.getPort());
    // save driver metadata to file(path:usr.dir)
    List<DriverMetadata> drivers = driverStore.list();
    if (drivers.size() == 0) {
      logger.warn("first driver:{} come ", launchDriverParameters.getPort());
    } else {
      logger.warn("not first driver:{} come, current drivers:{}",
          launchDriverParameters.getPort(), drivers);
    }
  }

  @Override
  public void run() {
    // do work in a thread
    doWork();
  }

  @Override
  public void doWork() {
    logger.info(
        "Start a work to launch driver {} , coordinatorIsAlive {}, createIscsiTargetNameFlag {}...",
        launchDriverParameters.getDriverType().name(), coordinatorIsAlive,
        createIscsiTargetNameFlag);
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
    logger.warn("3 current time : {}", df.format(new Date()));
    DriverKey driverKey;
    DriverMetadata driver;
    driverKey = new DriverKey(launchDriverParameters.getDriverContainerInstanceId(),
        launchDriverParameters.getVolumeId(), launchDriverParameters.getSnapshotId(),
        launchDriverParameters.getDriverType());

    driver = driverStore.get(driverKey);
    //If driver in upgrading ,launchDriverParameters will be set a new port and coordinator port
    if (driver.getPort() != launchDriverParameters.getPort()
        && driver.getCoordinatorPort() != launchDriverParameters.getCoordinatorPort()
        && driver.getInstanceId() != launchDriverParameters.getInstanceId()) {
      driver.setPort(launchDriverParameters.getPort());
      driver.setCoordinatorPort(launchDriverParameters.getCoordinatorPort());
      logger.warn("new instance is :{}", launchDriverParameters.getInstanceId());
      driver.setInstanceId(launchDriverParameters.getInstanceId());
      driverStore.save(driver);

    }

    Process process = null;
    String nbdDevice = null;
    long volumeId = driver.getVolumeId(true);
    // begin to start coodinator process.
    // If coordinator process is up ,should not start up it again
    if (!coordinatorIsAlive) {
      try {
        driver.setProcessId(0);
        //If has old volumeId ,use old volumeId to create driver workspace
        process = buildJvmProcess(volumeId);
        // read process id from pipe of driver process, this work only if driver process has no
        // dead loop to result in no output to pipe
        logger.warn("4 current time before scan coordinator process: {}", df.format(new Date()));
        scanProcessId(process, driver);
        logger.warn("5 current time after scan coordinator process: {}", df.format(new Date()));
      } catch (Exception e) {
        logger.error("Caught an exception when launching driver process, no such package.", e);
        driver.removeAction(DriverAction.START_SERVER);
        driverStore.remove(driverKey);
        return;
      }

      if (driver.getProcessId() == 0) {
        logger.error("coordinate process is 0,launch failed for volume {}.", driver.getVolumeId());
        if (process != null) {
          process.destroy();
        }
        updateDriverOnFailure(driver, false);
        return;
      } else if (driver.getDriverType() == DriverType.ISCSI && !createIscsiTargetNameFlag) {
        updateDriverOnSuccess(driver, false);
        return;
      } else {
        driverStore.save(driver);
      }

      if (createIscsiTargetNameFlag) {
        // add CREATE_TARGET into action set firstly to prevent target-scan thread and server-scan
        // thread to add unnecessary action
        driver.addAction(DriverAction.CREATE_TARGET);
        driver.removeAction(DriverAction.START_SERVER);
      }
    } else {
      if (createIscsiTargetNameFlag) {
        logger.debug("need to create target.");
        setChapAndAclForRebootCase(driver);
      }
    }

    try {
      if (driver.getDriverType() == DriverType.ISCSI && createIscsiTargetNameFlag) {
        Coordinator.Iface coordClient = null;
        EndPoint endpoint;
        endpoint = new EndPoint(driver.getHostName(), driver.getCoordinatorPort());
        coordClient = coordinatorClientFactory
            .build(endpoint, driverContainerConfiguration.getThriftClientTimeout()).getClient();
        GetStartupStatusRequest request = new GetStartupStatusRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setVolumeId(driver.getVolumeId());
        request.setSnapshotId(driver.getSnapshotId());
        GetStartupStatusResponse response = coordClient.getStartupStatus(request);
        if (!response.isStartupStatus()) {
          logger.warn("volume is stable false, volumeId: {}, not to create target now.",
              driver.getVolumeId());
          updateDriverOnFailure(driver, createIscsiTargetNameFlag);
          return;
        } else {
          logger.warn("volume is stable true, volumeId: {}, go to create target now.",
              driver.getVolumeId());
        }
      }

      boolean doneLaunchingDriver = (driver.getDriverType() != DriverType.ISCSI)
          || (createIscsiTargetNameFlag
          ? ((nbdDevice = iscsiProcessor.createIscsiTarget(driver.getNbdDevice(), volumeId))
          != null)
          : true);
      if (doneLaunchingDriver) {
        logger.debug("Successfully launched driver {}", driver);
        driver.setNbdDevice(nbdDevice);
        updateDriverOnSuccess(driver, createIscsiTargetNameFlag);
      } else {
        logger.error("Something wrong when launch driver {}", driver);
        updateDriverOnFailure(driver, createIscsiTargetNameFlag);
      }
    } catch (Exception e) {
      logger.error("Process to launch driver {} stopped.", driver.getDriverType().name(), e);
      updateDriverOnFailure(driver, createIscsiTargetNameFlag);
    }
    logger.warn("6 current time : {}", df.format(new Date()));
  }

  protected Process buildJvmProcess(long volId) throws Exception {
    logger.info("driver jvm config class: {}, main class: {}", driverJvmConfig.getClass(),
        driverJvmConfig.getMainClass());
    JavaProcessBuilder processBuilder = new JavaProcessBuilder(driverJvmConfig);

    DriverKey driverKey = new DriverKey(launchDriverParameters.getDriverContainerInstanceId(),
        volId, launchDriverParameters.getSnapshotId(), launchDriverParameters.getDriverType());

    driverWorkspaceManager.createWorkspace(driverStore.getVersion(), driverKey);
    String workingPath = driverWorkspaceManager.getWorkspace(driverStore.getVersion(), driverKey)
        .getPath();
    logger.info("driver workingPath:{}", workingPath);
    processBuilder.setWorkingDirectory(workingPath);
    processBuilder.addArgument(workingPath);

    //We use new volId to launch coordinator, so set the new volId for launchDriverParameters
    // always.
    for (String arg : launchDriverParameters.buildCommandLineParameters()) {
      processBuilder.addArgument(arg);
    }
    processBuilder
        .setMetricPort(launchDriverParameters.getPort() + driverJvmConfig.getJmxBasePort());
    processBuilder.setVolumeId(launchDriverParameters.getVolumeId());
    processBuilder.setSnapshotId(launchDriverParameters.getSnapshotId());

    Process process = processBuilder.startProcess();
    logger.info("started process {},{}", process, launchDriverParameters.getPort());
    return process;
  }

  protected void scanProcessId(Process process, DriverMetadata driver) {
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
    Scanner scanner = new Scanner(process.getInputStream());
    while (scanner.hasNext()) {
      logger.warn("4.1 current time : {}", df.format(new Date()));
      String processOutput = scanner.nextLine();
      logger.warn("Get process output {}", processOutput);
      logger.warn("4.2 current time : {}", df.format(new Date()));
      int processId = LaunchDriverWorker.decodeProcessid(processOutput);
      logger.warn("4.3 current time : {}", df.format(new Date()));
      if (processId != 0) {
        /*
         * Since driver itself is responsibility to check if it is all right when being launched,
         *  actually it is not necessary to check if the launching driver has already listened the
         *  specified port here. After a process ID being got from pipe by driver-container, the
         * driver should already be launched successfully.
         */
        if (!DriverContainerUtils.processExist(processId)) {
          logger.error("Driver process disappeared! No such process {} on port {} for driver {}",
              processId, driver.getPort(), driver);
        }

        logger.warn("Process for the driver {} and volume {} is up", driver.getDriverType().name(),
            driver.getVolumeId());
        //upgradePhrase value is 1 means coordinator process has been start up successfully
        driver.setUpgradePhrase(1);
        driver.setProcessId(processId);
        logger.warn("4.4 current time : {}", df.format(new Date()));
        break;
      }
    }
    scanner.close();
  }

  void updateDriverOnSuccess(DriverMetadata driver, boolean isTargetPhase) {
    if (isTargetPhase) {
      driver.removeAction(DriverAction.CREATE_TARGET);
    } else {
      driver.removeAction(DriverAction.START_SERVER);
    }

    switch (driver.getDriverStatus()) {
      case LAUNCHING:
      case RECOVERING:
        driver.setDriverStatus(DriverStatus.LAUNCHED);
        driverStore.save(driver);
        break;
      case MIGRATING:
        driverStore.save(driver);
        break;
      case LAUNCHED:
        driverStore.save(driver);
        break;
      case REMOVING:
        break;
      default:
        String errMsg = "Unable to handle driver with status " + driver.getDriverStatus();
        logger.error("{}", errMsg);
        throw new IllegalStateException(errMsg);
    }

    return;
  }

  void updateDriverOnFailure(DriverMetadata driver, boolean isTargetPhase) {
    if (isTargetPhase) {
      driver.removeAction(DriverAction.CREATE_TARGET);
    } else {
      driver.removeAction(DriverAction.START_SERVER);
    }
    switch (driver.getDriverStatus()) {
      case LAUNCHING:
        driver.setDriverStatus(DriverStatus.ERROR);
        driverStore.save(driver);
        break;
      case MIGRATING:
      case RECOVERING:
        break;
      default:
        String errMsg = "Unable to handle driver with status " + driver.getDriverStatus();
        logger.error("{}", errMsg);
        throw new IllegalStateException(errMsg);
    }
  }

  public LaunchDriverParameters getLaunchDriverParameters() {
    return launchDriverParameters;
  }

  public void setLaunchDriverParameters(LaunchDriverParameters launchDriverParameters) {
    this.launchDriverParameters = launchDriverParameters;
  }

  public DriverStore getDriverStore() {
    return driverStore;
  }

  public void setDriverStore(DriverStore driverStore) {
    this.driverStore = driverStore;
  }

  public PortContainerFactory getPortContainerFactory() {
    return portContainerFactory;
  }

  public void setPortContainerFactory(PortContainerFactory portContainerFactory) {
    this.portContainerFactory = portContainerFactory;
  }

  public JvmConfiguration getJvmConfig() {
    return driverJvmConfig;
  }

  public void setJvmConfig(JvmConfiguration driverJvmConfig) {
    this.driverJvmConfig = driverJvmConfig;
  }

  public void setIscsiProcessor(IscsiProcessor iscsiProcessor) {
    this.iscsiProcessor = iscsiProcessor;
  }


  public boolean isCoordinatorIsAlive() {
    return coordinatorIsAlive;
  }

  public void setCoordinatorIsAlive(boolean coordinatorIsAlive) {
    this.coordinatorIsAlive = coordinatorIsAlive;
  }

  public boolean isCreateIscsiTargetNameFlag() {
    return createIscsiTargetNameFlag;
  }

  public void setCreateIscsiTargetNameFlag(boolean createIscsiTargetNameFlag) {
    this.createIscsiTargetNameFlag = createIscsiTargetNameFlag;
  }

  public DriverWorkspaceManager getDriverWorkspaceManager() {
    return driverWorkspaceManager;
  }

  public void setDriverWorkspaceManager(DriverWorkspaceManager driverWorkspaceManager) {
    this.driverWorkspaceManager = driverWorkspaceManager;
  }

  void setChapAndAclForRebootCase(DriverMetadata driver) {
    String targetName = getTargetName(driver.getVolumeId(), driver.getSnapshotId());
    if (targetName == null) {
      logger.error("error to get target name");
      return;
    }
    iscsiProcessor.getIscsiTargetManager()
        .setChapControlStatus(targetName, driver.getChapControl());
    if (driver.getAclList() != null) {
      if (iscsiProcessor.getIscsiTargetManager().getIscsiAccessRuleList(targetName) == null) {
        logger.debug("target name {}, acl list {}", targetName, driver.getAclList());
        iscsiProcessor.getIscsiTargetManager().saveAccessRuleToMap(targetName, driver.getAclList());
      } else {
        logger.debug("target name and acl list put before");
      }
    } else {
      logger.debug("acl list is null");
    }
  }

  private String getTargetName(long volumeId, int snapshotId) {
    String targetName = null;
    targetName = lioNameBuilder.buildLioWwn(volumeId, snapshotId);
    return targetName;
  }

  public CoordinatorClientFactory getCoordinatorClientFactory() {
    return coordinatorClientFactory;
  }

  public void setCoordinatorClientFactory(CoordinatorClientFactory coordinatorClientFactory) {
    this.coordinatorClientFactory = coordinatorClientFactory;
  }
}
