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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import py.app.NetworkConfiguration;
import py.app.context.InstanceIdFileStore;
import py.app.healthcheck.HealthCheckerWithThriftImpl;
import py.common.struct.EndPoint;
import py.common.struct.EndPointParser;
import py.coordinator.IscsiTargetNameManagerFactory;
import py.coordinator.lio.LioCommandManager;
import py.coordinator.lio.LioCommandManagerConfiguration;
import py.coordinator.lio.LioManager;
import py.coordinator.lio.LioManagerConfiguration;
import py.coordinator.lio.LioNameBuilder;
import py.coordinator.lio.LioNameBuilderImpl;
import py.dih.client.DihClientFactory;
import py.dih.client.DihInstanceStore;
import py.dih.client.worker.HeartBeatWorkerFactory;
import py.driver.DriverType;
import py.drivercontainer.client.CoordinatorClientFactory;
import py.drivercontainer.delegate.DihDelegate;
import py.drivercontainer.driver.DriverAppContext;
import py.drivercontainer.driver.DriverWorkspaceProvider;
import py.drivercontainer.driver.store.DriverStore;
import py.drivercontainer.driver.store.DriverStoreImpl;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.drivercontainer.driver.store.DriverStoreManagerImpl;
import py.drivercontainer.driver.upgrade.QueryServer;
import py.drivercontainer.driver.upgrade.UpgradeDriverPoller;
import py.drivercontainer.driver.upgrade.UpgradeDriverPollerImpl;
import py.drivercontainer.driver.version.Version;
import py.drivercontainer.driver.version.VersionManager;
import py.drivercontainer.driver.version.file.VersionManagerImpl;
import py.drivercontainer.lio.saveconfig.jsonobj.SaveConfigBuilder;
import py.drivercontainer.scsi.ScsiManager;
import py.drivercontainer.scsi.ScsiUtil;
import py.drivercontainer.service.BootStrap;
import py.drivercontainer.service.DriverContainerImpl;
import py.drivercontainer.service.PortContainer;
import py.drivercontainer.service.PortContainerFactory;
import py.drivercontainer.service.PortContainerImpl;
import py.drivercontainer.service.taskqueue.TaskExecutor;
import py.drivercontainer.service.taskqueue.TaskExecutorImpl;
import py.drivercontainer.utils.PerformanceCsvParaser;
import py.drivercontainer.worker.ChangeDriverVolumeTask;
import py.drivercontainer.worker.GetIoLimitationWorkFactory;
import py.drivercontainer.worker.GetVolumeAccessRulesFromInfoWorkFactory;
import py.drivercontainer.worker.LaunchDriverWorkerFactory;
import py.drivercontainer.worker.MigratingScanWorker;
import py.drivercontainer.worker.RemoveDriverWorkerFactory;
import py.drivercontainer.worker.ReportDriverMetadataWorkerFactory;
import py.drivercontainer.worker.ScsiDeviceScanWorker;
import py.drivercontainer.worker.ServerScanWorker;
import py.drivercontainer.worker.SubmitUpgradeRequestWorkFactory;
import py.drivercontainer.worker.TargetScanWorker;
import py.drivercontainer.worker.UpdateIscsiAccessRuleFileSweepWorkFactory;
import py.icshare.DriverKey;
import py.infocenter.client.InformationCenterClientFactory;
import py.instance.DcType;
import py.instance.InstanceStore;
import py.instance.PortType;
import py.monitor.jmx.configuration.JmxAgentConfiguration;
import py.monitor.jmx.server.JmxAgent;
import py.periodic.Worker;
import py.periodic.WorkerFactory;
import py.periodic.impl.ExecutionOptionsReader;
import py.periodic.impl.PeriodicWorkExecutorImpl;
import py.scriptcontainer.client.ScriptContainerClientFactory;
import py.thrift.share.DriverKeyThrift;
import py.thrift.share.IoLimitationThrift;
import py.thrift.share.VolumeAccessRuleThrift;

/**
 * xx.
 */
@Configuration
@Import({NetworkConfiguration.class, JvmConfigurationForDriver.class, JmxAgentConfiguration.class,
    DriverContainerConfiguration.class, LioManagerConfiguration.class,
    LioCommandManagerConfiguration.class})
public class DriverContainerAppBeans {

  private static final Logger logger = LoggerFactory.getLogger(DriverContainerAppBeans.class);
  DriverWorkspaceProvider driverWorkspaceProvider;
  @Autowired
  private DriverContainerConfiguration driverContainerConfig;
  @Autowired
  private LioManagerConfiguration lioManagerConfiguration;
  @Autowired
  private LioCommandManagerConfiguration lioCommandManagerConfiguration;
  @Autowired
  private NetworkConfiguration networkConfiguration;
  @Autowired
  private InstanceStore instanceStore;
  @Autowired
  private JvmConfigurationForDriver jvmConfig;

  @Bean
  public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
    return new PropertySourcesPlaceholderConfigurer();
  }

  @Bean
  public JmxAgentConfiguration jmxAgentConfiguration() {
    JmxAgentConfiguration jmxAgentConfiguration = new JmxAgentConfiguration();
    return jmxAgentConfiguration;
  }


  /**
   * xx.
   */
  @Bean
  public JvmConfigurationManager jvmConfigManager() {
    JvmConfigurationManager manager = new JvmConfigurationManager();

    manager.setCoordinatorJvmConfig(jvmConfig);
    return manager;
  }


  /**
   * xx.
   */
  @Bean(name = "appContext")
  public DriverAppContext appContext() {
    DriverAppContext appContext = new DriverAppContext(driverContainerConfig.getAppName());
    appContext.setLocation(driverContainerConfig.getAppLocation());

    // control stream
    EndPoint endpointOfControlStream = EndPointParser
        .parseInSubnet(driverContainerConfig.getMainEndPoint(),
            networkConfiguration.getControlFlowSubnet());
    appContext.putEndPoint(PortType.CONTROL, endpointOfControlStream);

    // data stream
    EndPoint endpointOfDataIoStream;
    if (networkConfiguration.isEnableDataDepartFromControl()) {
      endpointOfDataIoStream = EndPointParser.parseInSubnet(driverContainerConfig.getMainEndPoint(),
          networkConfiguration.getOutwardFlowSubnet());
    } else {
      endpointOfDataIoStream = EndPointParser.parseInSubnet(driverContainerConfig.getMainEndPoint(),
          networkConfiguration.getControlFlowSubnet());
    }
    appContext.putEndPoint(PortType.IO, endpointOfDataIoStream);

    // monitor stream
    logger.warn("---------------------->{},{}", jmxAgentConfiguration(), networkConfiguration);
    EndPoint endpointOfMonitorStream = EndPointParser
        .parseInSubnet(jmxAgentConfiguration().getJmxAgentPort(),
            networkConfiguration.getMonitorFlowSubnet());
    appContext.putEndPoint(PortType.MONITOR, endpointOfMonitorStream);

    appContext.setInstanceIdStore(new InstanceIdFileStore(driverContainerConfig.getAppName(),
        driverContainerConfig.getAppName(), endpointOfControlStream.getPort()));
    return appContext;
  }

  @Bean
  public EndPoint localDihEp() {
    return EndPointParser.parseLocalEndPoint(driverContainerConfig.getLocalDihEndPoint(),
        appContext().getMainEndPoint().getHostName());
  }


  /**
   * xx.
   */
  @Bean
  public JmxAgent jmxAgent() throws Exception {
    JmxAgent jmxAgent = JmxAgent.getInstance();
    jmxAgent.setAppContext(appContext());
    return jmxAgent;
  }


  /**
   * xx.
   */
  @Bean(name = "healthChecker")
  public HealthCheckerWithThriftImpl healthChecker() {
    HealthCheckerWithThriftImpl healthChecker = new HealthCheckerWithThriftImpl(
        driverContainerConfig.getHealthCheckerRate(),
        appContext());
    healthChecker.setServiceClientClazz(py.thrift.fsserver.service.FileSystemService.Iface.class);
    healthChecker.setHeartBeatWorkerFactory(heartBeatWorkerFactory());
    return healthChecker;
  }


  /**
   * xx.
   */
  @Bean
  public HeartBeatWorkerFactory heartBeatWorkerFactory() {
    HeartBeatWorkerFactory heartBeatWorkerFactory = new HeartBeatWorkerFactory();
    heartBeatWorkerFactory.setRequestTimeout(driverContainerConfig.getThriftClientTimeout());
    heartBeatWorkerFactory.setLocalDihEndPoint(localDihEp());
    heartBeatWorkerFactory.setAppContext(appContext());
    heartBeatWorkerFactory.setDihClientFactory(dihClientFactory());
    if (driverContainerConfig.getDrivercontainerRole() == 1) {
      heartBeatWorkerFactory.setDcType(DcType.ALLSUPPORT);
      logger.warn("DcType: All support.");
    } else if (driverContainerConfig.getDrivercontainerRole() == 2) {
      heartBeatWorkerFactory.setDcType(DcType.SCSISUPPORT);
      logger.warn("DcType: only support scsi client.");
    } else {
      heartBeatWorkerFactory.setDcType(DcType.NORMALSUPPORT);
      logger.warn("DcType: normal support.");
    }
    return heartBeatWorkerFactory;
  }


  /**
   * xx.
   */
  @Bean(name = "DihInstanceStore")
  public DihInstanceStore instanceStore() {
    try {
      DihInstanceStore instanceStore = DihInstanceStore.getSingleton();
      instanceStore.setRequestTimeout(driverContainerConfig.getThriftClientTimeout());
      instanceStore.setDihEndPoint(localDihEp());
      instanceStore.setDihClientFactory(dihClientFactory());
      instanceStore.init();

      return instanceStore;
    } catch (Exception e) {
      logger.error("Something wrong when initialize dih instance store.", e);
      return null;
    }
  }

  @Bean
  public DihClientFactory dihClientFactory() {
    DihClientFactory dihClientFactory = new DihClientFactory(1, 3, 20000);
    return dihClientFactory;
  }


  /**
   * xx.
   */
  @Bean
  public ScriptContainerClientFactory scriptContainerClientFactory() {
    ScriptContainerClientFactory scriptContainerClientFactory = new ScriptContainerClientFactory();
    scriptContainerClientFactory.setInstanceStore(instanceStore);
    return scriptContainerClientFactory;
  }


  /**
   * xx.
   */
  @Bean
  public VersionManager versionManager() {
    String varPath = driverContainerConfig.getDriverAbsoluteRootPath();
    VersionManager versionManager = null;
    try {
      if (!Paths.get(varPath).toFile().exists()) {
        if (!Paths.get(varPath).toFile().mkdirs()) {
          logger.error("error to mkdirs {}", varPath);
        }
      }
      versionManager = new VersionManagerImpl(varPath);
    } catch (IOException e) {
      throw new RuntimeException();
    }

    return versionManager;
  }


  /**
   * xx.
   */
  @Bean
  public ScsiManager scsiManager() {
    ScsiUtil.setSaveConfigBuilder(saveConfigBuilder());
    Path path = Paths.get(driverContainerConfig.getDriverAbsoluteRootPath() + "/SCSI/");
    ScsiManager.setMetadataPath(path);
    ScsiManager scsiManager = ScsiManager.buildFromFile();
    scsiManager.setLioCommandManager(lioCommandManager());
    return scsiManager;
  }


  /**
   * xx.
   */
  @Bean
  public DriverContainerImpl driverContainer() throws Exception {
    DriverContainerImpl driverContainer = new DriverContainerImpl(appContext());
    driverContainer.setNetworkConfiguration(networkConfiguration);
    driverContainer.setBootStrap(bootstrap());
    driverContainer.setDriverContainerConfig(driverContainerConfig);
    driverContainer.setInformationCenterClientFactory(infoCenterClientFactory());
    driverContainer.setCoordinatorClientFactory(coordinatorClientFactory());
    driverContainer.setJvmConfigManager(jvmConfigManager());
    driverContainer.setDriverStoreManager(driverStoreManager());
    driverContainer.setVersionManager(versionManager());
    driverContainer.setLaunchDriverWorkerFactory(launchDriverWorkerFactory());
    driverContainer.setRemoveDriverWorkerFactory(removeDriverWorkerFactory());
    driverContainer.setPortContainerFactory(portContainerFactory());
    driverContainer.setPerformanceCsvParaser(performanceCsvParaser());
    driverContainer.setVolumeAccessRuleTable(volumeAccessRuleTable());
    driverContainer.setDriver2ItsIoLimitationsTable(driver2ItsIoLimitationsTable());
    driverContainer.setTaskExecutor(taskExecutor());
    driverContainer.setLioManager(lioManage());
    driverContainer.setLioNameBuilder(lioNameBuilder());
    driverContainer.setChangeDriverVolumeTask(changeDriverVolumeTask());
    driverContainer.setScsiManager(scsiManager());
    return driverContainer;
  }


  /**
   * xx.
   */
  @Bean
  public DriverContainerAppEngine driverContainerAppEngine() throws Exception {
    DriverContainerImpl driverContainer = driverContainer();

    DriverContainerAppEngine driverContainerAppEngine = new DriverContainerAppEngine(
        driverContainer);
    driverContainerAppEngine.setDcConfig(driverContainerConfig);
    driverContainerAppEngine.setVersionManager(versionManager());
    driverContainerAppEngine.setContext(appContext());
    driverContainerAppEngine.setHealthChecker(healthChecker());
    driverContainerAppEngine.setBootstrap(bootstrap());
    driverContainerAppEngine.setDriverUpgradeProcessor(driverUpgradeProcessor());
    driverContainerAppEngine.setAccessRulesPollerExecutor(getAccessRuleExecutor());
    driverContainerAppEngine.setAccessRulesUpdaterExecutor(updateInitiatorsAllowFileExecutor());
    driverContainerAppEngine.setSubmitUpgradeRequestRunner(submitUpgradeRequestExecutor());
    driverContainerAppEngine.setReporterExecutor(reportExecutor());
    driverContainerAppEngine.setServerScannerExecutor(serverScannerExecutor());
    driverContainerAppEngine.setTargetScannerExecutor(targetScannerExecutor());
    driverContainerAppEngine.setTaskExecutorRunner(taskExecutorExecutor());
    driverContainerAppEngine.setGetIoLimitationExecutor(getIoLimitationExecutor());
    driverContainerAppEngine.setMaxNetworkFrameSize(driverContainerConfig.getMaxNetworkFrameSize());
    driverContainerAppEngine.setQueryServer(queryServer());
    driverContainerAppEngine.setMigratingScanExecutor(migratingScanExecutor());
    driverContainerAppEngine.setScsiDeviceScanExecutor(scsiDeviceScanExecutor());

    driverContainer.setAppEngine(driverContainerAppEngine);
    return driverContainerAppEngine;
  }


  /**
   * xx.
   */
  @Bean
  public PerformanceCsvParaser performanceCsvParaser() {
    PerformanceCsvParaser performanceCsvParaser = new PerformanceCsvParaser();
    performanceCsvParaser
        .setDashboardLongestTimeSecond(driverContainerConfig.getDashboardLongestTimeSecond());
    return performanceCsvParaser;
  }


  /**
   * xx.
   */
  @Bean
  public BootStrap bootstrap() throws Exception {
    BootStrap bootstrap = new BootStrap();
    bootstrap.setAppContext(appContext());
    bootstrap.setPortContainerFactory(portContainerFactory());
    bootstrap.setDriverContainerConfig(driverContainerConfig);
    bootstrap.setStartLioServiceCommand(lioCommandManagerConfiguration.getStartLioserviceCommand());
    bootstrap.setStopLioServiceCommand(lioCommandManagerConfiguration.getStopLioserviceCommand());
    bootstrap.setLioSaveConfigCommand(lioCommandManagerConfiguration.getLioSaveConfig());
    bootstrap.setLioAutoAddDefaultPortalCommand(
        lioCommandManagerConfiguration.getLioAutoAddDefaultPortal());
    bootstrap.setDefaultSaveConfigFilePath(
        lioCommandManagerConfiguration.getDefaultSaveConfigFilePath());
    bootstrap.setDriverStoreManager(driverStoreManager());
    bootstrap.setVersionManager(versionManager());
    bootstrap.setDriverUpgradeProcessor(driverUpgradeProcessor());
    bootstrap.setDriverScanWorker(serverScanWorker());
    return bootstrap;
  }


  /**
   * xx.
   */
  @Bean
  public LioManager lioManage() {
    LioManager lioManager = new LioManager();
    lioManager.setNbdDeviceName(lioManagerConfiguration.getNbdDeviceName());
    lioManager.setBindNbdCmd(lioManagerConfiguration.getBindNbdCmd());
    lioManager.setUnbindNbdCmd(lioManagerConfiguration.getUnbindNbdCmd());
    lioManager.setTemplatePath(getClass().getResource("/config/saveconfig_bak.json").getPath());
    lioManager.setFilePath(lioManagerConfiguration.getSaveconfigPath());
    lioManager.setLioManagerCon(lioManagerConfiguration);
    lioManager.setSaveConfigBuilder(saveConfigBuilder());
    lioManager.setLioNameBuilder(lioNameBuilder());
    lioManager.setSessionCommand(lioManagerConfiguration.getSessionCommand());
    lioManager.setNbdDeviceMaxNum(lioCommandManagerConfiguration.getNbdDeviceMaxNum());
    return lioManager;
  }


  /**
   * xx.
   */
  @Bean
  public LioCommandManager lioCommandManager() {
    LioCommandManager lioCommandManager = new LioCommandManager();
    lioCommandManager.setNbdDeviceName(lioCommandManagerConfiguration.getNbdDeviceName());
    lioCommandManager.setBindNbdCmd(lioCommandManagerConfiguration.getBindNbdCmd());
    lioCommandManager.setUnbindNbdCmd(lioCommandManagerConfiguration.getUnbindNbdCmd());
    lioCommandManager.setCreateStorageCmd(lioCommandManagerConfiguration.getLioCreateStorage());
    lioCommandManager.setCreateTargetCmd(lioCommandManagerConfiguration.getLioCreateTarget());
    lioCommandManager.setCreateLunCmd(lioCommandManagerConfiguration.getLioCreateLun());
    lioCommandManager.setCreatePortalCmd(lioCommandManagerConfiguration.getLioCreatePortal());
    lioCommandManager.setClearConfigCmd(lioCommandManagerConfiguration.getLioClearConfig());
    lioCommandManager
        .setCreateAccuessRuleCmd(lioCommandManagerConfiguration.getLioCreateAccuessRule());
    lioCommandManager
        .setCreateChapPasswordCmd(lioCommandManagerConfiguration.getLioCreateChapPassword());
    lioCommandManager.setCreateChapUserCmd(lioCommandManagerConfiguration.getLioCreateChapUser());
    lioCommandManager.setDeleteStorageCmd(lioCommandManagerConfiguration.getLioDeleteStorage());
    lioCommandManager.setDeleteTargetCmd(lioCommandManagerConfiguration.getLioDeleteTarget());
    lioCommandManager.setSaveConfigCmd(lioCommandManagerConfiguration.getLioSaveConfig());
    lioCommandManager.setSessionCommand(lioCommandManagerConfiguration.getSessionCommand());
    lioCommandManager
        .setDeleteAccuessRuleCmd(lioCommandManagerConfiguration.getLioDeleteAccuessRule());
    lioCommandManager.setSaveConfigBuilder(saveConfigBuilder());
    lioCommandManager.setLioNameBuilder(lioNameBuilder());
    lioCommandManager.setDefaultPort(lioCommandManagerConfiguration.getDefaultLiotargetPort());
    lioCommandManager
        .setDefaultSaveconfigPath(lioCommandManagerConfiguration.getDefaultSaveConfigFilePath());
    lioCommandManager.setDeletePortalCmd(lioCommandManagerConfiguration.getLioDeletePortal());
    lioCommandManager
        .setCreateMutualChapUserCmd(lioCommandManagerConfiguration.getLioCreateMutualChapUser());
    lioCommandManager
        .setCreateMutualChapPasswordCmd(
            lioCommandManagerConfiguration.getLioCreateMutualChapPassword());
    lioCommandManager
        .setSetAttributeAuthenticationCmd(lioCommandManagerConfiguration.getLioChapControl());
    lioCommandManager
        .setSetEmulateTpuValueCmd(lioCommandManagerConfiguration.getSetEmulateTpualue());
    lioCommandManager.setSetAttributeDemoModeDiscoveryCmd(
        lioCommandManagerConfiguration.getLioDemoModeDiscovery());
    lioCommandManager.setSetAttributeDefaultCmdsnDepthCmd(
        lioCommandManagerConfiguration.getLioDefaultCmdsnDepth());
    lioCommandManager
        .setIoDepthEffectiveFlag(lioCommandManagerConfiguration.getIoDepthEffectiveFlag());
    lioCommandManager.setIoDepth(lioCommandManagerConfiguration.getIoDepth());
    lioCommandManager.setNbdDeviceMaxNum(lioCommandManagerConfiguration.getNbdDeviceMaxNum());
    lioCommandManager.setGetAutoAddDefaultPortalCmd(
        lioCommandManagerConfiguration.getLioGetAutoAddDefaultPortalValue());
    lioCommandManager
        .setSetAutoAddDefaultPortalCmd(lioCommandManagerConfiguration.getLioAutoAddDefaultPortal());
    lioCommandManager.setTargetCliCmdLogInfo(lioManagerConfiguration.getTargetcliLogFilePath(),
        lioManagerConfiguration.getTargetcliConsoleLogLevel(),
        lioManagerConfiguration.getTargetcliFileLogLevel());
    return lioCommandManager;
  }


  /**
   * xx.
   */
  @Bean
  public SaveConfigBuilder saveConfigBuilder() {
    SaveConfigBuilder builder;

    builder = new SaveConfigBuilder();
    builder.setLioCmdMaConfig(lioCommandManagerConfiguration);
    builder.setLioMaConfig(lioManagerConfiguration);

    return builder;
  }


  /**
   * xx.
   */
  @Bean
  public DihDelegate dihDelegate() {
    DihDelegate dihDelegate = new DihDelegate();
    dihDelegate.setDcConfig(driverContainerConfig);
    dihDelegate.setDihClientFactory(dihClientFactory());
    dihDelegate.setLocalDihEndPoint(localDihEp());

    return dihDelegate;
  }


  /**
   * xx.
   */
  @Bean
  public LaunchDriverWorkerFactory launchDriverWorkerFactory() {
    LaunchDriverWorkerFactory workerFactory = new LaunchDriverWorkerFactory();
    workerFactory.setDriverStoreManager(driverStoreManager());
    workerFactory.setInformationCenterClientFactory(infoCenterClientFactory());
    workerFactory.setJvmConfigManager(jvmConfigManager());
    workerFactory.setPortContainerFactory(portContainerFactory());
    workerFactory.setDriverContainerConfig(driverContainerConfig);
    workerFactory.setIscsiTargetNameManagerFactory(iscsiTargetNameManagerFactory());
    workerFactory.setLioNameBuilder(lioNameBuilder());
    workerFactory.setCoordinatorClientFactory(coordinatorClientFactory());
    return workerFactory;
  }


  /**
   * xx.
   */
  @Bean
  public RemoveDriverWorkerFactory removeDriverWorkerFactory() {
    RemoveDriverWorkerFactory workerFactory = new RemoveDriverWorkerFactory();
    workerFactory.setAppContext(appContext());
    workerFactory.setDriverStoreManager(driverStoreManager());
    workerFactory.setDriverContainerConfig(driverContainerConfig);
    workerFactory.setPortContainerFactory(portContainerFactory());
    workerFactory.setIscsiTargetNameManagerFactory(iscsiTargetNameManagerFactory());
    workerFactory.setLioNameBuilder(lioNameBuilder());
    workerFactory.setDihDelegate(dihDelegate());
    workerFactory.setInfoCenterClientFactory(infoCenterClientFactory());
    workerFactory.setCoordinatorClientFactory(coordinatorClientFactory());
    return workerFactory;
  }


  /**
   * xx.
   */
  @Bean
  public DriverStoreManager driverStoreManager() {
    String varPath = driverContainerConfig.getDriverAbsoluteRootPath();
    DriverStoreManager driverStoreManager = new DriverStoreManagerImpl();
    VersionManager versionManager = versionManager();
    try {
      for (DriverType driverType : DriverType.values()) {
        Version currentVersion = null;
        Version latestVersion = null;
        boolean migration;
        versionManager.lockVersion(driverType);
        if (driverType == DriverType.ISCSI) {
          currentVersion = versionManager.getCurrentVersion(DriverType.NBD);
          latestVersion = versionManager.getLatestVersion(DriverType.NBD);
          migration = versionManager.isOnMigration(DriverType.NBD);
        } else {
          currentVersion = versionManager.getCurrentVersion(driverType);
          latestVersion = versionManager.getLatestVersion(driverType);
          migration = versionManager.isOnMigration(driverType);
        }
        versionManager.unlockVersion(driverType);
        // Deploy driverContainer first time ,latestVersion and currentVersion both null ,and do
        // nothing
        if (currentVersion == null) {
          continue;
        } else {
          // When reboot driverContainer ,driver is not in upgrading,initial current driverStore
          if (!migration) {
            DriverStore driverStore = new DriverStoreImpl(Paths.get(varPath), currentVersion);
            driverStoreManager.put(currentVersion, driverStore);
          } else if (!currentVersion.equals(latestVersion) && migration) {

            // When reboot driverContainer ,driver is in upgrading,initial currentDriverStore and
            // latesrDriverStore
            DriverStore currentDriverStore = new DriverStoreImpl(Paths.get(varPath),
                currentVersion);
            driverStoreManager.put(currentVersion, currentDriverStore);
            DriverStore latestDriverStore = new DriverStoreImpl(Paths.get(varPath), latestVersion);
            driverStoreManager.put(latestVersion, latestDriverStore);
          }

        }
      }
    } catch (Exception e) {
      throw new RuntimeException();
    }
    return driverStoreManager;
  }

  /**
   * xx.
   */
  @Bean
  public QueryServer queryServer() {
    QueryServer queryServer = new QueryServer();
    queryServer.setDriverStoreManager(driverStoreManager());
    queryServer.setVersionManager(versionManager());
    return queryServer;
  }

  @Bean
  public Map<Long, List<VolumeAccessRuleThrift>> volumeAccessRuleTable() {
    Map<Long, List<VolumeAccessRuleThrift>> volumeAccessRuleTable = new ConcurrentHashMap<>();
    return volumeAccessRuleTable;
  }

  @Bean
  public Map<DriverKey, Future> driverTaskMap() {
    Map<DriverKey, Future> driverTaskMap = new HashMap<>();
    return driverTaskMap;
  }

  /**
   * xx.
   */
  @Bean
  public Map<DriverKeyThrift, List<IoLimitationThrift>> driver2ItsIoLimitationsTable() {
    Map<DriverKeyThrift, List<IoLimitationThrift>> driver2ItsIoLimitationsTable =
        new ConcurrentHashMap<DriverKeyThrift, List<IoLimitationThrift>>();
    return driver2ItsIoLimitationsTable;
  }


  /**
   * xx.
   */
  @Bean
  public PortContainerImpl nbdPortContainer() {
    PortContainerImpl nbdPortContainer = new PortContainerImpl(appContext(),
        driverContainerConfig.getNbdDriverName(), jvmConfig, driverContainerConfig);
    LinkedBlockingQueue<Integer> portQueue = new LinkedBlockingQueue<Integer>();
    String[] ports = driverContainerConfig.getNbdDriverPorts().split(":");
    if (ports.length == 2) {
      int min;
      int max;
      if (Integer.parseInt(ports[0]) < Integer.parseInt(ports[1])) {
        min = Integer.parseInt(ports[0]);
        max = Integer.parseInt(ports[1]);
      } else {
        min = Integer.parseInt(ports[1]);
        max = Integer.parseInt(ports[0]);
      }

      for (int i = min; i <= max; i++) {
        portQueue.add(i);
      }
    } else {
      portQueue.add(1234);
      portQueue.add(1235);
      portQueue.add(1236);
    }

    nbdPortContainer.setPortList(portQueue);
    return nbdPortContainer;
  }


  /**
   * xx.
   */
  @Bean
  public PortContainerImpl jscsiPortContainer() {
    PortContainerImpl jscsiPortContainer = new PortContainerImpl(appContext(),
        driverContainerConfig.getJscsiDriverName(), jvmConfig, driverContainerConfig);
    LinkedBlockingQueue<Integer> portQueue = new LinkedBlockingQueue<Integer>();
    String[] ports = driverContainerConfig.getJscsiDriverPorts().split(":");
    if (ports.length == 2) {
      int min;
      int max;
      if (Integer.parseInt(ports[0]) < Integer.parseInt(ports[1])) {
        min = Integer.parseInt(ports[0]);
        max = Integer.parseInt(ports[1]);
      } else {
        min = Integer.parseInt(ports[1]);
        max = Integer.parseInt(ports[0]);
      }

      for (int i = min; i <= max; i++) {
        portQueue.add(i);
      }
    } else {
      portQueue.add(3260);
      portQueue.add(3261);
      portQueue.add(3262);
    }
    jscsiPortContainer.setPortList(portQueue);
    return jscsiPortContainer;
  }


  /**
   * xx.
   */
  @Bean
  public PortContainerFactory portContainerFactory() {
    Map<DriverType, PortContainer> driverType2PortContainer = new HashMap<>();
    driverType2PortContainer.put(DriverType.JSCSI, jscsiPortContainer());
    driverType2PortContainer.put(DriverType.NBD, nbdPortContainer());
    driverType2PortContainer.put(DriverType.ISCSI, nbdPortContainer());
    PortContainerFactory portContainerFactory = new PortContainerFactory(driverType2PortContainer);
    return portContainerFactory;
  }


  /**
   * xx.
   */
  @Bean
  public TaskExecutor taskExecutor() {
    ExecutorService threadPool;
    TaskExecutorImpl taskExecutor;

    threadPool =
        new ThreadPoolExecutor(driverContainerConfig.getTaskExecutorCorePoolSize(),
            driverContainerConfig.getTaskExecutorMaxPoolSize(),
            driverContainerConfig.getTaskExecutorKeepAliveTimeSec(), TimeUnit.SECONDS,
            new SynchronousQueue<Runnable>(),
            r -> new Thread(r, "driver-action-executor"));

    taskExecutor = new TaskExecutorImpl(driverContainerConfig, threadPool, driverStoreManager());

    return taskExecutor;
  }


  /**
   * xx.
   */
  @Bean
  public PeriodicWorkExecutorImpl taskExecutorExecutor() {
    ExecutionOptionsReader optionReader;
    PeriodicWorkExecutorImpl executor;

    optionReader = new ExecutionOptionsReader(1, 1,
        (int) driverContainerConfig.getTaskExecutorRateMs(), null);
    executor = new PeriodicWorkExecutorImpl(optionReader, new WorkerFactory() {

      @Override
      public Worker createWorker() {
        return taskExecutor();
      }
    }, "task-executor-driver");

    return executor;
  }


  /**
   * xx.
   */
  @Bean
  public PeriodicWorkExecutorImpl serverScannerExecutor() throws Exception {
    ExecutionOptionsReader optionsReader = new ExecutionOptionsReader(1, 1,
        (int) driverContainerConfig.getServerScannerRateMs(), null);
    PeriodicWorkExecutorImpl sweepExecutor = new PeriodicWorkExecutorImpl(optionsReader,
        new WorkerFactory() {

          @Override
          public Worker createWorker() {
            ServerScanWorker worker;

            worker = new ServerScanWorker();
            worker.setAppContext(appContext());
            worker.setDcConfig(driverContainerConfig);
            worker.setDriverStoreManager(driverStoreManager());
            worker.setLaunchDriverWorkerFactory(launchDriverWorkerFactory());
            worker.setRemoveDriverWorkerFactory(removeDriverWorkerFactory());
            worker.setLocalDihEndPoint(localDihEp());
            worker.setPortContainerFactory(portContainerFactory());
            worker.setTaskExecutor(taskExecutor());
            worker.setVersionManager(versionManager());
            return worker;
          }
        }, "server-scanner");
    return sweepExecutor;
  }


  /**
   * xx.
   */
  @Bean
  public ServerScanWorker serverScanWorker() {
    ServerScanWorker worker;

    worker = new ServerScanWorker();
    worker.setAppContext(appContext());
    worker.setDcConfig(driverContainerConfig);
    worker.setDriverStoreManager(driverStoreManager());
    worker.setLaunchDriverWorkerFactory(launchDriverWorkerFactory());
    worker.setRemoveDriverWorkerFactory(removeDriverWorkerFactory());
    worker.setLocalDihEndPoint(localDihEp());
    worker.setPortContainerFactory(portContainerFactory());
    worker.setTaskExecutor(taskExecutor());
    worker.setVersionManager(versionManager());
    return worker;
  }


  /**
   * xx.
   */
  @Bean
  public PeriodicWorkExecutorImpl targetScannerExecutor() throws Exception {
    ExecutionOptionsReader optionsReader = new ExecutionOptionsReader(1, 1,
        (int) driverContainerConfig.getTargetScannerRateMs(), null);
    PeriodicWorkExecutorImpl sweepExecutor = new PeriodicWorkExecutorImpl(optionsReader,
        new WorkerFactory() {

          @Override
          public Worker createWorker() {
            TargetScanWorker worker;

            worker = new TargetScanWorker();
            worker.setAppContext(appContext());
            worker.setDcConfig(driverContainerConfig);
            worker.setDriverStoreManager(driverStoreManager());
            worker.setLaunchDriverWorkerFactory(launchDriverWorkerFactory());
            worker.setRemoveDriverWorkerFactory(removeDriverWorkerFactory());
            worker.setTaskExecutor(taskExecutor());
            worker.setIscsiTargetNameManagerFactory(iscsiTargetNameManagerFactory());
            worker.setLioNameBuilder(lioNameBuilder());
            worker.setVersionManager(versionManager());
            return worker;
          }
        }, "target-scanner");
    return sweepExecutor;
  }


  /**
   * xx.
   */
  @Bean
  public PeriodicWorkExecutorImpl scsiDeviceScanExecutor() throws Exception {
    ExecutionOptionsReader optionsReader = new ExecutionOptionsReader(1, 1,
        3000, null);
    PeriodicWorkExecutorImpl scsiDeviceScanExecutor = new PeriodicWorkExecutorImpl(optionsReader,
        new WorkerFactory() {

          @Override
          public Worker createWorker() {
            ScsiDeviceScanWorker worker;

            worker = new ScsiDeviceScanWorker();
            worker.setScsiManager(scsiManager());
            worker.setInformationCenterClientFactory(infoCenterClientFactory());
            worker.setAppContext(appContext());
            return worker;
          }
        }, "scsi-scanner");
    return scsiDeviceScanExecutor;
  }


  /**
   * xx.
   */
  @Bean
  public ExecutionOptionsReader getAccessRuleExcutionReader() {
    ExecutionOptionsReader getAccessExecutionReader = new ExecutionOptionsReader(1, 1,
        driverContainerConfig.getGetAccessRuleIntervalMs(), null);
    return getAccessExecutionReader;
  }


  /**
   * xx.
   */
  @Bean
  public PeriodicWorkExecutorImpl getAccessRuleExecutor() throws Exception {
    PeriodicWorkExecutorImpl getAccessRuleExecutor = new PeriodicWorkExecutorImpl(
        getAccessRuleExcutionReader(),
        getAccessRulesFromInfoWorkFactory(), "getAccessRuleFromInfo-worker");
    return getAccessRuleExecutor;
  }

  @Bean
  public LioNameBuilder lioNameBuilder() {
    LioNameBuilder lioNameBuilder = new LioNameBuilderImpl();
    return lioNameBuilder;
  }


  /**
   * xx.
   */
  @Bean
  public GetVolumeAccessRulesFromInfoWorkFactory getAccessRulesFromInfoWorkFactory() {
    GetVolumeAccessRulesFromInfoWorkFactory workFactory =
        new GetVolumeAccessRulesFromInfoWorkFactory();
    workFactory.setDriverStoreManager(driverStoreManager());
    workFactory.setInforCenterClientFactory(infoCenterClientFactory());
    workFactory.setVolumeAccessRuleTable(volumeAccessRuleTable());
    return workFactory;
  }


  /**
   * xx.
   */
  @Bean
  public ExecutionOptionsReader getIoLimitationExcutionReader() {
    ExecutionOptionsReader getIoLimitationExecutionReader = new ExecutionOptionsReader(1, 1,
        driverContainerConfig.getGetAccessRuleIntervalMs(), null);
    return getIoLimitationExecutionReader;
  }


  /**
   * xx.
   */
  @Bean
  public PeriodicWorkExecutorImpl getIoLimitationExecutor() throws Exception {
    PeriodicWorkExecutorImpl getIoLimitationExecutor = new PeriodicWorkExecutorImpl(
        getIoLimitationExcutionReader(),
        getIoLimitationWorkFactory(), "getIoLimitation-worker");
    return getIoLimitationExecutor;
  }


  /**
   * xx.
   */
  @Bean
  public GetIoLimitationWorkFactory getIoLimitationWorkFactory() {
    GetIoLimitationWorkFactory workFactory = new GetIoLimitationWorkFactory();
    workFactory.setInforCenterClientFactory(infoCenterClientFactory());
    workFactory.setDriver2ItsIoLimitationsTable(driver2ItsIoLimitationsTable());
    workFactory.setAppContext(appContext());
    return workFactory;
  }


  /**
   * xx.
   */
  @Bean
  public ExecutionOptionsReader updateInitiatorsAllowFileExcutionReader() {
    ExecutionOptionsReader getAccessExecutionReader = new ExecutionOptionsReader(1, 1,
        driverContainerConfig.getInitiatorsAllowMs(), null);
    return getAccessExecutionReader;
  }


  /**
   * xx.
   */
  @Bean
  public PeriodicWorkExecutorImpl updateInitiatorsAllowFileExecutor() throws Exception {
    PeriodicWorkExecutorImpl getAccessRuleExecutor = new PeriodicWorkExecutorImpl(
        updateInitiatorsAllowFileExcutionReader(), updateIscsiAccessRuleFileSweepWorkFactory(),
        "getAccessRule-worker");
    return getAccessRuleExecutor;
  }


  /**
   * xx.
   */
  @Bean
  public UpgradeDriverPoller upgradeDriverPoller() throws IOException {
    UpgradeDriverPoller upgradeDriverPoller = new UpgradeDriverPollerImpl(driverContainerConfig,
        versionManager(),
        driverStoreManager());
    return upgradeDriverPoller;
  }


  /**
   * xx.
   */
  @Bean
  public DriverUpgradeProcessor driverUpgradeProcessor() throws IOException {
    DriverUpgradeProcessor driverUpgradeProcessor = new DriverUpgradeProcessor();
    driverUpgradeProcessor.setVersionManager(versionManager());
    driverUpgradeProcessor.setUpgradeDriverPoller(upgradeDriverPoller());
    driverUpgradeProcessor.setDcConfig(driverContainerConfig);
    driverUpgradeProcessor.setDriverStoreManager(driverStoreManager());
    driverUpgradeProcessor.setLaunchDriverWorkerFactory(launchDriverWorkerFactory());
    driverUpgradeProcessor.setRemoveDriverWorkerFactory(removeDriverWorkerFactory());
    driverUpgradeProcessor.setPortContainerFactory(portContainerFactory());
    driverUpgradeProcessor.setTaskExecutor(taskExecutor());
    driverUpgradeProcessor.setDriverTaskMap(driverTaskMap());
    return driverUpgradeProcessor;
  }


  /**
   * xx.
   */
  @Bean
  public ExecutionOptionsReader submitUpgradeRequestExcutionReader() {
    ExecutionOptionsReader submitUpgradeRequestExecutionReader = new ExecutionOptionsReader(1, 1,
        driverContainerConfig.getSubmitUpgradeIntervalMs(), null);
    return submitUpgradeRequestExecutionReader;
  }


  /**
   * xx.
   */
  public SubmitUpgradeRequestWorkFactory submitUpgradeRequestWorkFactory() throws IOException {
    SubmitUpgradeRequestWorkFactory submitUpgradeRequestWorkerFactory =
        new SubmitUpgradeRequestWorkFactory();
    submitUpgradeRequestWorkerFactory.setDriverUpgradeProcessor(driverUpgradeProcessor());
    submitUpgradeRequestWorkerFactory.setDriverStoreManager(driverStoreManager());
    submitUpgradeRequestWorkerFactory.setDcConfig(driverContainerConfig);
    submitUpgradeRequestWorkerFactory.setJvmConfigManager(jvmConfigManager());
    return submitUpgradeRequestWorkerFactory;
  }


  /**
   * xx.
   */
  @Bean
  public PeriodicWorkExecutorImpl submitUpgradeRequestExecutor() throws Exception {
    PeriodicWorkExecutorImpl submitUpgradeRequestExecutor = new PeriodicWorkExecutorImpl(
        submitUpgradeRequestExcutionReader(), submitUpgradeRequestWorkFactory(),
        "submitUpgradeRequest-woker");
    return submitUpgradeRequestExecutor;
  }


  /**
   * xx.
   */
  @Bean
  public UpdateIscsiAccessRuleFileSweepWorkFactory updateIscsiAccessRuleFileSweepWorkFactory()
      throws Exception {
    UpdateIscsiAccessRuleFileSweepWorkFactory initiatorsWorkFactory =
        new UpdateIscsiAccessRuleFileSweepWorkFactory();
    initiatorsWorkFactory.setDriverContainerConfig(driverContainerConfig);
    initiatorsWorkFactory.setFilePath(lioManagerConfiguration.getSaveconfigPath());
    initiatorsWorkFactory.setIscsiTargetNameManagerFactory(iscsiTargetNameManagerFactory());
    initiatorsWorkFactory.setLioNameBuilder(lioNameBuilder());
    initiatorsWorkFactory.setDriverStoreManager(driverStoreManager());
    initiatorsWorkFactory.setInfoCenterClientFactory(infoCenterClientFactory());
    initiatorsWorkFactory.setVersionManager(versionManager());
    return initiatorsWorkFactory;
  }


  /**
   * xx.
   */
  @Bean
  public ExecutionOptionsReader reportExecutionReader() {
    ExecutionOptionsReader reportExecutionReader = new ExecutionOptionsReader(1, 1,
        driverContainerConfig.getReportIntervalMs(), null);
    return reportExecutionReader;
  }


  /**
   * xx.
   */
  @Bean
  public PeriodicWorkExecutorImpl reportExecutor() throws Exception {
    PeriodicWorkExecutorImpl reportExecutor = new PeriodicWorkExecutorImpl(reportExecutionReader(),
        reportWorkerFactory(), "report-driver-worker");
    return reportExecutor;
  }


  /**
   * xx.
   */
  @Bean
  public IscsiTargetNameManagerFactory iscsiTargetNameManagerFactory() {
    IscsiTargetNameManagerFactory iscsiTargetNameManagerFactory =
        new IscsiTargetNameManagerFactory();
    iscsiTargetNameManagerFactory.setIscsiTargetManager(lioCommandManager());
    return iscsiTargetNameManagerFactory;
  }


  /**
   * xx.
   */
  @Bean
  public ReportDriverMetadataWorkerFactory reportWorkerFactory() throws Exception {
    ReportDriverMetadataWorkerFactory reportWorkerFactory = new ReportDriverMetadataWorkerFactory();
    reportWorkerFactory.setInforCenterClientFactory(infoCenterClientFactory());
    reportWorkerFactory.setVersionManager(versionManager());
    reportWorkerFactory.setDriverStoreManager(driverStoreManager());
    reportWorkerFactory.setIscsiDriverPort(driverContainerConfig.getIscsiPort());
    reportWorkerFactory.setDriverContainerConfiguration(driverContainerConfig);
    reportWorkerFactory.setIscsiTargetNameManagerFactory(iscsiTargetNameManagerFactory());
    reportWorkerFactory.setLioNameBuilder(lioNameBuilder());
    reportWorkerFactory.setAppContext(appContext());
    reportWorkerFactory.setReportDriverClientSessionTryTimes(
        driverContainerConfig.getReportDriverClientSessionTryTimes());
    return reportWorkerFactory;
  }


  /**
   * xx.
   */
  @Bean
  public InformationCenterClientFactory infoCenterClientFactory() {
    InformationCenterClientFactory infoCenterClientFactory = new InformationCenterClientFactory(5,
        8, 20000);
    infoCenterClientFactory.getGenericClientFactory()
        .setMaxNetworkFrameSize(driverContainerConfig.getMaxNetworkFrameSize());
    infoCenterClientFactory.setInstanceStore(instanceStore);
    return infoCenterClientFactory;
  }

  @Bean
  public CoordinatorClientFactory coordinatorClientFactory() {
    CoordinatorClientFactory coordinatorClientFactory = new CoordinatorClientFactory(1, 2, 20000);
    return coordinatorClientFactory;
  }


  /**
   * xx.
   */
  @Bean
  public ExecutionOptionsReader migratingScanExecutionReader() {
    ExecutionOptionsReader migratingExecutionReader = new ExecutionOptionsReader(1, 1,
        driverContainerConfig.getMigratingScanIntervalMs(), null);
    return migratingExecutionReader;
  }


  /**
   * xx.
   */
  @Bean
  public PeriodicWorkExecutorImpl migratingScanExecutor() throws Exception {
    PeriodicWorkExecutorImpl migratingScanExecutor = new PeriodicWorkExecutorImpl(
        migratingScanExecutionReader(),
        new WorkerFactory() {
          @Override
          public Worker createWorker() {
            MigratingScanWorker worker;
            worker = new MigratingScanWorker();
            worker.setDcConfig(driverContainerConfig);
            worker.setDriverStoreManager(driverStoreManager());
            worker.setTaskExecutor(taskExecutor());
            worker.setCoordinatorClientFactory(coordinatorClientFactory());
            return worker;
          }
        }, "migration-scan-worker");
    return migratingScanExecutor;
  }


  /**
   * xx.
   */
  @Bean
  public ChangeDriverVolumeTask changeDriverVolumeTask() {
    ChangeDriverVolumeTask task = new ChangeDriverVolumeTask();
    task.setCoordinatorClientFactory(coordinatorClientFactory());
    task.setDcConfig(driverContainerConfig);
    return task;
  }
}
