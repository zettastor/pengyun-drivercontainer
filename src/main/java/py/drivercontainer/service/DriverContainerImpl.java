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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.AbstractConfigurationServer;
import py.RequestResponseHelper;
import py.app.NetworkConfiguration;
import py.app.context.AppContext;
import py.app.thrift.ThriftProcessorFactory;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.common.struct.EndPointParser;
import py.coordinator.lio.LioManager;
import py.coordinator.lio.LioNameBuilder;
import py.driver.DriverAction;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.driver.PortalType;
import py.drivercontainer.DriverContainerAppEngine;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.DriverVersion;
import py.drivercontainer.JvmConfiguration;
import py.drivercontainer.JvmConfigurationManager;
import py.drivercontainer.client.CoordinatorClientFactory;
import py.drivercontainer.driver.LaunchDriverParameters;
import py.drivercontainer.driver.store.DriverFileStore;
import py.drivercontainer.driver.store.DriverStore;
import py.drivercontainer.driver.store.DriverStoreImpl;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.drivercontainer.driver.store.exception.DuplicatedDriverException;
import py.drivercontainer.driver.upgrade.QueryServer;
import py.drivercontainer.driver.version.Version;
import py.drivercontainer.driver.version.VersionException;
import py.drivercontainer.driver.version.VersionManager;
import py.drivercontainer.driver.workspace.DriverWorkspaceManager;
import py.drivercontainer.driver.workspace.DriverWorkspaceManagerImpl;
import py.drivercontainer.exception.NoAvailablePortException;
import py.drivercontainer.scsi.ScsiManager;
import py.drivercontainer.service.taskqueue.TaskExecutor;
import py.drivercontainer.utils.DriverContainerUtils;
import py.drivercontainer.utils.PerformanceCsvParaser;
import py.drivercontainer.worker.ChangeDriverVolumeTask;
import py.drivercontainer.worker.LaunchDriverWorker;
import py.drivercontainer.worker.LaunchDriverWorkerFactory;
import py.drivercontainer.worker.RemoveDriverWorker;
import py.drivercontainer.worker.RemoveDriverWorkerFactory;
import py.exception.GenericThriftClientFactoryException;
import py.icshare.DriverKey;
import py.infocenter.client.InformationCenterClientFactory;
import py.informationcenter.AccessPermissionType;
import py.instance.InstanceId;
import py.performance.PerformanceParameter;
import py.thrift.coordinator.service.Coordinator;
import py.thrift.drivercontainer.service.DriverContainer;
import py.thrift.drivercontainer.service.FailedToApplyIscsiAccessRulesExceptionThrift;
import py.thrift.drivercontainer.service.FailedToApplyVolumeAccessRulesExceptionThrift;
import py.thrift.drivercontainer.service.FailedToCancelIscsiAccessRulesExceptionThrift;
import py.thrift.drivercontainer.service.FailedToCancelVolumeAccessRulesExceptionThrift;
import py.thrift.icshare.ReportDriverMetadataRequest;
import py.thrift.icshare.ReportDriverMetadataResponse;
import py.thrift.share.AccessPermissionTypeThrift;
import py.thrift.share.ApplyIscsiAccessRulesRequest;
import py.thrift.share.ApplyIscsiAccessRulesResponse;
import py.thrift.share.ApplyVolumeAccessRulesRequest;
import py.thrift.share.ApplyVolumeAccessRulesResponse;
import py.thrift.share.CancelIscsiAccessRulesRequest;
import py.thrift.share.CancelIscsiAccessRulesResponse;
import py.thrift.share.CancelVolumeAccessRulesRequest;
import py.thrift.share.CancelVolumeAccessRulesResponse;
import py.thrift.share.ChangeDriverBoundVolumeFailedThrift;
import py.thrift.share.ChangeDriverBoundVolumeRequest;
import py.thrift.share.ChangeDriverBoundVolumeResponse;
import py.thrift.share.ConnectPydDeviceOperationExceptionThrift;
import py.thrift.share.CreateBackstoresOperationExceptionThrift;
import py.thrift.share.CreateLoopbackLunsOperationExceptionThrift;
import py.thrift.share.CreateLoopbackOperationExceptionThrift;
import py.thrift.share.DriverFromRequestNotFoundExceptionThrift;
import py.thrift.share.DriverIpTargetThrift;
import py.thrift.share.DriverIsLaunchingExceptionThrift;
import py.thrift.share.DriverIsUpgradingExceptionThrift;
import py.thrift.share.DriverKeyThrift;
import py.thrift.share.DriverLaunchFailedExceptionThrift;
import py.thrift.share.DriverMetadataThrift;
import py.thrift.share.DriverNameExistsExceptionThrift;
import py.thrift.share.DriverTypeIsConflictExceptionThrift;
import py.thrift.share.DriverTypeThrift;
import py.thrift.share.ExistsClientExceptionThrift;
import py.thrift.share.ExistsDriverExceptionThrift;
import py.thrift.share.FailedToUmountDriverExceptionThrift;
import py.thrift.share.GetDriverConnectPermissionRequestThrift;
import py.thrift.share.GetDriverConnectPermissionResponseThrift;
import py.thrift.share.GetIoLimitationByDriverRequestThrift;
import py.thrift.share.GetIoLimitationByDriverResponseThrift;
import py.thrift.share.GetPerformanceFromPyMetricsResponseThrift;
import py.thrift.share.GetPerformanceParameterRequestThrift;
import py.thrift.share.GetPerformanceParameterResponseThrift;
import py.thrift.share.GetScsiDeviceOperationExceptionThrift;
import py.thrift.share.IoLimitationThrift;
import py.thrift.share.IscsiAccessRuleThrift;
import py.thrift.share.LaunchDriverRequestThrift;
import py.thrift.share.LaunchDriverResponseThrift;
import py.thrift.share.ListIscsiAccessRulesByDriverKeysRequest;
import py.thrift.share.ListIscsiAccessRulesByDriverKeysResponse;
import py.thrift.share.ListIscsiAppliedAccessRulesRequestThrift;
import py.thrift.share.ListIscsiAppliedAccessRulesResponseThrift;
import py.thrift.share.ListVolumeAccessRulesByVolumeIdsRequest;
import py.thrift.share.ListVolumeAccessRulesByVolumeIdsResponse;
import py.thrift.share.MountScsiDeviceRequest;
import py.thrift.share.MountScsiDeviceResponse;
import py.thrift.share.NoEnoughPydDeviceExceptionThrift;
import py.thrift.share.ReadPerformanceParameterFromFileExceptionThrift;
import py.thrift.share.ServiceHavingBeenShutdownThrift;
import py.thrift.share.ServiceIsNotAvailableThrift;
import py.thrift.share.SetIscsiChapControlRequestThrift;
import py.thrift.share.SetIscsiChapControlResponseThrift;
import py.thrift.share.SystemCpuIsNotEnoughThrift;
import py.thrift.share.SystemMemoryIsNotEnoughThrift;
import py.thrift.share.UmountDriverRequestThrift;
import py.thrift.share.UmountDriverResponseThrift;
import py.thrift.share.UmountScsiDeviceRequest;
import py.thrift.share.UmountScsiDeviceResponse;
import py.thrift.share.UnknownIpv4HostExceptionThrift;
import py.thrift.share.UnknownIpv6HostExceptionThrift;
import py.thrift.share.VolumeAccessRuleThrift;
import py.thrift.share.VolumeHasNotBeenLaunchedExceptionThrift;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;


public class DriverContainerImpl extends AbstractConfigurationServer implements
    DriverContainer.Iface, ThriftProcessorFactory {

  private static final Logger logger = LoggerFactory.getLogger(DriverContainerImpl.class);
  private final DriverContainer.Processor<DriverContainer.Iface> processor;
  private InformationCenterClientFactory informationCenterClientFactory;
  private CoordinatorClientFactory coordinatorClientFactory;
  private LaunchDriverWorkerFactory launchDriverWorkerFactory;
  private RemoveDriverWorkerFactory removeDriverWorkerFactory;
  private DriverContainerAppEngine driverContainerAppEngine;
  private AppContext appContext;
  private DriverContainerConfiguration driverContainerConfig;
  private TaskExecutor taskExecutor;
  private BootStrap bootStrap;
  private JvmConfigurationManager jvmConfigManager;
  private PortContainerFactory portContainerFactory;
  private PerformanceCsvParaser performanceCsvParaser;
  private NetworkConfiguration networkConfiguration;
  private Map<Long, List<VolumeAccessRuleThrift>> volumeAccessRuleTable;
  private Map<DriverKeyThrift, List<IscsiAccessRuleThrift>> iscsiAccessRuleTable;
  private DriverWorkspaceManager driverWorkspaceManager;
  private Map<DriverKeyThrift, List<IoLimitationThrift>> driver2ItsIoLimitationsTable;
  private LioManager lioManager;
  private LioNameBuilder lioNameBuilder;
  private ChangeDriverVolumeTask changeDriverVolumeTask;
  // shutdown flag
  private boolean shutDownFlag = false;
  private DriverContainerAppEngine appEngine;
  private DriverStoreManager driverStoreManager;
  private VersionManager versionManager;
  private ScsiManager scsiManager;

  public DriverContainerImpl(AppContext appContext) throws VersionException {
    processor = new DriverContainer.Processor<DriverContainer.Iface>(this);
    this.appContext = appContext;
  }

  @Override
  public TProcessor getProcessor() {
    return processor;
  }

  public void setNetworkConfiguration(NetworkConfiguration networkConfiguration) {
    this.networkConfiguration = networkConfiguration;
  }

  public PerformanceCsvParaser getPerformanceCsvParaser() {
    return performanceCsvParaser;
  }

  public void setPerformanceCsvParaser(PerformanceCsvParaser performanceCsvParaser) {
    this.performanceCsvParaser = performanceCsvParaser;
  }

  public JvmConfigurationManager getJvmConfigManager() {
    return jvmConfigManager;
  }

  public void setJvmConfigManager(JvmConfigurationManager jvmConfigManager) {
    this.jvmConfigManager = jvmConfigManager;
  }

  public InformationCenterClientFactory getInformationCenterClientFactory() {
    return informationCenterClientFactory;
  }

  public void setInformationCenterClientFactory(
      InformationCenterClientFactory informationCenterClientFactory) {
    this.informationCenterClientFactory = informationCenterClientFactory;
  }

  public ChangeDriverVolumeTask getChangeDriverVolumeTask() {
    return changeDriverVolumeTask;
  }

  public void setChangeDriverVolumeTask(ChangeDriverVolumeTask changeDriverVolumeTask) {
    this.changeDriverVolumeTask = changeDriverVolumeTask;
  }

  @Override
  public void ping() throws TException {
    logger.trace("pinged by a remote instance");
  }

  @Override
  public LaunchDriverResponseThrift launchDriver(LaunchDriverRequestThrift request)
      throws TException {
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
    logger.warn("{}", request);
    logger.warn("1 current time : {}", df.format(new Date()));
    if (shutDownFlag) {
      logger
          .warn("Can not deal with any request due to service driver container being shutdown ...");
      throw new ServiceHavingBeenShutdownThrift();
    }
    DriverKey driverKey;
    driverKey = new DriverKey(appContext.getInstanceId().getId(), request.getVolumeId(),
        request.getSnapshotId(),
        DriverType.valueOf(request.getDriverType().name()));

    DriverType driverType = DriverType.findByName(request.getDriverType().name());

    Version currentVersion = null;
    Version latestVersion = null;
    boolean onMigration = false;
    logger.info("driver type is :{} in launch", driverType);
    switch (driverType) {
      case NBD:
      case ISCSI:
        if (DriverVersion.isOnMigration.get(DriverType.NBD) == null) {
          try {
            versionManager.lockVersion(DriverType.NBD);
            DriverVersion.isOnMigration
                .put(DriverType.NBD, versionManager.isOnMigration(DriverType.NBD));
            versionManager.unlockVersion(DriverType.NBD);
          } catch (VersionException e) {
            logger.info("Catch an exception when get driverType:{} version file", driverType, e);
          }
        }
        currentVersion = DriverVersion.currentVersion.get(DriverType.NBD);
        latestVersion = DriverVersion.latestVersion.get(DriverType.NBD);
        onMigration = DriverVersion.isOnMigration.get(DriverType.NBD);
        break;
      default:
        logger.error("DriverType :{} is not supported", driverType);
    }

    logger
        .info("Launch currentVersion is :{} ,latestVersion is :{}", currentVersion, latestVersion);

    // if driver is upgrading ,can not launch driver
    if (currentVersion != null && !currentVersion.equals(latestVersion) && onMigration) {
      logger.error("Driver is upgrading,can not launch driver");
      throw new DriverIsUpgradingExceptionThrift();
    }

    if (driverStoreManager.get(currentVersion) == null) {
      String varPath = driverContainerConfig.getDriverAbsoluteRootPath();
      DriverStore driverStore = new DriverStoreImpl(Paths.get(varPath), currentVersion);
      driverStoreManager.put(currentVersion, driverStore);
    }

    DriverStore currentDriverStore;
    currentDriverStore = driverStoreManager.get(currentVersion);

    if ((currentDriverStore.get(driverKey) != null)) {
      logger.info("Request to launch driver has accepted before, do nothing ...");
      throw new ExistsDriverExceptionThrift();
    }

    JvmConfiguration jvmConfig = jvmConfigManager.getJvmConfig(driverType);

    long freeMem = DriverContainerUtils.getSysFreeMem();
    long coordinateJvmMinMem = DriverContainerUtils
        .getBytesSizeFromString(jvmConfig.getMinMemPoolSize());

    logger.warn("free mem:{},jvm mem:{}", freeMem, coordinateJvmMinMem);
    if (freeMem - coordinateJvmMinMem < driverContainerConfig.getSystemMemoryForceReserved()) {
      logger.info(
          "free memory in system is not enough for a new driver,current free mem: {}Mb,new "
              + "coordinator need {}Mb,then left memory is less than forced reserverd {}Mb",
          freeMem / (1024 * 1024), coordinateJvmMinMem / (1024 * 1024),
          driverContainerConfig.getSystemMemoryForceReserved() / (1024 * 1024));
      throw new SystemMemoryIsNotEnoughThrift();
    }

    if (driverType == DriverType.ISCSI) {
      if (!availableCpuForIscsiTarget(currentDriverStore)) {
        throw new SystemCpuIsNotEnoughThrift();
      }
    }

    logger.info("Start a worker to launch driver:{}", request);

    String network;
    String ifaceName = null;
    String hostname;
    String ipv6Addr = null;
    PortalType portalType = PortalType.IPV4;
    NetworkInterface networkIface;
    String queryServerHostname;

    try {
      if (networkConfiguration.isEnableDataDepartFromControl()) {
        network = networkConfiguration.getOutwardFlowSubnet();
      } else {
        network = networkConfiguration.getControlFlowSubnet();
      }
      networkIface = EndPointParser.getLocalHostLanInterface(network);

      ifaceName = networkIface.getName();
      hostname = EndPointParser.getIpv4Addr(network, networkIface).getHostAddress();
      if (driverType == DriverType.ISCSI
          && driverContainerConfig.parseIscsiPortalType() == PortalType.IPV6) {
        int indexOfPercent;

        portalType = PortalType.IPV6;
        ipv6Addr = EndPointParser
            .getIpv6Addr(driverContainerConfig.getIscsiPortalCidrNetwork(), networkIface)
            .getHostAddress();
        if ((indexOfPercent = ipv6Addr.indexOf("%")) != -1) {
          ipv6Addr = ipv6Addr.substring(0, indexOfPercent);
        }
      }

      queryServerHostname = EndPointParser
          .getLocalHostLanAddress(networkConfiguration.getControlFlowSubnet())
          .getHostAddress();
    } catch (UnknownHostException e) {
      if (e instanceof EndPointParser.UnknownIpv6HostException) {
        logger.error("Unable to parse IP address to launch driver (IPV6).", e);
        throw new UnknownIpv6HostExceptionThrift();
      }
      logger.error("Unable to parse IP address to launch driver (IPV4).", e);
      throw new UnknownIpv4HostExceptionThrift();
    } catch (Exception e) {
      logger.error("caught an exception", e);
      throw new DriverLaunchFailedExceptionThrift();
    }

    LaunchDriverParameters launchDriverParameters = new LaunchDriverParameters();
    launchDriverParameters.setDriverContainerInstanceId(appContext.getInstanceId().getId());
    launchDriverParameters.setAccountId(request.getAccountId());
    launchDriverParameters.setVolumeId(request.getVolumeId());
    launchDriverParameters.setDriverName(request.getDriverName());
    launchDriverParameters.setQueryServerIp(queryServerHostname);
    launchDriverParameters.setQueryServerPort(QueryServer.QueryServer_Port);
    launchDriverParameters.setSnapshotId(request.getSnapshotId());
    launchDriverParameters.setDriverType(driverType);
    // use this hostname to build driver and replace this before launching driver if the type of
    // driver is ISCSI and
    // driver server ip is set in configuration.
    launchDriverParameters.setHostName(hostname);
    launchDriverParameters.setPortalType(portalType);
    launchDriverParameters.setIpv6Addr(ipv6Addr);
    launchDriverParameters.setNicName(ifaceName);
    launchDriverParameters.setPort(0);
    launchDriverParameters.setInstanceId(RequestIdBuilder.get());
    launchDriverParameters.setDriverIp(hostname);

    logger.info("Driver launching parameters:{}", launchDriverParameters);

    /*
     * Init server port
     */
    PortContainer portContainer;
    portContainer = portContainerFactory.getPortContainer(launchDriverParameters.getDriverType());
    // port 0 is not allowed and is used to set default value of driver
    if (launchDriverParameters.getPort() == 0) {
      // get an available port from port container
      try {
        launchDriverParameters.setPort(portContainer.getAvailablePort());
      } catch (NoAvailablePortException e) {
        logger.error("Unable to find any available port for driver {}", driverType.name());
        throw new DriverLaunchFailedExceptionThrift();
      }
    }

    // Init coordinator port
    if (launchDriverParameters.getCoordinatorPort() == 0) {
      launchDriverParameters.setCoordinatorPort(
          launchDriverParameters.getPort() + driverContainerConfig.getCoordinatorBasePort());
    }

    if ((currentDriverStore.get(driverKey) != null)) {
      logger.info("Request to launch driver has accepted before, do nothing ...");
      throw new ExistsDriverExceptionThrift();
    }

    DriverMetadata driver = launchDriverParameters.buildDriver();
    driver.setDriverStatus(DriverStatus.LAUNCHING);

    // in drivercontainer
    // when launch driver set default chap control to 1
    driver.setChapControl(1);

    //Use synchronize to avoid  multi process get drivers from currentDriverStore at the same time
    synchronized (currentDriverStore) {
      List<DriverMetadata> driverMetadataList = currentDriverStore.list();
      for (DriverMetadata driverInStore : driverMetadataList) {
        if (driver.getDriverName() != null && driver.getDriverName()
            .equals(driverInStore.getDriverName())) {
          logger.error(
              "volumeId {} has driverType {} on it , driverType {} from request is conflict"
                  + " with it",
              driverInStore.getVolumeId(), driverInStore.getDriverType(), driver.getDriverType());
          throw new DriverNameExistsExceptionThrift();
        }

        if (driverInStore.getVolumeId() == driver.getVolumeId()
            && driverInStore.getSnapshotId() == driver.getSnapshotId()
            && conflictDriverType(driverInStore.getDriverType(), driver.getDriverType())) {
          logger.error(
              "volumeId {} has driverType {} on it , driverType {} from request is conflict "
                  + "with it",
              driverInStore.getVolumeId(), driverInStore.getDriverType(), driver.getDriverType());
          throw new DriverTypeIsConflictExceptionThrift();
        }
      }
      try {
        ((DriverFileStore) currentDriverStore).addOrFailImmediately(driver);
      } catch (DuplicatedDriverException e) {
        logger.error("Caught an exception", e);
        throw new ExistsDriverExceptionThrift();
      } catch (IOException e) {
        logger.error("Caught an exception", e);
        throw new FailedToUmountDriverExceptionThrift();
      }
    }

    // replace hostname before launching driver
    if (driverType == DriverType.ISCSI && !driverContainerConfig.getDriverServerIp().isEmpty()) {
      launchDriverParameters.setHostName(driverContainerConfig.getDriverServerIp());
    } else {
      launchDriverParameters.setHostName(hostname);
    }
    launchDriverWorkerFactory.setLaunchDriverParameters(launchDriverParameters);
    launchDriverWorkerFactory.setVersion(currentDriverStore.getVersion());

    logger.debug("Launching driver parameters {}", launchDriverParameters);
    LaunchDriverWorker worker = (LaunchDriverWorker) launchDriverWorkerFactory.createWorker();
    if (worker == null) {
      logger.error("Unable to create a worker to launch driver {}",
          launchDriverParameters.buildDriver());
      throw new DriverLaunchFailedExceptionThrift();
    }
    if (launchDriverParameters.getDriverType() == DriverType.ISCSI) {
      // if driver type is iscsi,should set flag true to create iscsi target.
      worker.setCreateIscsiTargetNameFlag(true);
      worker.setCoordinatorIsAlive(false);
    }

    // going to execute a coordinator process by specified worker
    taskExecutor
        .submit(driverKey, DriverAction.START_SERVER, worker, currentDriverStore.getVersion());

    LaunchDriverResponseThrift launchDriverResponseThrift = new LaunchDriverResponseThrift();
    launchDriverResponseThrift.setRequestId(request.getRequestId());
    launchDriverResponseThrift.addToRelatedDriverContainerIds(appContext.getInstanceId().getId());
    logger.warn("2 current time : {}", df.format(new Date()));
    return launchDriverResponseThrift;
  }

  @Override
  public MountScsiDeviceResponse mountScsiDevice(MountScsiDeviceRequest request)
      throws ConnectPydDeviceOperationExceptionThrift, CreateBackstoresOperationExceptionThrift,
      CreateLoopbackOperationExceptionThrift, CreateLoopbackLunsOperationExceptionThrift,
      GetScsiDeviceOperationExceptionThrift, NoEnoughPydDeviceExceptionThrift {
    logger.warn("mountScsiDevice volumeId {}, snapshotId {}, ip {}",
        request.getVolumeId(), request.getSnapshotId(), request.getDriverIp());
    try {
      scsiManager
          .mapScsiDevice(request.getVolumeId(), request.getSnapshotId(), request.getDriverIp());
    } catch (Exception e) {
      throw e;
    }
    MountScsiDeviceResponse response = new MountScsiDeviceResponse();
    response.setRequestId(request.getRequestId());
    response.setDriverContainerIdForScsi(appContext.getInstanceId().getId());
    return response;
  }

  @Override
  public synchronized UmountScsiDeviceResponse umountScsiDevice(UmountScsiDeviceRequest request) {
    logger.warn("umountScsiDevice volumeId {}, snapshotId {}, ip {}",
        request.getVolumeId(), request.getSnapshotId(), request.getDriverIp());
    if (!scsiManager
        .unmapScsiDevice(request.getVolumeId(), request.snapshotId, request.getDriverIp())) {
      logger.error("fail to umount.");
    }
    UmountScsiDeviceResponse response = new UmountScsiDeviceResponse();
    response.setRequestId(request.getRequestId());
    return response;
  }


  /**
   * xx.
   */
  public boolean conflictDriverType(DriverType exitType, DriverType newType) {
    if (exitType == DriverType.ISCSI && newType == DriverType.NBD) {
      return true;
    } else if (exitType == DriverType.ISCSI && newType == DriverType.ISCSI) {
      return true;
    } else if (exitType == DriverType.NBD && newType == DriverType.ISCSI) {
      return true;
    } else if (exitType == DriverType.NBD && newType == DriverType.NBD) {
      return true;
    }
    return false;

  }

  @Override
  public UmountDriverResponseThrift umountDriver(UmountDriverRequestThrift request)
      throws TException {
    logger.warn("umount driver for volume {}, request: {}", request.getVolumeId(), request);

    if (shutDownFlag) {
      logger.warn(
          "Cannot deal with any request due to service driver container is being shutdown ...");
      throw new ServiceHavingBeenShutdownThrift();
    }

    List<DriverIpTargetThrift> removeFailedDriverList = new ArrayList<>();

    if (request.getDriverIpTargetList() == null) {
      logger.warn("No detail information about driver with volume id {}.", request.getVolumeId());
      return new UmountDriverResponseThrift(request.getRequestId(), null);
    }

    List<DriverType> driverTypeList = new ArrayList<>();
    for (DriverIpTargetThrift driverTarget : request.getDriverIpTargetList()) {
      driverTypeList.add(DriverType.findByValue(driverTarget.getDriverType().getValue()));
    }

    for (DriverType driverType : driverTypeList) {
      Version currentVersion = null;
      Version latestVersion = null;
      boolean onMigration = false;
      switch (driverType) {
        case NBD:
        case ISCSI:
          currentVersion = DriverVersion.currentVersion.get(DriverType.NBD);
          latestVersion = DriverVersion.latestVersion.get(DriverType.NBD);
          onMigration = DriverVersion.isOnMigration.get(DriverType.NBD);
          break;
        default:
          logger.error("Driver type {} is not supported", driverType);
          continue;
      }

      if (currentVersion == null || driverStoreManager.get(currentVersion) == null) {
        logger.info("No driver with type {} is launched", driverType);
        continue;
      }
      if (getDriverKeyList(request, driverType) != null
          && getDriverKeyList(request, driverType).size() != 0) {
        if (!currentVersion.equals(latestVersion) && onMigration) {
          logger.warn("Driver is upgrading,can not remove driver");
          throw new DriverIsUpgradingExceptionThrift();
        } else {
          DriverStore umountDriverStore;

          umountDriverStore = driverStoreManager.get(currentVersion);
          removeFailedDriverList.addAll(removeDriverByVersion(getDriverKeyList(request, driverType),
              currentVersion, umountDriverStore));
        }
      }
    }
    return new UmountDriverResponseThrift(request.getRequestId(), removeFailedDriverList);
  }

  /**
   * When volume migrate on line , use the method to change driver which launched on the volume
   * volumeId.
   */
  @Override
  public ChangeDriverBoundVolumeResponse changeDriverBoundVolume(
      ChangeDriverBoundVolumeRequest request)
      throws ServiceHavingBeenShutdownThrift, TException, ChangeDriverBoundVolumeFailedThrift {
    logger.info("ChangeDriverBoundVolumeRequest {}", request);
    if (shutDownFlag) {
      logger.warn(
          "Cannot deal with any request due to service driver container is being shutdown ...");
      throw new ServiceHavingBeenShutdownThrift();
    }
    DriverKey oldDriverKey = RequestResponseHelper.buildDriverKeyFrom(request.getDriver());
    DriverType driverType = oldDriverKey.getDriverType();
    Version currentVersion = null;
    Version latestVersion = null;
    boolean onMigration = false;
    logger.info("driver type is :{} from request", driverType);
    switch (driverType) {
      case NBD:
      case ISCSI:
        if (DriverVersion.isOnMigration.get(DriverType.NBD) == null) {
          try {
            versionManager.lockVersion(DriverType.NBD);
            DriverVersion.isOnMigration
                .put(DriverType.NBD, versionManager.isOnMigration(DriverType.NBD));
            versionManager.unlockVersion(DriverType.NBD);
          } catch (VersionException e) {
            logger.warn("Catch an exception when get driverType:{} version file", driverType, e);
          }
        }
        currentVersion = DriverVersion.currentVersion.get(DriverType.NBD);
        latestVersion = DriverVersion.latestVersion.get(DriverType.NBD);
        onMigration = DriverVersion.isOnMigration.get(DriverType.NBD);
        break;
      default:
        logger.error("DriverType :{} is not supported", driverType);
    }
    logger.info("currentVersion is :{} ,latestVersion is :{}", currentVersion, latestVersion);
    // if driver is upgrading ,can not change it volume
    if (currentVersion != null && !currentVersion.equals(latestVersion) && onMigration) {
      logger.error("Driver is upgrading,can not launch driver");
      throw new DriverIsUpgradingExceptionThrift();
    }

    if (driverStoreManager.get(currentVersion) == null) {
      logger.error("Driver from request can not found in driverStore");
      throw new DriverFromRequestNotFoundExceptionThrift();
    }

    DriverStore currentDriverStore = driverStoreManager.get(currentVersion);

    if ((currentDriverStore.get(oldDriverKey) == null)) {
      logger.error("Driver from request can not found in driverStore");
      throw new DriverFromRequestNotFoundExceptionThrift();
    }

    DriverMetadata oldDriver = currentDriverStore.get(oldDriverKey);
    oldDriver.setDriverStatus(DriverStatus.MIGRATING);
    oldDriver.setMigratingVolumeId(request.getNewVolumeId());
    if (!currentDriverStore.save(oldDriver)) {
      throw new ChangeDriverBoundVolumeFailedThrift();
    }

    changeDriverVolumeTask.setDriverKey(oldDriverKey);
    changeDriverVolumeTask.setDriverStore(currentDriverStore);
    taskExecutor.submit(oldDriverKey, DriverAction.CHANGE_VOLUME, changeDriverVolumeTask,
        currentDriverStore.getVersion());

    ChangeDriverBoundVolumeResponse response = new ChangeDriverBoundVolumeResponse();
    response.setRequestId(request.getRequestId());
    logger.info("changeDriverBoundVolume response: {}", response);
    return response;
  }

  private List<DriverKey> getDriverKeyList(UmountDriverRequestThrift request,
      DriverType driverType) {
    List<DriverKey> driverKeyList = new ArrayList<>();
    for (DriverIpTargetThrift driverTarget : request.getDriverIpTargetList()) {
      DriverKey driverKey = new DriverKey(appContext.getInstanceId().getId(), request.getVolumeId(),
          driverTarget.getSnapshotId(),
          DriverType.findByValue(driverTarget.getDriverType().getValue()));
      if (driverKey.getDriverType() == driverType) {
        driverKeyList.add(driverKey);
      }
    }

    return driverKeyList;
  }

  private List<DriverIpTargetThrift> removeDriverByVersion(List<DriverKey> driverKeyList,
      Version version,
      DriverStore driverStore)
      throws ExistsClientExceptionThrift, DriverIsLaunchingExceptionThrift {
    List<DriverIpTargetThrift> removeFailedDriverList = new ArrayList<>();
    DriverMetadata driver;
    List<DriverMetadata> existsClientDriverList = new ArrayList<>();
    List<DriverMetadata> isLaunchingDriverList = new ArrayList<>();
    for (Iterator<DriverKey> iterator = driverKeyList.iterator(); iterator.hasNext(); ) {
      DriverKey driverKey = iterator.next();
      driver = driverStore.get(driverKey);
      if (driver == null) {
        logger.warn("No such driver with volume id {}, snapshot id {}", driverKey.getVolumeId(),
            driverKey.getSnapshotId());
        iterator.remove();
        continue;
      }

      if (driver.getClientHostAccessRule() != null && !driver.getClientHostAccessRule().isEmpty()) {
        existsClientDriverList.add(driver);
        continue;
      }

      if (driver.getDriverStatus() == DriverStatus.LAUNCHING) {
        isLaunchingDriverList.add(driver);
      }
    }
    if (existsClientDriverList != null && !existsClientDriverList.isEmpty()) {
      logger.warn("Unable to umount driver due to exists clients using drivers {}",
          existsClientDriverList);
      throw new ExistsClientExceptionThrift();
    }

    if (!isLaunchingDriverList.isEmpty()) {
      logger.error("Unable to umount launching drivers {}", isLaunchingDriverList);
      throw new DriverIsLaunchingExceptionThrift();
    }

    Map<DriverKey, Future<?>> futureTable = new HashMap<>();
    for (DriverKey driverKey : driverKeyList) {
      removeDriverWorkerFactory.setVersion(version);
      removeDriverWorkerFactory.setUpgrade(false);
      RemoveDriverWorker removeDriverWorker = removeDriverWorkerFactory.createWorker(driverKey);

      Future<?> future = taskExecutor
          .acceptAndSubmit(driverKey, DriverStatus.REMOVING, DriverAction.REMOVE,
              removeDriverWorker, version);
      if (future == null) {
        logger.error("Unable to submit task to remove driver {}", driverKey);
        DriverIpTargetThrift driverIpTarget = new DriverIpTargetThrift();
        driverIpTarget.setDriverContainerId(driverKey.getDriverContainerId());
        driverIpTarget.setSnapshotId(driverKey.getSnapshotId());
        driverIpTarget
            .setDriverType(DriverTypeThrift.findByValue(driverKey.getDriverType().getValue()));
        removeFailedDriverList.add(driverIpTarget);
        continue;
      }

      futureTable.put(driverKey, future);
      logger.debug("futureTable is:{}", futureTable);
    }

    for (Map.Entry<DriverKey, Future<?>> entry : futureTable.entrySet()) {
      Future<?> future = entry.getValue();
      try {
        logger.debug("start wait for remove driver");
        future
            .get(driverContainerConfig.getDriverOperationTimeout() + 30000, TimeUnit.MILLISECONDS);
        logger.debug("finished wait for remove driver");
      } catch (Exception e) {
        logger.error("Caught an exception", e);

        DriverIpTargetThrift driverIpTarget = new DriverIpTargetThrift();
        driverIpTarget.setDriverContainerId(entry.getKey().getDriverContainerId());
        driverIpTarget.setSnapshotId(entry.getKey().getSnapshotId());
        driverIpTarget
            .setDriverType(DriverTypeThrift.findByValue(entry.getKey().getDriverType().getValue()));
        removeFailedDriverList.add(driverIpTarget);
      }
    }
    return removeFailedDriverList;

  }

  public DriverContainerAppEngine getDriverContainerAppEngine() {
    return driverContainerAppEngine;
  }

  public void setDriverContainerAppEngine(DriverContainerAppEngine driverContainerAppEngine) {
    this.driverContainerAppEngine = driverContainerAppEngine;
  }

  /**
   * get the IOPS and Throughput of the selected volume in the past one hour.
   */
  @Override
  public GetPerformanceFromPyMetricsResponseThrift pullPerformanceFromPyMetrics(
      py.thrift.share.GetPerformanceParameterRequestThrift request)
      throws TException {
    logger.warn("{}", request);
    DriverType driverType = DriverType.valueOf(request.getDriverType().name());
    Version version = DriverVersion.currentVersion.get(driverType);

    if (version == null || driverStoreManager.get(version) == null) {
      logger.warn("volume:{} has not benn launched", request.getVolumeId());
      throw new VolumeHasNotBeenLaunchedExceptionThrift();
    }

    DriverKey driverKey = new DriverKey(appContext.getInstanceId().getId(), request.getVolumeId(),
        request.getSnapshotId(), DriverType.valueOf(request.getDriverType().name()));
    DriverMetadata driverMetadata;
    driverMetadata = driverStoreManager.get(version).get(driverKey);

    if (driverMetadata == null) {
      logger.warn("volume:{} has not benn launched", request.getVolumeId());
      throw new VolumeHasNotBeenLaunchedExceptionThrift();
    }
    return performanceCsvParaser.readPerformances(request.getVolumeId());
  }

  @Override
  public GetPerformanceParameterResponseThrift pullPerformanceParameter(
      GetPerformanceParameterRequestThrift request) throws VolumeHasNotBeenLaunchedExceptionThrift,
      ReadPerformanceParameterFromFileExceptionThrift, TException {
    logger.info("{}", request);
    DriverType driverType = DriverType.valueOf(request.getDriverType().name());
    Version version = DriverVersion.currentVersion.get(driverType);

    if (version == null || driverStoreManager.get(version) == null) {
      logger.warn("volume:{} has not benn launched", request.getVolumeId());
      throw new VolumeHasNotBeenLaunchedExceptionThrift();
    }
    DriverKey driverKey = new DriverKey(appContext.getInstanceId().getId(), request.getVolumeId(),
        request.getSnapshotId(), DriverType.valueOf(request.getDriverType().name()));
    driverWorkspaceManager = new DriverWorkspaceManagerImpl(
        new InstanceId(driverKey.getDriverContainerId()),
        driverContainerConfig.getDriverAbsoluteRootPath(),
        driverContainerConfig.getLibraryRootPath());
    logger.warn("try to get driver by:{}", driverKey);
    DriverMetadata driverMetadata;
    driverMetadata = driverStoreManager.get(version).get(driverKey);
    if (driverMetadata == null) {
      logger.warn("volume:{} has not benn launched", request.getVolumeId());
      throw new VolumeHasNotBeenLaunchedExceptionThrift();
    }

    String performanceName = null;
    switch (driverMetadata.getDriverType()) {
      case ISCSI:
        performanceName = DriverType.NBD.name();
        break;
      case NBD:
        performanceName = DriverType.NBD.name();
        break;
      case JSCSI:
        performanceName = DriverType.JSCSI.name();
        break;
      default:
        logger.error("not supported driver type");
        break;
    }
    String workingPath = driverWorkspaceManager.getWorkspace(version, driverKey).getPath();
    // String filePath = Paths.get(System.getProperty("user.dir"), "var", subPath, performanceName,
    String filePath = Paths.get(workingPath, performanceName, Long.toString(request.getVolumeId()))
        .toString();
    File file = new File(filePath);

    if (!file.exists()) {
      logger.warn("can not find the file {} for getting the performance parameter", filePath);
      throw new ReadPerformanceParameterFromFileExceptionThrift();
    }

    // compare the modify time and only the time has been been updated, then
    // get the new information
    PerformanceParameter performanceParameter;
    FileChannel fileChannel = null;
    FileLock fileLock = null;
    RandomAccessFile randomAccesssFile = null;
    GetPerformanceParameterResponseThrift response = new GetPerformanceParameterResponseThrift();

    try {
      randomAccesssFile = new RandomAccessFile(filePath, "rw");
      fileChannel = randomAccesssFile.getChannel();
      int times = 10;
      while (fileLock == null && times-- > 0) {
        try {
          fileLock = fileChannel.tryLock();
        } catch (IOException e) {
          logger.warn("catch an exception when get performance file lock,try again!");
        }

        if (fileLock == null) {
          Thread.sleep(50);
        }
      }

      byte[] context = new byte[(int) randomAccesssFile.length()];
      randomAccesssFile.readFully(context);

      performanceParameter = PerformanceParameter
          .fromJson(new String(context, Charset.defaultCharset()));
      response.setReadCounter(performanceParameter.getReadCounter());
      response.setReadDataSizeBytes(performanceParameter.getReadDataSizeBytes());
      response.setReadLatencyNs(performanceParameter.getReadLatencyNs());
      response.setWriteCounter(performanceParameter.getWriteCounter());
      response.setWriteDataSizeBytes(performanceParameter.getWriteDataSizeBytes());
      response.setWriteLatencyNs(performanceParameter.getWriteLatencyNs());
      response.setRecordTimeIntervalMs(performanceParameter.getRecordTimeIntervalMs());
      response.setRequestId(request.getRequestId());
      response.setVolumeId(request.getVolumeId());

    } catch (Exception e) {
      logger.warn("can not read file:{}", filePath, e);
      throw new ReadPerformanceParameterFromFileExceptionThrift();
    } finally {

      try {
        if (fileLock != null) {
          fileLock.release();
          fileLock.close();
        }

        if (fileChannel != null) {
          fileChannel.close();
        }

        if (randomAccesssFile != null) {
          randomAccesssFile.close();
        }
      } catch (IOException e) {
        logger.warn("close the fileLock failure");
      }

    }

    return response;
  }

  /**
   * To apply rules in request to iscsi driver.
   *
   * <p>now use UpdateIscsiAccessRuleFileSweepWork to update iscsi access rule info should delete
   * this code and the using of cancelVolumeAccessRules
   */
  @Override
  public ApplyVolumeAccessRulesResponse applyVolumeAccessRules(
      ApplyVolumeAccessRulesRequest request)
      throws FailedToApplyVolumeAccessRulesExceptionThrift, TException {
    logger.error("not implemented.");
    throw new NotImplementedException();
  }

  /**
   * To cancel rules specified in request.
   *
   * <p>now use UpdateIscsiAccessRuleFileSweepWork to update iscsi access rule info should delete
   * this code and the using of cancelVolumeAccessRules
   */
  @Override
  public CancelVolumeAccessRulesResponse cancelVolumeAccessRules(
      CancelVolumeAccessRulesRequest request)
      throws FailedToCancelVolumeAccessRulesExceptionThrift, TException {
    logger.error("not implemented.");
    throw new NotImplementedException();
  }

  /**
   * To list rules specified in request.
   *
   * <p>Coordinator may use it to get volume access on specified volume
   */
  @Override
  public ListVolumeAccessRulesByVolumeIdsResponse listVolumeAccessRulesByVolumeIds(
      ListVolumeAccessRulesByVolumeIdsRequest request)
      throws ServiceIsNotAvailableThrift, TException {
    ListVolumeAccessRulesByVolumeIdsResponse response;

    Set<Long> volumeIds = request.getVolumeIds();
    Map<Long, List<VolumeAccessRuleThrift>> volumesAccessRuleTable = new ConcurrentHashMap<>();
    synchronized (volumeAccessRuleTable) {
      if (!volumeAccessRuleTable.isEmpty()) {
        if (volumeIds != null && !volumeIds.isEmpty()) {
          for (long volumeId : volumeIds) {
            if (volumeAccessRuleTable.containsKey(volumeId)) {
              volumesAccessRuleTable.put(volumeId, volumeAccessRuleTable.get(volumeId));
            } else {
              logger.warn("volume {} not exist in driverStore", volumeId);
              volumesAccessRuleTable.put(volumeId, new ArrayList<>());
            }
          }
        }
      }
      response = new ListVolumeAccessRulesByVolumeIdsResponse(request.getRequestId(),
          volumesAccessRuleTable);
    }
    return response;
  }

  /**
   * To apply rules in request to iscsi driver.
   *
   * <p>Not use, UpdateIscsiAccessRuleFileSweepWork to update iscsi access rule info
   */
  @Override
  public ApplyIscsiAccessRulesResponse applyIscsiAccessRules(ApplyIscsiAccessRulesRequest request)
      throws FailedToApplyIscsiAccessRulesExceptionThrift, TException {
    logger.warn("{}", request);
    throw new NotImplementedException();
  }

  /**
   * To cancel rules specified in request.
   *
   * <p>Not use, UpdateIscsiAccessRuleFileSwApplyVolumeAccessRulesRequesteepWork to update iscsi
   * access rule info
   */
  @Override
  public CancelIscsiAccessRulesResponse cancelIscsiAccessRules(
      CancelIscsiAccessRulesRequest request)
      throws FailedToCancelIscsiAccessRulesExceptionThrift, TException {
    logger.warn("{}", request);
    throw new NotImplementedException();
  }

  @Override
  public GetIoLimitationByDriverResponseThrift getIoLimitationsByDriver(
      GetIoLimitationByDriverRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {
    logger.debug("getIOLimitationsByDriver {} driver2ItsIOLimitationsTable {}", request,
        driver2ItsIoLimitationsTable);
    if (shutDownFlag) {
      logger.warn(
          "Cannot deal with any request due to service driver container is being shutdown ...");
      throw new ServiceHavingBeenShutdownThrift();
    }
    GetIoLimitationByDriverResponseThrift response;
    DriverKeyThrift driverKeyThrift = request.getDriverKeyThrift();
    List<IoLimitationThrift> ioLimitationList = new ArrayList<>();

    // no need synchronized (driver2ItsIOLimitationsTable) because ConcurrentHashMap
    // get/put/remove/containsKey are sync interface
    if (driver2ItsIoLimitationsTable != null && !driver2ItsIoLimitationsTable.isEmpty()) {
      if (driver2ItsIoLimitationsTable.containsKey(driverKeyThrift)) {
        ioLimitationList = driver2ItsIoLimitationsTable.get(driverKeyThrift);
      } else {
        logger.warn("getIOLimitationsByDriver driver {} not exist in driver2ItsIOLimitationsTable",
            driverKeyThrift);
      }
    }
    response = new GetIoLimitationByDriverResponseThrift(request.getRequestId(), ioLimitationList);
    logger.debug("getIOLimitationsByDriver ioLimitationList {} response {}", ioLimitationList,
        response);
    return response;
  }

  /**
   * The method to get all applied successful access rules for lio or iet by driverKey.
   */
  public ListIscsiAppliedAccessRulesResponseThrift listIscsiAppliedAccessRules(
      ListIscsiAppliedAccessRulesRequestThrift request)
      throws ServiceHavingBeenShutdownThrift {
    logger.info("listIscsiAppliedAccessRules request :{}", request);
    if (shutDownFlag) {
      logger.warn(
          "Cannot deal with any request due to service driver container is being shutdown ...");
      throw new ServiceHavingBeenShutdownThrift();
    }
    ListIscsiAppliedAccessRulesResponseThrift response =
        new ListIscsiAppliedAccessRulesResponseThrift();
    response.setRequestId(request.getRequestId());
    DriverKey driverKey = RequestResponseHelper.buildDriverKeyFrom(request.getDriverKey());
    Map<String, List<String>> allowInitiatorTable;
    List<String> initiatorNames = new ArrayList<>();
    String targetName;
    allowInitiatorTable = lioManager.listIscsiAppliedAccuessRule();
    targetName = lioNameBuilder.buildLioWwn(driverKey.getVolumeId(), driverKey.getSnapshotId());
    if (allowInitiatorTable.get(targetName) != null) {
      initiatorNames.addAll(allowInitiatorTable.get(targetName));
    }
    response.setInitiatorNames(initiatorNames);
    logger.info("listIscsiAppliedAccessRules response :{}", response);
    return response;
  }

  /**
   * To list iscsi rules specified in request.
   *
   * <p>Not use, Coordinator may use later
   */
  @Override
  public ListIscsiAccessRulesByDriverKeysResponse listIscsiAccessRulesByDriverKeys(
      ListIscsiAccessRulesByDriverKeysRequest request)
      throws ServiceIsNotAvailableThrift, TException {

    logger.warn("ListIscsiAccessRulesByDriverKeys request :{}", request);
    ListIscsiAccessRulesByDriverKeysResponse response;
    Set<DriverKeyThrift> driverKeys = request.getDriverKeys();
    Map<DriverKeyThrift, List<IscsiAccessRuleThrift>> driversAccessRuleTable =
        new ConcurrentHashMap<DriverKeyThrift, List<IscsiAccessRuleThrift>>();
    synchronized (iscsiAccessRuleTable) {
      if (!iscsiAccessRuleTable.isEmpty()) {
        if (driverKeys != null && !driverKeys.isEmpty()) {
          for (DriverKeyThrift driverKey : driverKeys) {
            if (iscsiAccessRuleTable.containsKey(driverKey)) {
              driversAccessRuleTable.put(driverKey, iscsiAccessRuleTable.get(driverKey));
            } else {
              logger.warn("driverKey {} not exist in iscsiAccessRuleTable", driverKey);
              driversAccessRuleTable.put(driverKey, new ArrayList<>());
            }
          }
        }
      }
      response = new ListIscsiAccessRulesByDriverKeysResponse(request.getRequestId(),
          driversAccessRuleTable);
    }

    logger.info("ListIscsiAccessRulesByDriverKeys response:{}", response);
    return response;
  }

  @Override
  public void shutdown() throws TException {

    logger.warn("Going to shutdown the service ...");

    if (shutDownFlag == true) {
      logger.warn("The service is shutting down already, do nothing.");
      return;
    } else {
      shutDownFlag = true;
    }

    Thread shutdownThread = new Thread(new Runnable() {
      @Override
      public void run() {
        appEngine.stop();
        System.exit(0);
      }
    });

    shutdownThread.start();
  }


  @Override
  public ReportDriverMetadataResponse reportDriverMetadata(ReportDriverMetadataRequest request)
      throws ServiceIsNotAvailableThrift, DriverFromRequestNotFoundExceptionThrift, TException {
    List<DriverMetadata> currentDriverStoreReportList = null;
    List<DriverMetadata> latestDriverStoreReportList = new ArrayList<>();

    Version currentVersion = DriverVersion.currentVersion.get(DriverType.NBD);
    Version latestVersion = DriverVersion.latestVersion.get(DriverType.NBD);
    boolean onMigration = DriverVersion.isOnMigration.get(DriverType.NBD);

    if (currentVersion == null || driverStoreManager.get(currentVersion) == null) {
      logger.info("There has no driver to report");
      throw new DriverFromRequestNotFoundExceptionThrift();
    }

    // driver is not in upgrading
    if (!onMigration) {
      currentDriverStoreReportList = driverStoreManager.get(currentVersion).list();
      resetDriverClientInfo(request, currentDriverStoreReportList, currentVersion);

    } else if (!currentVersion.equals(latestVersion) && onMigration) {
      List<DriverMetadata> drivers = driverStoreManager.get(latestVersion).list();
      for (DriverMetadata driverMetadata : drivers) {
        // just modify driver client info with LAUNCHED status
        if (driverMetadata.getDriverStatus() == DriverStatus.LAUNCHED) {
          latestDriverStoreReportList.add(driverMetadata);
        }
      }
      resetDriverClientInfo(request, latestDriverStoreReportList, latestVersion);
      currentDriverStoreReportList = driverStoreManager.get(currentVersion).list();
      resetDriverClientInfo(request, currentDriverStoreReportList, currentVersion);
    }

    ReportDriverMetadataResponse response = new ReportDriverMetadataResponse();
    response.setRequestId(request.getRequestId());

    logger.info(response.toString());
    return response;
  }


  /**
   * xx.
   */
  public void resetDriverClientInfo(ReportDriverMetadataRequest request,
      List<DriverMetadata> driverStoreReportList,
      Version version) {

    Map<DriverKey, DriverMetadata> driverKey2DriverMetadata = new HashMap<>();

    logger.debug("request.getDrivers:{}", request.getDrivers());
    // get driver list combine with latestDriverStore and currentDriverStore,id driver both in
    // latestDriverStore and
    // currentDriverStore ,report which one in latestDriverStore
    for (DriverMetadata driverInStore : driverStoreReportList) {
      DriverKey driverKey = new DriverKey(driverInStore.getDriverContainerId(),
          driverInStore.getVolumeId(),
          driverInStore.getSnapshotId(), driverInStore.getDriverType());
      if (driverInStore.getDriverType() == DriverType.NBD) {
        driverKey2DriverMetadata.put(driverKey, driverInStore);
      }
    }
    for (DriverMetadataThrift driverMetadataThrift : request.getDrivers()) {
      if (DriverType.valueOf(driverMetadataThrift.getDriverType().name()) == DriverType.NBD) {
        Map<String, AccessPermissionType> clientHostAccessRules = new HashMap<>();
        DriverKey driverKeyThrift = new DriverKey(driverMetadataThrift.getDriverContainerId(),
            driverMetadataThrift.getVolumeId(), driverMetadataThrift.getSnapshotId(),
            DriverType.valueOf(driverMetadataThrift.getDriverType().name()));
        if (driverKey2DriverMetadata.get(driverKeyThrift) != null && driverKey2DriverMetadata
            .get(driverKeyThrift).getInstanceId() == driverMetadataThrift.getInstanceId()) {
          for (Entry<String, AccessPermissionTypeThrift> entry : driverMetadataThrift
              .getClientHostAccessRule().entrySet()) {
            clientHostAccessRules
                .put(entry.getKey(), AccessPermissionType.valueOf(entry.getValue().name()));
          }
          if (clientHostAccessRules != null && clientAltered(
              driverKey2DriverMetadata.get(driverKeyThrift), clientHostAccessRules)) {
            logger.warn("client has change from {} to {} driver:{}",
                driverKey2DriverMetadata.get(driverKeyThrift).getClientHostAccessRule(),
                clientHostAccessRules, driverKeyThrift);
            driverKey2DriverMetadata.get(driverKeyThrift)
                .setClientHostAccessRule(clientHostAccessRules);
            driverStoreManager.get(version).save(driverKey2DriverMetadata.get(driverKeyThrift));
          }
        } else {
          logger
              .info("InstanceId of driver :{} from request can not find in driverStore,do nothing",
                  driverMetadataThrift);
        }

      }
    }
  }

  protected boolean clientAltered(DriverMetadata driver,
      Map<String, AccessPermissionType> newClientAccessRuleInfo) {
    if (null == driver.getClientHostAccessRule()) {
      driver.setClientHostAccessRule(new HashMap<>());
    }

    Map<String, AccessPermissionType> existedClientsAccessRule = new HashMap<>(
        driver.getClientHostAccessRule());
    Map<String, AccessPermissionType> newClientsAccessRule = new HashMap<>(
        newClientAccessRuleInfo);
    return !existedClientsAccessRule.equals(newClientsAccessRule);
  }

  // Get PYD accessRule from Coordinator and ISCSI accessRule from initiators.allow file,send to
  // ControlCenter by response
  @Override
  public GetDriverConnectPermissionResponseThrift getDriverConnectPermission(
      GetDriverConnectPermissionRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, TException {
    logger.info("request:{}", request);
    GetDriverConnectPermissionResponseThrift responseThrift =
        new GetDriverConnectPermissionResponseThrift();
    Map<String, AccessPermissionTypeThrift> connectPermissionMap = new HashMap<>();
    Version version = DriverVersion.currentVersion.get(DriverType.NBD);

    if (version == null || driverStoreManager.get(version) == null) {
      logger.warn("Has no driver launched");
      responseThrift.setRequestId(request.getRequestId());
      responseThrift.setConnectPermissionMap(connectPermissionMap);
      return responseThrift;
    }
    if (shutDownFlag) {
      logger.warn(
          "Cannot deal with any request due to service driver container is being shutdown ...");
      throw new ServiceHavingBeenShutdownThrift();
    }

    DriverKey driverKey = new DriverKey(request.getDriverContainerId(), request.getVolumeId(),
        request.getSnapshotId(), DriverType.valueOf(request.getDriverType().name()));
    EndPoint endpoint;
    DriverMetadata driver;
    int networkErrorRetryCount = 0;
    int networkErrorMaxTryAmount = 3;
    driver = driverStoreManager.get(version).get(driverKey);
    endpoint = new EndPoint(appContext.getMainEndPoint().getHostName(),
        driver.getCoordinatorPort());
    List<String> clientAddressList;

    while (true) {
      try {
        Coordinator.Iface coordinatorClient = coordinatorClientFactory.build(endpoint).getClient();
        responseThrift = coordinatorClient.getDriverConnectPermission(request);
        break;
      } catch (GenericThriftClientFactoryException e) {
        logger.error("Caught an exception when build coordinator client {}", endpoint, e);
        throw new TException(e);

      } catch (TTransportException e) {
        if (networkErrorRetryCount++ < networkErrorMaxTryAmount) {
          logger.warn("Catch an exception when build coordinator client:{},retry {} times", e,
              networkErrorRetryCount);
          continue;

        } else {
          logger.error("Catch an exception when build coordinator client:{}", e);
          throw new TException(e);
        }
      }
    }
    logger.info("responseThrift:{}", responseThrift);
    return responseThrift;
  }

  /*
   * used to support iscsi chap control default enable chap for iet or lio
   * 0:disable
   * 1:enable
   */
  @Override
  public SetIscsiChapControlResponseThrift setIscsiChapControl(
      SetIscsiChapControlRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, TException {
    logger.info("request:{}", request);
    SetIscsiChapControlResponseThrift responseThrift = new SetIscsiChapControlResponseThrift();
    Version version = DriverVersion.currentVersion.get(DriverType.NBD);

    //TODO upgrade not process

    if (version == null || driverStoreManager.get(version) == null) {
      logger.warn("Has no driver launched");
      responseThrift.setRequestId(request.getRequestId());
      return responseThrift;
    }
    if (shutDownFlag) {
      logger.warn(
          "Cannot deal with any request due to service driver container is being shutdown ...");
      throw new ServiceHavingBeenShutdownThrift();
    }

    DriverKey driverKey = new DriverKey(request.getDriverContainerId(), request.getVolumeId(),
        request.getSnapshotId(), DriverType.valueOf(request.getDriverType().name()));
    DriverMetadata driver;
    driver = driverStoreManager.get(version).get(driverKey);

    logger.info("chap control from {} to {} on driver:{}", driver.getChapControl(),
        request.getChapControl(), driver);
    driver.setChapControl(request.getChapControl());

    driverStoreManager.get(version).save(driver);

    logger.info("responseThrift:{} driver {}", responseThrift, driver);

    return responseThrift;

  }

  boolean availableCpuForIscsiTarget(DriverStore currentDriverStore) {
    int cpuTotalNum = DriverContainerUtils.getSysCpuNum();
    int cpuNumReserved = driverContainerConfig.getSystemCpuNumReserved();
    int cpuPercentReserved = driverContainerConfig.getSystemCpuPercentReserved();
    int availableCpuNum = 0;
    logger
        .debug("cpu total num:{},num reserved:{},percent reserved:{}", cpuTotalNum, cpuNumReserved,
            cpuPercentReserved);
    if (cpuNumReserved == 0 && cpuPercentReserved == 0) {
      logger.debug("not limit target num");
      return true;
    } else if (cpuTotalNum == 1) {
      availableCpuNum = 1;
    } else {
      if (cpuNumReserved == 0) {
        if (cpuPercentReserved < 100) {
          availableCpuNum = cpuTotalNum - cpuTotalNum * cpuPercentReserved / 100;
        } else {
          logger.warn("invalid system.cpu.percent.reserved, not use this value");
          availableCpuNum = cpuTotalNum - 1;
        }
      } else {
        if (cpuNumReserved < cpuTotalNum) {
          availableCpuNum = cpuTotalNum - cpuNumReserved;
        } else {
          logger.warn("invalid system.cpu.num.reserved, not use this value");
          availableCpuNum = cpuTotalNum - 1;
        }
      }
    }
    int curTargetNum = 0;
    List<DriverMetadata> driverList = null;
    driverList = currentDriverStore.list(DriverType.ISCSI);
    curTargetNum = driverList.size();
    logger.debug("availableCpuNum:{},curTargetNum:{}", availableCpuNum, curTargetNum);
    if (availableCpuNum <= curTargetNum) {
      logger.error("no enough cpu to launch driver, availableCpuNum:{},curTargetNum:{}",
          availableCpuNum, curTargetNum);
      return false;
    }
    return true;
  }

  public void setLaunchDriverWorkerFactory(LaunchDriverWorkerFactory launchDriverWorkerFactory) {
    this.launchDriverWorkerFactory = launchDriverWorkerFactory;
  }

  public void setRemoveDriverWorkerFactory(RemoveDriverWorkerFactory removeDriverWorkerFactory) {
    this.removeDriverWorkerFactory = removeDriverWorkerFactory;
  }

  public AppContext getAppContext() {
    return appContext;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }

  public DriverContainerConfiguration getDriverContainerConfig() {
    return driverContainerConfig;
  }

  public void setDriverContainerConfig(DriverContainerConfiguration driverContainerConfig) {
    this.driverContainerConfig = driverContainerConfig;
  }

  public void setBootStrap(BootStrap bootStrap) {
    this.bootStrap = bootStrap;
  }

  public PortContainerFactory getPortContainerFactory() {
    return portContainerFactory;
  }

  public void setPortContainerFactory(PortContainerFactory portContainerFactory) {
    this.portContainerFactory = portContainerFactory;
  }

  public void setVolumeAccessRuleTable(
      Map<Long, List<VolumeAccessRuleThrift>> volumeAccessRuleTable) {
    this.volumeAccessRuleTable = volumeAccessRuleTable;
  }

  public void setDriver2ItsIoLimitationsTable(
      Map<DriverKeyThrift, List<IoLimitationThrift>> driver2ItsIoLimitationsTable) {
    this.driver2ItsIoLimitationsTable = driver2ItsIoLimitationsTable;
  }

  public void setTaskExecutor(TaskExecutor taskExecutor) {
    this.taskExecutor = taskExecutor;
  }

  public void setAppEngine(DriverContainerAppEngine appEngine) {
    this.appEngine = appEngine;
  }

  public DriverStoreManager getDriverStoreManager() {
    return driverStoreManager;
  }

  public void setDriverStoreManager(DriverStoreManager driverStoreManager) {
    this.driverStoreManager = driverStoreManager;
  }

  public VersionManager getVersionManager() {
    return versionManager;
  }

  public void setVersionManager(VersionManager versionManager) {
    this.versionManager = versionManager;
  }

  public LioNameBuilder getLioNameBuilder() {
    return lioNameBuilder;
  }

  public void setLioNameBuilder(LioNameBuilder lioNameBuilder) {
    this.lioNameBuilder = lioNameBuilder;
  }

  public void setLioManager(LioManager lioManager) {
    this.lioManager = lioManager;
  }

  public void setScsiManager(ScsiManager scsiManager) {
    this.scsiManager = scsiManager;
  }

  public void setCoordinatorClientFactory(CoordinatorClientFactory coordinatorClientFactory) {
    this.coordinatorClientFactory = coordinatorClientFactory;
  }
}
