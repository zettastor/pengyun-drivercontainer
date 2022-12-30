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

import java.io.File;
import java.io.IOException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import py.driver.PortalType;
import py.drivercontainer.utils.DriverContainerUtils;

//import py.coordinator.iscsi.IETManagerConfiguration;

@Configuration
@PropertySource({"classpath:config/drivercontainer.properties"})
//@Import({IETManagerConfiguration.class})
public class DriverContainerConfiguration {

  @Value("${drivercontainer.role}")
  private int drivercontainerRole = 1; ///1: normal + scsi client 2: only scsi client 3: only normal

  @Value("${jscsi.driver.name}")
  private String jscsiDriverName;

  @Value("${jscsi.driver.ports}")
  private String jscsiDriverPorts;

  @Value("${nbd.driver.name}")
  private String nbdDriverName;

  @Value("${nbd.driver.ports}")
  private String nbdDriverPorts;

  @Value("${iscsi.driver.port}")
  private int iscsiPort;

  @Value("${coordinator.base.port:1000}")
  private int coordinatorBasePort;

  @Value("${driver.server.ip:}")
  private String driverServerIp = "";

  @Value("${driver.thread.pool.size:10}")
  private int driverThreadPoolSize = 10;

  @Value("${driver.operation.timeout:30000}")
  private long driverOperationTimeout = 30000;

  @Value("${driver.shutdown.timeout.sec: 30}")
  private int driverShutdownTimeoutSec = 30;

  @Value("${driver.workspace.keep: true}")
  private boolean driverWorkspaceKeep = true;

  @Value("${driver.task.threads: 20}")
  private int driverTaskThreads = 20;

  @Value("${volume.unavailable.timeout.sec: 120}")
  private int volumeUnavailableTimeoutSec = 120;

  @Value("${volume.available.timeout.sec: 5}")
  private int volumeAvailableTimeoutSec = 5;

  @Value("${local.dih.endpoint}")
  private String localDihEndPoint;

  @Value("${app.name}")
  private String appName;

  @Value("${app.location}")
  private String appLocation;

  @Value("${app.main.endpoint}")
  private String mainEndPoint;

  @Value("${health.checker.rate}")
  private int healthCheckerRate;

  @Value("${thrift.client.timeout}")
  private long thriftClientTimeout;

  @Value("${dashboard.longest.time.second:600}")
  private long dashboardLongestTimeSecond = 600;

  @Value("${create.two.iscsi.target.switch:false}")
  private boolean createTwoIscsiTargetSwitch = false;

  @Value("${system.memory.force.reserved:1G}")
  private String systemMemoryForceReserved = "1G";

  @Value("${system.cpu.num.reserved:0}")
  private int systemCpuNumReserved = 0;

  @Value("${system.cpu.percent.reserved:0}")
  private int systemCpuPercentReserved = 0;

  @Value("${pyd.requirements.check.on.startup: true}")
  private boolean pydReqrCheckOnStartup = true;

  @Value("${check.iscsi.service.flag}")
  private boolean checkIscsiServiceFlag = false;

  @Value("${driver.recover.timeout}")
  private long driverRecoverTimeout = 10000;

  @Value("${driver.rootPath}")
  private String rootPath;
  // ---------------network size----------------------
  // datanode max thrift network frame size
  @Value("${max.network.frame.size:17000000}")
  private int maxNetworkFrameSize = 17 * 1000 * 1000;

  @Value("${thrift.transport.retries: 3}")
  private int thriftTransportRetries = 3;

  @Value("${thrift.transport.retry.interval.sec: 3}")
  private int thriftTransportRetryIntervalSec = 3;

  @Value("${driver.upgrade.limit:5}")
  private int driverUpgradeLimit = 5;

  @Value("${build.coordinator.client.timeout :3000}")
  private int buildCoordinatorClientTimeout = 3000;

  /**
   * Options for delegates.
   */
  private long instanceStatusTimerTimeoutSec = 30;

  /**
   * Options for workers.
   */

  @Value("${getAccessRule.interval.ms}")
  private int getAccessRuleIntervalMs;

  @Value("${InitiatorsAllow.interval.ms}")
  private int initiatorsAllowMs;

  @Value("${report.interval.ms}")
  private int reportIntervalMs;

  @Value("${report.driver.client.session.try.times:5}")
  private int reportDriverClientSessionTryTimes = 5;

  @Value("${migrating.scan.interval.ms}")
  private int migratingScanIntervalMs;

  @Value("${server.scanner.rate.ms:1000}")
  private long serverScannerRateMs = 1500;

  @Value("${target.scanner.rate.ms:1000}")
  private long targetScannerRateMs = 2000;

  @Value("${task.executor.rate.ms: 1700}")
  private long taskExecutorRateMs = 1700;

  /**
   * Options for driver task executor.
   */
  @Value("${task.executor.core.pool.size:5}")
  private int taskExecutorCorePoolSize = 5;

  @Value("${task.executor.max.pool.size:10}")
  private int taskExecutorMaxPoolSize = 10;

  @Value("${task.executor.keep.alive.time.sec:60}")
  private int taskExecutorKeepAliveTimeSec = 60;

  @Value("${library.root.path}")
  private String libraryRootPath = "/var/testing/_packages";

  @Value("${submit.upgrade.interval.ms:60000}")
  private int submitUpgradeIntervalMs = 60000;

  @Value("${iscsi.portal.type:IPV6}")
  private String iscsiPortalType = "IPV6";

  @Value("${iscsi.portal.cidr.network:}")
  private String iscsiPortalCidrNetwork = null;

  @Bean
  public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
    return new PropertySourcesPlaceholderConfigurer();
  }

  public int getDrivercontainerRole() {
    return drivercontainerRole;
  }

  public void setDrivercontainerRole(int drivercontainerRole) {
    this.drivercontainerRole = drivercontainerRole;
  }

  public String getRootPath() {
    return rootPath;
  }

  public void setRootPath(String rootPath) {
    this.rootPath = rootPath;
  }


  /**
   * xx.
   */
  public String getDriverAbsoluteRootPath() {
    File dir = new File(".");
    String absolutePath = null;
    try {
      absolutePath = dir.getCanonicalPath();
      absolutePath += (File.separator + getRootPath());
    } catch (IOException e) {
      e.printStackTrace();
    }
    return absolutePath;

  }

  public boolean isPydReqrCheckOnStartup() {
    return pydReqrCheckOnStartup;
  }

  public void setPydReqrCheckOnStartup(boolean pydReqrCheckOnStartup) {
    this.pydReqrCheckOnStartup = pydReqrCheckOnStartup;
  }

  public boolean isCheckIscsiServiceFlag() {
    return checkIscsiServiceFlag;
  }

  public void setCheckIscsiServiceFlag(boolean checkIscsiServiceFlag) {
    this.checkIscsiServiceFlag = checkIscsiServiceFlag;
  }

  public int getMigratingScanIntervalMs() {
    return migratingScanIntervalMs;
  }

  public void setMigratingScanIntervalMs(int migratingScanIntervalMs) {
    this.migratingScanIntervalMs = migratingScanIntervalMs;
  }

  public int getBuildCoordinatorClientTimeout() {
    return buildCoordinatorClientTimeout;
  }

  public void setBuildCoordinatorClientTimeout(int buildCoordinatorClientTimeout) {
    this.buildCoordinatorClientTimeout = buildCoordinatorClientTimeout;
  }

  public int getMaxNetworkFrameSize() {
    return maxNetworkFrameSize;
  }

  public void setMaxNetworkFrameSize(int maxNetworkFrameSize) {
    this.maxNetworkFrameSize = maxNetworkFrameSize;
  }

  public String getJscsiDriverName() {
    return jscsiDriverName;
  }

  public void setJscsiDriverName(String jscsiDriverName) {
    this.jscsiDriverName = jscsiDriverName;
  }

  public String getJscsiDriverPorts() {
    return jscsiDriverPorts;
  }

  public void setJscsiDriverPorts(String jscsiDriverPorts) {
    this.jscsiDriverPorts = jscsiDriverPorts;
  }

  public String getNbdDriverName() {
    return nbdDriverName;
  }

  public void setNbdDriverName(String nbdDriverName) {
    this.nbdDriverName = nbdDriverName;
  }

  public String getNbdDriverPorts() {
    return nbdDriverPorts;
  }

  public void setNbdDriverPorts(String nbdDriverPorts) {
    this.nbdDriverPorts = nbdDriverPorts;
  }

  public int getGetAccessRuleIntervalMs() {
    return getAccessRuleIntervalMs;
  }

  public void setGetAccessRuleIntervalMs(int getAccessRuleIntervalMs) {
    this.getAccessRuleIntervalMs = getAccessRuleIntervalMs;
  }

  public int getReportIntervalMs() {
    return reportIntervalMs;
  }

  public void setReportIntervalMs(int reportIntervalMs) {
    this.reportIntervalMs = reportIntervalMs;
  }

  public int getReportDriverClientSessionTryTimes() {
    return reportDriverClientSessionTryTimes;
  }

  public int getIscsiPort() {
    return iscsiPort;
  }

  public void setIscsiPort(int iscsiPort) {
    this.iscsiPort = iscsiPort;
  }

  public int getDriverThreadPoolSize() {
    return driverThreadPoolSize;
  }

  public void setDriverThreadPoolSize(int driverThreadPoolSize) {
    this.driverThreadPoolSize = driverThreadPoolSize;
  }

  public long getDriverOperationTimeout() {
    return driverOperationTimeout;
  }

  public void setDriverOperationTimeout(long driverOperationTimeout) {
    this.driverOperationTimeout = driverOperationTimeout;
  }

  public String getLocalDihEndPoint() {
    return localDihEndPoint;
  }

  public void setLocalDihEndPoint(String localDihEndPoint) {
    this.localDihEndPoint = localDihEndPoint;
  }

  public String getAppName() {
    return appName;
  }

  public void setAppName(String appName) {
    this.appName = appName;
  }

  public String getMainEndPoint() {
    return mainEndPoint;
  }

  public void setMainEndPoint(String mainEndPoint) {
    this.mainEndPoint = mainEndPoint;
  }

  public int getHealthCheckerRate() {
    return healthCheckerRate;
  }

  public void setHealthCheckerRate(int healthCheckerRate) {
    this.healthCheckerRate = healthCheckerRate;
  }

  public long getThriftClientTimeout() {
    return thriftClientTimeout;
  }

  public void setThriftClientTimeout(long thriftClientTimeout) {
    this.thriftClientTimeout = thriftClientTimeout;
  }

  public long getDashboardLongestTimeSecond() {
    return dashboardLongestTimeSecond;
  }

  public void setDashboardLongestTimeSecond(long dashboardLongestTimeSecond) {
    this.dashboardLongestTimeSecond = dashboardLongestTimeSecond;
  }

  public boolean getCreateTwoIscsiTargetSwitch() {
    return createTwoIscsiTargetSwitch;
  }

  public void setCreateTwoIscsiTargetSwitch(boolean createTwoIscsiTargetSwitch) {
    this.createTwoIscsiTargetSwitch = createTwoIscsiTargetSwitch;
  }

  public long getSystemMemoryForceReserved() {
    return DriverContainerUtils.getBytesSizeFromString(this.systemMemoryForceReserved);
  }

  public void setSystemMemoryForceReserved(String systemMemoryForceReserved) {
    this.systemMemoryForceReserved = systemMemoryForceReserved;
  }

  public int getSystemCpuNumReserved() {
    return this.systemCpuNumReserved;
  }

  public void setSystemCpuNumReserved(int systemCpuNumReserved) {
    this.systemCpuNumReserved = systemCpuNumReserved;
  }

  public int getSystemCpuPercentReserved() {
    return this.systemCpuPercentReserved;
  }

  public void setSystemCpuPercentReserved(int systemCpuPercentReserved) {
    this.systemCpuPercentReserved = systemCpuPercentReserved;
  }

  public int getCoordinatorBasePort() {
    return coordinatorBasePort;
  }

  public void setCoordinatorBasePort(int coordinatorBasePort) {
    this.coordinatorBasePort = coordinatorBasePort;
  }

  public int getThriftTransportRetries() {
    return thriftTransportRetries;
  }

  public void setThriftTransportRetries(int thriftTransportRetries) {
    this.thriftTransportRetries = thriftTransportRetries;
  }

  public int getThriftTransportRetryIntervalSec() {
    return thriftTransportRetryIntervalSec;
  }

  public void setThriftTransportRetryIntervalSec(int thriftTransportRetryIntervalSec) {
    this.thriftTransportRetryIntervalSec = thriftTransportRetryIntervalSec;
  }

  public int getDriverShutdownTimeoutSec() {
    return driverShutdownTimeoutSec;
  }

  public void setDriverShutdownTimeoutSec(int driverShutdownTimeoutSec) {
    this.driverShutdownTimeoutSec = driverShutdownTimeoutSec;
  }

  public int getVolumeUnavailableTimeoutSec() {
    return volumeUnavailableTimeoutSec;
  }

  public void setVolumeUnavailableTimeoutSec(int volumeUnavailableTimeoutSec) {
    this.volumeUnavailableTimeoutSec = volumeUnavailableTimeoutSec;
  }

  public int getVolumeAvailableTimeoutSec() {
    return volumeAvailableTimeoutSec;
  }

  public void setVolumeAvailableTimeoutSec(int volumeAvailableTimeoutSec) {
    this.volumeAvailableTimeoutSec = volumeAvailableTimeoutSec;
  }

  public boolean isDriverWorkspaceKeep() {
    return driverWorkspaceKeep;
  }

  public void setDriverWorkspaceKeep(boolean driverWorkspaceKeep) {
    this.driverWorkspaceKeep = driverWorkspaceKeep;
  }

  public int getInitiatorsAllowMs() {
    return initiatorsAllowMs;
  }

  public void setInitiatorsAllowMs(int initiatorsAllowMs) {
    this.initiatorsAllowMs = initiatorsAllowMs;
  }

  public int getDriverTaskThreads() {
    return driverTaskThreads;
  }

  public void setDriverTaskThreads(int driverTaskThreads) {
    this.driverTaskThreads = driverTaskThreads;
  }

  public long getDriverRecoverTimeout() {
    return driverRecoverTimeout;
  }

  public void setDriverRecoverTimeout(long driverRecoverTimeout) {
    this.driverRecoverTimeout = driverRecoverTimeout;
  }

  public int getTaskExecutorCorePoolSize() {
    return taskExecutorCorePoolSize;
  }

  public void setTaskExecutorCorePoolSize(int taskExecutorCorePoolSize) {
    this.taskExecutorCorePoolSize = taskExecutorCorePoolSize;
  }

  public int getTaskExecutorMaxPoolSize() {
    return taskExecutorMaxPoolSize;
  }

  public void setTaskExecutorMaxPoolSize(int taskExecutorMaxPoolSize) {
    this.taskExecutorMaxPoolSize = taskExecutorMaxPoolSize;
  }

  public int getTaskExecutorKeepAliveTimeSec() {
    return taskExecutorKeepAliveTimeSec;
  }

  public void setTaskExecutorKeepAliveTimeSec(int taskExecutorKeepAliveTimeSec) {
    this.taskExecutorKeepAliveTimeSec = taskExecutorKeepAliveTimeSec;
  }

  public long getTaskExecutorRateMs() {
    return taskExecutorRateMs;
  }

  public void setTaskExecutorRateMs(long taskExecutorRateMs) {
    this.taskExecutorRateMs = taskExecutorRateMs;
  }

  public long getServerScannerRateMs() {
    return serverScannerRateMs;
  }

  public void setServerScannerRateMs(long serverScannerRateMs) {
    this.serverScannerRateMs = serverScannerRateMs;
  }

  public long getTargetScannerRateMs() {
    return targetScannerRateMs;
  }

  public void setTargetScannerRateMs(long targetScannerRateMs) {
    this.targetScannerRateMs = targetScannerRateMs;
  }

  public long getInstanceStatusTimerTimeoutSec() {
    return instanceStatusTimerTimeoutSec;
  }

  public void setInstanceStatusTimerTimeoutSec(long instanceStatusTimerTimeoutSec) {
    this.instanceStatusTimerTimeoutSec = instanceStatusTimerTimeoutSec;
  }

  public String getDriverServerIp() {
    return driverServerIp;
  }

  public void setDriverServerIp(String driverServerIp) {
    this.driverServerIp = driverServerIp;
  }

  public int getDriverUpgradeLimit() {
    return driverUpgradeLimit;
  }

  public void setDriverUpgradeLimit(int driverUpgradeLimit) {
    this.driverUpgradeLimit = driverUpgradeLimit;
  }

  public String getLibraryRootPath() {
    return libraryRootPath;
  }

  public void setLibraryRootPath(String libraryRootPath) {
    this.libraryRootPath = libraryRootPath;
  }

  public int getSubmitUpgradeIntervalMs() {
    return submitUpgradeIntervalMs;
  }

  public void setSubmitUpgradeIntervalMs(int submitUpgradeIntervalMs) {
    this.submitUpgradeIntervalMs = submitUpgradeIntervalMs;
  }

  public String getAppLocation() {
    return appLocation;
  }

  public void setAppLocation(String appLocation) {
    this.appLocation = appLocation;
  }

  public String getIscsiPortalType() {
    return iscsiPortalType;
  }

  public void setIscsiPortalType(String iscsiPortalType) {
    this.iscsiPortalType = iscsiPortalType;
  }


  /**
   * xx.
   */
  public PortalType parseIscsiPortalType() {
    PortalType portalType;

    portalType = PortalType.valueOf(getIscsiPortalType().toUpperCase());
    if (portalType == null) {
      throw new IllegalArgumentException("unrecognized portal type: " + getIscsiPortalType());
    }

    return portalType;
  }


  /**
   * xx.
   */
  public String getIscsiPortalCidrNetwork() {
    if (iscsiPortalCidrNetwork == null || iscsiPortalCidrNetwork.isEmpty()) {
      return null;
    }

    return iscsiPortalCidrNetwork;
  }

  public void setIscsiPortalCidrNetwork(String iscsiPortalCidrNetwork) {
    this.iscsiPortalCidrNetwork = iscsiPortalCidrNetwork;
  }
}
