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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.app.context.AppContext;
import py.common.RequestIdBuilder;
import py.coordinator.IscsiTargetManager;
import py.coordinator.lio.LioNameBuilder;
import py.driver.DriverMetadata;
import py.driver.DriverType;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.DriverVersion;
import py.drivercontainer.driver.store.DriverStore;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.drivercontainer.driver.version.Version;
import py.drivercontainer.driver.version.VersionManager;
import py.icshare.DriverKey;
import py.infocenter.client.InformationCenterClientFactory;
import py.infocenter.client.InformationCenterClientWrapper;
import py.informationcenter.AccessPermissionType;
import py.periodic.Worker;
import py.thrift.icshare.ReportDriverMetadataRequest;
import py.thrift.share.AccessPermissionTypeThrift;
import py.thrift.share.DriverMetadataThrift;
import py.thrift.share.DriverStatusThrift;
import py.thrift.share.DriverTypeThrift;

/**
 * a worker report all the driver metadata of a driver container to info center at a fixed time.
 *
 */
public class ReportDriverMetadataWorker implements Worker {

  private static final Logger logger = LoggerFactory.getLogger(ReportDriverMetadataWorker.class);

  private InformationCenterClientFactory informationCenterClientFactory;

  private DriverStoreManager driverStoreManager;

  private int iscsiDriverPort;

  private int reportDriverClientSessionTryTimes;

  private DriverContainerConfiguration driverContainerConfiguration;

  private IscsiTargetManager iscsiTargetManager;

  private LioNameBuilder lioNameBuilder;

  private AppContext appContext;

  private VersionManager versionManager;

  private Map<Version, List<DriverMetadata>> reportDriverList;

  private InformationCenterClientWrapper icClientWrapper = null;

  @Override
  public void doWork() throws Exception {
    logger.debug("Worker to report driver to infocenter start ...");
    ReportDriverMetadataRequest request = new ReportDriverMetadataRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setDrivercontainerId(appContext.getInstanceId().getId());
    //use to save driverMetadata list by version
    Map<Version, List<DriverMetadata>> driverListByVersion = new HashMap<>();
    //Now,only support NBD driverType to report
    List<DriverType> driverTypeList = new ArrayList<>();
    driverTypeList.add(DriverType.NBD);

    for (DriverType driverType : driverTypeList) {
      List<DriverMetadata> driverList = new ArrayList<DriverMetadata>();
      Version currentVersion = DriverVersion.currentVersion.get(driverType);
      final Version latestVersion = DriverVersion.latestVersion.get(driverType);

      if (DriverVersion.isOnMigration.get(driverType) == null) {
        versionManager.lockVersion(driverType);
        DriverVersion.isOnMigration.put(driverType, versionManager.isOnMigration(driverType));
        versionManager.unlockVersion(driverType);
      }

      boolean onMigration = DriverVersion.isOnMigration.get(driverType);

      if (currentVersion == null) {
        continue;
      }

      if (driverStoreManager.get(currentVersion) == null) {
        logger.debug("No driver with type {} has been launched", driverType);
        continue;
      }

      DriverStore currentDriverStore = driverStoreManager.get(currentVersion);
      List<DriverMetadata> currentDrivers = currentDriverStore.list();
      if (!onMigration) {
        //Driver is not in upgrading , just need to report currentDriverStore
        if (currentDrivers.size() != 0) {
          driverList.addAll(currentDrivers);
          driverListByVersion.put(currentVersion, driverList);
        }
      } else if (!currentVersion.equals(latestVersion) && onMigration) {
        //When driver is in upgrading ,report which one in latestDriverStore by driverKey
        logger.debug("driverType:{},current version:{} and latest version:{}.", driverType,
            currentVersion, latestVersion);
        List<DriverMetadata> tempDriverList = new ArrayList<>();
        tempDriverList.addAll(currentDrivers);
        List<DriverMetadata> latestDriverStoreList = driverStoreManager.get(latestVersion).list();
        for (DriverMetadata driverMetadata : latestDriverStoreList) {
          driverList.add(driverMetadata);
          DriverKey driverKey = new DriverKey(driverMetadata.getDriverContainerId(),
              driverMetadata.getVolumeId(), driverMetadata.getSnapshotId(),
              driverMetadata.getDriverType());
          DriverMetadata currentDriver = currentDriverStore.get(driverKey);
          //If a driver both in latestDriverStore and currentDriverStore ,report the latest one
          if (tempDriverList.contains(currentDriver)) {
            tempDriverList.remove(currentDriver);
          }
        }
        driverListByVersion.put(latestVersion, driverList);
        driverListByVersion.put(currentVersion, tempDriverList);
      }
    }

    logger.info("report driver list {}", driverListByVersion);

    if (reportDriverList == null) {
      reportDriverList = driverListByVersion;
    }
    List<String> sessionsDetail = new ArrayList<>();
    Map<String, List<String>> iqnToClients = new HashMap<>();
    Map<String, List<String>> pydToClients = new HashMap<>();
    Map<String, String> pydToIqn = new HashMap<>();
    String[] cmd = {"targetcli", "sessions", "detail"};

    int tryIndex = 0;
    while (tryIndex++ <= reportDriverClientSessionTryTimes) {
      boolean result = execCmd(cmd, sessionsDetail);
      if (!result) {
        logger.error("error to execute targetcli sessions detail command.");
      } else {
        if (sessionsDetail.size() <= 1) { /// output: (no open sessions)
          //check driver has client host access rule before?
          // if has client before, but now is no open sessions, may cmd "targetctl restore" when
          // remove driver call this, so we will retry do get sessions
          boolean foundClientBefore = false;
          for (Version versionTemp : driverListByVersion.keySet()) {
            List<DriverMetadata> driverMetadataListTemp = driverListByVersion.get(versionTemp);
            for (DriverMetadata driverMetadataTemp : driverMetadataListTemp) {
              if (driverMetadataTemp.getClientHostAccessRule() != null
                  && !driverMetadataTemp.getClientHostAccessRule().isEmpty()) {
                logger.info(
                    "driver has client before, but now targecli cannot "
                        + "found open session. so retry it");
                foundClientBefore = true;
                break;
              }
            }
          }
          if (foundClientBefore) {
            sessionsDetail.clear();
            Thread.sleep(1000);
            continue;
          }

          result = false;
        } else {
          sortByIqn(sessionsDetail, pydToClients, pydToIqn);
          iscsiTargetManager.getIqnForPydDevice(pydToIqn);
          for (Map.Entry<String, List<String>> entry : pydToClients.entrySet()) {
            if (pydToIqn.containsKey(entry.getKey()) && pydToIqn.get(entry.getKey()) != null) {
              iqnToClients.put(pydToIqn.get(entry.getKey()), entry.getValue());
              logger.debug("map for iqn:{}, clients:{}", pydToIqn.get(entry.getKey()),
                  entry.getValue().toString());
            }
          }
        }
      }

      break;
    }

    for (Map.Entry<Version, List<DriverMetadata>> entry : driverListByVersion.entrySet()) {
      for (DriverMetadata driver : entry.getValue()) {
        try {
          DriverMetadataThrift driverToReport = RequestResponseHelper
              .buildThriftDriverMetadataFrom(driver);
          request.addToDrivers(driverToReport);

          if (driverToReport.getDriverType() != DriverTypeThrift.ISCSI) {
            continue;
          }

          driverToReport.setPort(iscsiDriverPort);

          if (driverToReport.getDriverStatus() == DriverStatusThrift.ERROR) {
            // It is not necessary to show clients on driver in error state.
            // Driver in error state will never be recovery.
            continue;
          }

          List<String> clientAddressList = null;
          Map<String, AccessPermissionType> clientHostAccessRules = new HashMap<>();
          Map<String, AccessPermissionTypeThrift> clientHostAccessRulesThrift = new HashMap<>();
          /*
           * When launched iscsi driver to a volume, console display nbd server port to user. This
           *  is because we implement iscsi module using nbd driver. Infocenter stores the result
           *  of launching driver and returns the result to console. NBD server port just used in
           *  service coordinator. So set the port as default iscsi port when report to infocenter.
           */
          String targetName = null;
          targetName = lioNameBuilder.buildLioWwn(driver.getVolumeId(true), driver.getSnapshotId());
          String iqn = targetName.toLowerCase();
          if (iqnToClients.containsKey(iqn)) {
            clientAddressList = iqnToClients.get(iqn);
          }

          if (clientAddressList == null) {
            clientAddressList = new ArrayList<>();
          }
          for (String host : clientAddressList) {
            if (host != null) {
              clientHostAccessRules.put(host, AccessPermissionType.READWRITE);
            }
          }

          if (clientAltered(driver, clientHostAccessRules)) {
            driver.setClientHostAccessRule(clientHostAccessRules);
            // just save client information of driver,so reduce synchronized zone here will not
            // override information which saved anywhere
            logger.debug("going to save clients of driver {}", driver);
            driverStoreManager.get(entry.getKey()).save(driver);
            logger.debug("success to save clients of driver {},{}", driver, clientHostAccessRules);
            for (Entry<String, AccessPermissionType> entry1 : clientHostAccessRules.entrySet()) {
              clientHostAccessRulesThrift.put(entry1.getKey(),
                  AccessPermissionTypeThrift.valueOf(entry1.getValue().name()));
            }
            driverToReport.setClientHostAccessRule(clientHostAccessRulesThrift);
          }

          logger.debug("success to get clients and accessRule of driver {}", driver);
        } catch (Exception e) {
          logger.warn("Unable to collect full driver info in report. Driver detail: {}, reason: ",
              driver, e);
          // Any exception occurred during collecting info of current driver should not have effect
          // on others. throw e;
        }
      }
    }
    if (request.getDrivers() == null || request.getDrivers().isEmpty()) {
      logger.debug("no driver metadata to report");
      request.setDrivers(new ArrayList<>());
    }

    if (icClientWrapper == null) {
      try {
        icClientWrapper = informationCenterClientFactory.build();
      } catch (Exception e) {
        logger.warn("catch an exception when build infocenter client {}", e);
        return;
      }
    }
    try {
      icClientWrapper.getClient().reportDriverMetadata(request);
    } catch (Exception e) {
      logger.warn("exception happened {}", e);
      try {
        InformationCenterClientWrapper icClientWrapperNew = informationCenterClientFactory.build();
        if (icClientWrapperNew.equals(icClientWrapper)) {
          return;
        }
        logger.warn("info center has switched from {} to {}", icClientWrapper.toString(),
            icClientWrapperNew.toString());
        icClientWrapper = icClientWrapperNew;
      } catch (Exception e1) {
        logger.error("catch an exception when build infocenter client {}", e1);
        return;
      }

      try {
        icClientWrapper.getClient().reportDriverMetadata(request);
      } catch (Exception e2) {
        logger.error("exception happened {}", e2);
        return;
      }
    }
    logger.debug("finish send drivers to info center client");
  }

  /**
   *  build the map from pyd device to clients, ex: /backstores/block/pyd3 <---> 10.0.2.232
   * targetcli sessions detail output format.
   * alias: server232 sid:1 type: Normal session-state: LOGGED_IN
   * name: iqn.1996-04.de.suse:01:4b4df92730f1 (NOT AUTHENTICATED)
   * mapped-lun: 0 backstore: block/pyd3 mode: rw
   * address: 10.0.2.232 (TCP)  cid: 0 connection-state: LOGGED_IN
   */
  private void sortByIqn(List<String> sessionsDetail, Map<String, List<String>> pydToClients,
      Map<String, String> pydToIqn) {
    String backstore = null;
    String client = null;
    for (String line : sessionsDetail) {
      line = line.trim();
      if (line.contains("backstore")) {
        String[] str = line.split("\\s+");
        if (str.length >= 4) {
          backstore = "/backstores/" + str[3];
        }
      }
      if (line.contains("address")) {
        String[] str = line.split("\\s+");
        if (str.length >= 2) {
          client = str[1];
        }
      }
      if (client != null && backstore == null) {
        logger.error("error to match iqn to client.");
        client = null;
      }
      if (backstore != null && client != null) {
        if (pydToClients.containsKey(backstore)) {
          pydToClients.get(backstore).add(client);
          logger.debug("other times, backstore:{}, client:{}", backstore, client);
        } else {
          List<String> clients = new ArrayList<>();
          clients.add(client);
          pydToClients.put(backstore, clients);
          logger.debug("first time, backstore:{}, client:{}", backstore, client);
          pydToIqn.put(backstore, null);
        }
        backstore = null;
        client = null;
      }
    }
  }

  boolean clientAltered(DriverMetadata driver,
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

  private boolean execCmd(String[] cmd, List<String> output) {
    logger.info("exec cmd:{}", Arrays.toString(cmd));
    BufferedReader reader = null;
    try {
      Process process = null;
      ProcessBuilder pbuilder = new ProcessBuilder(cmd);
      pbuilder.redirectErrorStream(true);
      process = pbuilder.start();
      reader = new BufferedReader(new InputStreamReader(process.getInputStream(), "UTF-8"));
      String line = null;
      while ((line = reader.readLine()) != null) {
        if (output != null) {
          output.add(line);
        }
      }
      process.waitFor();
      if (process.exitValue() != 0) {
        if (output != null && output.size() == 0) {
          return true;
        } else {
          logger.debug("cmd fail to execute, errno {}", process.exitValue());
          for (int i = 0; i < cmd.length; i++) {
            logger.debug("command parameter : [{}]", cmd[i]);
          }
        }
        return false;
      }
    } catch (IOException e) {
      logger.warn("IOException happens {}", e);
      return false;
    } catch (InterruptedException e) {
      logger.warn("InterruptedException happens {}", e);
      return false;
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          logger.error("caught an exception when try to close reader {}", e);
        }
      }
      logger.info("exec cmd:{} result:{}", Arrays.toString(cmd), output);
    }
    return true;
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

  public InformationCenterClientFactory getInformationCenterClientFactory() {
    return informationCenterClientFactory;
  }

  public void setInformationCenterClientFactory(
      InformationCenterClientFactory informationCenterClientFactory) {
    this.informationCenterClientFactory = informationCenterClientFactory;
  }

  public int getIscsiDriverPort() {
    return iscsiDriverPort;
  }

  public void setIscsiDriverPort(int iscsiDriverPort) {
    this.iscsiDriverPort = iscsiDriverPort;
  }

  public Map<Version, List<DriverMetadata>> getReportDriverList() {
    return reportDriverList;
  }

  public void setReportDriverList(Map<Version, List<DriverMetadata>> reportDriverList) {
    this.reportDriverList = reportDriverList;
  }

  public DriverContainerConfiguration getDriverContainerConfiguration() {
    return driverContainerConfiguration;
  }

  public void setDriverContainerConfiguration(
      DriverContainerConfiguration driverContainerConfiguration) {
    this.driverContainerConfiguration = driverContainerConfiguration;
  }

  public IscsiTargetManager getIscsiTargetManager() {
    return iscsiTargetManager;
  }

  public void setIscsiTargetManager(IscsiTargetManager iscsiTargetManager) {
    this.iscsiTargetManager = iscsiTargetManager;
  }

  public LioNameBuilder getLioNameBuilder() {
    return lioNameBuilder;
  }

  public void setLioNameBuilder(LioNameBuilder lioNameBuilder) {
    this.lioNameBuilder = lioNameBuilder;
  }

  public AppContext getAppContext() {
    return appContext;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }

  public int getReportDriverClientSessionTryTimes() {
    return reportDriverClientSessionTryTimes;
  }

  public void setReportDriverClientSessionTryTimes(int reportDriverClientSessionTryTimes) {
    this.reportDriverClientSessionTryTimes = reportDriverClientSessionTryTimes;
  }
}
