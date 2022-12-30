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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.common.RequestIdBuilder;
import py.coordinator.IscsiAclProcessType;
import py.coordinator.IscsiTargetManager;
import py.coordinator.lio.LioNameBuilder;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.driver.IscsiAccessRule;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.DriverVersion;
import py.drivercontainer.driver.store.DriverStore;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.drivercontainer.driver.version.Version;
import py.drivercontainer.driver.version.VersionManager;
import py.drivercontainer.lio.saveconfig.SaveConfig;
import py.drivercontainer.lio.saveconfig.jsonobj.SaveConfigImpl;
import py.infocenter.client.InformationCenterClientFactory;
import py.periodic.Worker;
import py.thrift.infocenter.service.InformationCenter;
import py.thrift.share.DriverKeyThrift;
import py.thrift.share.DriverTypeThrift;
import py.thrift.share.GetIscsiAccessRulesRequest;
import py.thrift.share.GetIscsiAccessRulesResponse;
import py.thrift.share.IscsiAccessRuleThrift;
import py.thrift.share.ReportIscsiAccessRulesRequest;

public class UpdateIscsiAccessRuleFileSweepWork implements Worker {

  private static final Logger logger = LoggerFactory
      .getLogger(UpdateIscsiAccessRuleFileSweepWork.class);
  private InformationCenterClientFactory infoCenterClientFactory;
  //    private InitiatorsAllowFileMapper initiatorsAllowFileMapper;
  private DriverContainerConfiguration driverContainerConfig;
  private String filePath;
  private IscsiTargetManager iscsiTargetManager;
  private LioNameBuilder lioNameBuilder;
  private DriverStoreManager driverStoreManager;
  private VersionManager versionManager;
  private Map<String, List<String>> retrievedAcls = new HashMap<String, List<String>>();
  private Map<String, Boolean> targetsCreated = new HashMap<String, Boolean>();

  @Override
  public void doWork() throws Exception {
    Version currentVersion = DriverVersion.currentVersion.get(DriverType.NBD);
    if (currentVersion == null) {
      return;
    }

    if (driverStoreManager.get(currentVersion) == null) {
      return;
    }
    List<DriverMetadata> driverMetadataList = driverStoreManager.get(currentVersion).list();
    for (DriverMetadata driver : driverMetadataList) {
      if (driver.getDriverType() == DriverType.ISCSI) {
        DriverStatus driverStatus = driver.getDriverStatus();
        if (driverStatus.equals(DriverStatus.LAUNCHED)) {
          applyIscsiAccessRule(driver);
        }

      }
    }
  }

  /**
   * applyIscsiAccessRule Iscsi using iscsiAccessRule Not volmeAccessRule Iet set initiatorname to
   * initial.allow & set user passwd using ietadm cmd lio set initiatorname user passwd to
   * configfile.
   */

  public void applyIscsiAccessRule(DriverMetadata driver) {
    InformationCenter.Iface client;
    //If driver has oldDriverKey means it has been changed volumeId fro volume migration .But iscsi
    // target and pyd client for ISCSI driver use the old volumeId to build targetName.

    DriverKeyThrift driverKey;
    long volumeId = driver.getVolumeId(true);
    driverKey = new DriverKeyThrift(driver.getDriverContainerId(), driver.getVolumeId(),
        driver.getSnapshotId(), DriverTypeThrift.valueOf(driver.getDriverType().name()));
    try {
      client = infoCenterClientFactory.build().getClient();
      GetIscsiAccessRulesRequest request = new GetIscsiAccessRulesRequest();
      request.setRequestId(RequestIdBuilder.get());
      request.setDriverKey(driverKey);
      GetIscsiAccessRulesResponse response = client.getIscsiAccessRules(request);
      List<IscsiAccessRuleThrift> rulesFromInfoCenter = response.getAccessRules();
      List<IscsiAccessRule> rules = new ArrayList<>();
      if (rulesFromInfoCenter != null) {
        for (IscsiAccessRuleThrift rule : rulesFromInfoCenter) {
          py.driver.IscsiAccessRule ruleTemp = RequestResponseHelper
              .convertIscsiAccessRuleThrift2IscsiAccessRule(rule);
          rules.add(ruleTemp);
        }
      }
      String iqn = lioNameBuilder.buildLioWwn(volumeId, driver.getSnapshotId());
      if (targetNotExist(iqn)) {
        return;
      }

      List<IscsiAccessRule> localIscsiAccessRules = iscsiTargetManager.getIscsiAccessRuleList(iqn);
      if (localIscsiAccessRules == null) {
        localIscsiAccessRules = new ArrayList<>();
        retrieveIscsiAcl(iqn, localIscsiAccessRules, rules);
      }

      //todo:when delete access rules, we must make sure no iscsi launched now
      applyIscsiAcl(iqn, localIscsiAccessRules, rules, driver);

      ReportIscsiAccessRulesRequest reportIscsiAccessRulesRequest =
          new ReportIscsiAccessRulesRequest();
      reportIscsiAccessRulesRequest.setRequestId(RequestIdBuilder.get());
      reportIscsiAccessRulesRequest.setDriverKey(driverKey);
      reportIscsiAccessRulesRequest.setAccessRules(response.getAccessRules());
      client.reportIscsiAccessRules(reportIscsiAccessRulesRequest);

    } catch (Exception e) {
      logger.warn("error while checking iscsi access rule {}", e);
    }
  }

  /**
   * apply acl for iscsi.
   */
  private void applyIscsiAcl(String iqn, List<IscsiAccessRule> localIscsiAccessRules,
      List<IscsiAccessRule> rules, DriverMetadata driver) {
    if (iscsiTargetManager.getChapControlStatus(iqn) == null
        || driver.getChapControl() != iscsiTargetManager.getChapControlStatus(iqn)) {
      logger.debug(" applyIscsiAccessRule driver new chap {} old chap {} iqn {}",
          driver.getChapControl(),
          iscsiTargetManager.getChapControlStatus(iqn), iqn);
      boolean ret = false;
      if (driver.getChapControl() == 0) {
        ret = iscsiTargetManager.setAttributeAuthenticationWrap(iqn, 0);
      } else if (driver.getChapControl() == 1) {
        ret = iscsiTargetManager.setAttributeAuthenticationWrap(iqn, 1);
      }
      if (!ret) {
        logger.warn("fail to set authentication");
        return;
      }
    }

    if (!listContentSame(localIscsiAccessRules, rules)) {
      logger.warn(" client rule from {} turn to {} {}", localIscsiAccessRules, rules, iqn);
      Version currentVersion = DriverVersion.currentVersion.get(DriverType.NBD);
      if (currentVersion == null) {
        logger.error("currentVersion == null");
        return;
      }

      if (driverStoreManager.get(currentVersion) == null) {
        logger.error("driverStoreManager.get(currentVersion) == null");
        return;
      }

      List<IscsiAclProcessType> opTypes = new ArrayList<>();
      List<IscsiAccessRule> diffRules = new ArrayList<>();
      //TO DO : if it is modification and not create or delete, need to compare more
      for (IscsiAccessRule list1element : localIscsiAccessRules) {
        String initiatorName = list1element.getInitiatorName();
        boolean found = false;
        for (IscsiAccessRule list2element : rules) {
          if (list2element.getInitiatorName().compareTo(initiatorName) == 0) {
            found = true;
            break;
          }
        }
        if (!found) {
          diffRules.add(list1element);
          opTypes.add(IscsiAclProcessType.DELETE);
        }
      }
      for (IscsiAccessRule list2element : rules) {
        String initiatorName = list2element.getInitiatorName();
        boolean found = false;
        for (IscsiAccessRule list1element : localIscsiAccessRules) {
          if (list1element.getInitiatorName().compareTo(initiatorName) == 0) {
            found = true;
            break;
          }
        }
        if (!found) {
          diffRules.add(list2element);
          opTypes.add(IscsiAclProcessType.CREATE);
        }
      }

      if (diffRules.size() == 0) {
        logger.warn("no diff acl to apply, need to check codes");
        diffRules = null;
        opTypes = null;
        return;
      }
      iscsiTargetManager.saveAccessRuleToConfigFile(iqn, diffRules, opTypes);
      driver.setAclList(iscsiTargetManager.getIscsiAccessRuleList(iqn));
      DriverStore driverStore = driverStoreManager.get(currentVersion);
      driverStore.saveAcl(driver);
    }
  }

  /**
   * retrieve iscsi acls to rule map when restart/upgrade drivercontainer.
   */
  private void retrieveIscsiAcl(String iqn, List<IscsiAccessRule> localIscsiAccessRules,
      List<IscsiAccessRule> rules) {
    /* retrievedAcls : the target and acls info getting from saveconfig.json
     * rules : acls info about iqn getting from info center.
     * for three cases
     * case 1: acls in rules > acls in retrievedAcls, restore acls in retrievedAcls to
     * localIscsiAccessRules, create the rest of acls in rules.
     * case 2: acls in rules == acls in retrievedAcls, restore acls in retrievedAcls to
     * localIscsiAccessRules and do nothing.
     * case 3: acls in rules < acls in retrievedAcls, restore acls in rules to
     * localIscsiAccessRules and delete the rest of acls in retrievedAcls.
     * */
    if (retrievedAcls != null && retrievedAcls.size() > 0 && retrievedAcls.containsKey(iqn)) {
      for (String nodeWwn : retrievedAcls.get(iqn)) {
        boolean nonExistAcl = true;
        for (IscsiAccessRule rule : rules) {
          logger.debug("initiator name : {}", rule.getInitiatorName());
          if (rule.getInitiatorName().equals(nodeWwn)) {
            logger.debug("add one acl, nodeWWN {}", nodeWwn);
            localIscsiAccessRules.add(rule);
            nonExistAcl = false;
          }
        }
        if (nonExistAcl) {
          iscsiTargetManager.deleteAccessRule(iqn, nodeWwn);
          logger.debug("delete useless access rule, iqn {}, nodeWWN {}", iqn, nodeWwn);
        }
      }
      retrievedAcls.remove(iqn);
      if (retrievedAcls.size() == 0) {
        retrievedAcls = null;
      }
      if (localIscsiAccessRules.size() > 0) {
        iscsiTargetManager.saveAccessRuleToMap(iqn, localIscsiAccessRules);
      }
    }
  }

  /**
   * check if target exist with respect to iqn for iscsi.
   *
   * @return true: nonexistent, false: existent
   */
  private boolean targetNotExist(String iqn) {
    if (targetsCreated.containsKey(iqn)) {
      boolean created = targetsCreated.get(iqn);
      if (!created) {
        boolean exist = checkTargetExist(iqn);
        if (exist) {
          targetsCreated.put(iqn, true);
        } else {
          logger.warn("target not created yet");
          return true;
        }
      }
    } else {
      boolean exist = checkTargetExist(iqn);
      if (exist) {
        targetsCreated.put(iqn, true);
      } else {
        targetsCreated.put(iqn, false);
        logger.warn("target not created");
        return true;
      }
    }
    return false;
  }


  /**
   * xx.
   */
  public boolean listContentEquals(List list1, List list2) {
    if (list1.size() != list2.size()) {
      return false;
    } else {
      for (Object object : list1) {
        if (!list2.contains(object)) {
          return false;
        }
      }
    }
    return true;
  }


  /**
   * xx.
   */
  public boolean listContentSame(List<IscsiAccessRule> list1, List<IscsiAccessRule> list2) {
    if (list1.size() != list2.size()) {
      return false;
    } else {
      for (IscsiAccessRule list1element : list1) {
        String initiatorName = list1element.getInitiatorName();
        boolean found = false;
        for (IscsiAccessRule list2element : list2) {
          if (list2element.getInitiatorName().compareTo(initiatorName) == 0) {
            found = true;
            break;
          }
        }
        if (!found) {
          return false;
        }
      }
      for (IscsiAccessRule list2element : list2) {
        String initiatorName = list2element.getInitiatorName();
        boolean found = false;
        for (IscsiAccessRule list1element : list1) {
          if (list1element.getInitiatorName().compareTo(initiatorName) == 0) {
            found = true;
            break;
          }
        }
        if (!found) {
          return false;
        }
      }
    }
    return true;
  }

  /* get targets and initiators map from saveconfig.json when drivercontainer bootstraps */

  /**
   * xx.
   */
  public void retrieveIscsiAccessRule() {
    if (retrievedAcls == null) {
      logger.error("retrievedAcls is null");
      return;
    }
    retrievedAcls.clear();
    SaveConfig config = new SaveConfigImpl(filePath);
    boolean res = config.load();
    if (res == false) {
      return;
    }
    retrievedAcls = config.getTargetsMap();
  }


  /**
   * xx.
   */
  public boolean checkTargetExist(String targetName) {
    SaveConfig config = new SaveConfigImpl(filePath);
    boolean res = config.load();
    if (res == false) {
      return false;
    }
    return config.existTarget(targetName);
  }

  public InformationCenterClientFactory getInfoCenterClientFactory() {
    return infoCenterClientFactory;
  }

  public void setInfoCenterClientFactory(InformationCenterClientFactory infoCenterClientFactory) {
    this.infoCenterClientFactory = infoCenterClientFactory;
  }

  public DriverStoreManager getDriverStoreManager() {
    return driverStoreManager;
  }

  public void setDriverStoreManager(DriverStoreManager driverStoreManager) {
    this.driverStoreManager = driverStoreManager;
  }

  public DriverContainerConfiguration getDriverContainerConfig() {
    return driverContainerConfig;
  }

  public void setDriverContainerConfig(DriverContainerConfiguration driverContainerConfig) {
    this.driverContainerConfig = driverContainerConfig;
  }

  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
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

  public VersionManager getVersionManager() {
    return versionManager;
  }

  public void setVersionManager(VersionManager versionManager) {
    this.versionManager = versionManager;
  }
}
