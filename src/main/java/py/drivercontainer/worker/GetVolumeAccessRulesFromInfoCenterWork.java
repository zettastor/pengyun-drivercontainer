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

package py.drivercontainer.worker;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.driver.DriverMetadata;
import py.driver.DriverType;
import py.drivercontainer.driver.store.DriverStore;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.icshare.exception.VolumeNotFoundException;
import py.infocenter.client.InformationCenterClientFactory;
import py.periodic.Worker;
import py.thrift.infocenter.service.InformationCenter;
import py.thrift.share.ListVolumeAccessRulesByVolumeIdsRequest;
import py.thrift.share.ListVolumeAccessRulesByVolumeIdsResponse;
import py.thrift.share.VolumeAccessRuleThrift;

/**
 * xx.
 */
public class GetVolumeAccessRulesFromInfoCenterWork implements Worker {

  private static final Logger logger = LoggerFactory
      .getLogger(GetVolumeAccessRulesFromInfoCenterWork.class);
  InformationCenter.Iface icClient = null;
  private InformationCenterClientFactory informationCenterClientFactory;
  private DriverStoreManager driverStoreManager;
  private Map<Long, List<VolumeAccessRuleThrift>> volumeAccessRuleTable;

  /**
   * xx.
   */

  //Get volume accessRule from infocenter and coordinator will get it from drivercontainer
  @Override
  public void doWork() throws Exception {
    Set<Long> volumeIds = new HashSet<Long>();
    for (DriverStore driverStore : driverStoreManager.values()) {
      List<DriverMetadata> driverMetadataList = driverStore.list();
      for (DriverMetadata driver : driverMetadataList) {
        if (driver.getDriverType() != DriverType.ISCSI) {
          volumeIds.add(driver.getVolumeId());
        }
      }
    }

    /* update volumeAccessRuleTable according info from infocenter */
    logger.debug("get volume acl from infoCenter for all volumes:{} volumeAccessRuleTable {}",
        volumeIds, volumeAccessRuleTable);
    Map<Long, List<VolumeAccessRuleThrift>> accessRuleTable = getVolumeAccessRulesFromInfoCenter(
        volumeIds);
    if (accessRuleTable == null) {
      return;
    }
    synchronized (volumeAccessRuleTable) {
      for (long volumeId : volumeAccessRuleTable.keySet()) {
        if (accessRuleTable.containsKey(volumeId)) {
          volumeAccessRuleTable.put(volumeId, accessRuleTable.get(volumeId));
        } else {
          volumeAccessRuleTable.remove(volumeId);
        }
      }

      for (Map.Entry<Long, List<VolumeAccessRuleThrift>> entry : accessRuleTable.entrySet()) {
        if (!volumeAccessRuleTable.containsKey(entry.getKey())) {
          volumeAccessRuleTable.put(entry.getKey(), entry.getValue());
        }
      }
    }
    volumeIds.clear();
  }


  /**
   * xx.
   */
  public Map<Long, List<VolumeAccessRuleThrift>> getVolumeAccessRulesFromInfoCenter(
      Set<Long> volumeIds)
      throws VolumeNotFoundException {

    Map<Long, List<VolumeAccessRuleThrift>> volumeAccessRule = new ConcurrentHashMap<>();
    try {
      if (icClient == null) {
        icClient = informationCenterClientFactory.build().getClient();
      }
      ListVolumeAccessRulesByVolumeIdsRequest rulesRequest =
          new ListVolumeAccessRulesByVolumeIdsRequest(
              RequestIdBuilder.get(), volumeIds);
      ListVolumeAccessRulesByVolumeIdsResponse response = icClient
          .listVolumeAccessRulesByVolumeIds(rulesRequest);

      volumeAccessRule.clear();

      if (response.getAccessRulesTable() != null && response.getAccessRulesTableSize() > 0) {
        volumeAccessRule = response.getAccessRulesTable();
      }
    } catch (Exception e) {
      logger.warn("Caught an exception when get access rules for volume {}, {}", volumeIds, e);
      try {
        InformationCenter.Iface icClientNew = informationCenterClientFactory.build().getClient();
        if (icClient == icClientNew) {
          return null;
        }
        icClient = icClientNew;
        ListVolumeAccessRulesByVolumeIdsRequest rulesRequest =
            new ListVolumeAccessRulesByVolumeIdsRequest(RequestIdBuilder.get(), volumeIds);
        ListVolumeAccessRulesByVolumeIdsResponse response = icClient
            .listVolumeAccessRulesByVolumeIds(rulesRequest);

        volumeAccessRule.clear();

        if (response.getAccessRulesTable() != null && response.getAccessRulesTableSize() > 0) {
          volumeAccessRule = response.getAccessRulesTable();
        }
      } catch (Exception e1) {
        logger.warn("Caught an exception {}", e);
        return null;
      }
    }

    return volumeAccessRule;
  }

  public void setInformationCenterClientFactory(
      InformationCenterClientFactory informationCenterClientFactory) {
    this.informationCenterClientFactory = informationCenterClientFactory;
  }

  public Map<Long, List<VolumeAccessRuleThrift>> getVolumeAccessRuleTable() {
    return volumeAccessRuleTable;
  }

  public void setVolumeAccessRuleTable(
      Map<Long, List<VolumeAccessRuleThrift>> volumeAccessRuleTable) {
    this.volumeAccessRuleTable = volumeAccessRuleTable;
  }

  public DriverStoreManager getDriverStoreManager() {
    return driverStoreManager;
  }

  public void setDriverStoreManager(DriverStoreManager driverStoreManager) {
    this.driverStoreManager = driverStoreManager;
  }
}
