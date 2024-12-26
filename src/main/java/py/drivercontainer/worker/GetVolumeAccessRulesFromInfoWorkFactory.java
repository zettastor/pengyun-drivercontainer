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

import java.util.List;
import java.util.Map;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.infocenter.client.InformationCenterClientFactory;
import py.periodic.Worker;
import py.periodic.WorkerFactory;
import py.thrift.share.VolumeAccessRuleThrift;

public class GetVolumeAccessRulesFromInfoWorkFactory implements WorkerFactory {

  private GetVolumeAccessRulesFromInfoCenterWork work;
  private InformationCenterClientFactory inforCenterClientFactory;
  private DriverStoreManager driverStoreManager;
  private Map<Long, List<VolumeAccessRuleThrift>> volumeAccessRuleTable;

  @Override
  public Worker createWorker() {
    if (work == null) {
      work = new GetVolumeAccessRulesFromInfoCenterWork();
      work.setDriverStoreManager(driverStoreManager);
      work.setInformationCenterClientFactory(inforCenterClientFactory);
      work.setVolumeAccessRuleTable(volumeAccessRuleTable);
    }
    return work;
  }

  public GetVolumeAccessRulesFromInfoCenterWork getWork() {
    return work;
  }

  public void setWork(GetVolumeAccessRulesFromInfoCenterWork work) {
    this.work = work;
  }

  public InformationCenterClientFactory getInforCenterClientFactory() {
    return inforCenterClientFactory;
  }

  public void setInforCenterClientFactory(InformationCenterClientFactory inforCenterClientFactory) {
    this.inforCenterClientFactory = inforCenterClientFactory;
  }


  public DriverStoreManager getDriverStoreManager() {
    return driverStoreManager;
  }

  public void setDriverStoreManager(DriverStoreManager driverStoreManager) {
    this.driverStoreManager = driverStoreManager;
  }

  public Map<Long, List<VolumeAccessRuleThrift>> getVolumeAccessRuleTable() {
    return volumeAccessRuleTable;
  }

  public void setVolumeAccessRuleTable(
      Map<Long, List<VolumeAccessRuleThrift>> volumeAccessRuleTable) {
    this.volumeAccessRuleTable = volumeAccessRuleTable;
  }
}
