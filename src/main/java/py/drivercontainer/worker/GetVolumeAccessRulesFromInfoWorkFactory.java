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
