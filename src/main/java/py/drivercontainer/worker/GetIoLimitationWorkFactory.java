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
import py.app.context.AppContext;
import py.infocenter.client.InformationCenterClientFactory;
import py.periodic.Worker;
import py.periodic.WorkerFactory;
import py.thrift.share.DriverKeyThrift;
import py.thrift.share.IoLimitationThrift;


public class GetIoLimitationWorkFactory implements WorkerFactory {

  private GetIoLimitationWork work;
  private InformationCenterClientFactory inforCenterClientFactory;
  private Map<DriverKeyThrift, List<IoLimitationThrift>> driver2ItsIoLimitationsTable;
  // used to get drivercontainedId
  private AppContext appContext;

  @Override
  public Worker createWorker() {
    if (work == null) {
      work = new GetIoLimitationWork();
      work.setInformationCenterClientFactory(inforCenterClientFactory);
      work.setDriver2ItsIoLimitationsTable(driver2ItsIoLimitationsTable);
      work.setAppContext(appContext);
    }
    return work;
  }

  public GetIoLimitationWork getWork() {
    return work;
  }

  public void setWork(GetIoLimitationWork work) {
    this.work = work;
  }

  public InformationCenterClientFactory getInforCenterClientFactory() {
    return inforCenterClientFactory;
  }

  public void setInforCenterClientFactory(InformationCenterClientFactory inforCenterClientFactory) {
    this.inforCenterClientFactory = inforCenterClientFactory;
  }

  public Map<DriverKeyThrift, List<IoLimitationThrift>> getDriver2ItsIoLimitationsTable() {
    return driver2ItsIoLimitationsTable;
  }

  public void setDriver2ItsIoLimitationsTable(
      Map<DriverKeyThrift, List<IoLimitationThrift>> driver2ItsIoLimitationsTable) {
    this.driver2ItsIoLimitationsTable = driver2ItsIoLimitationsTable;
  }

  public AppContext getAppContext() {
    return appContext;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }
}
