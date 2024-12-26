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
