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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.common.RequestIdBuilder;
import py.infocenter.client.InformationCenterClientFactory;
import py.periodic.Worker;
import py.thrift.infocenter.service.InformationCenter;
import py.thrift.share.DriverKeyThrift;
import py.thrift.share.GetIoLimitationRequestThrift;
import py.thrift.share.GetIoLimitationResponseThrift;
import py.thrift.share.IoLimitationThrift;

public class GetIoLimitationWork implements Worker {

  private static final Logger logger = LoggerFactory
      .getLogger(GetVolumeAccessRulesFromInfoCenterWork.class);
  private InformationCenterClientFactory informationCenterClientFactory;
  private Map<DriverKeyThrift, List<IoLimitationThrift>> driver2ItsIoLimitationsTable;
  private AppContext appContext;

  @Override
  public void doWork() throws Exception {
    logger.debug("Going to get ioLimitations from infoCenter for driverContainer:{}",
        appContext.getInstanceId().getId());
    try {
      Map<DriverKeyThrift, List<IoLimitationThrift>> curIoLimitationTable =
          getIoLimitationFromInfoCenter(appContext.getInstanceId().getId());
      if (curIoLimitationTable == null || curIoLimitationTable.isEmpty()) {
        logger.info("get ioLimitations from infoCenter failed, not update table");
        return;
      }

      logger.debug("CurIoLimitationTable:{} on {}", curIoLimitationTable,
          appContext.getInstanceId().getId());

      // no need synchronized (driver2ItsIOLimitationsTable) because ConcurrentHashMap
      // get/put/remove/containsKey are sync interface
      for (DriverKeyThrift driverKey : driver2ItsIoLimitationsTable.keySet()) {
        if (curIoLimitationTable.containsKey(driverKey)) {
          driver2ItsIoLimitationsTable.put(driverKey, curIoLimitationTable.get(driverKey));
        } else {
          driver2ItsIoLimitationsTable.remove(driverKey);
        }
      }
      for (Map.Entry<DriverKeyThrift, List<IoLimitationThrift>> entry : curIoLimitationTable
          .entrySet()) {
        if (!driver2ItsIoLimitationsTable.containsKey(entry.getKey())) {
          driver2ItsIoLimitationsTable.put(entry.getKey(), entry.getValue());
        }
      }

      logger.debug("updated driver2ItsIOLimitationsTable:{} ", driver2ItsIoLimitationsTable);
    } catch (Exception e) {
      logger.error("get ioLimitations from infoCenter exception, not update table", e);
    }

  }

  private Map<DriverKeyThrift, List<IoLimitationThrift>> getIoLimitationFromInfoCenter(
      long driverContainerId)
      throws Exception {
    Map<DriverKeyThrift, List<IoLimitationThrift>> limitationsTable = null;
    try {
      InformationCenter.Iface icClient = informationCenterClientFactory.build().getClient();
      GetIoLimitationRequestThrift request = new GetIoLimitationRequestThrift(
          RequestIdBuilder.get(), driverContainerId);
      GetIoLimitationResponseThrift response = icClient
          .getIoLimitationsInOneDriverContainer(request);
      limitationsTable = response.getMapDriver2ItsIoLimitations();
      logger.debug("getIoLimitationFromInfoCenter:request {} limitationsTable {}", request,
          limitationsTable);
    } catch (Exception e) {
      logger.error("Caught an exception when get ioLimitations on driverContainer {}",
          driverContainerId, e);
    }
    return limitationsTable;
  }

  public InformationCenterClientFactory getInformationCenterClientFactory() {
    return informationCenterClientFactory;
  }

  public void setInformationCenterClientFactory(
      InformationCenterClientFactory informationCenterClientFactory) {
    this.informationCenterClientFactory = informationCenterClientFactory;
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
