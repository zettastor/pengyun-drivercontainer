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

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.common.RequestIdBuilder;
import py.drivercontainer.scsi.ScsiManager;
import py.drivercontainer.scsi.ScsiMetadata;
import py.infocenter.client.InformationCenterClientFactory;
import py.infocenter.client.InformationCenterClientWrapper;
import py.periodic.Worker;
import py.thrift.icshare.ReportScsiDriverMetadataRequest;
import py.thrift.icshare.ScsiDeviceInfoThrift;
import py.thrift.share.ScsiDeviceStatusThrift;

public class ScsiDeviceScanWorker implements Runnable, Worker {

  private static final Logger logger = LoggerFactory.getLogger(ScsiDeviceScanWorker.class);
  InformationCenterClientWrapper icClientWrapper = null;
  private InformationCenterClientFactory informationCenterClientFactory;
  private ScsiManager scsiManager;
  private boolean retrieveDone = false;
  private AppContext appContext;

  @Override
  public void run() {
    // do work in a thread
    doWork();
  }

  @Override
  public void doWork() {
    logger.debug("ScsiDeviceScanWorker enter.");
    if (!retrieveDone && scsiManager != null) {
      retrieveDone = true;
      scsiManager.retrieveScsiDevice();
    }
    if (scsiManager == null || scsiManager.getScsiList() == null) {
      return;
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
      ReportScsiDriverMetadataRequest request = new ReportScsiDriverMetadataRequest();
      request.setRequestId(RequestIdBuilder.get());
      if (scsiManager.getScsiList().size() > 0) {
        List<ScsiDeviceInfoThrift> scsiList = new ArrayList<>(scsiManager.getScsiList().size());
        List<ScsiMetadata> list = scsiManager.getScsiList();
        for (ScsiMetadata item : list) {
          ScsiDeviceInfoThrift scsiInfo = new ScsiDeviceInfoThrift();
          scsiInfo.setVolumeId(item.getVolumeId());
          scsiInfo.setSnapshotId(item.getSnapshotId());
          scsiInfo.setDriverIp(item.getIp());
          scsiInfo.setScsiDevice(item.getScsiDevice());
          scsiInfo.setScsiDeviceStatus(item.getStatus());
          scsiList.add(scsiInfo);
        }
        request.setScsiList(scsiList);
      } else {
        List<ScsiDeviceInfoThrift> scsiList = new ArrayList<>(1);
        request.setScsiList(scsiList);
      }
      request.setDrivercontainerId(appContext.getInstanceId().getId());

      try {
        icClientWrapper.getClient().reportScsiDriverMetadata(request);
        logger.debug("report {}", request.toString());
      } catch (Exception e) {
        logger.warn("exception happened {}", e);
        try {
          InformationCenterClientWrapper icClientWrapperNew = informationCenterClientFactory
              .build();
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
          icClientWrapper.getClient().reportScsiDriverMetadata(request);
        } catch (Exception e2) {
          logger.error("exception happened {}", e2);
          return;
        }
      }
    } catch (Exception e) {
      logger.warn("Exception happens {}", e);
      return;
    }

    List<ScsiMetadata> list = scsiManager.getScsiList();
    for (ScsiMetadata item : list) {
      if (item.getStatus().equals(ScsiDeviceStatusThrift.CONNECTEXCEPTIONRECOVERING)) {
        scsiManager.mapScsiDeviceAfterRestart(item);
      } else if (item.getStatus().equals(ScsiDeviceStatusThrift.RECOVERY)
          || item.getStatus().equals(ScsiDeviceStatusThrift.NORMAL)) {
        scsiManager.checkPydConnection(item);
      }
    }
  }

  public InformationCenterClientFactory getInformationCenterClientFactory() {
    return informationCenterClientFactory;
  }

  public void setInformationCenterClientFactory(
      InformationCenterClientFactory informationCenterClientFactory) {
    this.informationCenterClientFactory = informationCenterClientFactory;
  }

  public ScsiManager getScsiManager() {
    return scsiManager;
  }

  public void setScsiManager(ScsiManager scsiManager) {
    this.scsiManager = scsiManager;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }
}
