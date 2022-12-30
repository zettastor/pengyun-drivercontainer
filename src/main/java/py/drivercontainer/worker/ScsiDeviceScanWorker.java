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
