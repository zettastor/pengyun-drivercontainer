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

package py.drivercontainer.scsi;

import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.thrift.share.ScsiDeviceStatusThrift;

public class ScsiMetadata {

  private static final Logger logger = LoggerFactory.getLogger(ScsiMetadata.class);

  private String wwn;

  private String iqn;

  private Long volumeId;

  private Integer snapshotId;

  private String pydDevice;

  private String ip;

  private String scsiDevice;

  private ScsiDeviceStatusThrift status;

  public ScsiMetadata() {

  }


  /**
   * xx.
   */
  public ScsiMetadata(String wwn, String iqn, Long volumeId, Integer snapshotId, String pydDevice,
      String ip,
      String scsiDevice) {
    this.wwn = wwn;
    this.iqn = iqn;
    this.volumeId = volumeId;
    this.snapshotId = snapshotId;
    this.pydDevice = pydDevice;
    this.ip = ip;
    this.scsiDevice = scsiDevice;
  }

  public String getWwn() {
    return wwn;
  }

  public void setWwn(String wwn) {
    this.wwn = wwn;
  }

  public String getIqn() {
    return iqn;
  }

  public void setIqn(String iqn) {
    this.iqn = iqn;
  }

  public Long getVolumeId() {
    return volumeId;
  }

  public void setVolumeId(Long volumeId) {
    this.volumeId = volumeId;
  }

  public Integer getSnapshotId() {
    return snapshotId;
  }

  public void setSnapshotId(Integer snapshotId) {
    this.snapshotId = snapshotId;
  }

  public String getPydDevice() {
    return pydDevice;
  }

  public void setPydDevice(String pydDevice) {
    this.pydDevice = pydDevice;
  }

  public String getIp() {
    return ip;
  }

  public void setIp(String ip) {
    this.ip = ip;
  }

  public String getScsiDevice() {
    return scsiDevice;
  }

  public void setScsiDevice(String scsiDevice) {
    this.scsiDevice = scsiDevice;
  }

  public ScsiDeviceStatusThrift getStatus() {
    return status;
  }

  public void setStatus(ScsiDeviceStatusThrift status) {
    this.status = status;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ScsiMetadata)) {
      return false;
    }
    ScsiMetadata that = (ScsiMetadata) o;
    return Objects.equals(getWwn(), that.getWwn()) && Objects.equals(getIqn(), that.getIqn())
        && Objects
        .equals(getVolumeId(), that.getVolumeId()) && Objects
        .equals(getSnapshotId(), that.getSnapshotId())
        && Objects.equals(getIp(), that.getIp());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getWwn(), getIqn(), getVolumeId(), getSnapshotId(), getIp());
  }

  @Override
  public String toString() {
    return "ScsiMetadata{" + "wwn='" + wwn + '\'' + ", iqn='" + iqn + '\'' + ", volumeId="
        + volumeId
        + ", snapshotId=" + snapshotId + ", pydDevice='" + pydDevice + '\'' + ", ip='" + ip + '\''
        + ", scsiDevice='" + scsiDevice + '\'' + ", status=" + status + '}';
  }
}
