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
