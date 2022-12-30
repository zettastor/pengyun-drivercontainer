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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.coordinator.lio.LioCommandManager;
import py.thrift.share.ConnectPydDeviceOperationExceptionThrift;
import py.thrift.share.CreateBackstoresOperationExceptionThrift;
import py.thrift.share.CreateLoopbackLunsOperationExceptionThrift;
import py.thrift.share.CreateLoopbackOperationExceptionThrift;
import py.thrift.share.GetScsiDeviceOperationExceptionThrift;
import py.thrift.share.NoEnoughPydDeviceExceptionThrift;
import py.thrift.share.ScsiDeviceStatusThrift;

public class ScsiManager {

  private static final Logger logger = LoggerFactory.getLogger(ScsiManager.class);

  @JsonIgnore
  private static final String METADATA = "scsi";

  @JsonIgnore
  private static final String METADATA_BAK = "scsi_bak";

  @JsonIgnore
  private static Path metadataPath;
  public List<ScsiMetadata> scsiList = new ArrayList<>(5);
  @JsonIgnore
  private LioCommandManager lioCommandManager;

  public ScsiManager() {

  }

  public ScsiManager(List<ScsiMetadata> list) {
    this.scsiList = list;
  }


  /**
   * xx.
   */
  public static ScsiManager buildFromFile() {
    Path filePath = ScsiManager.getMetadataPath();
    if (!Files.exists(filePath)) {
      logger.warn("directory is nonexist");
      return new ScsiManager();
    }

    Path path = Paths.get(filePath.toString(), ScsiManager.METADATA);
    Path pathBak = Paths.get(filePath.toString(), ScsiManager.METADATA_BAK);
    if (!Files.exists(path)) {
      if (!Files.exists(pathBak)) {
        logger.debug("no file currently");
        return new ScsiManager();
      } else {
        logger.warn("original file exists issue, use bak file");
        try {
          Files.copy(pathBak, path);
        } catch (IOException e) {
          logger.error("copy file exception {}", e);
          return new ScsiManager();
        }
      }
    }

    ObjectMapper objectMapper = new ObjectMapper();
    try {
      ScsiManager scsi = objectMapper.readValue(path.toFile(), ScsiManager.class);
      return scsi;
    } catch (Exception e) {
      logger.error("Caught an exception when read value of driver metadata from {}, {}",
          path.toString(), e);
      return new ScsiManager();
    }
  }

  public static Path getMetadataPath() {
    return metadataPath;
  }

  public static void setMetadataPath(Path metadataPath) {
    ScsiManager.metadataPath = metadataPath;
  }


  /**
   * xx.
   */
  public synchronized String mapScsiDevice(long volumeId, int snapshotId, String ip)
      throws ConnectPydDeviceOperationExceptionThrift, CreateBackstoresOperationExceptionThrift,
      CreateLoopbackOperationExceptionThrift, CreateLoopbackLunsOperationExceptionThrift,
      GetScsiDeviceOperationExceptionThrift, NoEnoughPydDeviceExceptionThrift {
    String device = ScsiUtil.getAvailablePydDevice();
    if (device == null) {
      throw new NoEnoughPydDeviceExceptionThrift();
    }
    String usedDeviceName = ScsiUtil.connectingExist(volumeId, snapshotId, ip);
    String pydDevice;
    if (usedDeviceName == null) {
      String currentDevice = ScsiUtil.connectBlockDevice(volumeId, snapshotId, ip, device);
      if (currentDevice == null) {
        throw new ConnectPydDeviceOperationExceptionThrift();
      }
      pydDevice = currentDevice;
    } else {
      pydDevice = usedDeviceName;
    }

    String iqn = ScsiUtil.createBlock(volumeId, snapshotId, pydDevice, null);
    if (iqn == null) {
      ScsiUtil.disconnectBlockDevice(pydDevice);
      throw new CreateBackstoresOperationExceptionThrift();
    }

    String wwn = ScsiUtil.createScsiDevice(null);
    if (wwn == null) {
      ScsiUtil.deleteBlock(iqn);
      ScsiUtil.disconnectBlockDevice(pydDevice);
      throw new CreateLoopbackOperationExceptionThrift();
    }

    if (!ScsiUtil.createLuns(wwn, iqn)) {
      ScsiUtil.deleteScsiDevice(wwn);
      ScsiUtil.deleteBlock(iqn);
      ScsiUtil.disconnectBlockDevice(pydDevice);
      throw new CreateLoopbackLunsOperationExceptionThrift();
    }

    if (!lioCommandManager.saveConfig()) {
      logger.error("error to save config.");
    }

    String name = ScsiUtil.getScsiDeviceName(iqn);
    if (name == null) {
      ScsiUtil.deleteScsiDevice(wwn);
      ScsiUtil.deleteBlock(iqn);
      ScsiUtil.disconnectBlockDevice(pydDevice);
      throw new GetScsiDeviceOperationExceptionThrift();
    }
    ScsiMetadata scsi = new ScsiMetadata(wwn, iqn, volumeId, snapshotId, pydDevice, ip, name);
    scsi.setStatus(ScsiDeviceStatusThrift.NORMAL);
    addScsiDevice(scsi);
    logger.warn("current scsiList size {} after mount", scsiList.size());
    return name;
  }

  private synchronized void addScsiDevice(ScsiMetadata scsi) {
    scsiList.add(scsi);
    saveToFile();
  }

  private synchronized boolean removeScsiDevice(String iqn) {
    boolean removed = false;
    Iterator<ScsiMetadata> itr = scsiList.iterator();
    while (itr.hasNext()) {
      ScsiMetadata item = itr.next();
      if (item.getIqn().equals(iqn)) {
        itr.remove();
        logger.warn("delete the element for iqn {}", iqn);
        removed = true;
        break;
      }
    }
    if (removed) {
      saveToFile();
    }
    return removed;
  }

  private synchronized void updateScsiDevice() {
    saveToFile();
  }


  /**
   * xx.
   */
  public void retrieveScsiDevice() {
    logger.warn("go to retrieve scsi device.");
    for (ScsiMetadata item : scsiList) {
      mapScsiDeviceAfterRestart(item);
    }
  }


  /**
   * xx.
   */
  public boolean checkPydConnection(ScsiMetadata item) {
    long volumeId = item.getVolumeId();
    int snapshotId = item.getSnapshotId();
    String device = item.getPydDevice();
    String ip = item.getIp();

    String usedDeviceName = ScsiUtil.connectingExist(volumeId, snapshotId, ip);
    if (usedDeviceName == null) {
      logger.debug("checkPydConnection case to enter.");
      ///need to set item to recovery status
      item.setStatus(ScsiDeviceStatusThrift.RECOVERY);
      String currentDevice = ScsiUtil.connectBlockDevice(volumeId, snapshotId, ip, device);
      if (currentDevice == null) {
        logger.error("error to connect device {}", device);
        return false;
      } else {
        logger.warn("connect pyd device successfully for volume {}.", volumeId);
        item.setStatus(ScsiDeviceStatusThrift.NORMAL);
      }
    }
    return true;
  }


  /**
   * xx.
   */
  public synchronized String mapScsiDeviceAfterRestart(ScsiMetadata item) {
    logger.warn("mapScsiDeviceAfterRestart restart case to enter.");
    long volumeId = item.getVolumeId();
    int snapshotId = item.getSnapshotId();
    String device = item.getPydDevice();
    String ip = item.getIp();
    String scsiDevice = item.getScsiDevice();
    String iqn = item.getIqn();
    final String wwn = item.getWwn();

    String usedDeviceName = ScsiUtil.connectingExist(volumeId, snapshotId, ip);
    String pydDevice;
    if (usedDeviceName == null) {
      ///need to set item to recovery status
      item.setStatus(ScsiDeviceStatusThrift.RECOVERY);
      String currentDevice = ScsiUtil.connectBlockDevice(volumeId, snapshotId, ip, device);
      if (currentDevice == null) {
        logger.error("error to connect device {}", device);
        item.setStatus(ScsiDeviceStatusThrift.CONNECTEXCEPTIONRECOVERING);
        return null;
      } else {
        logger.warn("connect pyd device successfully for volume {}.", volumeId);
      }
      pydDevice = currentDevice;
    } else {
      String name = ScsiUtil.getScsiDeviceName(iqn);
      if (name != null && name.equals(scsiDevice)) {
        logger.warn("process restart case. do nothing.");
        return name;
      } else {
        item.setStatus(ScsiDeviceStatusThrift.ERROR);
        logger.error("error:something wrong for volume {}. old scsiDevice {}, now scsiDevice {}",
            volumeId, scsiDevice, name);
        return null;
      }
    }
    logger.warn("mapScsiDeviceAfterRestart restart case to create block.");
    if (ScsiUtil.createBlock(volumeId, snapshotId, pydDevice, iqn) == null) {
      logger.error("error to create block for volume {}.", volumeId);
      item.setStatus(ScsiDeviceStatusThrift.ERROR);
      return null;
    }
    logger.warn("mapScsiDeviceAfterRestart restart case to create loopback.");
    if (ScsiUtil.createScsiDevice(wwn) == null) {
      logger.error("error to create loopback for wwn {}", wwn);
      item.setStatus(ScsiDeviceStatusThrift.ERROR);
      return null;
    }
    logger.warn("mapScsiDeviceAfterRestart restart case to create luns.");
    if (!ScsiUtil.createLuns(wwn, iqn)) {
      logger.error("error to create lun for iqn {}", iqn);
      item.setStatus(ScsiDeviceStatusThrift.ERROR);
      return null;
    }

    if (!lioCommandManager.saveConfig()) {
      logger.error("error to save config.");
    }

    String name = ScsiUtil.getScsiDeviceName(iqn);
    if (name == null) {
      logger.error("error to get scsi name.");
      item.setStatus(ScsiDeviceStatusThrift.ERROR);
      return null;
    }

    boolean changed = false;
    if (!item.getPydDevice().equals(pydDevice)) {
      logger.warn("the device is different, before {}, after {}", item.getPydDevice(), pydDevice);
      changed = true;
      item.setPydDevice(pydDevice);
    }
    if (!item.getScsiDevice().equals(name)) {
      logger.warn("the scsi name is different, before {}, after {}", item.getScsiDevice(), name);
      changed = true;
      item.setScsiDevice(name);
    }
    item.setStatus(ScsiDeviceStatusThrift.NORMAL);
    if (changed) {
      updateScsiDevice();
    }
    return name;
  }


  /**
   * xx.
   */
  public synchronized boolean unmapScsiDevice(long volumeId, int snapshotId, String ip) {
    // even if scsi device is not umounted and even more the mounted directory is opened,
    // delete scsi device or delete Block can be done successfully.
    // have tested locally. so no need to do umount operation.
    // ScsiUtil.umount(scsiDevice);

    ///set status to umounting
    String wwn = null;
    String iqn = null;
    boolean found = false;
    for (ScsiMetadata item : scsiList) {
      if (item.getVolumeId().equals(volumeId)
          && item.getSnapshotId().equals(snapshotId)
          && item.getIp().equals(ip)) {
        found = true;
        wwn = item.getWwn();
        iqn = item.getIqn();
        break;
      }
    }

    if (!found) {
      logger.warn("scsi device item not found in scsiList.");
    }
    // note that deleteScsiDevice and deleteBlock can change the execute sequence.
    // there is no dependency each other.
    if (wwn != null) {
      ScsiUtil.deleteScsiDevice(wwn);
    } else {
      logger.error("wwn is null for volume {}", volumeId);
    }

    if (iqn != null) {
      ScsiUtil.deleteBlock(iqn);
    } else {
      logger.error("iqn is null for volume {}", volumeId);
    }

    if (iqn != null) {
      boolean deleteSuccess = removeScsiDevice(iqn);
      if (!deleteSuccess) {
        logger.error("iqn {} not found", iqn);
      }
    }

    if (!lioCommandManager.saveConfig()) {
      logger.error("error to save config.");
    }

    String usedDeviceName = ScsiUtil.connectingExist(volumeId, snapshotId, ip);
    if (usedDeviceName != null) {
      if (!ScsiUtil.disconnectBlockDevice(usedDeviceName)) {
        logger.error("error to disconnect device {}", usedDeviceName);
        return false;
      }
    }

    logger.warn("current scsiList size {} after umount", scsiList.size());
    return true;
  }

  public List<ScsiMetadata> getScsiList() {
    return scsiList;
  }

  public void setScsiList(List<ScsiMetadata> scsiList) {
    this.scsiList = scsiList;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ScsiManager)) {
      return false;
    }
    ScsiManager that = (ScsiManager) o;
    return Objects.equals(getScsiList(), that.getScsiList());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getScsiList());
  }

  @Override
  public String toString() {
    return "ScsiManager{" + "scsiList=" + scsiList + '}';
  }


  /**
   * xx.
   */
  public boolean saveToFile() {
    Path filePath = ScsiManager.getMetadataPath();
    if (!filePath.toFile().exists()) {
      if (!filePath.toFile().mkdirs()) {
        logger.error("error to create directory {}", filePath.toString());
        return false;
      }
    }
    Path path = Paths.get(filePath.toString(), METADATA);
    Path pathBak = Paths.get(filePath.toString(), METADATA_BAK);
    try {
      if (path.toFile().exists()) {
        Files.copy(path, pathBak);
      } else {
        Files.createFile(path);
      }
    } catch (IOException e) {
      logger.error("Exception happens when create bak file {}", e);
      return false;
    }

    FileOutputStream fos = null;
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      String serviceMetadataString = objectMapper.writeValueAsString(this);
      fos = new FileOutputStream(path.toFile());
      fos.write(serviceMetadataString.getBytes("UTF-8"));
      fos.flush();
      fos.getFD().sync();
      if (pathBak.toFile().exists()) {
        if (!pathBak.toFile().delete()) {
          logger.error("error to delete bak file.");
        }
      }
      return true;
    } catch (Exception e) {
      logger.error("Catch an exception when save service {} to file {}, {}", this, path.toString(),
          e);
      return false;
    } finally {
      if (fos != null) {
        try {
          fos.close();
        } catch (IOException e) {
          logger.error("caught an exception when try to close FD:{}", e);
        }
      }
    }
  }

  public void setLioCommandManager(LioCommandManager lioCommandManager) {
    this.lioCommandManager = lioCommandManager;
  }
}
