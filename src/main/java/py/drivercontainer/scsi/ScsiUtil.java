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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.drivercontainer.lio.saveconfig.LioStorage;
import py.drivercontainer.lio.saveconfig.jsonobj.SaveConfigBuilder;
import py.drivercontainer.lio.saveconfig.jsonobj.SaveConfigImpl;

/**
 * xx.
 */
public class ScsiUtil {

  private static final Logger logger = LoggerFactory.getLogger(ScsiUtil.class);
  private static final String LIO_TARGET_PREFIX = "iqn.2017-08.zettastor.iqn";
  private static final String NBD_MAX_FILE = "/sys/module/pyd/parameters/nbds_max";
  private static SaveConfigBuilder saveConfigBuilder;


  /**
   * xx.
   */
  public static String buildLioWwn(long volumeId, int snapshotId) {
    String volume = String.valueOf(volumeId);
    int len = volume.length();
    //! keep two digit for snapshotId
    return String.format("%s-%s", volume.substring(len - 12, len),
        snapshotId);
  }


  /**
   * xx.
   */
  public static String getScsiDeviceName(String iqn) {
    StringBuilder stringBuilder = new StringBuilder(64);
    stringBuilder.append("lsscsi|grep -E \"");
    stringBuilder.append(iqn);
    stringBuilder.append("\"");
    String[] cmd = {"/bin/sh", "-c", stringBuilder.toString()};
    List<String> output = new ArrayList<String>();
    boolean res = execCmd(cmd, output);
    if (res) {
      if (output.size() == 0) {
        logger.warn("no output for lsscsi command");
        return null;
      }
      if (output.size() > 1) {
        logger.error("there are more than one line match the device, pls check it.");
        return null;
      }
      String[] str = output.get(0).split("\\s+");
      int len = str.length;
      String deviceName = str[len - 1];
      logger.debug("lsscsi output {} and device name {}", output.get(0), deviceName);
      return deviceName;
    } else {
      return null;
    }
  }


  /**
   * xx.
   */
  public static boolean umount(String device) {
    //check whether device is mounted firstly.
    String[] cmd = {"umount", device};
    boolean res = execCmd(cmd, null);
    logger.warn("go to umount device {}", device);
    if (res) {
      return true;
    }
    logger.warn("error to umount device {}", device);
    return false;
  }


  /**
   * xx.
   */
  public static String connectingExist(long volumeId, int snapshotId, String ip) {
    StringBuilder sb = new StringBuilder(128);
    sb.append("ps -ef | grep -E \"");
    sb.append(volumeId);
    sb.append("\\ +");
    sb.append(snapshotId);
    sb.append("\\ +");
    sb.append(ip);
    sb.append("\" | grep -v grep");

    String[] cmd = {"/bin/sh", "-c", sb.toString()};
    List<String> output = new ArrayList<String>();
    boolean res = execCmd(cmd, output);
    if (res) {
      if (output.size() == 0) {
        logger.warn("no output for ps -ef |grep volume {}, ip {}", volumeId, ip);
        return null;
      }
      if (output.size() < 2) {
        logger.error("there is a bad connection, pls check it.");
        return null;
      }
      String[] str = output.get(0).split("\\s+");
      int len = str.length;
      String deviceName = str[len - 1];
      logger.warn("exist connecting {} and device name {}", output.get(0), deviceName);
      return deviceName;
    } else {
      return null;
    }
  }

  /**
   * connect pyd device usage: pyd-client vol_id snapshot_id host pyd_device ex: ./pyd-client
   * 3305403884898771931 0 127.0.0.1 /dev/pyd0.
   */
  public static String connectBlockDevice(long volumeId, int snapshotId, String ip, String device) {
    String[] cmd = {"/opt/pyd/pyd-client",
        String.valueOf(volumeId),
        String.valueOf(snapshotId),
        ip,
        device};
    boolean res = false;
    res = execCmd(cmd, null);
    logger
        .warn("go to connect volume {}, snapshotId {}, ip {}, device {}", volumeId, snapshotId, ip,
            device);
    if (res) {
      return device;
    }
    logger.error("error to volume {} device {}", volumeId, device);
    return null;
  }


  /**
   * xx.
   */
  public static boolean disconnectBlockDevice(String device) {
    String[] cmd = {"/opt/pyd/pyd-client", "-f", device};
    boolean res = execCmd(cmd, null);
    logger.warn("go to disconnect device {}", device);
    if (res) {
      return true;
    }
    logger.error("error to disconnect device {}", device);
    return false;
  }

  //create block command example : "/usr/bin/targetcli /backstores/
  // block create name=iqn.2017-08.zettastor.iqn:12837457345772384823747-0 dev=/dev/pyd1"
  // or
  //create block command example : "/usr/bin/targetcli /backstores/
  // block create iqn.2017-08.zettastor.iqn:12837457345772384823747-0 /dev/pyd1"

  /**
   * xx.
   */
  public static String createBlock(long volumeId, int snapshotId, String device, String iqn) {
    if (iqn == null) {
      iqn = buildLioWwn(volumeId, snapshotId);
    }
    String[] cmd = {"/usr/bin/targetcli", "/backstores/block", "create", iqn, device};
    boolean res = execCmd(cmd, null);
    logger.warn("go to create backstores iqn {}, device {}", iqn, device);
    if (res) {
      return iqn;
    }
    logger.error("error to create backstores iqn {}, device {}", iqn, device);
    return null;
  }

  //create block command example : "/usr/bin/targetcli /
  // backstores/block delete iqn.2017-08.zettastor.iqn:12837457345772384823747-0"

  /**
   * xx.
   */
  public static boolean deleteBlock(String iqn) {
    String[] cmd = {"/usr/bin/targetcli", "/backstores/block", "delete", iqn};
    boolean res = execCmd(cmd, null);
    logger.warn("go to delete block iqn {}", iqn);
    if (res) {
      return true;
    }
    logger.error("error to delete block iqn {}", iqn);
    return false;
  }

  //below formats are allowed. but we select the second one.
  //create loopback command example : "/usr/bin/targetcli /loopback create wwn=naa.50014056f2348e9f"
  //create loopback command example : "/usr/bin/targetcli /loopback create naa.50014056f2348e9f"

  /**
   * xx.
   */
  public static String createScsiDevice(String wwn) {
    if (wwn == null) {
      StringBuilder sb = new StringBuilder(20);
      sb.append("naa.50014");
      String timeStr = String.valueOf(System.currentTimeMillis());
      sb.append(timeStr, 0, 11);
      wwn = sb.toString();
    }
    String[] cmd = {"/usr/bin/targetcli", "/loopback", "create", wwn};
    boolean res = execCmd(cmd, null);
    logger.warn("go to create wwn {}", wwn);
    if (res) {
      return wwn;
    }
    logger.error("error to create wwn {}", wwn);
    return null;
  }

  //delete loopback command example : "/usr/bin/targetcli /loopback delete naa.50014056f2348e9f"

  /**
   * xx.
   */
  public static boolean deleteScsiDevice(String wwn) {
    String[] cmd = {"/usr/bin/targetcli", "/loopback", "delete", wwn};
    boolean res = execCmd(cmd, null);
    logger.warn("go to delete wwn {}", wwn);
    if (res) {
      return true;
    }
    logger.error("error to delete wwn {}", wwn);
    return false;
  }

  //create loopback luns command example :
  // "/usr/bin/targetcli /loopback/naa.50014056f2348e9f/luns create storage_object=iqn"
  // or
  //create loopback luns command example :
  // "/usr/bin/targetcli /loopback/naa.50014056f2348e9f/luns create iqn"

  /**
   * xx.
   */
  public static boolean createLuns(String wwn, String iqn) {
    StringBuilder sb = new StringBuilder(36);
    sb.append("/loopback/");
    sb.append(wwn);
    sb.append("/luns");

    StringBuilder sb1 = new StringBuilder(128);
    sb1.append("/backstores/block/");
    sb1.append(iqn);
    String[] cmd = {"/usr/bin/targetcli", sb.toString(), "create", sb1.toString()};
    boolean res = execCmd(cmd, null);
    logger.warn("go to create luns with device {} for loopback wwn {}", iqn, wwn);
    if (res) {
      return true;
    }
    logger.error("error to create luns with iqn {} for loopback wwn {}", iqn, wwn);
    return false;
  }


  /**
   * xx.
   */
  public static boolean pydOccupied(String device) {
    StringBuilder sb = new StringBuilder(64);
    sb.append("ps -ef | grep -E \"");
    sb.append(device);
    sb.append("$\" | grep -v grep");

    String[] cmd = {"/bin/sh", "-c", sb.toString()};
    boolean res = execCmd(cmd, null);
    return res;
  }


  /**
   * xx.
   */
  public static String getAvailablePydDevice() {
    SaveConfigImpl saveConfigImpl;
    List<String> bindNbdDevList = new ArrayList<String>();
    saveConfigImpl = saveConfigBuilder.build();
    if (saveConfigImpl.load()) {
      logger.debug("save config loaded.");
      List<LioStorage> storages = saveConfigImpl.getStorages();
      if (storages != null && storages.size() != 0) {
        for (LioStorage storage : storages) {
          bindNbdDevList.add(storage.getDev());
          logger.debug("used pyd:{}", storage.getDev());
        }
      } else {
        if (storages == null) {
          logger.debug("storages is null.");
        } else {
          logger.debug("storages size is 0.");
        }
      }
    }
    logger.debug("bindNbdDevList {}", bindNbdDevList.toString());
    int nbdDeviceNum = getNbdMaxNum();
    String device = null;
    for (int i = 0; i < nbdDeviceNum; i++) {
      String nbdDevicePath = String.format("/dev/%s%d", "pyd", i);
      if (!new File(nbdDevicePath).exists()) {
        logger.warn("kernel device {} nonexist", nbdDevicePath);
        continue;
      }
      if (!bindNbdDevList.contains(nbdDevicePath)) {
        if (!pydOccupied(nbdDevicePath)) {
          logger.warn("Got an available nbd device {}", nbdDevicePath);
          device = nbdDevicePath;
          break;
        } else {
          logger.warn("nbdDevicePath is alive:{}", nbdDevicePath);
        }
      }
    }
    if (device == null) {
      logger.error("error: no pyd device available.");
    }
    return device;
  }

  private static int getNbdMaxNum() {
    int nbdMaxNum = 10;
    if (Files.exists(Paths.get(NBD_MAX_FILE))) {
      File file = new File(NBD_MAX_FILE);
      BufferedReader reader = null;
      try {

        reader = new BufferedReader(new FileReader(file));
        String tempString = null;
        if ((tempString = reader.readLine()) != null) {
          nbdMaxNum = Integer.parseInt(tempString);
          logger.debug("max nbd num {}", nbdMaxNum);
        }
      } catch (IOException e) {
        logger.error("catch an exception when operate BufferedReader, exception {}", e);
      } finally {
        if (reader != null) {
          try {
            reader.close();
          } catch (IOException e) {
            logger.error("catch an exception when close reader {}, exception {}", NBD_MAX_FILE, e);
          }
        }
      }
    } else {
      logger.error("{} is nonexistent", NBD_MAX_FILE);
    }
    return nbdMaxNum;
  }

  /**
   * execute command using ProcessBuilder.
   */
  static synchronized boolean execCmd(String[] cmd, List<String> output) {
    BufferedReader reader = null;
    try {
      Process process = null;
      ProcessBuilder pbuilder = new ProcessBuilder(cmd);
      pbuilder.redirectErrorStream(true);
      process = pbuilder.start();
      reader = new BufferedReader(new InputStreamReader(process.getInputStream(), "UTF-8"));
      String line = null;
      while ((line = reader.readLine()) != null) {
        if (output != null) {
          output.add(line);
        }
      }
      process.waitFor();
      if (process.exitValue() != 0) {
        if (output != null && output.size() == 0) {
          return true;
        } else {
          logger.debug("cmd fail to execute, errno {}", process.exitValue());
          for (int i = 0; i < cmd.length; i++) {
            logger.debug("command parameter : [{}]", cmd[i]);
          }
        }
        return false;
      }
    } catch (IOException e) {
      logger.warn("IOException happens {}", e);
      return false;
    } catch (InterruptedException e) {
      logger.warn("InterruptedException happens {}", e);
      return false;
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          logger.error("caught an exception when try to close reader {}", e);
        }
      }
    }
    return true;
  }

  public static SaveConfigBuilder getSaveConfigBuilder() {
    return saveConfigBuilder;
  }

  public static void setSaveConfigBuilder(SaveConfigBuilder saveConfigBuilder) {
    ScsiUtil.saveConfigBuilder = saveConfigBuilder;
  }
}
