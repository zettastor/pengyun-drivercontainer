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

package py.drivercontainer.driver.store;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.driver.DriverMetadata;
import py.driver.DriverType;
import py.driver.IscsiAccessRule;
import py.drivercontainer.driver.store.exception.DuplicatedDriverException;
import py.drivercontainer.driver.version.Version;
import py.icshare.DriverKey;
import py.processmanager.Pmdb;

/**
 * save or remove infomation of driver in file var/SPid_coordinator/volumeId/driverType/snapshotId.
 *
 */
public class DriverStoreImpl implements DriverFileStore {

  public static final String ACCESS_RULE_LIST_FILE_NAME = "Acl_list";
  private static final Logger logger = LoggerFactory.getLogger(DriverStoreImpl.class);
  private final Path fileStorePath;

  private final Version version;

  /**
   * Memory table maps from {@link DriverKey} to {@link DriverMetadata}. This table will be accessed
   * by multiple threads. To make it thread safe, all access on this table should be enclosed in
   * synchronized block.
   *
   * <p>Note that we use keyword 'synchronized' instead of {@link ConcurrentHashMap} to make the
   * table thread safe. Since it is possible to iterate all drivers in this table, the latter
   * utility cannot make this kind of access thread safe.
   */
  private final Map<DriverKey, DriverMetadata> driverKey2Driver = new HashMap<>();


  /**
   * xx.
   */
  public DriverStoreImpl(Path fileStorePath, Version version) {
    if (!(fileStorePath.toFile().exists() || fileStorePath.toFile().mkdirs())) {
      String errMsg = "Failed to create directory! Directory detail: " + fileStorePath;
      logger.error("{}", errMsg);
      throw new RuntimeException(errMsg);
    }

    this.fileStorePath = fileStorePath;
    this.version = version;
  }

  private Path makeDriverFilePath(Version version, DriverKey driverKey) {
    Path driverFilePath = Paths
        .get(fileStorePath.toString(), version.format(), Pmdb.COORDINATOR_PIDS_DIR_NAME,
            String.valueOf(driverKey.getVolumeId()), driverKey.getDriverType().name(),
            String.valueOf(driverKey.getSnapshotId()));

    return driverFilePath;
  }

  private Path makeDriverFileBakPath(Version version, DriverKey driverKey) {
    Path driverFileBakPath = Paths
        .get(fileStorePath.toString(), version.format(), Pmdb.COORDINATOR_PIDS_DIR_NAME,
            String.valueOf(driverKey.getVolumeId()), driverKey.getDriverType().toString(),
            String.valueOf(driverKey.getSnapshotId()) + "_bak");
    return driverFileBakPath;
  }

  private Path makeAclListFilePath(Version version, DriverKey driverKey) {
    Path driverFilePath = Paths
        .get(fileStorePath.toString(), version.format(), Pmdb.COORDINATOR_PIDS_DIR_NAME,
            String.valueOf(driverKey.getVolumeId()), driverKey.getDriverType().name(),
            DriverStoreImpl.ACCESS_RULE_LIST_FILE_NAME + String.valueOf(driverKey.getSnapshotId()));

    return driverFilePath;
  }

  private Path makeAclListFileBakPath(Version version, DriverKey driverKey) {
    Path driverFilePath = Paths
        .get(fileStorePath.toString(), version.format(), Pmdb.COORDINATOR_PIDS_DIR_NAME,
            String.valueOf(driverKey.getVolumeId()), driverKey.getDriverType().name(),
            DriverStoreImpl.ACCESS_RULE_LIST_FILE_NAME + String.valueOf(driverKey.getSnapshotId())
                + "_bak");

    return driverFilePath;
  }


  /**
   * xx.
   */
  public DriverKey makeDriverKey(DriverMetadata driver) {
    return new DriverKey(driver.getDriverContainerId(), driver.getVolumeId(),
        driver.getSnapshotId(),
        driver.getDriverType());
  }

  @Override
  public Version getVersion() {
    return version;
  }

  @Override
  public DriverMetadata get(DriverKey driverKey) {
    /*
     * For detailed comments that keyword 'synchronized' is used here, see comments on field
     * 'driverKey2Driver'.
     */
    synchronized (driverKey2Driver) {
      return driverKey2Driver.get(driverKey);
    }
  }

  @Override
  public void addOrFailImmediately(DriverMetadata driver)
      throws IOException, DuplicatedDriverException {
    DriverKey driverKey;

    driverKey = makeDriverKey(driver);
    /*
     * For detailed comments that keyword 'synchronized' is used here, see comments on field
     * 'driverKey2Driver'.
     */
    synchronized (driverKey2Driver) {
      if (driverKey2Driver.get(driverKey) != null) {
        throw new DuplicatedDriverException("Driver key: " + driverKey);
      }

      driverKey2Driver.put(driverKey, driver);
      if (!flush(driverKey)) {
        driverKey2Driver.remove(driverKey);
        throw new IOException("Fails to persist driver! Driver key: " + driverKey);
      }
    }
  }

  @Override
  public void saveOrBlockOnFailure(DriverMetadata driver) throws DuplicatedDriverException {
    saveToMemory(driver);

    DriverKey driverKey;

    driverKey = makeDriverKey(driver);

    final int intervalMillis = 20000;
    while (!flush(driverKey)) {
      logger.warn(
          "Failed to persist driver metadata! Unfortunately, we cannot fix this and need someone's"
              + " interference! Driver key: {}",
          driverKey);
      try {
        Thread.sleep(intervalMillis);
      } catch (InterruptedException e) {
        logger.warn("Caught an exception", e);
      }
    }
  }

  @Override
  public void deleteOrBlockOnFailure(DriverKey driverKey) {
    Path driverFilePath = makeDriverFilePath(version, driverKey);

    final int intervalMillis = 20000;

    while (driverFilePath.toFile().exists() && !driverFilePath.toFile().delete()) {
      logger.error(
          "Failed to remove driver file! Unfortunately, we cannot fix this and need someone's "
              + "interference! Driver file: {}.",
          driverFilePath);

      try {
        Thread.sleep(intervalMillis);
      } catch (InterruptedException e) {
        logger.warn("Caught an exception", e);
      }
    }

    // SPid directory hierarchy looks like this: SPid/volume_id/driver_type/snapshot_id, in which
    // 'snapshot_id' is
    // file. And the number '0' point to level 'driver_type', the number '1' point to level
    // 'vol_id'. Thus the
    // loop is within 2.
    for (int i = 0; i < 2; i++) {
      driverFilePath = driverFilePath.getParent();
      if (driverFilePath != null && driverFilePath.toFile() != null) {
        File[] fileStoreTypePath = driverFilePath.toFile().listFiles();
        while ((fileStoreTypePath != null && fileStoreTypePath.length == 0 && !driverFilePath
            .toFile().delete())) {
          logger.error(
              "Failed to remove driver directory! Unfortunately, we cannot fix this and need "
                  + "someone's interference! Driver directory: {}.", driverFilePath);

          try {
            Thread.sleep(intervalMillis);
          } catch (InterruptedException e) {
            logger.warn("Caught an exception", e);
          }
        }
      }
    }

    /*
     * For detailed comments that keyword 'synchronized' is used here, see comments on field
     * 'driverKey2Driver'.
     */
    synchronized (driverKey2Driver) {
      driverKey2Driver.remove(driverKey);
    }
  }

  @Override
  public boolean save(DriverMetadata driver) {
    saveOrBlockOnFailure(driver);
    return true;
  }

  @Override
  public List<DriverMetadata> list() {
    List<DriverMetadata> drivers = new ArrayList<DriverMetadata>();

    /*
     * For detailed comments that keyword 'synchronized' is used here, see comments on field
     *  'driverKey2Driver'.
     */
    synchronized (driverKey2Driver) {
      drivers.addAll(driverKey2Driver.values());
    }

    return drivers;
  }

  @Override
  public List<DriverMetadata> list(DriverType type) {
    List<DriverMetadata> drivers;

    drivers = new ArrayList<DriverMetadata>();
    /*
     * For detailed comments that keyword 'synchronized' is used here, see comments on field
     * 'driverKey2Driver'.
     */
    synchronized (driverKey2Driver) {
      for (Map.Entry<DriverKey, DriverMetadata> entry : driverKey2Driver.entrySet()) {
        if (entry.getKey().getDriverType() == type) {
          drivers.add(entry.getValue());
        }
      }
    }

    return drivers;
  }

  @Override
  public boolean remove(DriverKey driverKey) {
    deleteOrBlockOnFailure(driverKey);
    return true;
  }

  @Override
  public boolean load(Long driverContainerId) {
    File[] volumeIdDirs = Paths
        .get(fileStorePath.toString(), version.format(), Pmdb.COORDINATOR_PIDS_DIR_NAME)
        .toFile().listFiles();
    if (volumeIdDirs == null || volumeIdDirs.length == 0) {
      logger.warn("Empty directory {}, do nothing.",
          Paths.get(fileStorePath.toString(), version.format(),
              Pmdb.COORDINATOR_PIDS_DIR_NAME).toString());
      return true;
    }

    for (File volumeIdDir : volumeIdDirs) {
      if (!volumeIdDir.isDirectory()) {
        logger.warn("Empty directory {}, do nothing.", volumeIdDir);
        continue;
      }
      File[] driverTypes = volumeIdDir.listFiles();
      if (driverTypes != null) {
        for (File driverfile : driverTypes) {
          File[] snapshotIdFiles = driverfile.listFiles();
          if (snapshotIdFiles != null) {
            for (File file : snapshotIdFiles) {
              if (file.toString().contains(
                  DriverStoreImpl.ACCESS_RULE_LIST_FILE_NAME)) { // don't loop acl list file
                logger.debug("acl rule list file {}", file.toString());
                continue;
              }
              String volumeIdString = volumeIdDir.getName();
              String driverTypeString = driverfile.getName();
              String snapshotIdString = null;
              if (!file.getName().endsWith("bak")) {
                snapshotIdString = file.getName();
                DriverKey driverKey = new DriverKey(driverContainerId,
                    Long.parseLong(volumeIdString),
                    Integer.parseInt(snapshotIdString), DriverType.findByName(driverTypeString));
                load(driverKey);
              }
            }
          }
        }
      }

    }

    return true;
  }

  @Override
  public boolean load(DriverKey driverKey) {
    Path driverFilePath = makeDriverFilePath(version, driverKey);
    Path driverFileBakPath = makeDriverFileBakPath(version, driverKey);
    boolean loadDriverSuccess = false;
    if (!driverFilePath.toFile().exists()) {
      logger.error("No such file or directory {}", driverFilePath);
      return false;
    }

    DriverMetadata driver = DriverMetadata.buildFromFile(driverFilePath);
    if (driver == null) {
      if (driverFileBakPath.toFile().exists()) {
        renameFile(driverFileBakPath, driverFilePath);
        if ((driver = DriverMetadata.buildFromFile(driverFilePath)) != null) {
          loadDriverSuccess = true;
        }
      }

    } else {
      loadDriverSuccess = true;
    }

    // successfully load driver metadata from file, save it to memory table
    if (loadDriverSuccess) {
      Path aclListPath = makeAclListFilePath(version, driverKey);
      if (Files.exists(aclListPath)) {
        List<IscsiAccessRule> aclList = null;
        aclList = driver.buildFromFileForAclList(aclListPath);
        if (aclList == null) {
          Path aclListPathBak = makeAclListFileBakPath(version, driverKey);
          if (aclListPathBak.toFile().exists()) {
            renameFile(aclListPath, aclListPathBak);
            if ((aclList = driver.buildFromFileForAclList(aclListPath)) != null) {
              logger.warn("acl rule list is restored from bak file");
            } else {
              logger.error("acl rule list can not be restored from bak file");
            }
          }
        }
      }
      saveToMemory(driver);
    }

    return loadDriverSuccess;
  }

  @Override
  public boolean flush() {
    /*
     * For detailed comments that keyword 'synchronized' is used here, see comments on field
     *  'driverKey2Driver'.
     */
    synchronized (driverKey2Driver) {
      for (DriverKey driverKey : driverKey2Driver.keySet()) {
        if (!flush(driverKey)) {
          return false;
        }
      }
    }

    return true;
  }

  @Override
  public boolean flush(DriverKey driverKey) {
    DriverMetadata driver;

    /*
     * For detailed comments that keyword 'synchronized' is used here, see comments on field
     * 'driverKey2Driver'.
     */
    synchronized (driverKey2Driver) {
      driver = driverKey2Driver.get(driverKey);
    }

    if (driver == null) {
      logger.error("No such driver with id {}", driverKey);
      return false;
    }

    Path driverFilePath = makeDriverFilePath(version, driverKey);
    Path driverFileBakPath = makeDriverFileBakPath(version, driverKey);

    Path driverFileParentPath = driverFilePath.getParent();
    if (driverFileParentPath != null) {
      if (!driverFileParentPath.toFile().exists()) {
        if (!driverFileParentPath.toFile().mkdirs()) {
          logger.error("error to mkdirs.");
        }
      }
    }

    // driverMetadataFile not exist,save the new driver to it
    if (!driverFilePath.toFile().exists()) {
      if (!driver.saveToFile(driverFilePath)) {
        logger.error("Something wrong when flush driver metedata {}", driver);
        return false;

      }
      return true;
    }
    // backup DriverMetadataFile before change it content
    if (copyDriverMetdataFile(driverFilePath.toFile(), driverFileBakPath.toFile())) {
      if (!driver.saveToFile(driverFilePath)) {
        logger.error("Something wrong when flush driver metedata {}", driver);
        return false;

      } else {
        if (driverFileBakPath.toFile().exists()) {
          if (!driverFileBakPath.toFile().delete()) {
            logger.error("error to delete bak file.");
          }
        }
      }
    }
    return true;
  }


  /**
   * xx.
   */
  public void clearMomory() {
    synchronized (driverKey2Driver) {
      driverKey2Driver.clear();
    }
  }

  void renameFile(Path oldname, Path newname) {
    File oldfile = new File(oldname.toString());
    File newfile = new File(newname.toString());
    if (!oldfile.exists()) {
      return;
    }

    if (!oldfile.renameTo(newfile)) {
      logger.warn("Unable to rename {} to {}", oldfile, newfile);
    }
  }

  /**
   * Copy the given source file to the given destination file.
   *
   * @return true if copy successfully; or false.
   */
  boolean copyDriverMetdataFile(File srcFile, File dstFile) {
    FileOutputStream outputStream;
    FileInputStream inputStream;

    try {
      outputStream = new FileOutputStream(dstFile);
    } catch (IOException e) {
      logger.error("Unable to open file {}", dstFile, e);
      return false;
    }

    try {
      try {
        inputStream = new FileInputStream(srcFile);
      } catch (IOException e) {
        logger.error("Unable to open file {}", srcFile, e);
        return false;
      }

      try {
        int length;
        byte[] readBytes = new byte[1024];

        while ((length = inputStream.read(readBytes)) >= 0) {
          outputStream.write(readBytes, 0, length);
        }

        outputStream.getFD().sync();
        outputStream.flush();
        return true;
      } catch (Exception e) {
        logger.warn("Catch an exception when copy status file:{}", e);
        return false;
      } finally {
        try {
          inputStream.close();
        } catch (IOException e) {
          logger.error("Unable to close file {}", srcFile, e);
        }
      }
    } finally {
      try {
        outputStream.close();
      } catch (IOException e) {
        logger.error("Unable to close file {}", dstFile, e);
      }
    }
  }

  /**
   * Store the given driver to memory.
   *
   * @throws DuplicatedDriverException if memory already exists the driver with same key of the
   *                                   given one, and the existing one is different object from the
   *                                   given one.
   */
  void saveToMemory(DriverMetadata driver) throws DuplicatedDriverException {
    DriverKey driverKey;
    DriverMetadata driverInStore;

    driverKey = makeDriverKey(driver);
    /*
     * For detailed comments that keyword 'synchronized' is used here, see comments on field
     *  'driverKey2Driver'.
     */
    synchronized (driverKey2Driver) {
      driverInStore = driverKey2Driver.get(driverKey);
      if (driverInStore != null && driverInStore != driver) {
        String errMsg = "Duplicated driver! Driver key: " + driverKey;
        logger.error("{}", errMsg);
        throw new DuplicatedDriverException(errMsg);
      }

      driverKey2Driver.put(driverKey, driver);
    }
  }

  @Override
  public boolean saveAcl(DriverMetadata driver) {
    DriverKey driverKey;
    driverKey = makeDriverKey(driver);
    Path aclListFilePath = makeAclListFilePath(version, driverKey);
    Path aclListFileBakPath = makeAclListFileBakPath(version, driverKey);
    if (driver.saveToFileForAclList(aclListFilePath, aclListFileBakPath)) {
      return true;
    }
    return false;
  }
}
