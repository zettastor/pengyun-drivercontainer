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

package py.drivercontainer.driver.upgrade.test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.BlockingQueue;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import py.common.RequestIdBuilder;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.DriverUpgradeProcessor;
import py.drivercontainer.DriverVersion;
import py.drivercontainer.JvmConfigurationForDriver;
import py.drivercontainer.JvmConfigurationManager;
import py.drivercontainer.driver.store.DriverStore;
import py.drivercontainer.driver.store.DriverStoreImpl;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.drivercontainer.driver.store.DriverStoreManagerImpl;
import py.drivercontainer.driver.version.Version;
import py.drivercontainer.driver.version.VersionException;
import py.drivercontainer.driver.version.VersionManager;
import py.drivercontainer.driver.version.file.VersionImpl;
import py.drivercontainer.driver.version.file.VersionManagerImpl;
import py.drivercontainer.worker.SubmitUpgradeRequestWorker;
import py.test.TestBase;

public class SubmitUpgradeRequestTest extends TestBase {

  private DriverContainerConfiguration dcConfig;
  private SubmitUpgradeRequestWorker worker;
  private DriverUpgradeProcessor driverUpgradeProcessor;

  private JvmConfigurationManager jvmConfigManager;
  private DriverStoreManager driverStoreManager;
  private String versionPath = "/tmp/versionTest";
  private Version nbdCurVer = VersionImpl.get("2.3.0-internal-20170918000011");
  private Version nbdLatVer = VersionImpl.get("2.3.0-internal-20170918000022");

  @Before
  public void init() throws IOException, VersionException {
    File file = new File(versionPath);
    file.mkdirs();
    JvmConfigurationForDriver coordinatorJvmConfig = new JvmConfigurationForDriver();
    coordinatorJvmConfig.setMinMemPoolSize("1K");
    jvmConfigManager = new JvmConfigurationManager();
    jvmConfigManager.setCoordinatorJvmConfig(coordinatorJvmConfig);
    dcConfig = new DriverContainerConfiguration();
    dcConfig.setDriverUpgradeLimit(1);
    dcConfig.setSystemMemoryForceReserved("1K");
    driverStoreManager = new DriverStoreManagerImpl();
    worker = new SubmitUpgradeRequestWorker();
    driverUpgradeProcessor = new DriverUpgradeProcessor();
    Version version = VersionImpl.get("2.3.0-internal-20170918000011");
    DriverStore nbddriverStore = new DriverStoreImpl(Paths.get(versionPath), nbdCurVer);
    nbddriverStore.save(buildDriver(DriverType.NBD, DriverStatus.LAUNCHED, 1));
    driverStoreManager.put(version, nbddriverStore);
    VersionManager versionManager = new VersionManagerImpl(versionPath);
    versionManager.setCurrentVersion(DriverType.NBD, nbdCurVer);
    versionManager.setLatestVersion(DriverType.NBD, nbdLatVer);
    versionManager.setOnMigration(DriverType.NBD, false);
    DriverVersion.currentVersion.put(DriverType.NBD, nbdCurVer);
    DriverVersion.latestVersion.put(DriverType.NBD, nbdLatVer);
    DriverVersion.isOnMigration.put(DriverType.NBD, false);

    driverUpgradeProcessor.setVersionManager(versionManager);
    worker.setDriverUpgradeProcessor(driverUpgradeProcessor);
    worker.setDriverStoreManager(driverStoreManager);
    worker.setDcConfig(dcConfig);
    worker.setJvmConfigManager(jvmConfigManager);

  }

  //Set NBD and FSD latest version different from current version ,and submit 2 request ,
  // blockingQueue size is 2
  @Test
  public void submitTest() {

    try {
      worker.doWork();
    } catch (Exception e) {
      logger.warn("catch an exception", e);
    }
    BlockingQueue blockingQueue = driverUpgradeProcessor.getBlockingQueue();
    Assert.assertTrue(blockingQueue.size() == 1);
  }

  private DriverMetadata buildDriver(DriverType driverType, DriverStatus status, long volumeId) {
    DriverMetadata driver = new DriverMetadata();
    driver.setVolumeId(RequestIdBuilder.get());
    driver.setDriverType(driverType);
    driver.setDriverStatus(status);
    driver.setProcessId(Integer.MAX_VALUE);
    driver.setVolumeId(volumeId);

    return driver;
  }

  @After
  public void clean() {
    FileUtils.deleteQuietly(Paths.get("/tmp/versionTest").toFile());
  }

}
