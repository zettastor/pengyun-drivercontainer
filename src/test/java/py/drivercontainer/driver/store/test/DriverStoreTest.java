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

package py.drivercontainer.driver.store.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import py.common.RequestIdBuilder;
import py.common.Utils;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.drivercontainer.driver.store.DriverStoreImpl;
import py.drivercontainer.driver.version.Version;
import py.drivercontainer.driver.version.file.VersionImpl;
import py.icshare.DriverKey;
import py.test.TestBase;

/**
 * A class includes some test for {@link DriverStoreImpl}.
 *
 */
public class DriverStoreTest extends TestBase {

  private final String coordinatorPidsDirName = "SPid_coordinator";
  private String driverStoreDirectory = "/tmp/driver_store_test";
  // private DriverStoreImpl driverStore = new DriverStoreImpl(Paths.get(driverStoreDirectory));
  private Version version;
  private String versionStr = "2.3.0-internal-20170918000011";

  @Before
  @Override
  public void init() throws Exception {
    super.init();
    File clearDir = new File(driverStoreDirectory);
    // Utils.deleteEveryThingExceptDirectory(clearDir);
    version = VersionImpl.get(versionStr);

  }

  /**
   * The case stores 3 driver to file and one of them is broken after that. But the broken one would
   * not affect other 2 drivers. The test makes sure this.
   */
  @Test
  public void loadAllWithFailSomeOne() throws Exception {
    DriverStoreImpl driverStore = new DriverStoreImpl(Paths.get(driverStoreDirectory), version);
    Long driverContainerId = RequestIdBuilder.get();
    DriverMetadata okDriver1 = buildDriverWithStatus(DriverStatus.LAUNCHED, driverContainerId);
    DriverMetadata failDriver = buildDriverWithStatus(DriverStatus.LAUNCHED, driverContainerId);
    DriverMetadata okDriver2 = buildDriverWithStatus(DriverStatus.LAUNCHED, driverContainerId);

    driverStore.save(okDriver1);
    driverStore.save(okDriver2);
    driverStore.save(failDriver);
    // write empty string to fail driver file to make the driver metadata miss
    Path failDriverFilePath = Paths
        .get(driverStoreDirectory + "/" + versionStr, coordinatorPidsDirName,
            Long.toString(failDriver.getVolumeId()), failDriver.getDriverType().name(),
            Integer.toString(failDriver.getSnapshotId()));
    BufferedWriter reader = new BufferedWriter(new FileWriter(failDriverFilePath.toFile()));
    reader.write("");
    reader.close();

    ((DriverStoreImpl) driverStore).clearMomory();

    // check ok driver and fail driver
    Assert.assertTrue(driverStore.load(driverContainerId));
    Assert.assertNotNull(driverStore.get(new DriverKey(driverContainerId, okDriver1.getVolumeId(),
        okDriver1.getSnapshotId(), okDriver1.getDriverType())));
    Assert.assertNotNull(driverStore.get(driverStore.makeDriverKey(okDriver2)));
    Assert.assertNull(driverStore.get(driverStore.makeDriverKey(failDriver)));
  }

  @Test
  public void testListAllDriversWithSameType() throws Exception {
    final int nDrivers = 5;

    DriverStoreImpl driverStore = new DriverStoreImpl(Paths.get(driverStoreDirectory), version);
    Long driverContainerId = RequestIdBuilder.get();

    Set<DriverMetadata> nbdDrivers = new HashSet<>();
    Set<DriverMetadata> fsdDrivers = new HashSet<>();

    for (int i = 0; i < nDrivers; i++) {
      DriverMetadata driver = buildDriverWithStatus(DriverStatus.LAUNCHED, driverContainerId);

      driverStore.save(driver);
      nbdDrivers.add(driver);

      driver = buildDriverWithStatus(DriverStatus.LAUNCHED, driverContainerId);
      driver.setDriverType(DriverType.FSD);
      driverStore.save(driver);
      fsdDrivers.add(driver);
    }

    org.junit.Assert
        .assertEquals(nbdDrivers, new HashSet<DriverMetadata>(driverStore.list(DriverType.NBD)));
    org.junit.Assert
        .assertEquals(fsdDrivers, new HashSet<DriverMetadata>(driverStore.list(DriverType.FSD)));
  }

  @Test
  public void testSaveAndDelete() throws Exception {
    DriverStoreImpl driverStore = new DriverStoreImpl(Paths.get(driverStoreDirectory), version);
    Long driverContainerId = RequestIdBuilder.get();
    DriverMetadata driver = buildDriverWithStatus(DriverStatus.LAUNCHED, driverContainerId);
    driverStore.save(driver);

    File spidFile = Paths
        .get(driverStoreDirectory, version.format(), "SPid_coordinator",
            Long.toString(driver.getVolumeId()),
            driver.getDriverType().name(), Integer.toString(driver.getSnapshotId()))
        .toFile();
    org.junit.Assert.assertTrue(spidFile.exists());
    org.junit.Assert.assertTrue(spidFile.getParentFile().exists());
    org.junit.Assert.assertTrue(spidFile.getParentFile().getParentFile().exists());

    driverStore.remove(
        new DriverKey(driverContainerId, driver.getVolumeId(), driver.getSnapshotId(),
            driver.getDriverType()));
    org.junit.Assert.assertFalse(spidFile.exists());
    org.junit.Assert.assertFalse(spidFile.getParentFile().exists());
    org.junit.Assert.assertFalse(spidFile.getParentFile().getParentFile().exists());
  }

  private DriverMetadata buildDriverWithStatus(DriverStatus status, Long driverContainerId) {
    DriverMetadata driver = new DriverMetadata();
    driver.setDriverContainerId(driverContainerId);
    driver.setVolumeId(RequestIdBuilder.get());
    driver.setDriverType(DriverType.NBD);
    driver.setDriverStatus(status);
    driver.setProcessId(Integer.MAX_VALUE);

    return driver;
  }

  @Test
  public void test() {
    byte[] data = {1, 2};
    logger.debug("channelRead req {}", data);
    String sdata = new String(data);
    logger.debug("channelRead req {}", sdata);
  }

  @After
  public void clean() throws Exception {
    File clearDir = new File(driverStoreDirectory);
    Utils.deleteEveryThingExceptDirectory(clearDir);
  }

}
