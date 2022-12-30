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

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.drivercontainer.DriverVersion;
import py.drivercontainer.driver.store.DriverMemoryStoreImpl;
import py.drivercontainer.driver.store.DriverStore;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.drivercontainer.driver.store.DriverStoreManagerImpl;
import py.drivercontainer.driver.version.Version;
import py.drivercontainer.driver.version.VersionException;
import py.drivercontainer.driver.version.VersionManager;
import py.drivercontainer.driver.version.file.VersionImpl;
import py.drivercontainer.driver.version.file.VersionManagerImpl;
import py.drivercontainer.service.DoWorkTest;
import py.drivercontainer.service.DriverContainerImpl;
import py.informationcenter.AccessPermissionType;
import py.test.TestBase;
import py.thrift.icshare.ReportDriverMetadataRequest;
import py.thrift.share.AccessPermissionTypeThrift;
import py.thrift.share.DriverMetadataThrift;
import py.thrift.share.DriverStatusThrift;
import py.thrift.share.DriverTypeThrift;

public class ReportDriverMetadataTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(DoWorkTest.class);

  private DriverMetadataThrift driverMetadataThrift;
  private DriverContainerImpl driverContainerImpl;
  @Mock
  private AppContext appContext;


  private String clientVersionDirectory = "/tmp/clientVersionTest";


  private Map<String, AccessPermissionType> clientsAccessRule = new HashMap<>();
  private Map<String, AccessPermissionTypeThrift> clientsAccessRuleThrift = new HashMap<>();


  /**
   * xx.
   */
  @Before
  public void init() throws Exception {
    super.init();
    super.setLogLevel(Level.ALL);
    File file = new File(clientVersionDirectory);
    file.mkdirs();

    driverContainerImpl = new DriverContainerImpl(appContext);


  }

  @Test
  public void driverClientChangeTest() throws TException, VersionException, IOException {
    DriverStore driverStore = new DriverMemoryStoreImpl();
    final DriverStoreManager driverStoreManager = new DriverStoreManagerImpl();
    final ReportDriverMetadataRequest request = new ReportDriverMetadataRequest();
    clientsAccessRule.put("192.168.2.101", AccessPermissionType.READWRITE);
    driverStore.save(buildDriver(DriverType.NBD, 1L, clientsAccessRule));
    driverStore.save(buildDriver(DriverType.NBD, 2L, clientsAccessRule));
    Version version = VersionImpl.get("2.3.0-internal-20170918000011");
    final VersionManager versionManager = new VersionManagerImpl(clientVersionDirectory);
    DriverVersion.currentVersion.put(DriverType.NBD, version);
    DriverVersion.latestVersion.put(DriverType.NBD, version);
    DriverVersion.isOnMigration.put(DriverType.NBD, false);
    driverStoreManager.put(version, driverStore);
    driverContainerImpl.setDriverStoreManager(driverStoreManager);
    driverContainerImpl.setVersionManager(versionManager);
    clientsAccessRuleThrift.put("192.168.2.102", AccessPermissionTypeThrift.READ);
    driverMetadataThrift = buildDriverThrift(DriverTypeThrift.NBD, 1L, clientsAccessRuleThrift);
    List<DriverMetadataThrift> driversThrift = new ArrayList<DriverMetadataThrift>();
    driversThrift.add(driverMetadataThrift);
    request.setRequestId(1L);
    request.setDrivers(driversThrift);
    driverContainerImpl.reportDriverMetadata(request);
    for (DriverMetadata driver : driverStore.list()) {
      if (driver.getVolumeId() == 1L) {
        Assert.assertTrue(driver.getClientHostAccessRule().get("192.168.2.102")
            .equals(AccessPermissionType.READ));
      }

      if (driver.getVolumeId() == 2L) {
        Assert.assertTrue(driver.getClientHostAccessRule().get("192.168.2.101")
            .equals(AccessPermissionType.READWRITE));
      }
    }

    logger.warn("driverStore:{}", driverStore.list());
  }


  /**
   * xx.
   */
  public DriverMetadata buildDriver(DriverType driverType, long volumeId,
      Map<String, AccessPermissionType> clientsAccessRule) {
    DriverMetadata driver = new DriverMetadata();
    driver.setDriverType(driverType);
    driver.setVolumeId(volumeId);
    driver.setDriverStatus(DriverStatus.LAUNCHED);
    driver.setDriverContainerId(1L);
    driver.setClientHostAccessRule(clientsAccessRule);
    return driver;
  }


  /**
   * xx.
   */
  public DriverMetadataThrift buildDriverThrift(DriverTypeThrift driverType, long volumeId,
      Map<String, AccessPermissionTypeThrift> clientsAccessRule) {
    DriverMetadataThrift driver = new DriverMetadataThrift();
    driver.setDriverType(driverType);
    driver.setVolumeId(volumeId);
    driver.setDriverStatus(DriverStatusThrift.LAUNCHED);
    driver.setDriverContainerId(1L);
    driver.setClientHostAccessRule(clientsAccessRule);
    return driver;
  }


  @After
  public void clean() throws Exception {
    FileUtils.deleteQuietly(Paths.get(clientVersionDirectory).toFile());
  }


}
