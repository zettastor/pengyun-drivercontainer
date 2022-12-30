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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import py.app.context.AppContext;
import py.common.RequestIdBuilder;
import py.coordinator.IscsiTargetManager;
import py.coordinator.lio.LioNameBuilder;
import py.coordinator.lio.LioNameBuilderImpl;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.DriverVersion;
import py.drivercontainer.driver.store.DriverStore;
import py.drivercontainer.driver.store.DriverStoreImpl;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.drivercontainer.driver.store.DriverStoreManagerImpl;
import py.drivercontainer.driver.version.Version;
import py.drivercontainer.driver.version.VersionManager;
import py.drivercontainer.driver.version.file.VersionImpl;
import py.drivercontainer.driver.version.file.VersionManagerImpl;
import py.icshare.DriverKey;
import py.infocenter.client.InformationCenterClientFactory;
import py.infocenter.client.InformationCenterClientWrapper;
import py.informationcenter.AccessPermissionType;
import py.instance.InstanceId;
import py.test.TestBase;
import py.thrift.icshare.ReportDriverMetadataRequest;
import py.thrift.icshare.ReportDriverMetadataResponse;
import py.thrift.infocenter.service.InformationCenter;
import py.thrift.share.AccessPermissionTypeThrift;
import py.thrift.share.IscsiAccessRuleThrift;

/**
 * A class contains some test for {@link ReportDriverMetadataWorker}.
 *
 */
public class ReportDriverMetadataWorkerTest extends TestBase {

  private ReportDriverMetadataWorker worker = new ReportDriverMetadataWorker();

  @Mock
  private InformationCenterClientFactory infoCenterClientFactory;
  @Mock
  private InformationCenter.Iface client;
  private InformationCenterClientWrapper icClientWrapper = new InformationCenterClientWrapper(
      client);
  private DriverContainerConfiguration driverContainerConfig = new DriverContainerConfiguration();
  private LioNameBuilder lioNameBuilder = new LioNameBuilderImpl();
  private List<String> clientAddressList = new ArrayList<>();
  private String driverStoreDirectory = "/tmp/ReporeDriverTest";
  private DriverStoreManager driverStoreManager = new DriverStoreManagerImpl();
  private VersionManager versionManager;
  private DriverContainerConfiguration dcConfig;
  private IscsiTargetManager iscsiTargetManager;
  @Mock
  private AppContext appContext;
  @Mock
  private InstanceId instanceId;


  private String withoutUpgradeVersionDir = "/tmp/withoutUpgradeVersionTest";
  private String upgradeVersionDir = "/tmp/upgradeVersionTest";


  private Version nbdCurVer = VersionImpl.get("2.3.0-internal-20170918000011");
  private Version nbdLatVer = VersionImpl.get("2.3.0-internal-20170918000022");
  private ReportDriverMetadataResponse response = new ReportDriverMetadataResponse();


  /**
   * xx.
   */
  @Before
  public void init() throws Exception {
    super.init();
    File file = new File(driverStoreDirectory);
    file.mkdirs();

    File file1 = new File(withoutUpgradeVersionDir);
    file1.mkdirs();

    File file2 = new File(upgradeVersionDir);
    file2.mkdirs();

    DriverVersion.currentVersion.put(DriverType.NBD, nbdCurVer);
    DriverVersion.latestVersion.put(DriverType.NBD, nbdCurVer);
    DriverVersion.isOnMigration.put(DriverType.NBD, false);

    dcConfig = new DriverContainerConfiguration();
    worker.setInformationCenterClientFactory(infoCenterClientFactory);
    worker.setDriverContainerConfiguration(dcConfig);
    worker.setIscsiTargetManager(iscsiTargetManager);
    worker.setAppContext(appContext);
    when(infoCenterClientFactory.build()).thenReturn(icClientWrapper);
    when(icClientWrapper.getClient().reportDriverMetadata(any(ReportDriverMetadataRequest.class)))
        .thenReturn(response);
    when(appContext.getInstanceId()).thenReturn(instanceId);
    when(instanceId.getId()).thenReturn(1L);
  }

  @Test
  public void clientAlteredOrNot() throws Exception {
    Map<String, AccessPermissionType> clientsAccessRule = new HashMap<>();
    clientsAccessRule.put("10.0.1.116", AccessPermissionType.READWRITE);
    DriverMetadata driver = buildDriver(DriverType.NBD, null, 0);
    driver.setClientHostAccessRule(new HashMap<String, AccessPermissionType>(clientsAccessRule));

    Assert.assertFalse(worker.clientAltered(driver, clientsAccessRule));

    clientsAccessRule.put("10.0.1.117", AccessPermissionType.READWRITE);
    Assert.assertTrue(worker.clientAltered(driver, clientsAccessRule));

    driver.setClientHostAccessRule(null);
    Assert.assertFalse(worker.clientAltered(driver, new HashMap<String, AccessPermissionType>()));
  }

  @Test
  public void reportDriverMetadataWork() throws Exception {
    DriverStore nbddriverStore = new DriverStoreImpl(Paths.get(driverStoreDirectory), nbdCurVer);
    nbddriverStore.save(buildDriver(DriverType.NBD, DriverStatus.LAUNCHED, 1));
    nbddriverStore.save(buildDriver(DriverType.ISCSI, DriverStatus.LAUNCHED, 2));
    driverStoreManager.put(nbdCurVer, nbddriverStore);
    versionManager = new VersionManagerImpl(driverStoreDirectory);
    clientAddressList.add(null);
    worker.setDriverStoreManager(driverStoreManager);
    worker.setVersionManager(versionManager);
    for (int i = 0; i < 2; i++) {
      worker.doWork();
      clientAddressList.add("192.168.2.103");
    }
    for (DriverStore store : driverStoreManager.values()) {
      for (DriverMetadata driver : store.list()) {
        if (driver.getDriverType().equals(DriverType.ISCSI)) {
          for (String client : driver.getClientHostAccessRule().keySet()) {
            Assert.assertTrue(client.equals("192.168.2.103"));
          }
        }

      }
    }
  }

  /**
   * Test driversList which to report containers all driver in driverStoreManager.
   */
  @Test
  public void driverReportWithoutUpgradeTest() throws Exception {
    DriverStoreManager driverStoreManagerNoUpgrade = new DriverStoreManagerImpl();
    final VersionManager versionManagerNoUpgrade = new VersionManagerImpl(withoutUpgradeVersionDir);
    DriverStore nbdDriverStore = new DriverStoreImpl(Paths.get(withoutUpgradeVersionDir),
        nbdCurVer);
    nbdDriverStore.save(buildDriver(DriverType.NBD, DriverStatus.LAUNCHED, 1));
    nbdDriverStore.save(buildDriver(DriverType.NBD, DriverStatus.LAUNCHED, 2));

    driverStoreManagerNoUpgrade.put(nbdCurVer, nbdDriverStore);
    worker.setVersionManager(versionManagerNoUpgrade);
    worker.setDriverStoreManager(driverStoreManagerNoUpgrade);

    worker.doWork();
    Assert.assertTrue(worker.getReportDriverList().get(nbdCurVer).size() == 2);

  }

  /**
   * When currentStore and latestStore has the same driverKey,just report driver status for the
   * driverKey in latestStore.
   */
  @Test
  public void reportUpgradeDrivers() throws Exception {
    final DriverStoreManager driverStoreManagerUpgrade = new DriverStoreManagerImpl();
    final VersionManager versionManagerUpgrade = new VersionManagerImpl(upgradeVersionDir);

    //change latest version different to current version
    DriverVersion.latestVersion.put(DriverType.NBD, nbdLatVer);
    DriverVersion.isOnMigration.put(DriverType.NBD, true);

    DriverStore nbdCurrentDriverStore = new DriverStoreImpl(Paths.get(upgradeVersionDir),
        nbdCurVer);
    DriverKey driverKey1 = new DriverKey(1, 1, 1, DriverType.NBD);
    DriverKey driverKey2 = new DriverKey(1, 2, 2, DriverType.NBD);
    final DriverKey driverKey3 = new DriverKey(1, 3, 3, DriverType.NBD);

    nbdCurrentDriverStore.save(buildUniqueDriver(driverKey1, DriverStatus.REMOVING));
    nbdCurrentDriverStore.save(buildUniqueDriver(driverKey2, DriverStatus.LAUNCHED));

    DriverStore nbdLatestDriverStore = new DriverStoreImpl(Paths.get(upgradeVersionDir), nbdLatVer);
    nbdLatestDriverStore.save(buildUniqueDriver(driverKey1, DriverStatus.LAUNCHED));
    nbdLatestDriverStore.save(buildUniqueDriver(driverKey3, DriverStatus.LAUNCHED));

    driverStoreManagerUpgrade.put(nbdCurVer, nbdCurrentDriverStore);
    driverStoreManagerUpgrade.put(nbdLatVer, nbdLatestDriverStore);

    worker.setVersionManager(versionManagerUpgrade);
    worker.setDriverStoreManager(driverStoreManagerUpgrade);
    worker.doWork();

    Assert.assertTrue(worker.getReportDriverList().get(nbdCurVer).size() == 1);

    Assert.assertFalse(worker.getReportDriverList().get(nbdCurVer)
        .contains(driverStoreManagerUpgrade.get(nbdCurVer).get(driverKey1)));
    Assert.assertTrue(worker.getReportDriverList().get(nbdLatVer)
        .contains(driverStoreManagerUpgrade.get(nbdLatVer).get(driverKey1)));


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

  private DriverMetadata buildUniqueDriver(DriverKey driverKey, DriverStatus driverStatus) {
    DriverMetadata driver = new DriverMetadata();
    driver.setDriverType(driverKey.getDriverType());
    driver.setDriverStatus(DriverStatus.LAUNCHED);
    driver.setProcessId(Integer.MAX_VALUE);
    driver.setVolumeId(driverKey.getVolumeId());
    driver.setDriverContainerId(driverKey.getDriverContainerId());
    driver.setSnapshotId(driverKey.getSnapshotId());
    driver.setDriverStatus(driverStatus);

    return driver;
  }


  /**
   * xx.
   */
  public IscsiAccessRuleThrift newIscsiAccessRule(long ruleId, String initialName, String usr,
      String passwd, AccessPermissionTypeThrift perm) {
    IscsiAccessRuleThrift iscsiAccessRuleThrift = new IscsiAccessRuleThrift();
    iscsiAccessRuleThrift.setRuleId(ruleId);
    iscsiAccessRuleThrift.setInitiatorName(initialName);
    iscsiAccessRuleThrift.setUser(usr);
    iscsiAccessRuleThrift.setPassed(passwd);
    iscsiAccessRuleThrift.setPermission(perm);

    return iscsiAccessRuleThrift;
  }

  @After
  public void clean() throws Exception {
    FileUtils.deleteQuietly(Paths.get("/tmp/ReporeDriverTest").toFile());
    FileUtils.deleteQuietly(Paths.get("/tmp/driverSweep").toFile());
  }

}
