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

package py.drivercontainer.service;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.common.IscsiTargetNameBuilder;
import py.common.RequestIdBuilder;
import py.driver.DriverMetadata;
import py.driver.DriverType;
import py.drivercontainer.DriverVersion;
import py.drivercontainer.driver.store.DriverMemoryStoreImpl;
import py.drivercontainer.driver.store.DriverStore;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.drivercontainer.driver.store.DriverStoreManagerImpl;
import py.drivercontainer.driver.version.Version;
import py.drivercontainer.driver.version.VersionManager;
import py.drivercontainer.driver.version.file.VersionImpl;
import py.drivercontainer.driver.version.file.VersionManagerImpl;
import py.iet.file.mapper.InitiatorsAllowFileMapper;
import py.infocenter.client.InformationCenterClientFactory;
import py.infocenter.client.InformationCenterClientWrapper;
import py.informationcenter.AccessPermissionType;
import py.instance.InstanceId;
import py.test.TestBase;
import py.thrift.drivercontainer.service.FailedToCancelVolumeAccessRulesExceptionThrift;
import py.thrift.infocenter.service.InformationCenter;
import py.thrift.share.CancelVolumeAccessRulesRequest;

/**
 * A class includes some test for canceling volume access rules.
 *
 */
public class CancelVolumeAccessRulesTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(CancelVolumeAccessRulesTest.class);

  private DriverContainerImpl dcImpl;

  private Map<Long, DriverMetadata> driverTable = new HashMap<Long, DriverMetadata>();

  private DriverStore driverStore = new DriverMemoryStoreImpl();

  @Mock
  private InformationCenterClientFactory icClientFactory;
  @Mock
  private InformationCenterClientWrapper icClientWrapper;
  @Mock
  private InformationCenter.Iface icClient;

  @Mock
  private InitiatorsAllowFileMapper fileMapper;

  private Map<String, List<String>> allowInitiatorTable = new HashMap<String, List<String>>();

  @Mock
  private AppContext appContext;

  private long volumeIdForTest = 0;

  private Long driverContainerId = RequestIdBuilder.get();
  private VersionManager versionManager;
  private String driverStoreDirectory = "/tmp/CancleVolumeAccessTest";
  private DriverStoreManager driverStoreManager = new DriverStoreManagerImpl();

  @Override
  @Before
  public void init() throws Exception {
    super.init();
    File file = new File(driverStoreDirectory);
    file.mkdirs();
    versionManager = new VersionManagerImpl(driverStoreDirectory);
    // volume id is 0l
    driverStore.save(buildIscsiDriver(0L, driverContainerId, 0));
    driverStore.save(buildIscsiDriver(0L, driverContainerId, 1));
    Version version = VersionImpl.get("2.3.0-internal-20170918000011");
    versionManager.setCurrentVersion(DriverType.NBD, version);
    DriverVersion.currentVersion.put(DriverType.NBD, version);
    driverStoreManager.put(version, driverStore);

    List<String> allowInitiatorList = new ArrayList<String>();
    allowInitiatorList.add("10.0.1.116");
    allowInitiatorList.add("10.0.1.117");
    List<String> allowInitiatorList1 = new ArrayList<String>();
    allowInitiatorList1.add("10.0.1.116");
    allowInitiatorList1.add("10.0.1.117");
    allowInitiatorTable.put(IscsiTargetNameBuilder.build(volumeIdForTest, 0), allowInitiatorList);
    allowInitiatorTable.put(IscsiTargetNameBuilder.build(volumeIdForTest, 1), allowInitiatorList1);

    dcImpl = new DriverContainerImpl(null);
    dcImpl.setAppContext(appContext);
    dcImpl.setDriverStoreManager(driverStoreManager);
    dcImpl.setInformationCenterClientFactory(icClientFactory);
    dcImpl.setVersionManager(versionManager);

    when(icClientWrapper.getClient()).thenReturn(icClient);
    when(icClientFactory.build()).thenReturn(icClientWrapper);

    when(fileMapper.getInitiatorAllowTable()).thenReturn(allowInitiatorTable);
    when(fileMapper.load()).thenReturn(true);
    when(fileMapper.flush()).thenReturn(true);

    InstanceId instanceId = new InstanceId(driverContainerId);
    when(appContext.getInstanceId()).thenReturn(instanceId);
  }

  /**
   * Fake a file mapper without backend file but memory structure. After canceling volume access
   * rules, no access exist in memory.
   */
  @Test
  public void testCancelVolumeAccessRules()
      throws FailedToCancelVolumeAccessRulesExceptionThrift, TException {
    List<Long> ruleIdList = new ArrayList<Long>();
    ruleIdList.add(0L);
    ruleIdList.add(1L);
    CancelVolumeAccessRulesRequest request = new CancelVolumeAccessRulesRequest();
    request.setRequestId(0L);
    request.setAccountId(0L);
    request.setVolumeId(0L);
    request.setRuleIds(ruleIdList);

    Map<String, AccessPermissionType> authorizationTable = new HashMap<>();
    authorizationTable.put("10.0.1.116", AccessPermissionType.READWRITE);
    authorizationTable.put("10.0.1.117", AccessPermissionType.READWRITE);

    when(icClientWrapper.getVolumeAccessRules(anyLong(), anyLong(), any(List.class)))
        .thenReturn(authorizationTable);
    dcImpl.cancelVolumeAccessRules(request);

    Assert.assertTrue(allowInitiatorTable.get(IscsiTargetNameBuilder.build(0L, 0)).size() == 0);
    Assert.assertTrue(allowInitiatorTable.get(IscsiTargetNameBuilder.build(0L, 1)).size() == 0);
  }


  /**
   * xx.
   */
  public DriverMetadata buildIscsiDriver(long volumeId, long driverContainerId, int snapshotId) {
    DriverMetadata driver = new DriverMetadata();
    driver.setDriverContainerId(driverContainerId);
    driver.setDriverType(DriverType.ISCSI);
    driver.setVolumeId(volumeId);
    driver.setSnapshotId(snapshotId);
    return driver;
  }

  @After
  public void clean() {
    FileUtils.deleteQuietly(Paths.get("/tmp/CancleVolumeAccessTest").toFile());
  }
}
