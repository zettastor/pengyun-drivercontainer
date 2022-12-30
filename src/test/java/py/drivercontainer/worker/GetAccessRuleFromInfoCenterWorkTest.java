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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.drivercontainer.driver.store.DriverMemoryStoreImpl;
import py.drivercontainer.driver.store.DriverStore;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.drivercontainer.driver.store.DriverStoreManagerImpl;
import py.drivercontainer.driver.version.Version;
import py.drivercontainer.driver.version.file.VersionImpl;
import py.infocenter.client.InformationCenterClientFactory;
import py.infocenter.client.InformationCenterClientWrapper;
import py.test.TestBase;
import py.thrift.infocenter.service.InformationCenter;
import py.thrift.share.AccessPermissionTypeThrift;
import py.thrift.share.DriverKeyThrift;
import py.thrift.share.DriverTypeThrift;
import py.thrift.share.IscsiAccessRuleThrift;
import py.thrift.share.ListIscsiAccessRulesByDriverKeysResponse;
import py.thrift.share.ListVolumeAccessRulesByVolumeIdsResponse;
import py.thrift.share.VolumeAccessRuleThrift;

public class GetAccessRuleFromInfoCenterWorkTest extends TestBase {

  private static final Logger logger = LoggerFactory
      .getLogger(GetAccessRuleFromInfoCenterWorkTest.class);
  @Mock
  InformationCenterClientFactory inforCenterClientFactory;
  private DriverStore driverStore = new DriverMemoryStoreImpl();
  @Mock
  private InformationCenter.Iface icClient;
  @Mock
  private InformationCenterClientWrapper icClientWrapper;
  private GetVolumeAccessRulesFromInfoCenterWork work;

  private DriverStoreManager driverStoreManager = new DriverStoreManagerImpl();


  /**
   * xx.
   */
  @Before
  public void init() throws Exception {
    super.init();
    super.setLogLevel(Level.ALL);
    final Version version = VersionImpl.get("2.3.0-internal-20170918000011");

    driverStore.save(buildDriver(1L, DriverType.NBD));
    driverStore.save(buildDriver(2L, DriverType.ISCSI));
    driverStore.save(buildDriver(1L, DriverType.ISCSI));

    driverStoreManager.put(version, driverStore);
    work = new GetVolumeAccessRulesFromInfoCenterWork();
    work.setInformationCenterClientFactory(inforCenterClientFactory);

    work.setDriverStoreManager(driverStoreManager);
    final List<VolumeAccessRuleThrift> rules1 = new ArrayList<>();
    final List<VolumeAccessRuleThrift> rules2 = new ArrayList<>();
    final List<VolumeAccessRuleThrift> rules3 = new ArrayList<>();
    VolumeAccessRuleThrift volumeAccessRuleThrift1 = new VolumeAccessRuleThrift();
    final VolumeAccessRuleThrift volumeAccessRuleThrift2 = new VolumeAccessRuleThrift();
    final VolumeAccessRuleThrift volumeAccessRuleThrift3 = new VolumeAccessRuleThrift();
    volumeAccessRuleThrift1.setRuleId(0);
    volumeAccessRuleThrift1.setIncomingHostName("192.168.2.101");
    volumeAccessRuleThrift1.setPermission(AccessPermissionTypeThrift.READWRITE);

    volumeAccessRuleThrift2.setRuleId(1);
    volumeAccessRuleThrift2.setIncomingHostName("192.168.2.102");
    volumeAccessRuleThrift2.setPermission(AccessPermissionTypeThrift.READWRITE);

    volumeAccessRuleThrift3.setRuleId(2);
    volumeAccessRuleThrift3.setIncomingHostName("192.168.2.103");
    volumeAccessRuleThrift3.setPermission(AccessPermissionTypeThrift.READWRITE);

    rules1.add(volumeAccessRuleThrift1);
    rules1.add(volumeAccessRuleThrift2);

    rules2.add(volumeAccessRuleThrift1);
    rules2.add(volumeAccessRuleThrift2);
    rules2.add(volumeAccessRuleThrift3);

    rules3.add(volumeAccessRuleThrift1);

    Map<Long, List<VolumeAccessRuleThrift>> volumeAccessRuleTable = new ConcurrentHashMap<>();
    volumeAccessRuleTable.put(1L, rules3);
    volumeAccessRuleTable.put(2L, rules3);
    work.setVolumeAccessRuleTable(volumeAccessRuleTable);

    Map<Long, List<VolumeAccessRuleThrift>> volumeAccessRuleTableInfo = new ConcurrentHashMap<>();
    volumeAccessRuleTableInfo.put(1L, rules1);
    volumeAccessRuleTableInfo.put(3L, rules2);

    final List<IscsiAccessRuleThrift> rules4 = new ArrayList<>();
    IscsiAccessRuleThrift iscsiAccessRuleThrift4 = new IscsiAccessRuleThrift();
    iscsiAccessRuleThrift4.setInitiatorName("iqn.2003-01.zettor:127.0.0.1");
    iscsiAccessRuleThrift4.setUser("root");
    iscsiAccessRuleThrift4.setPassed("123456");
    iscsiAccessRuleThrift4.setOutUser("root");
    iscsiAccessRuleThrift4.setOutPassed("123456");
    iscsiAccessRuleThrift4.setPermission(AccessPermissionTypeThrift.READWRITE);

    rules4.add(iscsiAccessRuleThrift4);
    DriverKeyThrift driverKey4 = new DriverKeyThrift(4L, 1L, 0, DriverTypeThrift.ISCSI);
    DriverKeyThrift driverKey5 = new DriverKeyThrift(5L, 1L, 0, DriverTypeThrift.ISCSI);
    DriverKeyThrift driverKey6 = new DriverKeyThrift(6L, 1L, 0, DriverTypeThrift.ISCSI);

    Map<DriverKeyThrift, List<IscsiAccessRuleThrift>> iscsiAccessRuleTableInfo =
        new ConcurrentHashMap<>();
    iscsiAccessRuleTableInfo.put(driverKey4, rules4);
    iscsiAccessRuleTableInfo.put(driverKey6, rules4);

    when(inforCenterClientFactory.build()).thenReturn(icClientWrapper);
    when(icClientWrapper.getClient()).thenReturn(icClient);
    when(icClient.listVolumeAccessRulesByVolumeIds(any()))
        .thenReturn(new ListVolumeAccessRulesByVolumeIdsResponse(1L, volumeAccessRuleTableInfo));

    when(icClient.listIscsiAccessRulesByDriverKeys(any()))
        .thenReturn(new ListIscsiAccessRulesByDriverKeysResponse(1L, iscsiAccessRuleTableInfo));
  }

  @Test
  public void getAccessRuleTest() throws Exception {
    for (int i = 0; i < 3; i++) {
      work.doWork();
      Map<Long, List<VolumeAccessRuleThrift>> accessByVolumeId = (work.getVolumeAccessRuleTable());
      Assert.assertTrue(accessByVolumeId.containsKey(1L));
      Assert.assertTrue(accessByVolumeId.containsKey(3L));
      Assert.assertTrue(!accessByVolumeId.containsKey(2L));
      Assert.assertTrue(accessByVolumeId.get(1L).size() == 2);
    }

  }


  /**
   * xx.
   */
  public DriverMetadata buildDriver(long volumeId, DriverType driverType) {
    DriverMetadata driver = new DriverMetadata();
    driver.setDriverContainerId(1);
    driver.setDriverType(driverType);
    driver.setVolumeId(volumeId);
    driver.setSnapshotId(0);
    driver.setDriverStatus(DriverStatus.LAUNCHED);
    driver.setHostName("192.168.2.101");
    return driver;
  }

}
