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
import py.app.context.AppContext;
import py.infocenter.client.InformationCenterClientFactory;
import py.infocenter.client.InformationCenterClientWrapper;
import py.instance.InstanceId;
import py.test.TestBase;
import py.thrift.infocenter.service.InformationCenter;
import py.thrift.share.DriverKeyThrift;
import py.thrift.share.DriverTypeThrift;
import py.thrift.share.GetIoLimitationResponseThrift;
import py.thrift.share.IoLimitationThrift;


public class GetIoLimitationWorkTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(GetIoLimitationWorkTest.class);
  @Mock
  InformationCenterClientFactory inforCenterClientFactory;
  @Mock
  private InformationCenter.Iface icClient;
  @Mock
  private InformationCenterClientWrapper icClientWrapper;
  @Mock
  private AppContext appContext;

  private GetIoLimitationWork work;
  private DriverKeyThrift driverKey1;
  private DriverKeyThrift driverKey2;
  private DriverKeyThrift driverKey3;


  /**
   * xx.
   */
  @Before
  public void init() throws Exception {
    super.init();
    super.setLogLevel(Level.ALL);

    work = new GetIoLimitationWork();
    work.setInformationCenterClientFactory(inforCenterClientFactory);
    work.setAppContext(appContext);

    List<IoLimitationThrift> limit1 = new ArrayList<>();
    List<IoLimitationThrift> limit2 = new ArrayList<>();
    final List<IoLimitationThrift> limit3 = new ArrayList<>();
    IoLimitationThrift ioLimitationThrift1 = new IoLimitationThrift();
    IoLimitationThrift ioLimitationThrift2 = new IoLimitationThrift();
    final IoLimitationThrift ioLimitationThrift3 = new IoLimitationThrift();

    limit1.add(ioLimitationThrift1);
    limit1.add(ioLimitationThrift2);

    limit2.add(ioLimitationThrift1);
    limit2.add(ioLimitationThrift2);
    limit2.add(ioLimitationThrift3);

    limit3.add(ioLimitationThrift1);

    driverKey1 = new DriverKeyThrift(0, 1, 0, DriverTypeThrift.ISCSI);
    driverKey2 = new DriverKeyThrift(0, 2, 0, DriverTypeThrift.ISCSI);
    driverKey3 = new DriverKeyThrift(0, 3, 0, DriverTypeThrift.ISCSI);

    Map<DriverKeyThrift, List<IoLimitationThrift>> ioLimitTable = new ConcurrentHashMap<>();
    ioLimitTable.put(driverKey1, limit3);
    ioLimitTable.put(driverKey2, limit3);
    work.setDriver2ItsIoLimitationsTable(ioLimitTable);

    Map<DriverKeyThrift, List<IoLimitationThrift>> ioLimitTableInfo = new ConcurrentHashMap<>();
    ioLimitTableInfo.put(driverKey1, limit1);
    ioLimitTableInfo.put(driverKey3, limit2);

    InstanceId instanceId = new InstanceId(1);
    when(appContext.getInstanceId()).thenReturn(instanceId);

    when(inforCenterClientFactory.build()).thenReturn(icClientWrapper);
    when(icClientWrapper.getClient()).thenReturn(icClient);
    when(icClient.getIoLimitationsInOneDriverContainer(any()))
        .thenReturn(new GetIoLimitationResponseThrift(1L, 1, ioLimitTableInfo));

  }

  @Test
  public void getIoLimitTableTest() throws Exception {
    for (int i = 0; i < 3; i++) {
      work.doWork();
      Map<DriverKeyThrift, List<IoLimitationThrift>> ioLimitTable = (work
          .getDriver2ItsIoLimitationsTable());
      Assert.assertTrue(ioLimitTable.containsKey(driverKey1));
      Assert.assertTrue(ioLimitTable.containsKey(driverKey3));
      Assert.assertTrue(!ioLimitTable.containsKey(driverKey2));
      Assert.assertTrue(ioLimitTable.get(driverKey1).size() == 2);
    }

  }

}
