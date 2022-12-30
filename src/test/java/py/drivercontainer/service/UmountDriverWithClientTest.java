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
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import py.app.context.AppContext;
import py.common.RequestIdBuilder;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.DriverVersion;
import py.drivercontainer.driver.store.DriverStore;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.drivercontainer.driver.store.DriverStoreManagerImpl;
import py.drivercontainer.driver.version.Version;
import py.drivercontainer.driver.version.VersionManager;
import py.drivercontainer.driver.version.file.VersionImpl;
import py.drivercontainer.driver.version.file.VersionManagerImpl;
import py.drivercontainer.service.taskqueue.TaskExecutor;
import py.drivercontainer.worker.RemoveDriverWorker;
import py.drivercontainer.worker.RemoveDriverWorkerFactory;
import py.icshare.DriverKey;
import py.informationcenter.AccessPermissionType;
import py.instance.InstanceId;
import py.test.TestBase;
import py.thrift.drivercontainer.service.DriverContainer;
import py.thrift.share.DriverIpTargetThrift;
import py.thrift.share.DriverTypeThrift;
import py.thrift.share.ExistsClientExceptionThrift;
import py.thrift.share.UmountDriverRequestThrift;

public class UmountDriverWithClientTest extends TestBase {

  @Mock
  private DriverStore driverStore;

  @Mock
  private RemoveDriverWorkerFactory removeDriverWorkerFactory;
  @Mock
  private RemoveDriverWorker removeDriverWorker;

  @Mock
  private TaskExecutor taskExecutor;
  @Mock
  private Future future;

  @Mock
  private PortContainerFactory portContainerFactory;
  @Mock
  private PortContainer portContainer;

  @Mock
  private AppContext appContext;

  private DriverContainerImpl dcImpl;

  private VersionManager versionManager;

  private String driverStoreDirectory = "/tmp/UnmountDriverWithClientTest";
  private DriverStoreManager driverStoreManager = new DriverStoreManagerImpl();

  @Override
  @Before
  public void init() throws Exception {
    super.init();

    File file = new File(driverStoreDirectory);
    file.mkdirs();
    versionManager = new VersionManagerImpl(driverStoreDirectory);
    Version version = VersionImpl.get("2.3.0-internal-20170918000011");
    versionManager.setCurrentVersion(DriverType.NBD, version);
    versionManager.setLatestVersion(DriverType.NBD, version);
    versionManager.setOnMigration(DriverType.NBD, false);
    DriverVersion.currentVersion.put(DriverType.NBD, version);
    DriverVersion.latestVersion.put(DriverType.NBD, version);
    DriverVersion.isOnMigration.put(DriverType.NBD, false);
    driverStoreManager.put(version, driverStore);
    final DriverContainerConfiguration driverConfiguration = new DriverContainerConfiguration();
    when(portContainerFactory.getPortContainer(any(DriverType.class))).thenReturn(portContainer);
    dcImpl = new DriverContainerImpl(appContext);
    dcImpl.setDriverStoreManager(driverStoreManager);
    dcImpl.setVersionManager(versionManager);
    dcImpl.setTaskExecutor(taskExecutor);
    dcImpl.setRemoveDriverWorkerFactory(removeDriverWorkerFactory);
    dcImpl.setDriverContainerConfig(driverConfiguration);
    dcImpl.setPortContainerFactory(portContainerFactory);
    InstanceId instanceId = new InstanceId(RequestIdBuilder.get());
    when(appContext.getInstanceId()).thenReturn(instanceId);
  }

  /**
   * Test case in which request to umount nbd driver is refused due to exists clients using the
   * driver.
   */
  @Test
  public void umountNbdDriverWithClients() throws Exception {
    List<String> clientList = new ArrayList<String>();
    clientList.add("10.0.1.1");
    Map<String, AccessPermissionType> clientsAccessRule = new HashMap<>();
    for (String host : clientList) {
      clientsAccessRule.put(host, AccessPermissionType.READWRITE);
    }
    DriverMetadata driver = buildDriverWithStatus(DriverStatus.LAUNCHED);
    driver.setClientHostAccessRule(clientsAccessRule);
    when(driverStore.get(any(DriverKey.class))).thenReturn(driver);

    UmountDriverRequestThrift request = new UmountDriverRequestThrift();
    List<DriverIpTargetThrift> driverIps = new ArrayList<DriverIpTargetThrift>();
    driverIps.add(new DriverIpTargetThrift(0, "10.0.1.127", DriverTypeThrift.NBD, 0L));
    request.setDriverIpTargetList(driverIps);
    boolean exceptionCached = false;
    try {
      dcImpl.umountDriver(request);
    } catch (ExistsClientExceptionThrift e) {
      exceptionCached = true;
    } catch (Exception e) {
      logger.warn("caught an exception", e);
    }
    Assert.assertTrue(exceptionCached);
  }

  private DriverMetadata buildDriverWithStatus(DriverStatus status) {
    DriverMetadata driver = new DriverMetadata();
    driver.setVolumeId(RequestIdBuilder.get());
    driver.setDriverType(DriverType.NBD);
    driver.setDriverStatus(status);
    driver.setProcessId(Integer.MAX_VALUE);

    return driver;
  }

  @After
  public void clean() {
    FileUtils.deleteQuietly(Paths.get("/tmp/UnmountDriverWithClientTest").toFile());
  }

}
