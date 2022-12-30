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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.common.RequestIdBuilder;
import py.driver.DriverAction;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.driver.store.DriverStore;
import py.drivercontainer.driver.store.DriverStoreImpl;
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
import py.instance.InstanceId;
import py.test.TestBase;
import py.thrift.share.DriverIpTargetThrift;
import py.thrift.share.DriverTypeThrift;
import py.thrift.share.UmountDriverRequestThrift;

public class UmountDriverOneFailedTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(UmountDriverOneFailedTest.class);
  private DriverStore driverStore;
  @Mock
  private RemoveDriverWorkerFactory removeDriverWorkerFactory;
  @Mock
  private RemoveDriverWorker removeDriverWorker;

  @Mock
  private TaskExecutor taskExecutor;
  @Mock
  private Future removeDriverFuture;

  @Mock
  private PortContainerFactory portContainerFactory;
  @Mock
  private PortContainer portContainer;

  @Mock
  private AppContext appContext;

  private DriverContainerImpl dcImpl;

  private VersionManager versionManager;

  private String driverStoreDirectory = "/tmp/UnmountFailedDriverTest";
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
    driverStore = new DriverStoreImpl(Paths.get(driverStoreDirectory), version);

    final DriverContainerConfiguration driverConfiguration = new DriverContainerConfiguration();
    when(portContainerFactory.getPortContainer(any(DriverType.class))).thenReturn(portContainer);
    InstanceId instanceId = new InstanceId(RequestIdBuilder.get());
    when(appContext.getInstanceId()).thenReturn(instanceId);
    driverStore
        .save(buildIscsiDriver(0, DriverStatus.LAUNCHED, DriverType.NBD,
            appContext.getInstanceId().getId()));
    driverStore
        .save(buildIscsiDriver(1, DriverStatus.LAUNCHED, DriverType.ISCSI,
            appContext.getInstanceId().getId()));
    driverStore
        .save(buildIscsiDriver(2, DriverStatus.LAUNCHED, DriverType.NBD,
            appContext.getInstanceId().getId()));
    dcImpl = new DriverContainerImpl(appContext);
    driverStoreManager.put(version, driverStore);
    dcImpl.setDriverStoreManager(driverStoreManager);
    dcImpl.setVersionManager(versionManager);
    dcImpl.setTaskExecutor(taskExecutor);
    dcImpl.setRemoveDriverWorkerFactory(removeDriverWorkerFactory);
    dcImpl.setDriverContainerConfig(driverConfiguration);
    dcImpl.setPortContainerFactory(portContainerFactory);
    when(removeDriverWorkerFactory.createWorker(any(DriverKey.class)))
        .thenReturn(removeDriverWorker);
    when(taskExecutor.submit(any(DriverKey.class), any(DriverAction.class), any(Runnable.class),
        any(Version.class)))
        .thenReturn(removeDriverFuture);

  }

  @Test
  public void umountAllDrivers() {
    UmountDriverRequestThrift request = new UmountDriverRequestThrift();
    List<DriverIpTargetThrift> driverIps = new ArrayList<DriverIpTargetThrift>();
    driverIps.add(
        new DriverIpTargetThrift(0, "10.0.1.127", DriverTypeThrift.NBD,
            appContext.getInstanceId().getId()));
    driverIps.add(new DriverIpTargetThrift(1, "10.0.1.127", DriverTypeThrift.ISCSI,
        appContext.getInstanceId().getId()));
    request.setDriverIpTargetList(driverIps);
    try {
      dcImpl.umountDriver(request);
      Mockito.verify(taskExecutor, Mockito.times(driverIps.size()))
          .acceptAndSubmit(any(DriverKey.class),
              any(DriverStatus.class), any(DriverAction.class), any(Runnable.class),
              any(Version.class));
    } catch (Exception e) {
      logger.error("caught exception", e);
    }

  }


  /**
   * xx.
   */
  public DriverMetadata buildIscsiDriver(int snapShotId, DriverStatus driverStatus,
      DriverType driverType,
      long dcId) {
    DriverMetadata driver = new DriverMetadata();
    driver.setSnapshotId(snapShotId);
    driver.setDriverType(driverType);
    driver.setDriverStatus(driverStatus);
    driver.setVolumeId(0L);
    driver.setDriverContainerId(dcId);
    return driver;
  }

  @After
  public void clean() {
    FileUtils.deleteQuietly(Paths.get("/tmp/UnmountFailedDriverTest").toFile());
  }
}
