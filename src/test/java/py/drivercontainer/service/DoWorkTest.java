/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/ 

package py.drivercontainer.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import junit.framework.Assert;
import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.drivercontainer.driver.store.DriverMemoryStoreImpl;
import py.drivercontainer.driver.store.DriverStore;
import py.drivercontainer.worker.LaunchDriverWorker;
import py.drivercontainer.worker.LaunchDriverWorkerFactory;
import py.iet.file.mapper.InitiatorsAllowFileMapper;
import py.infocenter.client.InformationCenterClientFactory;
import py.test.TestBase;

public class DoWorkTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(DoWorkTest.class);
  private DriverStore driverStore = new DriverMemoryStoreImpl();
  @Mock
  private InitiatorsAllowFileMapper initiatorsAllowFileMapper;
  @Mock
  private AppContext appContext;
  @Mock
  private LaunchDriverWorkerFactory launchDriverWorkerFactory;
  @Mock
  private LaunchDriverWorker launchDriverWorker;
  @Mock
  private ExecutorService driverThreadPool;
  @Mock
  private InformationCenterClientFactory infoCenterClientFactory;
  @Mock
  private DriverMetadata driver;

  //    private DoubleCheckTrigger doubleCheckTrigger;
  @Override
  @Before
  public void init() throws Exception {
    super.init();
    super.setLogLevel(Level.ALL);
    driverStore.save(buildIscsiDriver(1L));
    driverStore.save(buildIscsiDriver(2L));
    driverStore.save(buildIscsiDriver(3L));
    driverStore.save(buildIscsiDriver(4L));

  }

  @Test
  public void testDoubleCheck() throws Exception {
    List<String> sweepVolumId = new ArrayList<String>();
    List<String> driverStoreList = new ArrayList<String>();
    for (int i = 0; i < 3; i++) {
      for (DriverMetadata driver : driverStore.list()) {
        if (driver.getDriverType() == DriverType.ISCSI) {
          sweepVolumId.add("volum" + driver.getVolumeId());
        }
      }


    }
    for (DriverMetadata driver : driverStore.list()) {
      driverStoreList.add("volum" + driver.getVolumeId());
    }
    Assert.assertTrue(sweepVolumId.containsAll(driverStoreList));
  }


  /**
   * xx.
   */
  public DriverMetadata buildIscsiDriver(long volumeId) {
    DriverMetadata driver = new DriverMetadata();
    driver.setDriverType(DriverType.ISCSI);
    driver.setVolumeId(volumeId);
    driver.setDriverStatus(DriverStatus.LAUNCHED);
    return driver;
  }

}
