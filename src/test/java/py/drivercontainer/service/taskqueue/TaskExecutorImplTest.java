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

package py.drivercontainer.service.taskqueue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import py.driver.DriverAction;
import py.driver.DriverMetadata;
import py.driver.DriverType;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.driver.store.DriverMemoryStoreImpl;
import py.drivercontainer.driver.store.DriverStore;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.drivercontainer.driver.store.DriverStoreManagerImpl;
import py.drivercontainer.driver.version.Version;
import py.drivercontainer.driver.version.file.VersionImpl;
import py.icshare.DriverKey;
import py.test.TestBase;

/**
 * A class contains some tests for {@link TaskExecutorImpl}.
 *
 */
public class TaskExecutorImplTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(TaskExecutorImplTest.class);

  @Override
  public void init() throws Exception {
    super.init();

    super.setLogLevel(Level.DEBUG);
  }

  /**
   * In this test plan, it submits a task to executor and expect a true value.
   */
  @Test
  public void testRunningOneTask() throws Exception {
    final AtomicBoolean isSuccess = new AtomicBoolean(false);

    final Runnable task = new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          logger.error("Caught an exception", e);
          return;
        }

        isSuccess.set(true);
      }
    };

    DriverKey key = new DriverKey(0L, 0L, 0, DriverType.NBD);
    DriverMetadata driver = new DriverMetadata();
    driver.setDriverContainerId(key.getDriverContainerId());
    driver.setVolumeId(key.getVolumeId());
    driver.setSnapshotId(key.getSnapshotId());
    driver.setDriverType(key.getDriverType());

    DriverStore store = new DriverMemoryStoreImpl();
    store.save(driver);
    Version version = VersionImpl.get("2.3.0-internal-20170918000011");
    DriverStoreManager driverStoreManager = new DriverStoreManagerImpl();
    driverStoreManager.put(version, store);

    ExecutorService threadPool = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS,
        new SynchronousQueue<>());
    TaskExecutorImpl taskExecutorImpl = new TaskExecutorImpl(
        Mockito.mock(DriverContainerConfiguration.class),
        threadPool, driverStoreManager);
    Future<?> future = taskExecutorImpl.submit(key, DriverAction.START_SERVER, task, version);
    taskExecutorImpl.doWork();
    future.get();
    Assert.assertTrue(isSuccess.get());
  }

  @After
  public void cleanup() {
    super.setLogLevel(Level.WARN);
  }
}
