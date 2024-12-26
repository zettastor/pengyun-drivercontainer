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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.test.TestBase;

/**
 * A class contains some tests for {@link TaskFuture}.
 *
 */
public class TaskFutureTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(TaskFutureTest.class);

  @Override
  public void init() throws Exception {
    super.init();
  }

  /**
   * In this test plan, we expect the task future could synchronize two threads.
   */
  @Test
  public void testFutureGet() throws Exception {
    final TaskFuture future = new TaskFuture();
    final AtomicBoolean isSuccess = new AtomicBoolean(false);

    Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          future.get();
        } catch (InterruptedException | ExecutionException e) {
          logger.error("Caught an exception", e);
          return;
        }

        isSuccess.set(true);
      }
    };
    thread.start();

    Thread.sleep(2000);
    // Although after 2 second, it is expected that thread still in stuck without future set
    // submitted.
    Assert.assertFalse(isSuccess.get());
    Assert.assertFalse(future.isDone());

    Future<?> newFuture = Mockito.mock(Future.class);
    Mockito.when(newFuture.isDone()).thenReturn(true);

    future.setSubmittedToThreadPool(newFuture);
    thread.join();
    Assert.assertTrue(isSuccess.get());
    Assert.assertTrue(future.isDone());
  }
}
