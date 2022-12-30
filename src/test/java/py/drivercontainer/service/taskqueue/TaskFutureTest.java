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
