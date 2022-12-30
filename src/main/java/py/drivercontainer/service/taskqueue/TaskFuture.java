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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang.NotImplementedException;

public class TaskFuture implements Future<Void> {

  private final CountDownLatch submitLatch = new CountDownLatch(1);

  private final AtomicReference<Future<?>> future = new AtomicReference<Future<?>>(null);

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    throw new NotImplementedException();
  }

  @Override
  public boolean isCancelled() {
    throw new NotImplementedException();
  }

  @Override
  public boolean isDone() {
    return (future.get() != null) && (future.get().isDone());
  }

  @Override
  public Void get() throws InterruptedException, ExecutionException {
    submitLatch.await();
    future.get().get();
    return null;
  }

  @Override
  public Void get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    final long startTime = System.currentTimeMillis();
    boolean isInTime;

    isInTime = submitLatch.await(timeout, unit);
    if (!isInTime) {
      throw new TimeoutException("Timeout: " + timeout + ", Unit: " + unit.name());
    }

    long currentTime = System.currentTimeMillis();
    timeout = unit.toMillis(timeout) - (currentTime - startTime);
    future.get().get(timeout, TimeUnit.MILLISECONDS);

    return null;
  }

  public void setSubmittedToThreadPool(Future<?> future) {
    this.future.set(future);
    submitLatch.countDown();
  }
}
