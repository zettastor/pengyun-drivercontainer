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
