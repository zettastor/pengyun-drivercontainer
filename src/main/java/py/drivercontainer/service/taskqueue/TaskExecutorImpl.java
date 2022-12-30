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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.driver.DriverAction;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.driver.store.DriverStore;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.drivercontainer.driver.version.Version;
import py.icshare.DriverKey;

/**
 * Action on driver which will spend some time (e.g. {@link DriverAction#START_SERVER}, {@link
 * DriverAction#REMOVE} ...) is suggested to be submitted to this instance. And it will dispatch the
 * tasks to some thread.
 *
 */
public class TaskExecutorImpl implements TaskExecutor {

  private static final Logger logger = LoggerFactory.getLogger(TaskExecutorImpl.class);

  private final DriverContainerConfiguration dcConfig;
  private final ExecutorService taskThreadPool;
  // private final DriverStore driverStore;
  private final DriverStoreManager driverStoreManager;

  /**
   * The field will be accessed by multiple threads invoking {@link TaskExecutorImpl#doWork()} and
   * task submission relative methods (e.g {@link TaskExecutorImpl#submit(TaskContext)}). To keep it
   * thread-safe, use keyword 'synchronized' to enclose implementation codes.
   */
  private final List<TaskContext> taskList = new ArrayList<>();

  /**
   * The field will be accessed by multiple threads invoking {@link TaskExecutorImpl#doWork()} and
   * task submission relative methods (e.g {@link TaskExecutorImpl#submit(TaskContext)}). To keep it
   * thread-safe, use keyword 'synchronized' to enclose implementation codes.
   */
  private final Map<TaskIdentifier, TaskFuture> futureTable = new HashMap<>();


  /**
   * xx.
   */
  public TaskExecutorImpl(DriverContainerConfiguration dcConfig, ExecutorService taskThreadPool,
      DriverStoreManager driverStoreManager) {
    super();
    this.dcConfig = dcConfig;
    this.taskThreadPool = taskThreadPool;
    this.driverStoreManager = driverStoreManager;

  }


  /**
   * xx.
   */
  public Future<?> submit(DriverKey driverKey, DriverAction futureAction, Runnable task,
      Version version) {
    TaskIdentifier identifier;
    TaskContext context;

    identifier = new TaskIdentifierImpl(driverKey, futureAction, version);
    context = new TaskContextImpl(identifier, task);
    return submit(context);
  }

  /**
   * The reason use keyword 'synchronized' here could be found in comments of {@link
   * TaskExecutorImpl#taskList} and {@link TaskExecutorImpl#futureTable}.
   */
  public synchronized Future<?> submit(TaskContext task) {
    boolean existSameActionOnDriver = futureTable.get(task.getIdentifier()) != null;
    if (existSameActionOnDriver) {
      logger
          .warn("Already exist an action {} on driver {}", task.getIdentifier().getAction().name(),
              task.getIdentifier().getDriverKey());
      return null;
    }

    taskList.add(task);

    TaskFuture future = new TaskFuture();
    futureTable.put(task.getIdentifier(), future);
    return future;
  }

  @Override
  public Future<?> acceptAndSubmit(DriverKey driverKey, DriverStatus newStatus,
      DriverAction futureAction,
      Runnable task, Version version) {
    TaskIdentifier identifier;
    TaskContext context;

    identifier = new TaskIdentifierImpl(driverKey, futureAction, version);
    context = new TaskContextImpl(identifier, task);
    return acceptAndSubmit(context, newStatus);
  }

  @Override
  public Future<?> acceptAndSubmit(TaskContext context, DriverStatus newStatus) {
    Version version;
    DriverKey driverKey;
    DriverAction driverAction;
    final DriverStore driverStore;

    version = context.getIdentifier().getVersion();
    driverKey = context.getIdentifier().getDriverKey();
    driverAction = context.getIdentifier().getAction();
    driverStore = driverStoreManager.get(version);
    DriverMetadata driver;
    driver = driverStore.get(driverKey);
    if (driver.setDriverStatusIfValid(newStatus, driverAction) != DriverStatus.UNKNOWN) {
      driverStore.save(driver);
      return submit(context);
    } else {
      logger.warn("Failed to accept task: {}! Driver key: {}; Status: {}; Adding actions: {}",
          context.getIdentifier(), driverKey, driver.getDriverStatus(),
          driver.listAndCopyActions());
      return null;
    }
  }

  /**
   * The reason use keyword 'synchronized' here could be found in comments of {@link
   * TaskExecutorImpl#taskList} and {@link TaskExecutorImpl#futureTable}.
   */
  @Override
  public synchronized void doWork() throws Exception {
    for (int i = 0; i < 6; i++) {
      //When driver has conflict action,the task will be skipped wait for next thread schedule,but
      // the new thread will probably conflict check server and target thread ,so we use for cycle
      // to execute current task multi times.
      long sleepTime = (long) (10 * Math.pow(2, i));
      Thread.sleep(sleepTime);
      Iterator<TaskContext> contextIter = taskList.iterator();
      while (contextIter.hasNext()) {
        TaskContext context;
        DriverKey driverKey;
        DriverAction futureAction;
        context = contextIter.next();
        driverKey = context.getIdentifier().getDriverKey();
        futureAction = context.getIdentifier().getAction();

        Version version = context.getIdentifier().getVersion();
        DriverMetadata driver;
        driver = driverStoreManager.get(version).get(driverKey);
        // Since it is possible to remove a driver under recover, there could both recovering
        // context and removing context in executor. After the driver removed, the recovering
        // context (start server or create target)is still there.
        if (driver == null) {
          futureTable.remove(context.getIdentifier());
          contextIter.remove();
          continue;
        }
        if (driver.hasConflictActionWith(futureAction)) {
          logger.info(
              "Skip applying action {} on driver {} for that already exists a conflict actions {}"
                  + " on it.", futureAction.name(), driverKey, driver.listAndCopyActions());
          continue;
        }

        Future<?> newFuture;

        try {
          newFuture = taskThreadPool.submit(context.getTask());
        } catch (RejectedExecutionException e) {
          logger.warn("Thread pool is busy!");
          return;
        }

        logger.info("Applying action {} on driver {} ...", futureAction, driverKey);
        driver.addAction(futureAction);
        driverStoreManager.get(version).save(driver);
        TaskFuture taskFuture = futureTable.remove(context.getIdentifier());
        taskFuture.setSubmittedToThreadPool(newFuture);

        contextIter.remove();
      }
    }
  }
}
