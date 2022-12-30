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

import java.util.concurrent.Future;
import py.driver.DriverAction;
import py.driver.DriverStatus;
import py.drivercontainer.driver.version.Version;
import py.icshare.DriverKey;
import py.periodic.Worker;

/**
 * Interfaces definition for task life-cycle phases.
 * <li>Task Accepted</li>
 * <li>Task Submitted</li>
 * <li>Task Running</li>
 *
 */
public interface TaskExecutor extends Worker {

  /**
   * Submit the given task to the executor.
   *
   * @param driverKey    key of driver
   * @param futureAction action which is going to be added to responding driver
   * @param version      version of driver
   * @return instance of {@link Future} if the executor accepts the given task, or null.
   */
  public Future<?> submit(DriverKey driverKey, DriverAction futureAction, Runnable task,
      Version version);

  /**
   * Submit the given task to the executor.
   *
   * @param context task context
   * @return instance of {@link Future} if the executor accepts the given task, or null.
   */
  public Future<?> submit(TaskContext context);

  /**
   * Accept the given task and submit it to the executor.
   *
   * @param driverKey    key of driver
   * @param newStatus    accepting status (e.g with 'ING' appendix) of driver.
   * @param futureAction action which is going to be added to responding driver
   * @param version      version of driver
   * @return instance of {@link Future} if the executor accepts the given task, or null.
   */
  public Future<?> acceptAndSubmit(DriverKey driverKey, DriverStatus newStatus,
      DriverAction futureAction,
      Runnable task, Version version);

  /**
   * Accept the given task and submit it to the executor.
   *
   * @param context   task context
   * @param newStatus accepting status (e.g with 'ING' appendix) of driver.
   * @return instance of {@link Future} if the executor accepts the given task, or null.
   */
  public Future<?> acceptAndSubmit(TaskContext context, DriverStatus newStatus);
}
