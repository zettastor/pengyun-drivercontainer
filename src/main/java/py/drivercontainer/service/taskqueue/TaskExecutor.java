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
