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

package py.drivercontainer.driver.store;

import java.util.Map;
import py.drivercontainer.driver.version.Version;

/**
 * One type of driver with different version cannot be in same driver store and two types of driver
 * with same driver version could be in same driver store. That is each version of driver has its
 * own driver store.
 *
 * <p>This class is a table mapping version to driver store.
 *
 */
public interface DriverStoreManager extends Map<Version, DriverStore> {

}
