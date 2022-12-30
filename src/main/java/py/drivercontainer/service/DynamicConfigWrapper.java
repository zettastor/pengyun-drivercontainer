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

package py.drivercontainer.service;

import java.util.Map;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jboss.netty.util.internal.ConcurrentHashMap;

@Deprecated
public class DynamicConfigWrapper {

  private static final Logger logger = Logger.getLogger(DynamicConfigWrapper.class);
  private Map<String, String> dynamicParameters;

  private DynamicConfigWrapper() {
    dynamicParameters = new ConcurrentHashMap<String, String>();
    // initialize this class by spring configuration files
    dynamicParameters.put("log.level", "DEBUG");
  }

  public static DynamicConfigWrapper getInstance() {
    return LazyHolder.singletonInstance;
  }

  public Map<String, String> getDynamicParameters() {
    return dynamicParameters;
  }


  /**
   * xx.
   */
  public void setParammeter(String name, String value) {
    // store the change of this parameter.
    dynamicParameters.put(name, value);
    // apply the change
    if (name.equals("log.level")) {
      logger.debug("leve change to " + value);
      LogManager.getRootLogger().setLevel(Level.toLevel(value));
    } else {
      logger.debug("can't set this parameter " + name);
    }
  }

  private static class LazyHolder {

    private static final DynamicConfigWrapper singletonInstance = new DynamicConfigWrapper();
  }
}
