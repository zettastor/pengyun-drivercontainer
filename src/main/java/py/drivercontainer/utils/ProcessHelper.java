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

package py.drivercontainer.utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProcessHelper {

  private static final Logger logger = LoggerFactory.getLogger(ProcessHelper.class);


  /**
   * xx.
   */
  public Process startNewJavaProcess(final String mainClass, final String[] arguments)
      throws IOException {
    ProcessBuilder processBuilder = createProcess(mainClass, arguments);
    Process process = processBuilder.start();
    return process;
  }

  private ProcessBuilder createProcess(final String mainClass, final String[] arguments) {
    String jvm = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
    String classpath = System.getProperty("java.class.path");
    logger.debug("jvm is at {}", jvm);

    List<String> jvmArgs = new ArrayList<String>();
    jvmArgs.add("-cp");
    jvmArgs.add(classpath);
    jvmArgs.add(mainClass);
    jvmArgs.addAll(Arrays.asList(arguments));

    logger.debug("The jvm arguments are {}", StringUtils.join(jvmArgs, " "));
    ProcessBuilder processBuilder =
        new ProcessBuilder(jvm, StringUtils.join(jvmArgs, " "));
    return processBuilder;
  }
}
