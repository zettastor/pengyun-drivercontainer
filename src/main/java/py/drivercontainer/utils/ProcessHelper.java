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
