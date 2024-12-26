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

package py.drivercontainer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource({"classpath:config/fsd-jvm.properties"})
public class JvmConfigurationForFsd implements JvmConfiguration {

  @Value("${fsd.main.class:py.fs.service.Launcher}")
  private String mainClass = "py.fs.service.Launcher";

  @Value("${fsd.initial.mem.pool.size:2048m}")
  private String initialMemPoolSize = "2048m";

  @Value("${fsd.min.mem.pool.size:2048m}")
  private String minMemPoolSize = "2048m";

  @Value("${fsd.max.mem.pool.size:4096m}")
  private String maxMemPoolSize = "4096m";

  @Value("${fsd.max.gc.pause.ms:200}")
  private long maxGcPauseMillis = 200;

  @Value("${fsd.max.direct.memory.size:1G}")
  private String maxDirectMemorySize = "1G";

  @Value("${fsd.gc.pause.internal.ms:1000}")
  private long gcPauseIntervalMillis = 1000;

  @Value("${fsd.parallel.gc.threads:10}")
  private int parallelGcThreads = 10;

  @Value("${fsd.yourkit.agent.path:@null}")
  private String yourkitAgentPath = "";

  @Value("${fsd.jmx.base.port}")
  private int jmxBasePort;

  @Value("${fsd.jmx.enable:false}")
  private boolean jmxEnable = false;

  @Value("${fsd.initial.metaspace.size:20m}")
  private String initMetaSpaceSize = "20m";

  @Value("${fsd.g1.rs.updating.pause.time.percent:10}")
  private int g1rSetUpdatingPauseTimePercent = 10;

  @Value("${fsd.conc.gc.threads:0}")
  private int concGcThreads = 0;

  @Value("${fsd.netty.leak.detection.level:disabled}")
  private String nettyLeakDetectionLevel = "disabled";

  @Value("${fsd.netty.leak.detection.target.records:20}")
  private String nettyLeakDetectionTargetRecords = "20";

  @Value("${fsd.netty.allocator.maxOrder:11}")
  private int nettyAllocatorMaxOrder = 11;


  public String getMainClass() {
    return mainClass;
  }

  public void setMainClass(String mainClass) {
    this.mainClass = mainClass;
  }

  public String getInitialMemPoolSize() {
    return initialMemPoolSize;
  }

  public void setInitialMemPoolSize(String initialMemPoolSize) {
    this.initialMemPoolSize = initialMemPoolSize;
  }

  public String getMinMemPoolSize() {
    return minMemPoolSize;
  }

  public void setMinMemPoolSize(String minMemPoolSize) {
    this.minMemPoolSize = minMemPoolSize;
  }

  public String getMaxMemPoolSize() {
    return maxMemPoolSize;
  }

  public void setMaxMemPoolSize(String maxMemPoolSize) {
    this.maxMemPoolSize = maxMemPoolSize;
  }

  public long getMaxGcPauseMillis() {
    return maxGcPauseMillis;
  }

  public void setMaxGcPauseMillis(long maxGcPauseMillis) {
    this.maxGcPauseMillis = maxGcPauseMillis;
  }

  public String getMaxDirectMemorySize() {
    return maxDirectMemorySize;
  }

  public void setMaxDirectMemorySize(String maxDirectMemorySize) {
    this.maxDirectMemorySize = maxDirectMemorySize;
  }

  public long getGcPauseIntervalMillis() {
    return gcPauseIntervalMillis;
  }

  public void setGcPauseIntervalMillis(long gcPauseIntervalMillis) {
    this.gcPauseIntervalMillis = gcPauseIntervalMillis;
  }

  public int getParallelGcThreads() {
    return parallelGcThreads;
  }

  public void setParallelGcThreads(int parallelGcThreads) {
    this.parallelGcThreads = parallelGcThreads;
  }

  public String getYourkitAgentPath() {
    return yourkitAgentPath;
  }

  public void setYourkitAgentPath(String yourkitAgentPath) {
    this.yourkitAgentPath = yourkitAgentPath;
  }

  public int getJmxBasePort() {
    return jmxBasePort;
  }

  public void setJmxBasePort(int jmxBasePort) {
    this.jmxBasePort = jmxBasePort;
  }

  public boolean isJmxEnable() {
    return jmxEnable;
  }

  public void setJmxEnable(boolean jmxEnable) {
    this.jmxEnable = jmxEnable;
  }

  @Override
  public String getInitMetaSpaceSize() {
    return initMetaSpaceSize;
  }

  public int getG1rSetUpdatingPauseTimePercent() {
    return g1rSetUpdatingPauseTimePercent;
  }

  @Override
  public int getConcGcThreads() {
    return concGcThreads;
  }

  @Override
  public String getNettyLeakDetectionLevel() {
    return nettyLeakDetectionLevel;
  }

  public void setNettyLeakDetectionLevel(String nettyLeakDetectionLevel) {
    this.nettyLeakDetectionLevel = nettyLeakDetectionLevel;
  }

  @Override
  public String getNettyLeakDetectionTargetRecords() {
    return nettyLeakDetectionTargetRecords;
  }

  public void setNettyLeakDetectionTargetRecords(String nettyLeakDetectionTargetRecords) {
    this.nettyLeakDetectionTargetRecords = nettyLeakDetectionTargetRecords;
  }

  @Override
  public int getNettyAllocatorMaxOrder() {
    return nettyAllocatorMaxOrder;
  }

}
