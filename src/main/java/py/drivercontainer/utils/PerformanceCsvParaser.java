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
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.struct.Pair;
import py.thrift.share.GetPerformanceFromPyMetricsResponseThrift;

public class PerformanceCsvParaser {

  private static final Logger logger = LoggerFactory.getLogger(PerformanceCsvParaser.class);
  private static final String tmpDir = System.getProperty("java.io.tmpdir") + "/";
  private static final int leastLine = 10;
  private static final String readIOPSFileName = "readRequestCounter";
  private static final String readThroughputFileName = "readRequestTotalSize";
  private static final String writeIOPSFileName = "writeRequestCounter";
  private static final String writeThroughputFileName = "writeRequestTotalSize";
  private long dashboardLongestTimeSecond;

  public static void main(String[] args) {
    System.out.println(tmpDir);
  }

  private Pair<Long, Long> parseLine(String line) throws NumberFormatException {
    String[] timeAndValue = line.split(",");
    if (timeAndValue.length != 2) {
      throw new NumberFormatException();
    }
    Pair<Long, Long> timeValue = new Pair<>();
    timeValue.setFirst(Long.parseLong(timeAndValue[0]));
    timeValue.setSecond(Long.parseLong(timeAndValue[1]));
    return timeValue;
  }

  private int calculateMaxLines(List<String> lines) {
    if (lines.size() < leastLine) {
      return 0;
    }
    try {
      Pair<Long, Long> lessValue = parseLine(lines.get(1));
      Pair<Long, Long> largerValue = parseLine(lines.get(2));
      long interval = largerValue.getFirst() - lessValue.getFirst();
      int maxLines = (int) (dashboardLongestTimeSecond / interval + 1);
      return maxLines;
    } catch (NumberFormatException ne) {
      return 0;
    }
  }

  private Map<Long, Long> readCsv(Long volumeId, String partOfFileName, boolean iops) {
    String dir = tmpDir + volumeId;
    File dirFile = new File(dir);
    if (dirFile == null || !dirFile.isDirectory()) {
      logger.error("cannot read performance files {} doesn't exist or not a dir", dir);
      return null;
    }
    String[] files = dirFile.list();
    int matchCount = 0;
    String realFileName = null;
    if (files != null) {
      for (String filename : files) {
        if (filename.contains(partOfFileName)) {
          matchCount++;
          realFileName = filename;
        }
      }
    }
    if (matchCount > 1) {
      logger.error("there is more than 1 file match {}", partOfFileName);
      return null;
    }
    String filePath = dir + "/" + realFileName;

    FileChannel fileChannel = null;
    FileLock fileLock = null;
    RandomAccessFile randomAccesssFile = null;
    ArrayList<String> lines = new ArrayList<String>();
    ArrayList<Pair<Long, Long>> timeValueList = new ArrayList<>();

    try {
      randomAccesssFile = new RandomAccessFile(filePath, "rw");
      fileChannel = randomAccesssFile.getChannel();
      int times = 10;
      while (fileLock == null && times-- > 0) {
        fileLock = fileChannel.tryLock();
        if (fileLock == null) {
          Thread.sleep(50);
        }
      }
      String line;
      while ((line = randomAccesssFile.readLine()) != null) {
        lines.add(line);
      }

    } catch (Exception e) {
      logger.warn("can not read file: {} ", filePath, e);
    } finally {

      try {
        if (fileLock != null) {
          fileLock.release();
          fileLock.close();
        }

        if (fileChannel != null) {
          fileChannel.close();
        }

        if (randomAccesssFile != null) {
          randomAccesssFile.close();
        }
      } catch (IOException e) {
        logger.warn("close the fileLock failure");
      }

    }
    if (lines.size() > 0) {
      FileWriter fw = null;
      try {
        int maxLine = calculateMaxLines(lines);
        while (maxLine > 0 && lines.size() > maxLine) {
          lines.remove(1);
        }

        fw = new FileWriter(filePath);
        for (String linee : lines) {
          String s = linee + "\n";
          fw.write(s, 0, s.length());
        }
        fw.flush();
        fw.close();
      } catch (IOException e) {
        logger.error("cannot write file", e);
      } finally {
        if (fw != null) {
          try {
            fw.close();
          } catch (Exception e) {
            logger.error("fail to close file writer.");
          }
        }
      }
      lines.remove(0);
      for (String eachline : lines) {
        try {
          Pair<Long, Long> timeValue = parseLine(eachline);
          timeValueList.add(timeValue);
        } catch (NumberFormatException ne) {
          logger.error("bad line while reading csv {}", eachline);
        }
      }
    }

    Map<Long, Long> timeValueMap = new HashMap<Long, Long>();
    for (int i = 0; i < timeValueList.size() - 1; i++) {
      long preTime = timeValueList.get(i).getFirst();
      long curTime = timeValueList.get(i + 1).getFirst();
      long preCount = timeValueList.get(i).getSecond();
      long curCount = timeValueList.get(i + 1).getSecond();
      long averageValue = (curCount - preCount) / (curTime - preTime);
      if (averageValue < 0) {
        averageValue = 0;
      }
      if (!iops) {
        averageValue = averageValue / 1024;
      }
      timeValueMap.put(curTime, averageValue);
    }

    return timeValueMap;
  }

  private void mergeSameValue(Map<Long, Long> value1, Map<Long, Long> value2) {
    List<Long> keysToRemoveInValue1 = new ArrayList<Long>();
    List<Long> keysToRemoveInValue2 = new ArrayList<Long>();
    for (Long key : value1.keySet()) {
      if (value2.get(key) == null) {
        keysToRemoveInValue1.add(key);
      }
    }
    for (Long key : value2.keySet()) {
      if (value1.get(key) == null) {
        keysToRemoveInValue2.add(key);
      }
    }
    for (Long key : keysToRemoveInValue1) {
      value1.remove(key);
    }
    for (Long key : keysToRemoveInValue2) {
      value2.remove(key);
    }
  }


  /**
   * xx.
   */
  public GetPerformanceFromPyMetricsResponseThrift readPerformances(long volumeId) {
    final GetPerformanceFromPyMetricsResponseThrift response =
        new GetPerformanceFromPyMetricsResponseThrift();

    Map<Long, Long> readIops;
    Map<Long, Long> readThroughput;
    Map<Long, Long> writeIops;
    final Map<Long, Long> writeThroughput;

    readIops = readCsv(volumeId, readIOPSFileName, true);
    readThroughput = readCsv(volumeId, readThroughputFileName, false);
    writeIops = readCsv(volumeId, writeIOPSFileName, true);
    writeThroughput = readCsv(volumeId, writeThroughputFileName, false);

    mergeSameValue(readIops, writeIops);
    mergeSameValue(readThroughput, writeThroughput);

    response.setReadIoPs(readIops);
    response.setWriteIoPs(writeIops);
    response.setReadThroughput(readThroughput);
    response.setWriteThroughput(writeThroughput);

    return response;
  }

  public long getDashboardLongestTimeSecond() {
    return dashboardLongestTimeSecond;
  }

  public void setDashboardLongestTimeSecond(long dashboardLongestTimeSecond) {
    this.dashboardLongestTimeSecond = dashboardLongestTimeSecond;
  }

}
