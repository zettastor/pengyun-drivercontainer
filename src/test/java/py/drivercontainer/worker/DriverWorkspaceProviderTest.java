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

package py.drivercontainer.worker;

import java.io.File;
import java.io.IOException;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import py.driver.DriverType;
import py.drivercontainer.driver.DriverWorkspaceProvider;
import py.icshare.DriverKey;
import py.test.TestBase;

public class DriverWorkspaceProviderTest extends TestBase {

  public static final String FILE_SEPARATOR = File.separator;
  private static final Logger logger = LoggerFactory.getLogger(DriverWorkspaceProviderTest.class);
  long volumeId;
  DriverType driverType;
  int snapShotId;
  DriverWorkspaceProvider driverWorkspaceProvider;
  String workingPath;
  DriverKey driverKey;


  /**
   * xx.
   */
  @Before
  public void init() {

    super.setLogLevel(Level.ALL);
    volumeId = 0L;
    driverType = DriverType.NBD;
    snapShotId = 0;
    driverWorkspaceProvider = new DriverWorkspaceProvider();
    driverKey = new DriverKey(0, volumeId, snapShotId, driverType);

  }

  @Test
  public void createWorkspaceTest() throws IOException {
    workingPath = driverWorkspaceProvider.createWorkspace("var", driverKey);
    File workdir = new File(workingPath);
    Assert.assertTrue(workdir.exists());
  }

  @Test
  public void deleteWorkspaceTest() {
    workingPath = driverWorkspaceProvider.createWorkspace("var", driverKey);
    driverWorkspaceProvider.deleteWorkspace(workingPath);
    File workdir = new File(workingPath);
    Assert.assertTrue(!workdir.exists());
  }


  /**
   * xx.
   */
  @After
  public void clearVar() throws IOException {
    File dir = new File(".");
    String varPath = null;
    varPath = dir.getCanonicalPath();
    varPath += (FILE_SEPARATOR + "var");
    String command = String.format("rm -rf %s ", varPath);
    Process process;
    try {
      process = Runtime.getRuntime().exec(command);
      process.waitFor();
    } catch (IOException e) {
      logger.error("caught exception", e);
    } catch (InterruptedException e) {
      logger.error("caught exception", e);
    }

  }


}
