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

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import py.app.context.AppContext;
import py.test.TestBase;
import py.thrift.share.ChangeDriverBoundVolumeRequest;

public class ChangeDriverBoundVolumeTest extends TestBase {

  private DriverContainerImpl dcImpl;
  @Mock
  private AppContext appContext;

  @Before
  public void init() throws Exception {
    dcImpl = new DriverContainerImpl(appContext);
    super.init();
  }


  @Test
  public void testChangeDriverBindVolume() {
    ChangeDriverBoundVolumeRequest changeDriverBoundVolumeRequest =
        new ChangeDriverBoundVolumeRequest();

  }
}
