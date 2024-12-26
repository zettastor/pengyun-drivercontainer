

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
