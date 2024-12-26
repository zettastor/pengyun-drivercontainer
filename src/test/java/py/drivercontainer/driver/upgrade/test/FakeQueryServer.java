

package py.drivercontainer.driver.upgrade.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.drivercontainer.driver.upgrade.QueryServer;

public class FakeQueryServer extends QueryServer {

  private static final Logger logger = LoggerFactory.getLogger(FakeQueryServer.class);

  private byte delta;

  public FakeQueryServer() throws Exception {
    super();
    start();
  }

}
