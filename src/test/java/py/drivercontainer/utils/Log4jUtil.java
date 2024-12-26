

package py.drivercontainer.utils;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Log4jUtil {

  private static final Logger logger = LoggerFactory
      .getLogger(Log4jUtil.class);

  public static void main(String[] args) {
    LogManager.getRootLogger().setLevel(Level.DEBUG);
    logger.debug("hello world!");
  }
}
