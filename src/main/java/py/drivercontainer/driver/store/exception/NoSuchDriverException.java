

package py.drivercontainer.driver.store.exception;

import java.util.NoSuchElementException;

/**
 * Thrown by various accessor methods to indicate that the driver being requested does not exist.
 *
 */
public class NoSuchDriverException extends NoSuchElementException {

  /**
   * xx.
   */
  private static final long serialVersionUID = 1L;

  public NoSuchDriverException() {
    super();
  }

  public NoSuchDriverException(String s) {
    super(s);
  }
}
