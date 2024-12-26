

package py.drivercontainer.driver.store.exception;

/**
 * Signal that a driver with same key and different reference is saved to driver store.
 *
 */
public class DuplicatedDriverException extends IllegalStateException {

  private static final long serialVersionUID = 1L;

  public DuplicatedDriverException() {
    super();
  }

  public DuplicatedDriverException(String s) {
    super(s);
  }

  public DuplicatedDriverException(String message, Throwable cause) {
    super(message, cause);
  }

  public DuplicatedDriverException(Throwable cause) {
    super(cause);
  }
}
