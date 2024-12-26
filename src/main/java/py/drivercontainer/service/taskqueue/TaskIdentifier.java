
package py.drivercontainer.service.taskqueue;

import py.driver.DriverAction;
import py.drivercontainer.driver.version.Version;
import py.icshare.DriverKey;

public interface TaskIdentifier {

  public DriverKey getDriverKey();

  public DriverAction getAction();

  public Version getVersion();
}
