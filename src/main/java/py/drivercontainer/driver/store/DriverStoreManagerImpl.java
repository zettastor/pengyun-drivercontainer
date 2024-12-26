

package py.drivercontainer.driver.store;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import py.drivercontainer.driver.version.Version;

/**
 * An implementation of {@link DriverStoreManager}. This is actually an alias of {@link HashMap}.
 *
 */
public class DriverStoreManagerImpl extends ConcurrentHashMap<Version, DriverStore> implements
    DriverStoreManager {

  private static final long serialVersionUID = 1L;
}
