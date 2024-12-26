
package py.drivercontainer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import py.driver.DriverType;
import py.drivercontainer.driver.version.Version;

/**
 * This class use to save currentVersion ,latestVersion and isOnMigration value ,avoid to the other
 * process read version file too frequentlly.
 */
public class DriverVersion {

  public static Map<DriverType, Version> currentVersion = new ConcurrentHashMap<>();
  public static Map<DriverType, Version> latestVersion = new ConcurrentHashMap<>();
  public static Map<DriverType, Boolean> isOnMigration = new ConcurrentHashMap<>();

}
