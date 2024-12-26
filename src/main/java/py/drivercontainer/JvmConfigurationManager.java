

package py.drivercontainer;

import py.driver.DriverType;

public class JvmConfigurationManager {

  private JvmConfigurationForDriver coordinatorJvmConfig;



  /**
   * xx.
   */
  public JvmConfiguration getJvmConfig(DriverType driverType) {
    switch (driverType) {
      case NBD:
      case ISCSI:
        return coordinatorJvmConfig;
      default:
        throw new IllegalArgumentException("Invalid driver type " + driverType.name());
    }
  }

  public JvmConfigurationForDriver getCoordinatorJvmConfig() {
    return coordinatorJvmConfig;
  }

  public void setCoordinatorJvmConfig(JvmConfigurationForDriver coordinatorJvmConfig) {
    this.coordinatorJvmConfig = coordinatorJvmConfig;
  }
}
