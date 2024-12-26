

package py.drivercontainer.service;

import java.util.HashMap;
import java.util.Map;
import py.driver.DriverType;

public class PortContainerFactory {

  private Map<DriverType, PortContainer> driverType2PortContainer = new HashMap<>();

  public PortContainerFactory(Map<DriverType, PortContainer> driverType2PortContainer) {
    this.driverType2PortContainer = driverType2PortContainer;
  }


  /**
   * xx.
   */
  public PortContainer getPortContainer(DriverType driverType) {
    switch (driverType) {
      case ISCSI:
        return driverType2PortContainer.get(DriverType.NBD);
      case JSCSI:
        return driverType2PortContainer.get(DriverType.JSCSI);
      case NBD:
        return driverType2PortContainer.get(DriverType.NBD);
      case FSD:
        return driverType2PortContainer.get(DriverType.FSD);
      default:
        return null;
    }
  }
}
