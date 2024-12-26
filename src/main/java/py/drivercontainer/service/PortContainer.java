

package py.drivercontainer.service;

import py.drivercontainer.exception.NoAvailablePortException;

public interface PortContainer {

  public int getAvailablePort() throws NoAvailablePortException;

  public int getAvailableCoordinatorPort(int port) throws NoAvailablePortException;

  public int getAvailableJmxPort(int port) throws NoAvailablePortException;

  public void addAvailablePort(int port);

  public void removeUnavailablePort(int port);

  public boolean containes(int port);
}
