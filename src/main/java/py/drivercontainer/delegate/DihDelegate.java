/*
 * Copyright (c) 2022. PengYunNetWork
 *
 * This program is free software: you can use, redistribute, and/or modify it
 * under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 *  You should have received a copy of the GNU Affero General Public License along with
 *  this program. If not, see <http://www.gnu.org/licenses/>.
 */

package py.drivercontainer.delegate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.struct.EndPoint;
import py.dih.client.DihClientFactory;
import py.dih.client.DihServiceBlockingClientWrapper;
import py.driver.DriverMetadata;
import py.drivercontainer.DriverContainerConfiguration;
import py.exception.GenericThriftClientFactoryException;
import py.instance.Instance;
import py.instance.InstanceStatus;
import py.thrift.share.InstanceHasFailedAleadyExceptionThrift;
import py.thrift.share.InstanceNotExistsExceptionThrift;

/**
 * Distributed instance-hub service delegate which wraps some common used method communicating with
 * DIH for driver-container.
 *
 */
public class DihDelegate {

  private static final Logger logger = LoggerFactory.getLogger(DihDelegate.class);

  private DriverContainerConfiguration dcConfig;

  private DihClientFactory dihClientFactory;

  private EndPoint localDihEndpoint;

  public DihDelegate() {
  }


  /**
   * xx.
   */
  public DihDelegate(DriverContainerConfiguration dcConfig, DihClientFactory dihClientFactory,
      EndPoint localDihEndpoint) {
    super();
    this.dcConfig = dcConfig;
    this.dihClientFactory = dihClientFactory;
    this.localDihEndpoint = localDihEndpoint;
  }

  /**
   * Get instances of the given driver server.
   *
   * <p>Since there is no instance ID of driver server in {@link DriverMetadata}, it is possible to
   * find more than one instances of driver server.
   *
   * @return a list of instances of driver server.
   */
  public List<Instance> getDriverServerInstance(DriverMetadata driver)
      throws InterruptedException, TimeoutException, TException, Exception {
    int retryRemaining = dcConfig.getThriftTransportRetries();
    DihServiceBlockingClientWrapper dihClientWrapper;
    Instance driverServer = null;
    EndPoint endpoint;
    List<Instance> instances;

    instances = new ArrayList<Instance>();
    endpoint = new EndPoint(driver.getHostName(), driver.getCoordinatorPort());
    while (retryRemaining-- > 0) {
      try {
        dihClientWrapper = dihClientFactory.build(localDihEndpoint);
      } catch (GenericThriftClientFactoryException e) {
        if (retryRemaining == 0) {
          throw e;
        }

        logger.warn("Unable to build thrift client to DIH service on {}", localDihEndpoint, e);
        Thread.sleep(TimeUnit.SECONDS.toMillis(dcConfig.getThriftTransportRetryIntervalSec()));
        continue;
      }

      try {
        driverServer = dihClientWrapper.getInstance(endpoint);
      } catch (TTransportException e) {
        if (retryRemaining == 0) {
          throw e;
        }

        logger.warn("Something wrong with connection to DIH service on {}", localDihEndpoint, e);
        Thread.sleep(TimeUnit.SECONDS.toMillis(dcConfig.getThriftTransportRetryIntervalSec()));
        continue;
      }

      if (driverServer != null) {
        instances.add(driverServer);
      }
    }

    return instances;
  }

  /**
   * Wait the given driver server degrade into the given status or lower than the given status. If
   * the given status is null value, then wait instance being removed from distributed.
   */
  public void waitDriverServerDegrade(DriverMetadata driver, InstanceStatus status)
      throws InterruptedException, TException, TimeoutException, Exception {
    if (status == InstanceStatus.HEALTHY || status == InstanceStatus.SUSPEND) {
      String errMsg = "Unhandled instance status " + status.name();
      logger.error("{}", errMsg);
      throw new IllegalArgumentException(errMsg);
    }

    int intervalSec = 2;
    int retryRemaining;
    List<Instance> driverServers;

    retryRemaining = (int) (dcConfig.getInstanceStatusTimerTimeoutSec() / intervalSec);
    while (retryRemaining-- > 0) {
      driverServers = getDriverServerInstance(driver);

      if (status == null && driverServers.isEmpty()) {
        return;
      }

      if (status != null) {
        boolean hasDegraded = true;

        for (Instance driverServer : driverServers) {
          if (driverServer.getStatus().getValue() < status.getValue()) {
            hasDegraded = false;
          }
        }

        if (hasDegraded) {
          return;
        }
      }

      if (retryRemaining == 0) {
        logger.error("Timeout for driver {} turning into target status {}", driver, status);
        throw new TimeoutException();
      }

      logger.info(
          "Wait for driver {} in distributed instance-hub becoming target status...Target status:"
              + " {}; Current status: {}", driver, status, driverServers);
      Thread.sleep(TimeUnit.SECONDS.toMillis(intervalSec));
    }
  }

  /**
   * Request distributed instance-hub to turn driver server into failed status.
   */
  public void turnDriverServerToFailed(DriverMetadata driver) throws InterruptedException,
      GenericThriftClientFactoryException, TTransportException, TException, Exception {
    int retryRemaining = dcConfig.getThriftTransportRetries();
    DihServiceBlockingClientWrapper dihClientWrapper;
    List<Instance> driverServers;

    while (retryRemaining-- > 0) {
      driverServers = getDriverServerInstance(driver);

      try {
        dihClientWrapper = dihClientFactory.build(localDihEndpoint);
      } catch (GenericThriftClientFactoryException e) {
        if (retryRemaining == 0) {
          throw e;
        }

        logger.warn("Unable to build thrift client to DIH service on {}", localDihEndpoint, e);
        Thread.sleep(TimeUnit.SECONDS.toMillis(dcConfig.getThriftTransportRetryIntervalSec()));
        continue;
      }

      try {
        for (Instance driverServer : driverServers) {
          try {
            dihClientWrapper.turnInstanceToFailed(driverServer.getId().getId());
          } catch (InstanceHasFailedAleadyExceptionThrift e) {
            logger.info("Instance for driver {} has already been turned to failed yet.",
                driverServer);
          } catch (InstanceNotExistsExceptionThrift e) {
            logger.info(
                "Instance for driver {} has already been removed from distributed instance-hub.",
                driverServer);
          }
        }
      } catch (TTransportException e) {
        if (retryRemaining == 0) {
          throw e;
        }

        logger.warn("Something wrong with connection to DIH service on {}", localDihEndpoint, e);
        Thread.sleep(TimeUnit.SECONDS.toMillis(dcConfig.getThriftTransportRetryIntervalSec()));
        continue;
      }
    }
  }

  /**
   * Remove driver server (e.g. Coordinator, FS Server) from distributed instance-hub.
   */
  public void removeDriverServer(DriverMetadata driver) throws InterruptedException,
      GenericThriftClientFactoryException, TTransportException, TException, TimeoutException,
      Exception {
    List<Instance> driverServers;

    driverServers = getDriverServerInstance(driver);
    if (driverServers.isEmpty()) {
      logger.warn("No such driver server {} in distributed instance-hub.", driver);
      return;
    }

    for (Instance driverServer : driverServers) {
      if (driverServer.getStatus() == InstanceStatus.HEALTHY
          || driverServer.getStatus() == InstanceStatus.SUSPEND) {
        String errMsg = "Illegal driver status " + driverServer.getStatus().name();
        logger.error("{}", errMsg);
        throw new IllegalArgumentException(errMsg);
      }
    }

    logger.warn("Turning driver server {} to failed in distributed instance-hub", driver);
    turnDriverServerToFailed(driver);

    waitDriverServerDegrade(driver, InstanceStatus.FAILED);
  }

  public DriverContainerConfiguration getDcConfig() {
    return dcConfig;
  }

  public void setDcConfig(DriverContainerConfiguration dcConfig) {
    this.dcConfig = dcConfig;
  }

  public DihClientFactory getDihClientFactory() {
    return dihClientFactory;
  }

  public void setDihClientFactory(DihClientFactory dihClientFactory) {
    this.dihClientFactory = dihClientFactory;
  }

  public EndPoint getLocalDihEndpoint() {
    return localDihEndpoint;
  }

  public void setLocalDihEndPoint(EndPoint localDihEndpoint) {
    this.localDihEndpoint = localDihEndpoint;
  }
}
