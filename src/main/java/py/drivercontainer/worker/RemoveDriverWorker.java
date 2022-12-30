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

package py.drivercontainer.worker;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.IscsiTargetNameBuilder;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.coordinator.IscsiTargetManager;
import py.coordinator.lio.LioNameBuilder;
import py.driver.DriverAction;
import py.driver.DriverMetadata;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.client.CoordinatorClientFactory;
import py.drivercontainer.delegate.DihDelegate;
import py.drivercontainer.driver.store.DriverStore;
import py.drivercontainer.driver.workspace.DriverWorkspaceManager;
import py.drivercontainer.driver.workspace.DriverWorkspaceManagerImpl;
import py.drivercontainer.service.PortContainer;
import py.drivercontainer.service.PortContainerFactory;
import py.drivercontainer.utils.DriverContainerUtils;
import py.exception.GenericThriftClientFactoryException;
import py.icshare.DriverKey;
import py.infocenter.client.InformationCenterClientFactory;
import py.infocenter.client.InformationCenterClientWrapper;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.periodic.Worker;
import py.thrift.coordinator.service.Coordinator;
import py.thrift.coordinator.service.ShutdownRequest;
import py.thrift.share.CancelDriversRulesRequest;
import py.thrift.share.DriverKeyThrift;
import py.thrift.share.DriverTypeThrift;
import py.thrift.share.ServiceHavingBeenShutdownThrift;

/**
 * A class as worker to remove existing launched driver.
 *
 */
public class RemoveDriverWorker implements Worker, Runnable {

  private static final Logger logger = LoggerFactory.getLogger(RemoveDriverWorker.class);

  private final DriverKey driverKey;
  InformationCenterClientWrapper icClientWrapper = null;
  private DriverStore driverStore;
  private IscsiTargetManager iscsiTargetManager;
  private DriverContainerConfiguration driverContainerConfig;
  private PortContainerFactory portContainerFactory;
  private LioNameBuilder lioNameBuilder;
  private DihDelegate dihDelegate;
  private InformationCenterClientFactory infoCenterClientFactory;
  private CoordinatorClientFactory coordinatorClientFactory;

  //if driver is upgrade ,set upgrade as true,reomve it just need to shutdown coordinator ,should
  // not remove iscsiTarget
  private boolean upgrade;

  public RemoveDriverWorker(DriverKey driverKey) {
    this.driverKey = driverKey;
  }

  @Override
  public void run() {
    try {
      doWork();
    } catch (Exception e) {
      logger.error("Caught an exception", e);
    } finally {
      DriverMetadata driver = driverStore.get(driverKey);
      if (driver != null) {
        driver.removeAction(DriverAction.REMOVE);
      }
    }
  }

  // TODO: all exception happened in this process was not handled yet.
  @Override
  public void doWork() {
    DriverMetadata driver;
    driver = driverStore.get(driverKey);
    if (driver == null) {
      logger.warn("No such driver with volume id {}", driverKey.getVolumeId());
      return;
    }
    if (icClientWrapper == null) {
      try {
        icClientWrapper = infoCenterClientFactory.build();
      } catch (Exception e) {
        logger.warn("catch an exception when build infocenter client {}", e);
        return;
      }
    }

    EndPoint endpoint;

    try {
      switch (driver.getDriverType()) {
        case NBD:
          endpoint = new EndPoint(driver.getHostName(), driver.getCoordinatorPort());
          logger.info("umount driver endpoint is :{}", endpoint);
          // shutdown coordinator with not-graceful flag. Since coordinator will return
          // unrecognized handler to PYD and make upper layer application being broken in graceful
          // shutdown period, it is suggested that shutdown coordinator with not-graceful flag.
          shutdownCoordinator(endpoint, false);
          break;
        case ISCSI:
          // When driver is in upgrading ,the upgrade will be set true ,it means remove the old
          // driver,just stop coordinator process ,shold not remove iscsi target
          if (!upgrade) {
            try {
              if (!processRemovingIscsiTarget(driver.getNbdDevice(),
                  driver.getVolumeId(true),
                  driver.getSnapshotId())) {
                // TODO: We should think about next status when timeout removing driver.
                // Maybe 'umount_error' is a good choice.
                logger.error("Something wrong when removing iscsi target");
                return;
              }

              // cancel the relationship of driverKeys
              CancelDriversRulesRequest cancelDriversRulesRequest = new CancelDriversRulesRequest();
              cancelDriversRulesRequest.setRequestId(RequestIdBuilder.get());
              cancelDriversRulesRequest.setDriverKey(
                  new DriverKeyThrift(driverKey.getDriverContainerId(), driverKey.getVolumeId(),
                      driverKey.getSnapshotId(),
                      DriverTypeThrift.valueOf(driverKey.getDriverType().name())));

              try {
                icClientWrapper.getClient().cancelDriversRules(cancelDriversRulesRequest);
              } catch (Exception e) {
                logger.warn("exception happened {}", e);
                try {
                  InformationCenterClientWrapper icClientWrapperNew = infoCenterClientFactory
                      .build();
                  if (icClientWrapperNew.equals(icClientWrapper)) {
                    return;
                  }
                  logger.warn("info center has switched from {} to {}", icClientWrapper.toString(),
                      icClientWrapperNew.toString());
                  icClientWrapper = icClientWrapperNew;
                } catch (Exception e1) {
                  logger.warn("catch an exception when build infocenter client {}", e1);
                  return;
                }

                try {
                  icClientWrapper.getClient().cancelDriversRules(cancelDriversRulesRequest);
                } catch (Exception e2) {
                  logger.warn("exception happened {}", e2);
                  return;
                }
              }

            } catch (Exception e) {
              logger.warn("Catch an exception when processRemovingISCSITarget {}", e);
            }
          }
          endpoint = new EndPoint(driver.getHostName(), driver.getCoordinatorPort());
          logger.info("umount endpoint in :{} is :{}", driver, endpoint);
          // shutdown coordinator with not-graceful flag. Since coordinator will return
          // unrecognized handler to PYD and make upper layer application being broken in graceful
          // shutdown period, it is suggested that shutdown coordinator with not-graceful flag.
          shutdownCoordinator(endpoint, false);
          break;
        default:
          logger.error("Invalid driver type: {}", driver.getDriverType().name());
          throw new IllegalStateException("Invalid driver type.");
      }
    } catch (TException e) {
      logger.error("Caught an internal error", e);
      throw new RuntimeException("Internal error!", e);
    }
    logger.warn("Wait for driver service stopping ...");

    int checkIntervalSec = 2;
    int retryRemaining;

    retryRemaining = driverContainerConfig.getDriverShutdownTimeoutSec() / checkIntervalSec;
    if (retryRemaining == 0) {
      retryRemaining = 1;
    }

    while (retryRemaining-- > 0) {
      logger.warn("driver process is :{}", driver.getProcessId());
      if (!DriverContainerUtils.processFound(driver.getProcessId(), driver.getCoordinatorPort())) {
        logger.warn("Successfully shutdown driver :{} service.", driver);
        break;
      }

      try {
        Thread.sleep(TimeUnit.SECONDS.toMillis(checkIntervalSec));
      } catch (InterruptedException e) {
        logger.warn("Interval was interrupted!");
      }
    }

    // We should think about next status when timeout removing driver. Maybe 'umount_error' is a
    // good choice.
    if (retryRemaining <= 0) {
      try {
        if (DriverContainerUtils
            .isProcessExist(driver.getProcessId(), driver.getCoordinatorPort())) {
          logger.error("Timeout removing driver {}", driver.getDriverType().name());
          return;
        }
      } catch (Exception e) {
        logger.error("isProcessExist exception {}", e);
      }
    }
    DriverWorkspaceManager driverWorkspaceManager = new DriverWorkspaceManagerImpl(
        new InstanceId(driver.getDriverContainerId()),
        driverContainerConfig.getDriverAbsoluteRootPath(),
        driverContainerConfig.getLibraryRootPath());
    if (!driverContainerConfig.isDriverWorkspaceKeep()) {
      try {
        driverWorkspaceManager.deleteWorkspace(driverStore.getVersion(), driverKey);
      } catch (IOException e) {
        logger.error("Caught an exception", e);
      }
    }

    /*
     * After removing driver, it is necessary to remove driver server instance from distributed
     *  instance-hub. Or it will be displayed on web and will be weird. Any exception occurred
     *  during the procedure should not cause failure to remove driver. The effort to complete this
     *  is to show kind view of the driver to user.
     */
    try {
      dihDelegate.waitDriverServerDegrade(driver, InstanceStatus.SICK);
      dihDelegate.removeDriverServer(driver);
    } catch (Exception e) {
      // It is supposed that any failure removing driver instance should not cause removing driver
      // failure. Just log an error but not throw out.
      logger.error(
          "Caught an exception removing server instance for driver {} from distributed"
              + " instance-hub", driver, e);
    }

    synchronized (driverStore) {
      driverStore.remove(driverKey);
    }
    // re-collection driver port
    PortContainer portContainer = portContainerFactory.getPortContainer(driver.getDriverType());
    portContainer.addAvailablePort(driver.getPort());

    // remove csv backup files
    FileUtils.deleteQuietly(Paths.get("/tmp", Long.toString(driverKey.getVolumeId())).toFile());
  }

  /**
   * Remove iscsi target and some configuration for iscsi target.
   *
   */
  public boolean processRemovingIscsiTarget(String nbdDevice, long volumeId, int snapShotId) throws
      Exception {
    String targetName = null;
    targetName = lioNameBuilder.buildLioWwn(volumeId, driverKey.getSnapshotId());
    logger.info("Going to delete target {} for iscsi driver", targetName);

    if (!iscsiTargetManager.deleteTarget(targetName, nbdDevice, volumeId, snapShotId)) {
      String failMsg = "Failed to delete iscsi target" + targetName;
      logger.error(failMsg);
      return false;
    }

    Process processLio = Runtime.getRuntime()
        .exec("/usr/bin/targetctl restore /etc/target/saveconfig.json");
    processLio.waitFor();

    if (driverContainerConfig.getCreateTwoIscsiTargetSwitch()) {
      String secondTargetName = IscsiTargetNameBuilder
          .buildSecondIqnName(volumeId, driverKey.getSnapshotId());
      logger.debug("Going to delete second target {} for iscsi driver", secondTargetName);
      if (!iscsiTargetManager.deleteTarget(secondTargetName, nbdDevice, volumeId, snapShotId)) {
        String failMsg = "Failed to delete iscsi target" + secondTargetName;
        logger.error(failMsg);
        return false;
      }
    }

    return true;
  }

  /**
   * Send 'shutdown' request to coordinator service.
   *
   * @param endpoint for coordinator service.
   * @throws TException if internal error was caught from remote.
   */
  public void shutdownCoordinator(EndPoint endpoint, boolean graceful) throws TException {
    logger.warn("going to shutdown coordinator for :{}", endpoint);
    Coordinator.Iface coordClient = null;
    int retryRemaining = driverContainerConfig.getThriftTransportRetries();

    while (retryRemaining-- > 0) {
      try {
        coordClient = coordinatorClientFactory
            .build(endpoint, driverContainerConfig.getThriftClientTimeout())
            .getClient();
        ShutdownRequest request = new ShutdownRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setGraceful(graceful);
        coordClient.shutdown(request);
      } catch (GenericThriftClientFactoryException | TTransportException e) {
        logger.warn("Unable to connect coordinator service: ", e);
        return; ///when can't connect to coordinator and think it as dead.
      } catch (ServiceHavingBeenShutdownThrift e) {
        logger.warn("Service was already shutdown!");
        return;
      } catch (TException e) {
        logger.error("TException happens.");
        //when can't connect to coordinator and think it as dead.
        return;
      }
    }

    if (retryRemaining <= 0) {
      logger.info(
          "After some retries, still cannot connect to Coordinator service, maybe it was being"
              + " shutdown before.");
    }
  }

  public DriverContainerConfiguration getDriverContainerConfig() {
    return driverContainerConfig;
  }

  public void setDriverContainerConfig(DriverContainerConfiguration driverContainerConfig) {
    this.driverContainerConfig = driverContainerConfig;
  }

  public PortContainerFactory getPortContainerFactory() {
    return portContainerFactory;
  }

  public void setPortContainerFactory(PortContainerFactory portContainerFactory) {
    this.portContainerFactory = portContainerFactory;
  }

  public IscsiTargetManager getIscsiTargetManager() {
    return iscsiTargetManager;
  }

  public void setIscsiTargetManager(IscsiTargetManager iscsiTargetManager) {
    this.iscsiTargetManager = iscsiTargetManager;
  }

  public LioNameBuilder getLioNameBuilder() {
    return lioNameBuilder;
  }

  public void setLioNameBuilder(LioNameBuilder lioNameBuilder) {
    this.lioNameBuilder = lioNameBuilder;
  }

  public DihDelegate getDihDelegate() {
    return dihDelegate;
  }

  public void setDihDelegate(DihDelegate dihDelegate) {
    this.dihDelegate = dihDelegate;
  }

  public boolean isUpgrade() {
    return upgrade;
  }

  public void setUpgrade(boolean upgrade) {
    this.upgrade = upgrade;
  }

  public DriverStore getDriverStore() {
    return driverStore;
  }

  public void setDriverStore(DriverStore driverStore) {
    this.driverStore = driverStore;
  }

  public InformationCenterClientFactory getInfoCenterClientFactory() {
    return infoCenterClientFactory;
  }

  public void setInfoCenterClientFactory(InformationCenterClientFactory infoCenterClientFactory) {
    this.infoCenterClientFactory = infoCenterClientFactory;
  }

  public CoordinatorClientFactory getCoordinatorClientFactory() {
    return coordinatorClientFactory;
  }

  public void setCoordinatorClientFactory(CoordinatorClientFactory coordinatorClientFactory) {
    this.coordinatorClientFactory = coordinatorClientFactory;
  }
}
