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
import java.util.concurrent.TimeoutException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.dih.client.DihClientFactory;
import py.dih.client.DihServiceBlockingClientWrapper;
import py.driver.DriverMetadata;
import py.drivercontainer.DriverContainerConfiguration;
import py.exception.GenericThriftClientFactoryException;
import py.instance.Group;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.test.TestBase;

/**
 * A class contains some tests for {@link DihDelegate}.
 *
 */
public class DihDelegateTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(DihDelegateTest.class);

  private DriverContainerConfiguration dcConfig;

  @Mock
  private DihClientFactory dihClientFactory;

  @Mock
  private DihServiceBlockingClientWrapper dihClientWrapper;

  private DihDelegate dihDelegate;

  @Override
  public void init() throws Exception {
    super.init();

    dcConfig = new DriverContainerConfiguration();
    dcConfig.setThriftTransportRetries(2);
    dcConfig.setInstanceStatusTimerTimeoutSec(4);

    dihDelegate = new DihDelegate();
    dihDelegate.setDcConfig(dcConfig);
    dihDelegate.setDihClientFactory(dihClientFactory);
    dihDelegate.setLocalDihEndPoint(Mockito.mock(EndPoint.class));
  }

  @Test
  public void testGetInstanceSuccess() throws Exception {
    Mockito.when(dihClientFactory.build(Mockito.any(EndPoint.class))).thenReturn(dihClientWrapper);
    Mockito.when(dihClientWrapper.getInstance(Mockito.any(EndPoint.class)))
        .thenReturn(stubInstance(InstanceStatus.HEALTHY));

    try {
      dihDelegate.getDriverServerInstance(stubDriver());
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      Assert.fail();
    }
  }

  @Test
  public void testGetInstanceTimeout() throws Exception {
    Mockito.when(dihClientFactory.build(Mockito.any(EndPoint.class)))
        .thenThrow(new GenericThriftClientFactoryException());

    try {
      dihDelegate.getDriverServerInstance(stubDriver());
      Assert.fail();
    } catch (GenericThriftClientFactoryException e) {
      logger.warn("Timeout");
    }

    Mockito.when(dihClientFactory.build(Mockito.any(EndPoint.class))).thenReturn(dihClientWrapper);
    Mockito.when(dihClientWrapper.getInstance(Mockito.any(EndPoint.class)))
        .thenThrow(new TTransportException());
    try {
      dihDelegate.getDriverServerInstance(stubDriver());
      Assert.fail();
    } catch (TTransportException e) {
      logger.warn("Timeout");
    }
  }

  @Test
  public void testWaitDriverBecomingTargetStatusSuccess() throws Exception {
    Mockito.when(dihClientFactory.build(Mockito.any(EndPoint.class))).thenReturn(dihClientWrapper);
    Mockito.when(dihClientWrapper.getInstance(Mockito.any(EndPoint.class)))
        .thenReturn(stubInstance(InstanceStatus.SICK));
    dihDelegate.waitDriverServerDegrade(stubDriver(), InstanceStatus.SICK);

    Mockito.when(dihClientWrapper.getInstance(Mockito.any(EndPoint.class))).thenReturn(null);
    dihDelegate.waitDriverServerDegrade(stubDriver(), null);
  }

  @Test
  public void testWaitDriverBecomingTargetStatusTimeout() throws Exception {
    Mockito.when(dihClientFactory.build(Mockito.any(EndPoint.class))).thenReturn(dihClientWrapper);
    Mockito.when(dihClientWrapper.getInstance(Mockito.any(EndPoint.class)))
        .thenReturn(stubInstance(InstanceStatus.SICK));
    try {
      dihDelegate.waitDriverServerDegrade(stubDriver(), null);
      Assert.fail();
    } catch (TimeoutException e) {
      logger.warn("Timeout");
    }
  }

  @Test
  public void testTurnInstanceToFailedTimeout() throws Exception {
    List<Instance> instances = new ArrayList<>();
    instances.add(stubInstance(InstanceStatus.SICK));

    dihDelegate = new DihDelegate() {
      @Override
      public List<Instance> getDriverServerInstance(DriverMetadata driver)
          throws InterruptedException, TimeoutException, TException, Exception {
        return instances;
      }
    };
    dihDelegate.setDcConfig(dcConfig);
    dihDelegate.setDihClientFactory(dihClientFactory);
    dihDelegate.setLocalDihEndPoint(Mockito.mock(EndPoint.class));

    Mockito.when(dihClientFactory.build(Mockito.any(EndPoint.class)))
        .thenThrow(new GenericThriftClientFactoryException());

    try {
      dihDelegate.turnDriverServerToFailed(stubDriver());
      Assert.fail();
    } catch (GenericThriftClientFactoryException e) {
      logger.warn("Timeout");
    }

    Mockito.when(dihClientFactory.build(Mockito.any(EndPoint.class))).thenReturn(dihClientWrapper);
    Mockito.when(dihClientWrapper.turnInstanceToFailed(Mockito.anyLong()))
        .thenThrow(new TTransportException());
    try {
      dihDelegate.turnDriverServerToFailed(stubDriver());
      Assert.fail();
    } catch (TTransportException e) {
      logger.warn("Timeout");
    }
  }

  private Instance stubInstance(InstanceStatus status) {
    InstanceId instanceId;
    EndPoint endpoint;
    Instance instance;

    instanceId = new InstanceId(RequestIdBuilder.get());
    endpoint = new EndPoint("255.255.255.255", 0);

    instance = new Instance(instanceId, new Group(0), "test", status);

    return instance;
  }

  private DriverMetadata stubDriver() {
    DriverMetadata driverMetadata = new DriverMetadata();
    driverMetadata.setHostName("255.255.255.255");
    driverMetadata.setPort(0);

    return driverMetadata;
  }
}
