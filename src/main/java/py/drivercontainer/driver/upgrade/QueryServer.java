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

package py.drivercontainer.driver.upgrade;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.drivercontainer.driver.version.VersionManager;
import py.periodic.UnableToStartException;

/*
 * QueryServer is a socket server(port now default 9201), used communicate with pyd
 * init in driverContainer
 * read magic,volId and snapId from pyd, send driverIp and port to pyd
 * magic->long          volId->long     snapshotId->int
 * driverIp->16bytes    port->int
 *
 */
public class QueryServer implements Runnable {

  public static final int QueryServer_Port = 9201;
  private static final Logger logger = LoggerFactory.getLogger(QueryServer.class);
  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;
  private ChannelFuture serverChannelFuture;
  private AtomicBoolean stop = new AtomicBoolean(false);
  private ServerBootstrap bootstrap;
  private DriverStoreManager driverStoreManager;
  private VersionManager versionManager;



  /**
   * xx.
   */
  public void bind(int port) throws Exception {
    bossGroup = new NioEventLoopGroup();
    workerGroup = new NioEventLoopGroup();
    bootstrap = new ServerBootstrap();
    bootstrap.group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .option(ChannelOption.SO_BACKLOG, 1024)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .childOption(ChannelOption.SO_REUSEADDR, true)
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) throws Exception {
            ch.pipeline().addLast(new QueryServerHandler(driverStoreManager, versionManager));
          }
        });
    // bind port
    this.serverChannelFuture = bootstrap.bind(port).sync();
    logger.debug("QueryServer start successfully port={} ", QueryServer_Port);
  }


  /**
   * xx.
   */
  public void start() throws UnableToStartException {
    logger.warn("QueryServer start port={}", QueryServer_Port);
    try {
      this.bind(QueryServer_Port);
      logger.warn("QueryServer start success");
    } catch (Exception e) {
      logger.error("QueryServer start fail {}", e);
      throw new UnableToStartException();
    }
  }


  /**
   * xx.
   */
  public void close() {
    logger.debug("QueryServer close port={}", QueryServer_Port);

    try {
      if (bossGroup != null) {
        Future<?> future1 = bossGroup.shutdownGracefully();
        future1.get(5000, TimeUnit.MILLISECONDS);
      }
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      logger.warn("Caught an exception", e);
    }
    try {
      if (workerGroup != null) {
        Future<?> future1 = workerGroup.shutdownGracefully();
        future1.get(5000, TimeUnit.MILLISECONDS);
      }
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      logger.warn("Caught an exception", e);
    }

    try {
      serverChannelFuture.channel().close();
    } catch (Exception e) {
      logger.warn("Caught an exception", e);
    }
  }

  @Override
  public void run() {
    try {
      start();
      logger.warn("QueryServer start successfully");
    } catch (UnableToStartException t) {
      logger.error("caught an exception", t);
    }
  }


  public DriverStoreManager getDriverStoreManager() {
    return driverStoreManager;
  }

  public void setDriverStoreManager(DriverStoreManager driverStoreManager) {
    this.driverStoreManager = driverStoreManager;
  }

  public VersionManager getVersionManager() {
    return versionManager;
  }

  public void setVersionManager(VersionManager versionManager) {
    this.versionManager = versionManager;
  }
}