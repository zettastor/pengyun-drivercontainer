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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.drivercontainer.driver.store.DriverStore;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.drivercontainer.driver.version.Version;
import py.drivercontainer.driver.version.VersionManager;


/*
 * QueryServerHandler is handler class for QueryServer processing channel read and write
 * read magic,volId and snapId from pyd, send driverIp and port to pyd
 * magic->long          volId->long     snapshotId->int
 * driverIp->16bytes    port->int
 */
// @ChannelHandler.Sharable
public class QueryServerHandler extends ChannelInboundHandlerAdapter {

  public static final long NBD_QUERYSERVER_MAGIC = 0x12340001;
  public static final int L_QUERYSERVER_MAGIC = 8;
  public static final int L_QUERYSERVER_VOL = 8;
  public static final int L_QUERYSERVER_SNAP = 4;
  public static final int INDEX_MAGIC = 0;
  public static final int INDEX_VOL = 8;
  public static final int INDEX_SNAP = 16;
  public static final int L_COORDINATOR_IP = 16;
  public static final int L_COORDINATOR_PORT = 4;
  private static final Logger logger = LoggerFactory.getLogger(QueryServerHandler.class);
  public long magicId;
  public long volid;
  public int snapid;
  public byte[] coordinatorIp = new byte[L_COORDINATOR_IP];
  public int coordinatorPort;
  private ByteBuf cumulation;
  private DriverStoreManager driverStoreManager;
  private VersionManager versionManager;


  /**
   * xx.
   */
  public QueryServerHandler(DriverStoreManager driverStoreManager, VersionManager versionManager) {
    this.driverStoreManager = driverStoreManager;
    this.versionManager = versionManager;
    this.cumulation = null;
  }


  /**
   * xx.
   */
  public static byte[] intToBytes(int value) {
    String intValue = String.valueOf(value);
    byte[] tmp = intValue.getBytes();
    return tmp;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (!(msg instanceof ByteBuf)) {
      ctx.fireChannelRead(msg);
      logger.error("msg failed type={}", msg.getClass());
      return;
    }

    int neededLength = getRequestLen();
    ByteBuf data = ((ByteBuf) msg).order(ByteOrder.LITTLE_ENDIAN);
    try {
      // first check if there is cumulation.
      if (cumulation != null) {
        logger.debug("channelRead cumulation  ");
        // confirm that the body is complete.
        if (data.readableBytes() + cumulation.readableBytes() >= neededLength) {
          logger.debug("channelRead cumulation more readable={} ", cumulation.isReadable());
          // a new package will produce, if cumulation not readable shouldn't use wrappedBuffer
          ByteBuf pack = null;
          if (cumulation.isReadable()) {
            int leftSize = neededLength - cumulation.readableBytes();
            // LITTLE_ENDIAN needed otherwise data not right
            pack = Unpooled.wrappedBuffer(cumulation, data.slice(data.readerIndex(), leftSize))
                .order(ByteOrder.LITTLE_ENDIAN);
            data.skipBytes(leftSize);
          } else {
            pack = data.slice(data.readerIndex(), neededLength);
            data.skipBytes(neededLength);
          }

          if (data.isReadable()) {
            // data may contain serveral packages, so increase the reference, netty will not
            // release it
            data.retain();
          } else {
            data = null;
          }
          Validate.isTrue(neededLength == pack.readableBytes(), "pack failed length error!");
          responseAndSend(ctx, pack);
          // not used should release
          pack.release();
          cumulation = null;
          //should not return remainder data if any need to process
          //return;
        } else {
          logger.debug("channelRead cumulation less ");
          // can not produce a new package, wrap the data, when the next bytebuf comes, a new
          // package can produce.
          cumulation = Unpooled.wrappedBuffer(cumulation, data);
          data = null;
          return;
        }
      }
      // process data which is remaining
      if (data != null) {
        while (data.isReadable()) {
          int readableBytes = data.readableBytes();
          if (readableBytes < neededLength) {
            logger.debug("channelRead data less ");
            // can not produce a package, so waiting for next bytebuf for producing a new package.
            cumulation = data;
            break;
          } else if (readableBytes == neededLength) {
            logger.debug("channelRead data equal idx={}", data.readerIndex());
            // produce a package rightly.
            ByteBuf pack = null;
            pack = Unpooled.wrappedBuffer(data.slice(data.readerIndex(), neededLength))
                .order(ByteOrder.LITTLE_ENDIAN);
            responseAndSend(ctx, pack);
            data.skipBytes(neededLength);
            pack.release();
            data = null;
            break;
          } else {
            logger.debug("channelRead data more idx={}", data.readerIndex());
            // there are more than one packages, should not release by netty
            data.retain();
            ByteBuf pack = null;
            pack = Unpooled.wrappedBuffer(data.slice(data.readerIndex(), neededLength))
                .order(ByteOrder.LITTLE_ENDIAN);
            responseAndSend(ctx, pack);
            data.skipBytes(neededLength);
            pack.release();
          }
        }
      }
    } catch (Exception e) {
      logger.warn("caught an exception {}, try connect next", e);
      if (data != null) {
        data.release();
      }
      if (cumulation != null) {
        cumulation.release();
      }
      // close socket
      ctx.fireChannelInactive();
    }
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    /* write to channel */
    ctx.flush();
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
    logger.warn("channelUnregistered: {}, {}, header={}", cumulation, ctx.channel());
    ctx.fireChannelUnregistered();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    logger.warn("channelInactive: {}, {}, header={}", cumulation, ctx.channel());
    if (cumulation != null) {
      cumulation.release();
      cumulation = null;
    }
    ctx.fireChannelInactive();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    /* release context */
    ctx.close();
  }

  /*
   * process a complete package resolving from socket, first get magic vloId and snapshotId from
   * package, then get host and port from driverStore, and send to pyd via socket
   *
   */

  /**
   * xx.
   */
  public void responseAndSend(ChannelHandlerContext ctx, ByteBuf data) throws Exception {
    byte[] req = new byte[data.readableBytes()];
    data.readBytes(req);
    logger.debug("responseAndSend req={} len={} readable={}", req, req.length, data.isReadable());
    data.resetReaderIndex();

    magicId = data.getLong(INDEX_MAGIC);
    volid = data.getLong(INDEX_VOL);
    snapid = data.getInt(INDEX_SNAP);
    if (magicId != NBD_QUERYSERVER_MAGIC) {
      logger.error("responseAndSend  magic({}) fail", magicId);
      // if magic failed throw exception, pyd will process
      throw new IOException("write failed");
    }

    logger.debug("responseAndSend magic={} vol={} snap={}", magicId, volid, snapid);

    int sendSize = L_COORDINATOR_IP + L_COORDINATOR_PORT;
    ByteBuf byteBuf = Unpooled.buffer(sendSize);
    boolean success = getCoordinatorIpAndPort(volid, snapid);

    if (!success) {
      byte[] zeroBytes = new byte[0];
      byteBuf.writeBytes(zeroBytes);
      byteBuf.writeInt(0);
    } else {
      byteBuf.writeBytes(coordinatorIp);
      byteBuf.writeInt(coordinatorPort);
    }

    logger.debug("responseAndSend vol={} host={} port={} channel={}", volid, coordinatorIp,
        coordinatorPort, ctx.channel().remoteAddress().toString());

    ChannelFuture cf = ctx.channel().write(byteBuf);
    ctx.flush();
    if (!cf.isSuccess()) {
      logger.error("channelRead6 failed:{} ", cf.cause());
      throw new IOException("write failed");
    }

  }

  /*
   * get host and port from driverStore according volid and snapid
   * success return true
   * in dc when upgrade the newest coordinator will start in latest, but when all current upgrade ok
   * then change latest to current, so long time wait, we should check latest first
   * after driver in latest startup, then delete current record and shutdown relative coordinator
   *
   */

  /**
   * xx.
   */
  public boolean getCoordinatorIpAndPort(long volid, int snapid) {
    logger.debug("getCoordinatorIPAndPort vol={} snapid={} ip={}",
        volid, snapid, coordinatorIp);
    try {
      for (DriverType driverType : DriverType.values()) {
        Version currentVersion = null;
        Version latestVersion = null;

        switch (driverType) {
          case FSD:
            versionManager.lockVersion(driverType);
            currentVersion = versionManager.getCurrentVersion(DriverType.FSD);
            latestVersion = versionManager.getLatestVersion(DriverType.FSD);
            versionManager.unlockVersion(driverType);
            break;
          case NBD:
          case ISCSI:
            versionManager.lockVersion(driverType);
            currentVersion = versionManager.getCurrentVersion(DriverType.NBD);
            latestVersion = versionManager.getLatestVersion(DriverType.NBD);
            versionManager.unlockVersion(driverType);
            break;
          default:
            logger.debug("Driver type {} is not supported", driverType);
            continue;
        }

        /* latest process */
        if (latestVersion != null) {
          DriverStore latestDriverStore = driverStoreManager.get(latestVersion);
          boolean latestSuccess = false;

          if (latestDriverStore != null) {
            latestSuccess = getIpAndPortFromDriverStore(latestDriverStore, volid, snapid);
            if (latestSuccess) {
              logger.debug("getCoordinatorIPAndPort success from latestDriverStore ip {} port {}",
                  coordinatorIp, coordinatorPort);
              return true;
            }
          }
        }

        /* current process */
        if (currentVersion == null) {  // not launch
          logger.warn("Driver type {} currentVersion is null.", driverType);
          continue;
        }
        DriverStore driverStore = driverStoreManager.get(currentVersion);
        boolean success = false;

        if (driverStore == null) {
          logger.warn("driverStore is null driverType {} currentVersion {}", driverType,
              currentVersion);
          continue;
        }

        success = getIpAndPortFromDriverStore(driverStore, volid, snapid);
        if (success) {
          logger.warn("getCoordinatorIPAndPort success from currentStore ip {} port {}",
              coordinatorIp, coordinatorPort);
          return true;
        }
      }
    } catch (Exception e) {
      logger.error("getCoordinatorIPAndPort4 {}", e);
    }

    logger.debug("getCoordinatorIPAndPort5 vol={} snap={} ip={} port={}  ", volid, snapid,
        coordinatorIp, coordinatorPort);
    return false;
  }


  /**
   * xx.
   */
  public boolean getIpAndPortFromDriverStore(DriverStore driverStore, long volid, int snapid) {
    logger.debug("getIPAndPortFromDriverStore driverStore={} vol={} snapid={} ip={}", driverStore,
        volid, snapid, coordinatorIp);
    Arrays.fill(coordinatorIp, (byte) 0);
    coordinatorPort = 0;
    try {
      List<DriverMetadata> driverMetadataList = driverStore.list();
      for (DriverMetadata driver : driverMetadataList) {
        DriverStatus driverStatus = driver.getDriverStatus();
        long voltmp = driver.getVolumeId(true);
        int snaptmp = driver.getSnapshotId();
        //when migrating only volid changed
        //if old not exists voltmp equals curVoltmp
        long curVoltmp = driver.getVolumeId(false);

        if ((driverStatus == DriverStatus.LAUNCHING && driver.getUpgradePhrase() == 1)
            || driverStatus == DriverStatus.LAUNCHED
            || driverStatus == DriverStatus.RECOVERING
            || driverStatus == DriverStatus.MIGRATING) {
          if (volid == voltmp && snapid == snaptmp || volid == curVoltmp && snapid == snaptmp) {
            // coordinator_ip should keep length
            byte[] tmp = driver.getHostName().getBytes();
            if (driver.getDriverType() == DriverType.ISCSI) {
              byte[] localIp = "127.0.0.1".getBytes();
              System.arraycopy(localIp, 0, coordinatorIp, 0, localIp.length);
            } else {
              System.arraycopy(tmp, 0, coordinatorIp, 0, tmp.length);
            }
            coordinatorPort = driver.getPort();
            return true;
          }
        }
      }
    } catch (Exception e) {
      logger.error("getCoordinatorIPAndPort4 {}", e);
    }
    logger.warn("getCoordinatorIPAndPort5 vol={} snap={} ip={} port={}  ", volid, snapid,
        coordinatorIp, coordinatorPort);
    return false;
  }

  public int getRequestLen() {
    return L_QUERYSERVER_MAGIC + L_QUERYSERVER_VOL + L_QUERYSERVER_SNAP;
  }

}
