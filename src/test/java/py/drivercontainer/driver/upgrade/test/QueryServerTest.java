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

package py.drivercontainer.driver.upgrade.test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.drivercontainer.driver.store.DriverStore;
import py.drivercontainer.driver.store.DriverStoreImpl;
import py.drivercontainer.driver.store.DriverStoreManager;
import py.drivercontainer.driver.store.DriverStoreManagerImpl;
import py.drivercontainer.driver.upgrade.QueryServerHandler;
import py.drivercontainer.driver.version.Version;
import py.drivercontainer.driver.version.VersionManager;
import py.drivercontainer.driver.version.file.VersionImpl;
import py.drivercontainer.driver.version.file.VersionManagerImpl;
import py.test.TestBase;

public class QueryServerTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(QueryServerTest.class);
  private FakeQueryServer queryServer;
  private QueryServerHandler queryServerHandler;
  private Thread thread;
  private DriverStore driverStore;

  public QueryServerTest() throws Exception {
  }


  /**
   * xx.
   */
  public static byte[] intToBytes2(int value) {
    byte[] src = new byte[4];
    src[0] = (byte) ((value >> 24) & 0xFF);
    src[1] = (byte) ((value >> 16) & 0xFF);
    src[2] = (byte) ((value >> 8) & 0xFF);
    src[3] = (byte) (value & 0xFF);
    return src;
  }


  /**
   * xx.
   */
  public static byte[] longToByte(long number) {
    long temp = number;
    byte[] b = new byte[8];
    for (int i = 0; i < b.length; i++) {
      b[i] = new Long(temp & 0xff).byteValue();
      temp = temp >> 8;
    }
    return b;
  }


  /**
   * xx.
   */
  public static int byteToInt(byte[] b) {
    final int s0 = b[0] & 0xff;
    int s1 = b[1] & 0xff;
    int s2 = b[2] & 0xff;
    int s3 = b[3] & 0xff;
    s3 <<= 24;
    s2 <<= 16;
    s1 <<= 8;
    int s = 0;
    s = s0 | s1 | s2 | s3;
    return s;
  }


  /**
   * xx.
   */
  @Before
  public void before() throws Exception {
    Version version = VersionImpl.get("2.3.0-internal-20170918000011");
    String driverStoreDirectory = "/tmp/driver_store_test";
    driverStore = new DriverStoreImpl(Paths.get(driverStoreDirectory), version);
    DriverStoreManager driverStoreManager = new DriverStoreManagerImpl();
    VersionManager versionManager = new VersionManagerImpl(driverStoreDirectory);
    Long driverContainerId = RequestIdBuilder.get();
    DriverMetadata okDriver1 = buildDriverWithStatus(DriverStatus.LAUNCHED, driverContainerId);
    DriverMetadata okDriver2 = buildDriverWithStatus(DriverStatus.LAUNCHED, driverContainerId);
    driverStore.save(okDriver1);
    // driverStore.save(okDriver2);
    queryServerHandler = new QueryServerHandler(driverStoreManager, versionManager);

    queryServer = new FakeQueryServer();
    queryServer.setDriverStoreManager(driverStoreManager);
    queryServer.setVersionManager(versionManager);
    thread = new Thread(queryServer);
    thread.start();
  }

  @Test
  public void testIntToBytes() throws Exception {
    byte[] port = new byte[8];
    int p = 8080;
    port = queryServerHandler.intToBytes(p);
    String tmp = new String(port);
    int result = Integer.parseInt(tmp);
    logger.warn("intToBytes result={}", result);
    assert (result == p);

  }

  @Test
  public void testGetCoordinatorIpAndPort() throws Exception {
    queryServerHandler.getIpAndPortFromDriverStore(driverStore, 12345L, 0);
    logger.warn("testGetCoordinatorIPAndPort ip={} port={}",
        new String(queryServerHandler.coordinatorIp),
        queryServerHandler.coordinatorPort);
    assert (queryServerHandler.coordinatorPort == 8080);
  }

  @Test
  public void read() throws Exception {
    byte[] req = {48, 120, 49, 50, 51, 52, 48, 48, 48, 49, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0,
        0, 0, 0, 50, 55, 53, 50, 50, 49, 55, 51, 51, 51, 48, 51, 53, 56, 54, 51, 56, 53, 56, 0, 0,
        0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 48, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0,
        0, 0, 0, 0, 0};

    logger.warn("channelRead req={} len={}", req, req.length);
    String sdata = new String(req, "utf-8");
    logger.warn("channelRead s_data={} ", sdata, sdata.length());

    byte[] tmp1 = new byte[32];
    byte[] tmp2 = new byte[32];
    byte[] tmp3 = new byte[32];

    for (int i = 0; i < 32; i++) {
      tmp1[i] = req[i];
      tmp2[i] = req[i + 32];
      tmp3[i] = req[i + 64];
    }
    logger.warn("channelRead tmp1={} tmp2={} tmp3={}", tmp1, tmp2, tmp3);
    String smagic = new String(tmp1);
    String svolid = new String(tmp2);
    String ssnap = new String(tmp3);
    logger.warn("channelRead magic={} vol={} snap={}", smagic, svolid, ssnap);

    byte[] tmp = new byte[4];
    tmp[0] = 'a';
    logger.warn("channelRead tmp={}", new String(tmp));

    // byte[] req1 ={1, 0, 52, 18, 0, 0, 0, 0, 50, -113, 70, 32, 21, -41, 49, 38, 0, 0, 0,0};
    // byte[] req1 = longToByte(0x12340001); //[1, 0, 52, 18, 0, 0, 0, 0]
    // byte[] req1 ={ 0, 0, 0, 0, 18, 52,0,1};

    byte[] req1 = {1, 0, 52, 18, 0, 0, 0, 0, -91, -20, -121, 43, -47, -6, -29, 48, 0, 0, 0, 0};
    logger.warn("channelRead req1={}", req1);
    ByteBuf buf = Unpooled.copiedBuffer(req1).order(ByteOrder.LITTLE_ENDIAN);
    // buf.order(ByteOrder.LITTLE_ENDIAN);
    long magicId = buf.getLong(0);
    // int magic_id1 = buf.getInt(8);
    logger.warn("channelRead magic={} vol={} snap={}", magicId);

    byte[] req2 = {0, 0, 4, -46};
    logger.warn("channelRead req2={}", req2);
    ByteBuf buf1 = Unpooled.copiedBuffer(req2);
    long magicId1 = buf1.getInt(0);
    logger.warn("channelRead magic={} ", magicId1);
    assert (magicId1 == 1234);

  }

  @Test
  public void readMerge() throws Exception {
    // ByteBuf cumulation = null;
    ByteBuf cumulation = Unpooled.buffer();
    byte[] req1 = {0, 0, 0, 0, 18, 52, 0, 1};
    ByteBuf msgData = Unpooled.copiedBuffer(req1);
    byte[] req2 = {0, 0, 4, -46};
    // ByteBuf cumulation = Unpooled.copiedBuffer(req2);
    int cumulen = 0;

    if (cumulation == null) {
      cumulen = 0;
    } else {
      cumulen = cumulation.readableBytes();
    }

    logger.warn("channelRead00 s1={} s2={} ", msgData.readableBytes(), cumulen);
    ByteBuf data = null;
    if (msgData.readableBytes() + cumulen == 8) {
      logger.warn("channelRead11 s1={} s2={} ", msgData.readableBytes(), cumulen);
      cumulation = Unpooled.wrappedBuffer(cumulation, msgData);
      data = cumulation;

      cumulation = Unpooled.buffer();
      logger
          .warn("channelRead11 end s1={} s2={} ", data.readableBytes(), cumulation.readableBytes());

    } else if (msgData.readableBytes() + cumulation.readableBytes() > 2) {
      logger.warn("channelRead12");
      // a new package will produce.
      int leftSize = 8 - cumulation.readableBytes();
      data = Unpooled.wrappedBuffer(cumulation, msgData.slice(msgData.readerIndex(), 0));
      msgData.skipBytes(2);
      cumulation = msgData;
      logger.warn("channelRead12 end s1={} s2={} ", msgData.readableBytes(),
          cumulation.readableBytes());
    } else {
      logger.warn("channelRead13 ");
      cumulation = Unpooled.wrappedBuffer(cumulation, msgData);
      logger.warn("channelRead13 end s1={} s2={} ", msgData.readableBytes(),
          cumulation.readableBytes());
    }

    byte[] req = new byte[data.readableBytes()];
    data.readBytes(req);
    logger.warn("channelRead req={} len={} readable={}", req, req.length, data.isReadable());

  }

  @Test
  public void write() throws Exception {

    ByteBuf byteBuf = Unpooled.buffer(4);
    byteBuf.writeInt(1234);

    byte[] req = new byte[byteBuf.readableBytes()];
    byteBuf.readBytes(req).order(ByteOrder.BIG_ENDIAN);

    byte[] req2 = {-46, 4, 0, 0};
    logger.warn("write req={} len={} reqbyte={} req2byte={}", req, req.length, byteToInt(req),
        byteToInt(req2));

    String s = "127.0.0.1";
    byte[] src1 = "127.0.0.1".getBytes();
    // byte src2[] = {10, 20, 30, 40, 50, 0, 0};
    byte[] src2 = new byte[16];
    System.arraycopy(src1, 0, src2, 0, src1.length);
    logger.warn("write src1={} src2={}", src1, src2);
  }

  @After
  public void after() {
    queryServer.close();
  }

  private DriverMetadata buildDriverWithStatus(DriverStatus status, Long driverContainerId) {
    DriverMetadata driver = new DriverMetadata();
    driver.setDriverContainerId(driverContainerId);
    driver.setVolumeId(12345);
    driver.setDriverType(DriverType.NBD);
    driver.setDriverStatus(status);
    driver.setProcessId(Integer.MAX_VALUE);
    driver.setHostName("1.0.0.1");
    driver.setPort(8080);
    return driver;
  }

}
