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

package py.drivercontainer.service;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mock;
import py.app.context.AppContext;
import py.common.RequestIdBuilder;
import py.coordinator.lio.LioCommandManagerConfiguration;
import py.coordinator.lio.LioManager;
import py.coordinator.lio.LioManagerConfiguration;
import py.coordinator.lio.LioNameBuilder;
import py.coordinator.lio.LioNameBuilderImpl;
import py.driver.IscsiAccessRule;
import py.drivercontainer.DriverContainerConfiguration;
import py.drivercontainer.lio.saveconfig.jsonobj.SaveConfigBuilder;
import py.drivercontainer.lio.saveconfig.jsonobj.SaveConfigImpl;
import py.drivercontainer.utils.DriverContainerUtils;
import py.iet.file.mapper.InitiatorsAllowFileMapper;
import py.test.TestBase;
import py.thrift.share.DriverKeyThrift;
import py.thrift.share.DriverTypeThrift;
import py.thrift.share.ListIscsiAppliedAccessRulesRequestThrift;
import py.thrift.share.ListIscsiAppliedAccessRulesResponseThrift;
import py.thrift.share.ServiceHavingBeenShutdownThrift;

/**
 * The test use to get applied iscsi accuesrules for lio and iet.
 */
public class GetAppliedIscsiAccuessRuleTest extends TestBase {

  private InitiatorsAllowFileMapper fileMapper;
  private String fileMapperPath = "/tmp/fileMapperPath";
  private String saveconfigPath = "/tmp/save.json";
  private String templatePath = "src/test/resources/config/lio.json";
  private String initialTemplatePath = "src/test/resources/config/initialtargetname.json";
  private String ietTargetName = "Zettastor.IQN:140846884855690593-0";
  private String lioTargetName1 = "iqn.2017-08.zettastor.iqn:140846884855690593-0";
  private String lioTargetName2 = "iqn.2017-08.zettastor.iqn:140846884855690593-1";
  private String initiatorName = "iqn.1994-05.com.redhat:bb113e6aa101";
  private DriverContainerImpl driverContainer;
  @Mock
  private AppContext appContext;
  private DriverContainerConfiguration dcConfig;
  private LioNameBuilder lioNameBuilder;
  private SaveConfigBuilder saveConfigBuilder;
  private SaveConfigImpl saveConfigImpl;
  private LioManager lioManager;
  private LioManagerConfiguration lioManagerCon;
  private LioCommandManagerConfiguration lioCmdMaConfig;


  /**
   * xx.
   */
  public void init() throws Exception {
    driverContainer = new DriverContainerImpl(appContext);
    dcConfig = new DriverContainerConfiguration();
    //iet initial
    fileMapper = new InitiatorsAllowFileMapper();
    fileMapper.setFilePath(fileMapperPath);
    Map<String, List<String>> initiatorAllowTable = new HashMap<>();
    List<String> initList = new ArrayList<>();
    initList.add(initiatorName);
    initiatorAllowTable.put(ietTargetName, initList);
    fileMapper.setInitiatorAllowTable(initiatorAllowTable);
    fileMapper.flush();

    //lio initial
    DriverContainerUtils.init();
    File file = new File(saveconfigPath);
    if (!file.exists()) {
      file.getParentFile().mkdirs();
      file.createNewFile();
    }
    copyContent(initialTemplatePath, saveconfigPath);
    lioManager = new LioManager();
    lioNameBuilder = new LioNameBuilderImpl();

    lioManagerCon = new LioManagerConfiguration();
    lioManagerCon.setRestoreCommand("ls /tmp");

    lioCmdMaConfig = new LioCommandManagerConfiguration();
    lioCmdMaConfig.setDefaultSaveConfigFilePath(saveconfigPath);

    saveConfigBuilder = new SaveConfigBuilder();
    saveConfigBuilder.setLioCmdMaConfig(lioCmdMaConfig);
    saveConfigBuilder.setLioMaConfig(lioManagerCon);

    lioManager.setSaveConfigBuilder(saveConfigBuilder);
    lioManager.setLioManagerCon(lioManagerCon);
    lioManager.setLioNameBuilder(lioNameBuilder);
    lioManager.setTemplatePath(templatePath);
    lioManager.setFilePath(saveconfigPath);
    lioManager.newTargetWrap(lioTargetName1, "192.168.2.1", "/dev/pyd0");
    lioManager.newTargetWrap(lioTargetName2, "192.168.2.1", "/dev/pyd0");
    driverContainer.setLioNameBuilder(lioNameBuilder);
    driverContainer.setLioManager(lioManager);

  }

  /**
   * Get applied accuessRules for lio.
   */
  @Test
  public void testGetAccuessRuleForLio() throws ServiceHavingBeenShutdownThrift {
    driverContainer.setDriverContainerConfig(dcConfig);
    ListIscsiAppliedAccessRulesRequestThrift request =
        new ListIscsiAppliedAccessRulesRequestThrift();
    request.setRequestId(RequestIdBuilder.get());
    request.setDriverKey(buildDriverKey(0));
    ListIscsiAppliedAccessRulesResponseThrift response;
    response = driverContainer.listIscsiAppliedAccessRules(request);
    //before apply accuess rule ,get nothing
    Assert.assertTrue(response.getInitiatorNames().size() == 0);

    //goint to apply one accuessRule to lio target
    List<IscsiAccessRule> rules = new ArrayList<>();
    IscsiAccessRule rule1 = new IscsiAccessRule();
    rule1.setInitiatorName(initiatorName);
    rules.add(rule1);
    lioManager.saveAccessRuleToMap(lioTargetName1, rules);
    response = driverContainer.listIscsiAppliedAccessRules(request);
    Assert.assertTrue(response.getInitiatorNames().size() == 1);
    Assert.assertTrue(response.getInitiatorNames().get(0).equals(initiatorName));

    //test get acculessRule by lioTargetName2
    request.setRequestId(RequestIdBuilder.get());
    request.setDriverKey(buildDriverKey(1));
    response = driverContainer.listIscsiAppliedAccessRules(request);
    //before apply accuess rule ,get nothing
    Assert.assertTrue(response.getInitiatorNames().size() == 0);

    //goint to apply one accuessRule to lio target
    lioManager.saveAccessRuleToMap(lioTargetName2, rules);
    response = driverContainer.listIscsiAppliedAccessRules(request);
    Assert.assertTrue(response.getInitiatorNames().size() == 1);
    Assert.assertTrue(response.getInitiatorNames().get(0).equals(initiatorName));
  }



  /**
   * xx.
   */
  public DriverKeyThrift buildDriverKey(int snapshotId) {
    DriverKeyThrift driverKeyThrift = new DriverKeyThrift();
    driverKeyThrift.setDriverContainerId(1);
    driverKeyThrift.setVolumeId(140846884855690593L);
    driverKeyThrift.setSnapshotId(snapshotId);
    driverKeyThrift.setDriverType(DriverTypeThrift.ISCSI);
    return driverKeyThrift;

  }


  /**
   * xx.
   */
  public void copyContent(String srcFile, String destFile) throws IOException {

    FileWriter fw = new FileWriter(destFile);
    FileReader fr = new FileReader(srcFile);

    int ch;
    while ((ch = fr.read()) != -1) {
      fw.write(ch);
    }
    fw.close();
    fr.close();
  }

}
