package org.apache.rocketmq.namesrv.kvconfig;

import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.namesrv.NameServerInstanceTest;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class MQCloudKVConfigManagerTest {
    private MQCloudKVConfigManager kvConfigManager;

    private NamesrvConfig namesrvConfig;

    @Before
    public void setup() throws Exception {
        namesrvConfig = new NamesrvConfig();
        namesrvConfig.setMqCloudDomain("localhost:8080");
        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(9876);
        NamesrvController nameSrvController = new NamesrvController(namesrvConfig, nettyServerConfig);
        kvConfigManager = new MQCloudKVConfigManager(nameSrvController);
    }

    @Test
    public void testLoad() {
        kvConfigManager.load();
        Assert.assertTrue(new File(namesrvConfig.getKvConfigPath()).exists());
    }

    @Test
    public void testLoadFromMQCloud() {
        kvConfigManager.loadFromMQCloud();
        Assert.assertTrue(new File(namesrvConfig.getKvConfigPath()).exists());
    }
}