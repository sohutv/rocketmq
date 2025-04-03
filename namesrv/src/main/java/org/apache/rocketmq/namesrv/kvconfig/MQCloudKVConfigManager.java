package org.apache.rocketmq.namesrv.kvconfig;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.namesrv.NamesrvUtil;
import org.apache.rocketmq.common.utils.HttpTinyClient;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.namesrv.NamesrvController;

import java.io.File;
import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.List;

/**
 * 由于NS具有无状态特性，如果存储了kv，则变成有状态，维护困难。
 * 故启动后如果本地存储不存在从MQCloud拉取一份，这样NS变成近似无状态了，便于部署。
 */
public class MQCloudKVConfigManager extends KVConfigManager {

    public MQCloudKVConfigManager(NamesrvController namesrvController) {
        super(namesrvController);
    }

    @Override
    public void load() {
        File file = new File(namesrvController.getNamesrvConfig().getKvConfigPath());
        if (!file.exists()) {
            loadFromMQCloud();
        }
        super.load();
    }

    /**
     * 从MQCloud拉取KV配置
     */
    public void loadFromMQCloud() {
        String mqCloudDomain = namesrvController.getNamesrvConfig().getMqCloudDomain();
        if (mqCloudDomain == null || mqCloudDomain.isEmpty()) {
            return;
        }
        String addr = NetworkUtil.getLocalAddress() + ":" + namesrvController.getNettyServerConfig().getListenPort();
        List<String> paramValues = Arrays.asList("addr", addr);
        String kvConfigUrl = "http://" + mqCloudDomain + "/rocketmq/kv/config";
        int times = 1;
        while (true) {
            try {
                HttpTinyClient.HttpResult result = HttpTinyClient.httpGet(kvConfigUrl, null, paramValues, "UTF-8", 3000);
                if (HttpURLConnection.HTTP_OK == result.code && persistKvConfig(result.content)) {
                    break;
                }
                log.error("loadFromMQCloud error, times:{}, code:{}, content:{}", times, result.code, result.content);
            } catch (Throwable e) {
                log.error("loadFromMQCloud times:{}", times, e);
            }
            times++;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 持久化KV配置
     *
     * @param content
     * @return 是否成功，无需持久化也返回true
     */
    private boolean persistKvConfig(String content) {
        if (content == null || content.isEmpty()) {
            log.info("no need to persistKvConfig");
            return true;
        }
        try {
            JSONObject jsonObject = JSON.parseObject(content);
            JSONObject resultJSONObject = jsonObject.getJSONObject("result");
            // 响应数据为空
            if (resultJSONObject == null) {
                return true;
            }
            JSONObject kvConfigMap = new JSONObject();
            kvConfigMap.put(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG, resultJSONObject);
            JSONObject configTable = new JSONObject();
            configTable.put("configTable", kvConfigMap);
            lock.readLock().lockInterruptibly();
            try {
                MixAll.string2File(configTable.toJSONString(), namesrvController.getNamesrvConfig().getKvConfigPath());
            } finally {
                lock.readLock().unlock();
            }
            log.info("persistKvConfig success, content:{}", content);
            return true;
        } catch (Exception e) {
            log.error("persistKvConfig:{} error", content, e);
            return false;
        }
    }
}
