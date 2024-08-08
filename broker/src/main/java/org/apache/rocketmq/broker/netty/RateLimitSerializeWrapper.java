package org.apache.rocketmq.broker.netty;

import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashMap;
import java.util.Map;

/**
 * 限速序列化包装
 *
 * @author yongfeigao
 * @date 2024年7月19日
 */
public class RateLimitSerializeWrapper extends RemotingSerializable {
    private DataVersion dataVersion;
    // topic限流
    private Map<String, Double> rateLimitMap = new HashMap<>();

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }

    public Map<String, Double> getRateLimitMap() {
        return rateLimitMap;
    }

    public void setRateLimitMap(Map<String, Double> rateLimitMap) {
        this.rateLimitMap = rateLimitMap;
    }
}
