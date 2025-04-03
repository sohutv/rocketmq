package org.apache.rocketmq.remoting.protocol.body;

import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.List;

/**
 * broker限流数据
 *
 * @author yongfeigao
 * @date 2022年2月22日
 */
public class BrokerRateLimitData extends RemotingSerializable {
    private DataVersion dataVersion;
    // topic实时数据
    private List<TopicRateLimit> topicRateLimitList;

    public List<TopicRateLimit> getTopicRateLimitList() {
        return topicRateLimitList;
    }

    public void setTopicRateLimitList(List<TopicRateLimit> topicRateLimitList) {
        this.topicRateLimitList = topicRateLimitList;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }
}
