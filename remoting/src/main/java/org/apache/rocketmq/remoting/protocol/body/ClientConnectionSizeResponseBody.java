package org.apache.rocketmq.remoting.protocol.body;

import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * 客户端链接大小响应体
 *
 * @author yongfeigao
 * @date 2024年10月25日
 */
public class ClientConnectionSizeResponseBody extends RemotingSerializable {
    // 生产者大小
    private int producerSize;
    // 生产者链接大小
    private int producerConnectionSize;
    // 消费者大小
    private int consumerSize;
    // 消费者链接大小
    private int consumerConnectionSize;

    // 系统生产者大小
    private int systemProducerSize;
    // 系统生产者链接大小
    private int systemProducerConnectionSize;
    // 系统消费者大小
    private int systemConsumerSize;
    // 系统消费者链接大小
    private int systemConsumerConnectionSize;

    public int getProducerSize() {
        return producerSize;
    }

    public void setProducerSize(int producerSize) {
        this.producerSize = producerSize;
    }

    public void addProducerSize(int producerSize) {
        this.producerSize += producerSize;
    }

    public int getProducerConnectionSize() {
        return producerConnectionSize;
    }

    public void setProducerConnectionSize(int producerConnectionSize) {
        this.producerConnectionSize = producerConnectionSize;
    }

    public void addProducerConnectionSize(int producerConnectionSize) {
        this.producerConnectionSize += producerConnectionSize;
    }

    public int getConsumerSize() {
        return consumerSize;
    }

    public void addConsumerSize(int consumerSize) {
        this.consumerSize += consumerSize;
    }

    public void setConsumerSize(int consumerSize) {
        this.consumerSize = consumerSize;
    }

    public int getConsumerConnectionSize() {
        return consumerConnectionSize;
    }

    public void setConsumerConnectionSize(int consumerConnectionSize) {
        this.consumerConnectionSize = consumerConnectionSize;
    }

    public void addConsumerConnectionSize(int consumerConnectionSize) {
        this.consumerConnectionSize += consumerConnectionSize;
    }

    public int getSystemProducerSize() {
        return systemProducerSize;
    }

    public void setSystemProducerSize(int systemProducerSize) {
        this.systemProducerSize = systemProducerSize;
    }

    public void addSystemProducerSize(int systemProducerSize) {
        this.systemProducerSize += systemProducerSize;
    }

    public int getSystemProducerConnectionSize() {
        return systemProducerConnectionSize;
    }

    public void setSystemProducerConnectionSize(int systemProducerConnectionSize) {
        this.systemProducerConnectionSize = systemProducerConnectionSize;
    }

    public void addSystemProducerConnectionSize(int systemProducerConnectionSize) {
        this.systemProducerConnectionSize += systemProducerConnectionSize;
    }

    public int getSystemConsumerSize() {
        return systemConsumerSize;
    }

    public void setSystemConsumerSize(int systemConsumerSize) {
        this.systemConsumerSize = systemConsumerSize;
    }

    public void addSystemConsumerSize(int systemConsumerSize) {
        this.systemConsumerSize += systemConsumerSize;
    }

    public int getSystemConsumerConnectionSize() {
        return systemConsumerConnectionSize;
    }

    public void setSystemConsumerConnectionSize(int systemConsumerConnectionSize) {
        this.systemConsumerConnectionSize = systemConsumerConnectionSize;
    }

    public void addSystemConsumerConnectionSize(int systemConsumerConnectionSize) {
        this.systemConsumerConnectionSize += systemConsumerConnectionSize;
    }
}
