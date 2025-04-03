package org.apache.rocketmq.store.stats;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.body.PercentileStat;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * BrokerStoreStat管理
 * 
 * @author yongfeigao
 * @date 2020年4月28日
 */
public class BrokerStoreStatManager {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_STATS_LOGGER_NAME);

    public static final int ONE_MINITE_IN_MILLIS = 60000;
    // 耗时统计
    private TimeSectionPercentile timeSectionPercentile;
    // broker存储状态
    private volatile PercentileStat brokerStoreStat;

    public BrokerStoreStatManager() {
        timeSectionPercentile = new TimeSectionPercentile(10000);
        // 初始化
        init();
    }

    /**
     * 采样任务初始化
     */
    public void init() {
        // 数据采样线程
        Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "BrokerStoreStatManager");
            }
        }).scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    sample();
                } catch (Throwable ignored) {
                    log.warn("sample err:{}", ignored.getMessage());
                }
            }
        }, ONE_MINITE_IN_MILLIS, ONE_MINITE_IN_MILLIS, TimeUnit.MILLISECONDS);
    }

    /**
     * 采样并生成统计结果
     */
    private void sample() {
        timeSectionPercentile.sample();
        // 统计结果封装
        PercentileStat brokerStoreStat = new PercentileStat();
        brokerStoreStat.setStatTime((int) (System.currentTimeMillis() / ONE_MINITE_IN_MILLIS));
        brokerStoreStat.setAvg(timeSectionPercentile.avg());
        brokerStoreStat.setMax(timeSectionPercentile.max());
        brokerStoreStat.setPercent99(timeSectionPercentile.percentile(0.99));
        brokerStoreStat.setPercent90(timeSectionPercentile.percentile(0.9));
        brokerStoreStat.setCount(timeSectionPercentile.getTotalCount());
        this.brokerStoreStat = brokerStoreStat;
        log.info("brokerStoreStat:{}", brokerStoreStat);
    }

    public void increment(int timeInMillis) {
        timeSectionPercentile.increment(timeInMillis);
    }
    
    public PercentileStat getBrokerStoreStat() {
        return brokerStoreStat;
    }
}
