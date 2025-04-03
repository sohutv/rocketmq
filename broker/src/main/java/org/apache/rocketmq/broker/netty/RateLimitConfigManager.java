package org.apache.rocketmq.broker.netty;

import org.apache.rocketmq.broker.util.TokenBucketRateLimiter;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

/**
 * 限速配置管理器
 *
 * @author yongfeigao
 * @date 2024年7月19日
 */
public class RateLimitConfigManager extends ConfigManager {

    private RateLimitHandler rateLimitHandler;

    public RateLimitConfigManager(RateLimitHandler rateLimitHandler) {
        this.rateLimitHandler = rateLimitHandler;
        load();
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    @Override
    public String configFilePath() {
        return StorePathConfigHelper.getRateLimitConfigStorePath(
                rateLimitHandler.getBrokerController().getMessageStoreConfig().getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            RateLimitSerializeWrapper rateLimitSerializeWrapper =
                    RateLimitSerializeWrapper.fromJson(jsonString, RateLimitSerializeWrapper.class);
            if (rateLimitSerializeWrapper != null) {
                rateLimitSerializeWrapper.getRateLimitMap().forEach((resource, rate) -> {
                    rateLimitHandler.getRateLimiterMap().compute(resource, (k, v) -> v == null ? new TokenBucketRateLimiter(rate) : v.setRate(rate));
                });
                // For compatible
                if (rateLimitSerializeWrapper.getDataVersion() != null) {
                    rateLimitHandler.getDataVersion().assignNewOne(rateLimitSerializeWrapper.getDataVersion());
                }
            }
        }
    }

    @Override
    public String encode(boolean prettyFormat) {
        RateLimitSerializeWrapper rateLimitSerializeWrapper = new RateLimitSerializeWrapper();
        // 设置topic限流
        rateLimitHandler.getRateLimiterMap().forEach((k, v) -> {
            // 默认的阈值不需要持久化
            if ((k.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX) && v.getQps() == rateLimitHandler.getBrokerController().getBrokerConfig().getSendRetryMsgRateLimitQps())
                    || v.getQps() == rateLimitHandler.getBrokerController().getBrokerConfig().getSendMsgRateLimitQps()) {
                return;
            }
            rateLimitSerializeWrapper.getRateLimitMap().put(k, v.getQps());
        });
        rateLimitSerializeWrapper.setDataVersion(rateLimitHandler.getDataVersion());
        return rateLimitSerializeWrapper.toJson(prettyFormat);
    }
}
