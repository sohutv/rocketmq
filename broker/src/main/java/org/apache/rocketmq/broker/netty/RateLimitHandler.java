package org.apache.rocketmq.broker.netty;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.RejectedExecutionHandler;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.util.TokenBucketRateLimiter;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.*;
import org.apache.rocketmq.remoting.protocol.body.TopicRateLimit;

import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 限流处理器
 * 
 * @author yongfeigao
 * @date 2022年2月16日
 */
@ChannelHandler.Sharable
public class RateLimitHandler extends SimpleChannelInboundHandler<RemotingCommand> {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_REMOTING_NAME);

    private BrokerController brokerController;

    // 1秒的微妙
    private static final int MICROSECONDS = 1000000;

    // 限流器map
    private ConcurrentMap<String, TokenBucketRateLimiter> rateLimiterMap = new ConcurrentHashMap<>();
    
    // 限流器处理线程池
    private EventExecutorGroup rateLimitEventExecutorGroup;

    // 限流配置管理器
    private RateLimitConfigManager rateLimitConfigManager;

    private DataVersion dataVersion = new DataVersion();

    public RateLimitHandler(BrokerController brokerController) {
        this.brokerController = brokerController;
        rateLimitEventExecutorGroup = new DefaultEventExecutorGroup(
                brokerController.getNettyServerConfig().getServerWorkerThreads(), new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            public Thread newThread(Runnable r) {
                return new Thread(r, "RateLimitThread_" + this.threadIndex.incrementAndGet());
            }
        }, 10000, new RejectedExecutionHandler() {
            public void rejected(Runnable task, SingleThreadEventExecutor executor) {
                log.error("RateLimitHandler reject task, {} pendingTasks:{}", getThreadName(executor),
                        executor.pendingTasks());
                task.run();
            }
        });
        rateLimitConfigManager = new RateLimitConfigManager(this);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand cmd) throws Exception {
        // 非当前线程池直接触发下一个事件
        if (!ctx.executor().inEventLoop()) {
            ctx.fireChannelRead(cmd);
            return;
        }
        if (!brokerController.getBrokerConfig().isEnableRateLimit() || cmd == null
                || cmd.getType() != RemotingCommandType.REQUEST_COMMAND) {
            ctx.fireChannelRead(cmd);
            return;
        }
        // 获取限流资源
        String resource = getResource(cmd);
        if (resource == null) {
            ctx.fireChannelRead(cmd);
            return;
        }
        // 限流流量
        double limitQps = brokerController.getBrokerConfig().getSendMsgRateLimitQps();
        if (cmd.getCode() == RequestCode.CONSUMER_SEND_MSG_BACK) {
            limitQps = brokerController.getBrokerConfig().getSendRetryMsgRateLimitQps();
        }
        final double finalLimitQps = limitQps;
        // 获取或创建限速器
        TokenBucketRateLimiter rateLimiter = rateLimiterMap.computeIfAbsent(resource,
                k -> new TokenBucketRateLimiter(finalLimitQps));
        boolean acquired = rateLimiter.acquire();
        // 不需要限流
        if (acquired) {
            ctx.fireChannelRead(cmd);
            return;
        }
        // 需要限流，打印限流日志
        long lastNeedWaitMicrosecs = rateLimiter.getLastNeedWaitMicrosecs();
        log.warn("{}:{} code:{} RateLimit needWait:{}microsecs",
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()), resource, cmd.getCode(), lastNeedWaitMicrosecs);
        // 响应客户端
        if (!cmd.isOnewayRPC()) {
            RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                    "[RateLimit] for " + resource + " need wait:" + lastNeedWaitMicrosecs + "microsecs");
            response.setOpaque(cmd.getOpaque());
            ctx.writeAndFlush(response);
        }
    }

    /**
     * 获取限流资源
     * 
     * @param cmd
     * @return
     */
    private String getResource(RemotingCommand cmd) {
        if (cmd.getExtFields() == null) {
            return null;
        }
        String resource = null;
        switch (cmd.getCode()) {
        case RequestCode.CONSUMER_SEND_MSG_BACK:
            resource = cmd.getExtFields().get("group");
            if (resource != null) {
                resource = MixAll.getRetryTopic(resource);
            }
            break;
        case RequestCode.SEND_MESSAGE_V2:
        case RequestCode.SEND_BATCH_MESSAGE:
            resource = cmd.getExtFields().get("b");
        case RequestCode.SEND_MESSAGE:
            if (resource == null) {
                resource = cmd.getExtFields().get("topic");
            }
            break;
        }
        return resource;
    }

    private String getThreadName(SingleThreadEventExecutor executor) {
        try {
            Field field = SingleThreadEventExecutor.class.getDeclaredField("thread");
            field.setAccessible(true);
            Thread thread = (Thread) field.get(executor);
            if (thread != null) {
                return thread.getName();
            }
        } catch (Exception e) {
            log.warn("getThreadName error:{}", e.toString());
        }
        return null;
    }

    public EventExecutorGroup getRateLimitEventExecutorGroup() {
        return rateLimitEventExecutorGroup;
    }

    public ConcurrentMap<String, TokenBucketRateLimiter> getRateLimiterMap() {
        return rateLimiterMap;
    }
    
    public List<TopicRateLimit> getTopicRateLimitList() {
        List<TopicRateLimit> list = new LinkedList<>();
        for (Entry<String, TokenBucketRateLimiter> entry : rateLimiterMap.entrySet()) {
            String topic = entry.getKey();
            TokenBucketRateLimiter rateLimiter = entry.getValue();
            TopicRateLimit topicRateLimit = new TopicRateLimit();
            topicRateLimit.setTopic(topic);
            topicRateLimit.setLimitQps(rateLimiter.getQps());
            topicRateLimit.setLastNeedWaitMicrosecs(rateLimiter.getLastNeedWaitMicrosecs());
            topicRateLimit.setLastRateLimitTimestamp(rateLimiter.getLastRateLimitTimestamp());
            list.add(topicRateLimit);
        }
        return list;
    }

    /**
     * 设置默认限流速率
     * 
     * @param defaultLimitQps
     */
    public void setDefaultLimitQps(double defaultLimitQps) {
        for (String resource : rateLimiterMap.keySet()) {
            if (!resource.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                rateLimiterMap.get(resource).setRate(defaultLimitQps);
            }
        }
        updateDataVersion();
        rateLimitConfigManager.persist();
    }

    /**
     * 设置重试消息限流速率
     * 
     * @param sendMsgBackLimitQps
     */
    public void setSendMsgBackLimitQps(double sendMsgBackLimitQps) {
        for (String resource : rateLimiterMap.keySet()) {
            if (resource.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                rateLimiterMap.get(resource).setRate(sendMsgBackLimitQps);
            }
        }
        updateDataVersion();
        rateLimitConfigManager.persist();
    }

    public double updateLimitQps(String resouce, double qps) {
        TokenBucketRateLimiter tokenBucketRateLimiter = rateLimiterMap.get(resouce);
        if (tokenBucketRateLimiter == null) {
            return -1;
        }
        double prevQps = tokenBucketRateLimiter.getQps();
        tokenBucketRateLimiter.setRate(qps);
        updateDataVersion();
        rateLimitConfigManager.persist();
        return prevQps;
    }

    public void updateDataVersion() {
        long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
        dataVersion.nextVersion(stateMachineVersion);
    }

    public BrokerController getBrokerController() {
        return brokerController;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public RateLimitConfigManager getRateLimitConfigManager() {
        return rateLimitConfigManager;
    }
}
