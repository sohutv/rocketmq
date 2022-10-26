package org.apache.rocketmq.broker.netty;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeaderV2;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.AsyncNettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.ResponseFuture;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.EventExecutorGroup;

public class RateLimitHandlerTest {
    private static RemotingServer remotingServer;
    private static RemotingClient remotingClient;

    public static RemotingServer createRemotingServer() throws InterruptedException {
        NettyServerConfig config = new NettyServerConfig();
        RemotingServer remotingServer = new NettyRemotingServer(config);
        RateLimitHandler handler = new RateLimitHandler(8, 1000, 100);
        Pair<EventExecutorGroup, ChannelHandler> rateLimitPair = new Pair<>(handler.getRateLimitEventExecutorGroup(),
                handler);
        ((NettyRemotingServer) remotingServer).addCustomHandlerBeforeServerHandler(rateLimitPair);
        ExecutorService es = Executors.newCachedThreadPool();
        remotingServer.registerProcessor(0, new AsyncNettyRequestProcessor() {
            @Override
            public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
                request.setRemark("Hi " + ctx.channel().remoteAddress());
                return request;
            }

            @Override
            public boolean rejectRequest() {
                return false;
            }
        }, es);

        remotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, new AsyncNettyRequestProcessor() {
            @Override
            public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
                request.setRemark("[" + RequestCode.CONSUMER_SEND_MSG_BACK + "]" + ctx.channel().remoteAddress());
                return request;
            }

            @Override
            public boolean rejectRequest() {
                return false;
            }
        }, es);

        remotingServer.registerProcessor(RequestCode.SEND_MESSAGE, new AsyncNettyRequestProcessor() {
            @Override
            public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
                request.setRemark("[" + RequestCode.SEND_MESSAGE + "]" + ctx.channel().remoteAddress());
                return request;
            }

            @Override
            public boolean rejectRequest() {
                return false;
            }
        }, es);

        remotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, new AsyncNettyRequestProcessor() {
            @Override
            public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
                request.setRemark("[" + RequestCode.SEND_MESSAGE_V2 + "]" + ctx.channel().remoteAddress());
                return request;
            }

            @Override
            public boolean rejectRequest() {
                return false;
            }
        }, es);

        remotingServer.registerProcessor(RequestCode.SEND_BATCH_MESSAGE, new AsyncNettyRequestProcessor() {
            @Override
            public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
                request.setRemark("[" + RequestCode.SEND_BATCH_MESSAGE + "]" + ctx.channel().remoteAddress());
                return request;
            }

            @Override
            public boolean rejectRequest() {
                return false;
            }
        }, es);

        remotingServer.start();

        return remotingServer;
    }

    public static RemotingClient createRemotingClient() {
        return createRemotingClient(new NettyClientConfig());
    }

    public static RemotingClient createRemotingClient(NettyClientConfig nettyClientConfig) {
        RemotingClient client = new NettyRemotingClient(nettyClientConfig);
        client.start();
        return client;
    }

    @BeforeClass
    public static void setup() throws InterruptedException {
        remotingServer = createRemotingServer();
        remotingClient = createRemotingClient();
    }

    @AfterClass
    public static void destroy() {
        remotingClient.shutdown();
        remotingServer.shutdown();
    }
    
    @Test
    public void testInvokeSync() throws InterruptedException, RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException {
        RequestHeader requestHeader = new RequestHeader();
        requestHeader.setCount(1);
        requestHeader.setMessageTitle("Welcome");
        RemotingCommand request = RemotingCommand.createRequestCommand(0, requestHeader);
        RemotingCommand response = remotingClient.invokeSync("localhost:8888", request, 1000 * 3);
        assertTrue(response != null);
        assertThat(response.getLanguage()).isEqualTo(LanguageCode.JAVA);
        assertThat(response.getExtFields()).hasSize(2);

    }

    @Test
    public void testInvokeOneway() throws InterruptedException, RemotingConnectException, RemotingTimeoutException,
            RemotingTooMuchRequestException, RemotingSendRequestException {

        RemotingCommand request = RemotingCommand.createRequestCommand(0, null);
        request.setRemark("messi");
        remotingClient.invokeOneway("localhost:8888", request, 1000 * 3);
    }

    @Test
    public void testInvokeAsync() throws InterruptedException, RemotingConnectException, RemotingTimeoutException,
            RemotingTooMuchRequestException, RemotingSendRequestException {

        final CountDownLatch latch = new CountDownLatch(1);
        RemotingCommand request = RemotingCommand.createRequestCommand(0, null);
        request.setRemark("messi");
        remotingClient.invokeAsync("localhost:8888", request, 1000 * 3, new InvokeCallback() {
            @Override
            public void operationComplete(ResponseFuture responseFuture) {
                latch.countDown();
                assertTrue(responseFuture != null);
                assertThat(responseFuture.getResponseCommand().getLanguage()).isEqualTo(LanguageCode.JAVA);
                assertThat(responseFuture.getResponseCommand().getExtFields()).hasSize(2);
            }
        });
        latch.await();
    }

    @Test
    public void testSendMsg() throws InterruptedException, RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException {
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        requestHeader.setProducerGroup("testGroup");
        requestHeader.setTopic("abc-topic");
        requestHeader.setDefaultTopicQueueNums(1);
        requestHeader.setQueueId(1);
        requestHeader.setBornTimestamp(System.currentTimeMillis());
        requestHeader.setReconsumeTimes(0);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, requestHeader);
        RemotingCommand response = remotingClient.invokeSync("localhost:8888", request, 1000 * 60);
        assertTrue(response != null);
        assertTrue(response.getRemark().contains("[" + RequestCode.SEND_MESSAGE + "]"));
    }

    @Test
    public void testSendMsgRateLimit() throws InterruptedException, RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException {
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        String topic = "abc-topic";
        requestHeader.setProducerGroup("testGroup");
        requestHeader.setTopic(topic);
        requestHeader.setDefaultTopicQueueNums(1);
        requestHeader.setQueueId(1);
        requestHeader.setBornTimestamp(System.currentTimeMillis());
        requestHeader.setReconsumeTimes(0);
        for (int i = 0; i < 50000; ++i) {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, requestHeader);
            RemotingCommand response = remotingClient.invokeSync("localhost:8888", request, 1000 * 60);
            assertTrue(response != null);
            if (response.getCode() == 2) {
                System.out.println(i + "=" + response.getCode() + "=" + response.getRemark());
                assertTrue(response.getRemark().contains(topic));
                Thread.sleep(1000);
            } else {
                assertTrue(response.getRemark().contains("[" + RequestCode.SEND_MESSAGE + "]"));
            }
        }
    }

    @Test
    public void testSendMsgV2() throws InterruptedException, RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException {
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        requestHeader.setProducerGroup("testGroup");
        requestHeader.setTopic("abc-topic");
        requestHeader.setDefaultTopicQueueNums(1);
        requestHeader.setQueueId(1);
        requestHeader.setBornTimestamp(System.currentTimeMillis());
        requestHeader.setReconsumeTimes(0);
        SendMessageRequestHeaderV2 requestHeaderV2 = SendMessageRequestHeaderV2
                .createSendMessageRequestHeaderV2(requestHeader);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE_V2, requestHeaderV2);
        RemotingCommand response = remotingClient.invokeSync("localhost:8888", request, 1000 * 60);
        assertTrue(response != null);
        assertTrue(response.getRemark().contains("[" + RequestCode.SEND_MESSAGE_V2 + "]"));
    }

    @Test
    public void testSendMsgV2RateLimit() throws InterruptedException, RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException {
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        String topic = "abc-topic";
        requestHeader.setProducerGroup("testGroup");
        requestHeader.setTopic(topic);
        requestHeader.setDefaultTopicQueueNums(1);
        requestHeader.setQueueId(1);
        requestHeader.setBornTimestamp(System.currentTimeMillis());
        requestHeader.setReconsumeTimes(0);
        SendMessageRequestHeaderV2 requestHeaderV2 = SendMessageRequestHeaderV2
                .createSendMessageRequestHeaderV2(requestHeader);
        for (int i = 0; i < 50000; ++i) {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE_V2, requestHeaderV2);
            RemotingCommand response = remotingClient.invokeSync("localhost:8888", request, 1000 * 60);
            assertTrue(response != null);
            if (response.getCode() == 2) {
                System.out.println(i + "=" + response.getCode() + "=" + response.getRemark());
                assertTrue(response.getRemark().contains(topic));
                Thread.sleep(1000);
            } else {
                assertTrue(response.getRemark().contains("[" + RequestCode.SEND_MESSAGE_V2 + "]"));
            }
        }
    }

    @Test
    public void testSendMsgBatch() throws InterruptedException, RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException {
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        requestHeader.setProducerGroup("testGroup");
        requestHeader.setTopic("abc-topic");
        requestHeader.setDefaultTopicQueueNums(1);
        requestHeader.setQueueId(1);
        requestHeader.setBornTimestamp(System.currentTimeMillis());
        requestHeader.setReconsumeTimes(0);
        SendMessageRequestHeaderV2 requestHeaderV2 = SendMessageRequestHeaderV2
                .createSendMessageRequestHeaderV2(requestHeader);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_BATCH_MESSAGE, requestHeaderV2);
        RemotingCommand response = remotingClient.invokeSync("localhost:8888", request, 1000 * 60);
        assertTrue(response != null);
        assertTrue(response.getRemark().contains("[" + RequestCode.SEND_BATCH_MESSAGE + "]"));
    }

    @Test
    public void testSendMsgBatchRateLimit() throws InterruptedException, RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException {
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        String topic = "abc-topic";
        requestHeader.setProducerGroup("testGroup");
        requestHeader.setTopic(topic);
        requestHeader.setDefaultTopicQueueNums(1);
        requestHeader.setQueueId(1);
        requestHeader.setBornTimestamp(System.currentTimeMillis());
        requestHeader.setReconsumeTimes(0);
        SendMessageRequestHeaderV2 requestHeaderV2 = SendMessageRequestHeaderV2
                .createSendMessageRequestHeaderV2(requestHeader);
        for (int i = 0; i < 50000; ++i) {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_BATCH_MESSAGE, requestHeaderV2);
            RemotingCommand response = remotingClient.invokeSync("localhost:8888", request, 1000 * 60);
            assertTrue(response != null);
            if (response.getCode() == 2) {
                System.out.println(i + "=" + response.getCode() + "=" + response.getRemark());
                assertTrue(response.getRemark().contains(topic));
                Thread.sleep(1000);
            } else {
                assertTrue(response.getRemark().contains("[" + RequestCode.SEND_BATCH_MESSAGE + "]"));
            }
        }
    }

    @Test
    public void testSendMsgBack() throws InterruptedException, RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException {
        ConsumerSendMsgBackRequestHeader requestHeader = new ConsumerSendMsgBackRequestHeader();
        requestHeader.setGroup("testGroup");
        requestHeader.setOriginTopic("abc-topic");
        requestHeader.setOffset(1L);
        requestHeader.setDelayLevel(2);
        requestHeader.setOriginMsgId("123abc");
        requestHeader.setMaxReconsumeTimes(14);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK,
                requestHeader);
        RemotingCommand response = remotingClient.invokeSync("localhost:8888", request, 1000 * 60);
        assertTrue(response != null);
        assertTrue(response.getRemark().contains("[" + RequestCode.CONSUMER_SEND_MSG_BACK + "]"));
    }

    @Test
    public void testSendMsgBackRateLimit() throws InterruptedException, RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException {
        ConsumerSendMsgBackRequestHeader requestHeader = new ConsumerSendMsgBackRequestHeader();
        String group = "testGroup";
        requestHeader.setGroup(group);
        requestHeader.setOriginTopic("abc-topic");
        requestHeader.setOffset(1L);
        requestHeader.setDelayLevel(2);
        requestHeader.setOriginMsgId("123abc");
        requestHeader.setMaxReconsumeTimes(14);
        for (int i = 0; i < 1000; ++i) {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK,
                    requestHeader);
            RemotingCommand response = remotingClient.invokeSync("localhost:8888", request, 1000 * 60);
            assertTrue(response != null);
            if (response.getCode() == 2) {
                System.out.println(i + "=" + response.getCode() + "=" + response.getRemark());
                assertTrue(response.getRemark().contains(group));
                Thread.sleep(1000);
            } else {
                System.out.println(i + "+" + response.getCode() + "=" + response.getRemark());
                assertTrue(response.getRemark().contains("[" + RequestCode.CONSUMER_SEND_MSG_BACK + "]"));
            }
        }
    }

    @Test
    public void testThreadRejectRateLimit() throws InterruptedException, RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException {
        int threads = 40;
        ExecutorService es = Executors.newFixedThreadPool(threads);
        List<Future<?>> list = new ArrayList<>();
        for (int t = 0; t < threads; ++t) {
            list.add(es.submit(new SendMessageTask(t)));
        }
        list.forEach(f->{
            try {
                f.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
    
    @Test
    public void testSendMsgAndBackMsgRateLimit() throws InterruptedException, RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException {
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        String topic = "abc-topic";
        requestHeader.setProducerGroup("testGroup");
        requestHeader.setTopic(topic);
        requestHeader.setDefaultTopicQueueNums(1);
        requestHeader.setQueueId(1);
        requestHeader.setBornTimestamp(System.currentTimeMillis());
        requestHeader.setReconsumeTimes(0);
        
        
        ConsumerSendMsgBackRequestHeader requestHeader2 = new ConsumerSendMsgBackRequestHeader();
        String group = "testGroup";
        requestHeader2.setGroup(group);
        requestHeader2.setOriginTopic("abc-topic");
        requestHeader2.setOffset(1L);
        requestHeader2.setDelayLevel(2);
        requestHeader2.setOriginMsgId("123abc");
        requestHeader2.setMaxReconsumeTimes(14);
        
        for (int i = 0; i < Integer.MAX_VALUE; ++i) {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, requestHeader);
            RemotingCommand response = remotingClient.invokeSync("localhost:8888", request, 1000 * 60);
            assertTrue(response != null);
            if (response.getCode() == 2) {
                System.out.println(i + "=" + response.getCode() + "=" + response.getRemark());
                assertTrue(response.getRemark().contains(topic));
                Thread.sleep(1000);
            } else {
                assertTrue(response.getRemark().contains("[" + RequestCode.SEND_MESSAGE + "]"));
            }
            
            RemotingCommand request2 = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK,
                    requestHeader2);
            RemotingCommand response2 = remotingClient.invokeSync("localhost:8888", request2, 1000 * 60);
            assertTrue(response2 != null);
            if (response2.getCode() == 2) {
                System.out.println(i + "=" + response2.getCode() + "=" + response2.getRemark());
                assertTrue(response2.getRemark().contains(group));
                Thread.sleep(1000);
            } else {
                assertTrue(response2.getRemark().contains("[" + RequestCode.CONSUMER_SEND_MSG_BACK + "]"));
            }
        }
    }
    
    class SendMessageTask implements Runnable {
        private int taskId;
        
        public SendMessageTask(int taskId) {
            this.taskId = taskId;
        }
        
        @Override
        public void run() {
            ConsumerSendMsgBackRequestHeader requestHeader = new ConsumerSendMsgBackRequestHeader();
            String group = "testGroup";
            requestHeader.setGroup(group);
            requestHeader.setOriginTopic("abc-topic");
            requestHeader.setOffset((long) taskId);
            requestHeader.setOriginMsgId("123abc");
            requestHeader.setMaxReconsumeTimes(14);
            RemotingClient rc = createRemotingClient();
            try {
                for (int i = 0; i < 1000; ++i) {
                    requestHeader.setDelayLevel(i);
                    RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, requestHeader);
                    RemotingCommand response = rc.invokeSync("localhost:8888", request, 1000 * 60);
                    assertTrue(response != null);
                    if (response.getCode() == 2) {
                        assertTrue(response.getRemark().contains(group));
                    } else {
                        assertTrue(response.getRemark().contains("[" + RequestCode.CONSUMER_SEND_MSG_BACK + "]"));
                    }
                }
                rc.shutdown();
                System.out.println("task " + taskId + " over");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        
    }
}


class RequestHeader implements CommandCustomHeader {
    @CFNullable
    private Integer count;

    @CFNullable
    private String messageTitle;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public String getMessageTitle() {
        return messageTitle;
    }

    public void setMessageTitle(String messageTitle) {
        this.messageTitle = messageTitle;
    }
}
