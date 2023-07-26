/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.remoting.activity;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.remoting.channel.RemotingChannel;
import org.apache.rocketmq.proxy.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.*;
import org.apache.rocketmq.remoting.protocol.header.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class ConsumerManagerActivity extends AbstractRemotingActivity {
    public ConsumerManagerActivity(RequestPipeline requestPipeline, MessagingProcessor messagingProcessor) {
        super(requestPipeline, messagingProcessor);
    }

    @Override
    protected RemotingCommand processRequest0(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        switch (request.getCode()) {
            case RequestCode.GET_CONSUMER_LIST_BY_GROUP: {
                return getConsumerListByGroup(ctx, request, context);
            }
            case RequestCode.LOCK_BATCH_MQ: {
                return lockBatchMQ(ctx, request, context);
            }
            case RequestCode.UNLOCK_BATCH_MQ: {
                return unlockBatchMQ(ctx, request, context);
            }
            case RequestCode.UPDATE_CONSUMER_OFFSET:
            case RequestCode.QUERY_CONSUMER_OFFSET:
            case RequestCode.SEARCH_OFFSET_BY_TIMESTAMP:
            case RequestCode.GET_MIN_OFFSET:
            case RequestCode.GET_MAX_OFFSET:
            case RequestCode.GET_EARLIEST_MSG_STORETIME: {
                return request(ctx, request, context, Duration.ofSeconds(3).toMillis());
            }
            case RequestCode.GET_CONSUMER_CONNECTION_LIST: {
                return getConsumerConnectionList(ctx, request, context);
            }
            case RequestCode.GET_CONSUMER_RUNNING_INFO: {
                return getConsumerRunningInfo(ctx, request, context);
            }
            case RequestCode.RESET_CONSUMER_CLIENT_OFFSET: {
                return resetConsumerClientOffset(ctx, request, context);
            }
            case RequestCode.CONSUME_MESSAGE_DIRECTLY: {
                return consumeMessageDirectly(ctx, request, context);
            }
            case RequestCode.INVOKE_BROKER_TO_GET_CONSUMER_STATUS: {
                return getConsumerStatusFromClient(ctx, request, context);
            }
            default:
                break;
        }
        return null;
    }

    protected RemotingCommand getConsumerListByGroup(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(GetConsumerListByGroupResponseHeader.class);
        GetConsumerListByGroupRequestHeader header = (GetConsumerListByGroupRequestHeader) request.decodeCommandCustomHeader(GetConsumerListByGroupRequestHeader.class);
        ConsumerGroupInfo consumerGroupInfo = messagingProcessor.getConsumerGroupInfo(header.getConsumerGroup());
        GetConsumerListByGroupResponseBody body = new GetConsumerListByGroupResponseBody();
        body.setConsumerIdList(consumerGroupInfo.getAllClientId());
        response.setBody(body.encode());
        response.setCode(ResponseCode.SUCCESS);
        return response;
    }

    protected RemotingCommand getConsumerConnectionList(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(GetConsumerConnectionListRequestHeader.class);
        GetConsumerConnectionListRequestHeader header = (GetConsumerConnectionListRequestHeader) request.decodeCommandCustomHeader(GetConsumerConnectionListRequestHeader.class);
        ConsumerGroupInfo consumerGroupInfo = messagingProcessor.getConsumerGroupInfo(header.getConsumerGroup());
        if (consumerGroupInfo != null) {
            ConsumerConnection bodydata = new ConsumerConnection();
            bodydata.setConsumeFromWhere(consumerGroupInfo.getConsumeFromWhere());
            bodydata.setConsumeType(consumerGroupInfo.getConsumeType());
            bodydata.setMessageModel(consumerGroupInfo.getMessageModel());
            bodydata.getSubscriptionTable().putAll(consumerGroupInfo.getSubscriptionTable());

            for (ClientChannelInfo info : consumerGroupInfo.getChannelInfoTable().values()) {
                Connection connection = new Connection();
                connection.setClientId(info.getClientId());
                connection.setLanguage(info.getLanguage());
                connection.setVersion(info.getVersion());
                connection.setClientAddr(RemotingHelper.parseChannelRemoteAddr(info.getChannel()));

                bodydata.getConnectionSet().add(connection);
            }

            byte[] body = bodydata.encode();
            response.setBody(body);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);

            return response;
        }

        response.setCode(ResponseCode.CONSUMER_NOT_ONLINE);
        response.setRemark("the consumer group[" + header.getConsumerGroup() + "] not online");
        return response;
    }

    protected RemotingCommand lockBatchMQ(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        LockBatchRequestBody requestBody = LockBatchRequestBody.decode(request.getBody(), LockBatchRequestBody.class);
        Set<MessageQueue> mqSet = requestBody.getMqSet();
        if (mqSet.isEmpty()) {
            response.setBody(requestBody.encode());
            response.setRemark("MessageQueue set is empty");
            return response;
        }

        String brokerName = new ArrayList<>(mqSet).get(0).getBrokerName();
        messagingProcessor.request(context, brokerName, request, Duration.ofSeconds(3).toMillis())
            .thenAccept(r -> writeResponse(ctx, context, request, r))
            .exceptionally(t -> {
                writeErrResponse(ctx, context, request, t);
                return null;
            });
        return null;
    }

    protected RemotingCommand unlockBatchMQ(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        UnlockBatchRequestBody requestBody = UnlockBatchRequestBody.decode(request.getBody(), UnlockBatchRequestBody.class);
        Set<MessageQueue> mqSet = requestBody.getMqSet();
        if (mqSet.isEmpty()) {
            response.setBody(requestBody.encode());
            response.setRemark("MessageQueue set is empty");
            return response;
        }

        String brokerName = new ArrayList<>(mqSet).get(0).getBrokerName();
        messagingProcessor.request(context, brokerName, request, Duration.ofSeconds(3).toMillis())
            .thenAccept(r -> writeResponse(ctx, context, request, r))
            .exceptionally(t -> {
                writeErrResponse(ctx, context, request, t);
                return null;
            });
        return null;
    }

    /**
     * Get consumer running info.
     *
     * @param ctx
     * @param request
     * @param context
     * @return
     * @throws RemotingCommandException
     */
    public RemotingCommand getConsumerRunningInfo(ChannelHandlerContext ctx, RemotingCommand request,
                                                  ProxyContext context) throws Exception {
        // 解析请求头
        GetConsumerRunningInfoRequestHeader requestHeader = (GetConsumerRunningInfoRequestHeader)
                request.decodeCommandCustomHeader(GetConsumerRunningInfoRequestHeader.class);
        String consumerGroup = requestHeader.getConsumerGroup();
        String clientId = requestHeader.getClientId();
        ConsumerGroupInfo consumerGroupInfo = messagingProcessor.getConsumerGroupInfo(consumerGroup);
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        if (consumerGroupInfo == null) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("consumer group <%s> not exist", consumerGroup));
            return response;
        }
        ClientChannelInfo clientChannelInfo = findClientChannelInfo(consumerGroupInfo, clientId);
        if (clientChannelInfo == null) {
            response.setCode(ResponseCode.REQUEST_CODE_NOT_SUPPORTED);
            response.setRemark(String.format("consumer group <%s> clientId <%s> not connect me", consumerGroup, clientId));
            return response;
        }
        // 向客户端转发请求
        RemotingChannel remotingChannel = (RemotingChannel) clientChannelInfo.getChannel();
        CompletableFuture<RemotingCommand> responseFuture = remotingChannel.invokeToClient(request);
        responseFuture.thenAccept(r -> {
            writeResponse(ctx, context, request, r);
        }).exceptionally(t -> {
            log.error("get consumer:{} clientId:{} running info error", consumerGroup, clientId, t);
            writeErrResponse(ctx, context, request, t);
            return null;
        });
        return null;
    }

    /**
     * 直接消费
     *
     * @param ctx
     * @param request
     * @param context
     * @return
     * @throws RemotingCommandException
     */
    public RemotingCommand consumeMessageDirectly(ChannelHandlerContext ctx, RemotingCommand request,
                                                  ProxyContext context) throws Exception {
        // 解析请求头
        ConsumeMessageDirectlyResultRequestHeader requestHeader = (ConsumeMessageDirectlyResultRequestHeader)
                request.decodeCommandCustomHeader(ConsumeMessageDirectlyResultRequestHeader.class);
        String consumerGroup = requestHeader.getConsumerGroup();
        String clientId = requestHeader.getClientId();
        ConsumerGroupInfo consumerGroupInfo = messagingProcessor.getConsumerGroupInfo(consumerGroup);
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        if (consumerGroupInfo == null) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("consumer group <%s> not exist", consumerGroup));
            return response;
        }
        ClientChannelInfo clientChannelInfo = findClientChannelInfo(consumerGroupInfo, clientId);
        if (clientChannelInfo == null) {
            response.setCode(ResponseCode.REQUEST_CODE_NOT_SUPPORTED);
            response.setRemark(String.format("consumer group <%s> clientId <%s> not connect me", consumerGroup, clientId));
            return response;
        }
        // 向客户端转发请求
        RemotingChannel remotingChannel = (RemotingChannel) clientChannelInfo.getChannel();
        CompletableFuture<RemotingCommand> responseFuture = remotingChannel.invokeToClient(request);
        responseFuture.thenAccept(r -> {
            writeResponse(ctx, context, request, r);
        }).exceptionally(t -> {
            log.error("consumeMessageDirectly consumer:{} clientId:{} error", consumerGroup, clientId, t);
            writeErrResponse(ctx, context, request, t);
            return null;
        });
        return null;
    }

    /**
     * 重置客户端的offset.
     * @param ctx
     * @param request
     * @param context
     * @return
     * @throws RemotingCommandException
     */
    public RemotingCommand resetConsumerClientOffset(ChannelHandlerContext ctx, RemotingCommand request,
                                                     ProxyContext context) throws Exception {
        ResetOffsetRequestHeader requestHeader = (ResetOffsetRequestHeader)
                request.decodeCommandCustomHeader(ResetOffsetRequestHeader.class);
        ConsumerGroupInfo consumerGroupInfo = messagingProcessor.getConsumerGroupInfo(requestHeader.getGroup());
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        if (consumerGroupInfo == null) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("consumer group <%s> not exist", requestHeader.getGroup()));
            return response;
        }
        Collection<ClientChannelInfo> clientChannelInfos = consumerGroupInfo.getChannelInfoTable().values();
        if (clientChannelInfos == null) {
            response.setCode(ResponseCode.REQUEST_CODE_NOT_SUPPORTED);
            response.setRemark(String.format("consumer group <%s> not connect me", requestHeader.getGroup()));
            return response;
        }
        for (ClientChannelInfo clientChannelInfo : clientChannelInfos) {
            RemotingChannel remotingChannel = (RemotingChannel) clientChannelInfo.getChannel();
            remotingChannel.invokeToClientOneway(request);
            log.info("invokeClientToResetOffset success. clientAddr:{} clientId:{} topic:{} consumer:{} timestamp:{}",
                    remotingChannel.getRemoteAddress(), remotingChannel.getClientId(), requestHeader.getTopic(),
                    requestHeader.getGroup(), requestHeader.getTimestamp());
        }
        return null;
    }

    /**
     * Get consumer status from client.
     *
     * @param ctx
     * @param request
     * @param context
     * @return
     * @throws RemotingCommandException
     */
    public RemotingCommand getConsumerStatusFromClient(ChannelHandlerContext ctx, RemotingCommand request,
                                                  ProxyContext context) throws Exception {
        // 解析请求头
        GetConsumerStatusRequestHeader requestHeader = (GetConsumerStatusRequestHeader)
                request.decodeCommandCustomHeader(GetConsumerStatusRequestHeader.class);
        String consumerGroup = requestHeader.getGroup();
        String clientId = requestHeader.getClientAddr();
        ConsumerGroupInfo consumerGroupInfo = messagingProcessor.getConsumerGroupInfo(consumerGroup);
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        if (consumerGroupInfo == null) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("consumer group <%s> not exist", consumerGroup));
            return response;
        }
        ClientChannelInfo clientChannelInfo = findClientChannelInfo(consumerGroupInfo, clientId);
        if (clientChannelInfo == null) {
            response.setCode(ResponseCode.REQUEST_CODE_NOT_SUPPORTED);
            response.setRemark(String.format("consumer group <%s> clientId <%s> not connect me", consumerGroup, clientId));
            return response;
        }
        // 向客户端转发请求
        GetConsumerStatusRequestHeader header = new GetConsumerStatusRequestHeader();
        header.setTopic(requestHeader.getTopic());
        header.setGroup(consumerGroup);
        RemotingCommand getConsumerStatusRequest =
                RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT, requestHeader);
        RemotingChannel remotingChannel = (RemotingChannel) clientChannelInfo.getChannel();
        CompletableFuture<RemotingCommand> responseFuture = remotingChannel.invokeToClient(getConsumerStatusRequest);
        responseFuture.thenAccept(r -> {
            Map<String, Map<MessageQueue, Long>> consumerStatusTable = new HashMap<String, Map<MessageQueue, Long>>();
            if (r.getBody() != null) {
                GetConsumerStatusBody body = GetConsumerStatusBody.decode(r.getBody(), GetConsumerStatusBody.class);
                consumerStatusTable.put(clientId, body.getMessageQueueTable());
            }
            response.setCode(ResponseCode.SUCCESS);
            GetConsumerStatusBody resBody = new GetConsumerStatusBody();
            resBody.setConsumerTable(consumerStatusTable);
            response.setBody(resBody.encode());
            writeResponse(ctx, context, request, response);
        }).exceptionally(t -> {
            log.error("get consumer:{} clientId:{} running info error", consumerGroup, clientId, t);
            writeErrResponse(ctx, context, request, t);
            return null;
        });
        return null;
    }

    /**
     * 查找客户端连接
     *
     * @param consumerGroupInfo
     * @param clientId
     * @return
     */
    public ClientChannelInfo findClientChannelInfo(ConsumerGroupInfo consumerGroupInfo, String clientId) {
        return consumerGroupInfo.getChannelInfoTable().values().stream()
                .filter(clientChannelInfo -> clientChannelInfo.getClientId().equals(clientId))
                .findFirst()
                .orElse(null);
    }
}
