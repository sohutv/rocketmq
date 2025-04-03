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
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.Connection;
import org.apache.rocketmq.remoting.protocol.body.ProducerConnection;
import org.apache.rocketmq.remoting.protocol.body.ProducerTableInfo;
import org.apache.rocketmq.remoting.protocol.header.GetAllProducerInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetProducerConnectionListRequestHeader;

import java.util.Collection;
import java.util.Iterator;

/**
 * 生产者管理
 *
 * @author yongfeigao
 * @date 2023/6/19
 */
public class ProducerManagerActivity extends AbstractRemotingActivity {

    public ProducerManagerActivity(RequestPipeline requestPipeline, MessagingProcessor messagingProcessor) {
        super(requestPipeline, messagingProcessor);
    }

    @Override
    protected RemotingCommand processRequest0(ChannelHandlerContext ctx, RemotingCommand request,
                                              ProxyContext context) throws Exception {
        switch (request.getCode()) {
            case RequestCode.GET_PRODUCER_CONNECTION_LIST:
                return getProducerConnectionList(ctx, request);
            case RequestCode.GET_ALL_PRODUCER_INFO:
                return getAllProducerInfo(ctx, request);
            default:
                break;
        }
        return null;
    }

    private RemotingCommand getProducerConnectionList(ChannelHandlerContext ctx,
                                                      RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetProducerConnectionListRequestHeader requestHeader =
                (GetProducerConnectionListRequestHeader) request.decodeCommandCustomHeader(GetProducerConnectionListRequestHeader.class);
        ProducerConnection producerConnection = new ProducerConnection();
        Collection<ClientChannelInfo> channelInfos = messagingProcessor.getProducerGroupChannelInfo(requestHeader.getProducerGroup());
        if (channelInfos != null) {
            Iterator<ClientChannelInfo> it = channelInfos.iterator();
            while (it.hasNext()) {
                ClientChannelInfo info = it.next();
                Connection connection = new Connection();
                connection.setClientId(info.getClientId());
                connection.setLanguage(info.getLanguage());
                connection.setVersion(info.getVersion());
                connection.setClientAddr(RemotingHelper.parseChannelRemoteAddr(info.getChannel()));
                producerConnection.getConnectionSet().add(connection);
            }
            byte[] body = producerConnection.encode();
            response.setBody(body);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }
        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark("the producer group[" + requestHeader.getProducerGroup() + "] not exist");
        return response;
    }

    private RemotingCommand getAllProducerInfo(ChannelHandlerContext ctx,
                                               RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetAllProducerInfoRequestHeader requestHeader =
                request.decodeCommandCustomHeader(GetAllProducerInfoRequestHeader.class);
        ProducerTableInfo producerTable = messagingProcessor.getServiceManager().getProducerManager()
                .getProducerTable(requestHeader.isExcludeSystemGroup());
        if (producerTable != null) {
            byte[] body = producerTable.encode();
            response.setBody(body);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }
        response.setCode(ResponseCode.SYSTEM_ERROR);
        return response;
    }
}
