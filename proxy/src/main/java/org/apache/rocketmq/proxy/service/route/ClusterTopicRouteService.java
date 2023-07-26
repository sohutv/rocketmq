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
package org.apache.rocketmq.proxy.service.route;

import com.google.common.net.HostAndPort;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.namesrv.DefaultTopAddressing;
import org.apache.rocketmq.common.namesrv.TopAddressing;
import org.apache.rocketmq.proxy.common.Address;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ClusterTopicRouteService extends TopicRouteService {

    private TopAddressing proxyAddressing;

    private String proxyAddress;

    public ClusterTopicRouteService(MQClientAPIFactory mqClientAPIFactory) {
        super(mqClientAPIFactory);
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        if (config.getNamesrvAddr() == null) {
            Map<String, String> map = new HashMap<>();
            map.put("protocol", "2");
            proxyAddressing = new DefaultTopAddressing(null, map, MixAll.getWSAddr());
            fetchProxyAddress();
            scheduledExecutorService.scheduleAtFixedRate(
                    this::fetchProxyAddress,
                    Duration.ofMinutes(1).toMillis(),
                    Duration.ofMinutes(1).toMillis(),
                    TimeUnit.MILLISECONDS
            );
        }
    }

    private void fetchProxyAddress() {
        String tmpProxyAddress = proxyAddressing.fetchNSAddr();
        if (tmpProxyAddress != null && !tmpProxyAddress.equals(proxyAddress)) {
            log.info("proxy address changed from {} to {}", proxyAddress, tmpProxyAddress);
            proxyAddress = tmpProxyAddress;
        }
    }

    @Override
    public MessageQueueView getCurrentMessageQueueView(String topicName) throws Exception {
        return getAllMessageQueueView(topicName);
    }

    @Override
    public ProxyTopicRouteData getTopicRouteForProxy(List<Address> requestHostAndPortList,
        String topicName) throws Exception {
        TopicRouteData topicRouteData = getAllMessageQueueView(topicName).getTopicRouteData();

        ProxyTopicRouteData proxyTopicRouteData = new ProxyTopicRouteData();
        proxyTopicRouteData.setQueueDatas(topicRouteData.getQueueDatas());

        if (requestHostAndPortList != null) {
            for (BrokerData brokerData : topicRouteData.getBrokerDatas()) {
                ProxyTopicRouteData.ProxyBrokerData proxyBrokerData = new ProxyTopicRouteData.ProxyBrokerData();
                proxyBrokerData.setCluster(brokerData.getCluster());
                proxyBrokerData.setBrokerName(brokerData.getBrokerName());
                for (Long brokerId : brokerData.getBrokerAddrs().keySet()) {
                    proxyBrokerData.getBrokerAddrs().put(brokerId, requestHostAndPortList);
                }
                proxyTopicRouteData.getBrokerDatas().add(proxyBrokerData);
            }
        } else {
            // broker根据名字排序
            List<BrokerData> brokerDataList = topicRouteData.getBrokerDatas();
            Collections.sort(brokerDataList);

            // proxy根据ip排序
            String[] proxyAddressArray = proxyAddress.split(";");
            List<Address> proxyAddressList = Arrays.stream(proxyAddressArray).sorted().map(addr -> {
                String[] ipAndPort = addr.split(":");
                return new Address(Address.AddressScheme.IPv4, HostAndPort.fromParts(ipAndPort[0], Integer.parseInt(ipAndPort[1])));
            }).collect(Collectors.toList());

            for (int i = 0; i < brokerDataList.size(); ++i) {
                BrokerData brokerData = brokerDataList.get(i);
                ProxyTopicRouteData.ProxyBrokerData proxyBrokerData = new ProxyTopicRouteData.ProxyBrokerData();
                proxyBrokerData.setCluster(brokerData.getCluster());
                proxyBrokerData.setBrokerName(brokerData.getBrokerName());
                int index = i % proxyAddressList.size();
                List<Address> addressList = new ArrayList<>();
                addressList.add(proxyAddressList.get(index));
                for (Long brokerId : brokerData.getBrokerAddrs().keySet()) {
                    proxyBrokerData.getBrokerAddrs().put(brokerId, addressList);
                }
                proxyTopicRouteData.getBrokerDatas().add(proxyBrokerData);
            }
        }

        return proxyTopicRouteData;
    }

    @Override
    public String getBrokerAddr(String brokerName) throws Exception {
        TopicRouteWrapper topicRouteWrapper = getAllMessageQueueView(brokerName).getTopicRouteWrapper();
        return topicRouteWrapper.getMasterAddr(brokerName);
    }

    @Override
    public AddressableMessageQueue buildAddressableMessageQueue(MessageQueue messageQueue) throws Exception {
        String brokerAddress = getBrokerAddr(messageQueue.getBrokerName());
        return new AddressableMessageQueue(messageQueue, brokerAddress);
    }
}
