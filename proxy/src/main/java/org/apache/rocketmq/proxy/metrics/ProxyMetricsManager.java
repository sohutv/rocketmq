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
package org.apache.rocketmq.proxy.metrics;

import com.google.common.base.Splitter;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.exporter.logging.LoggingMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporterBuilder;
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.*;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.broker.metrics.ConsumerAttr;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.metrics.NopObservableLongGauge;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.common.thread.ThreadPoolWrapper;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.apache.rocketmq.remoting.metrics.RemotingMetricsManager;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.*;
import static org.apache.rocketmq.proxy.metrics.ProxyMetricsConstant.*;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_PROTOCOL_TYPE;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.PROTOCOL_TYPE_REMOTING;

public class ProxyMetricsManager implements StartAndShutdown {
    private final static Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private static ProxyConfig proxyConfig;
    private final static Map<String, String> LABEL_MAP = new HashMap<>();
    public static Supplier<AttributesBuilder> attributesBuilderSupplier;

    // client connection metrics
    public static ObservableLongGauge producerConnection = new NopObservableLongGauge();
    public static ObservableLongGauge consumerConnection = new NopObservableLongGauge();

    public static ObservableLongGauge processorWatermark = new NopObservableLongGauge();

    private OtlpGrpcMetricExporter metricExporter;
    private PeriodicMetricReader periodicMetricReader;
    private PrometheusHttpServer prometheusHttpServer;
    private LoggingMetricExporter loggingMetricExporter;

    private ServiceManager serviceManager;

    public static ObservableLongGauge proxyUp = null;

    public static void initLocalMode(BrokerMetricsManager brokerMetricsManager, ProxyConfig proxyConfig) {
        if (proxyConfig.getMetricsExporterType() == BrokerConfig.MetricsExporterType.DISABLE) {
            return;
        }
        ProxyMetricsManager.proxyConfig = proxyConfig;
        LABEL_MAP.put(LABEL_NODE_TYPE, NODE_TYPE_PROXY);
        LABEL_MAP.put(LABEL_CLUSTER_NAME, proxyConfig.getProxyClusterName());
        LABEL_MAP.put(LABEL_NODE_ID, proxyConfig.getProxyName());
        LABEL_MAP.put(LABEL_PROXY_MODE, proxyConfig.getProxyMode().toLowerCase());
        initMetrics(brokerMetricsManager.getBrokerMeter(), BrokerMetricsManager::newAttributesBuilder);
    }

    public static ProxyMetricsManager initClusterMode(ProxyConfig proxyConfig, ServiceManager serviceManager) {
        ProxyMetricsManager.proxyConfig = proxyConfig;
        ProxyMetricsManager proxyMetricsManager = new ProxyMetricsManager();
        proxyMetricsManager.serviceManager = serviceManager;
        return proxyMetricsManager;
    }

    public static AttributesBuilder newAttributesBuilder() {
        AttributesBuilder attributesBuilder;
        if (attributesBuilderSupplier == null) {
            attributesBuilder = Attributes.builder();
            LABEL_MAP.forEach(attributesBuilder::put);
            return attributesBuilder;
        }
        attributesBuilder = attributesBuilderSupplier.get();
        LABEL_MAP.forEach(attributesBuilder::put);
        return attributesBuilder;
    }

    private static void initMetrics(Meter meter, Supplier<AttributesBuilder> attributesBuilderSupplier) {
        ProxyMetricsManager.attributesBuilderSupplier = attributesBuilderSupplier;

        proxyUp = meter.gaugeBuilder(GAUGE_PROXY_UP)
            .setDescription("proxy status")
            .ofLongs()
            .buildWithCallback(measurement -> measurement.record(1, newAttributesBuilder().build()));
    }

    public ProxyMetricsManager() {
    }

    private boolean checkConfig() {
        if (proxyConfig == null) {
            return false;
        }
        BrokerConfig.MetricsExporterType exporterType = proxyConfig.getMetricsExporterType();
        if (!exporterType.isEnable()) {
            return false;
        }

        switch (exporterType) {
            case OTLP_GRPC:
                return StringUtils.isNotBlank(proxyConfig.getMetricsGrpcExporterTarget());
            case PROM:
                return true;
            case LOG:
                return true;
        }
        return false;
    }

    @Override
    public void start() throws Exception {
        BrokerConfig.MetricsExporterType metricsExporterType = proxyConfig.getMetricsExporterType();
        if (metricsExporterType == BrokerConfig.MetricsExporterType.DISABLE) {
            return;
        }
        if (!checkConfig()) {
            log.error("check metrics config failed, will not export metrics");
            return;
        }

        String labels = proxyConfig.getMetricsLabel();
        if (StringUtils.isNotBlank(labels)) {
            List<String> kvPairs = Splitter.on(',').omitEmptyStrings().splitToList(labels);
            for (String item : kvPairs) {
                String[] split = item.split(":");
                if (split.length != 2) {
                    log.warn("metricsLabel is not valid: {}", labels);
                    continue;
                }
                LABEL_MAP.put(split[0], split[1]);
            }
        }
        if (proxyConfig.isMetricsInDelta()) {
            LABEL_MAP.put(LABEL_AGGREGATION, AGGREGATION_DELTA);
        }
        LABEL_MAP.put(LABEL_NODE_TYPE, NODE_TYPE_PROXY);
        LABEL_MAP.put(LABEL_CLUSTER_NAME, proxyConfig.getProxyClusterName());
        LABEL_MAP.put(LABEL_NODE_ID, proxyConfig.getLocalServeAddr());
        LABEL_MAP.put(LABEL_PROXY_MODE, proxyConfig.getProxyMode().toLowerCase());

        SdkMeterProviderBuilder providerBuilder = SdkMeterProvider.builder()
            .setResource(Resource.empty());

        if (metricsExporterType == BrokerConfig.MetricsExporterType.OTLP_GRPC) {
            String endpoint = proxyConfig.getMetricsGrpcExporterTarget();
            if (!endpoint.startsWith("http")) {
                endpoint = "https://" + endpoint;
            }
            OtlpGrpcMetricExporterBuilder metricExporterBuilder = OtlpGrpcMetricExporter.builder()
                .setEndpoint(endpoint)
                .setTimeout(proxyConfig.getMetricGrpcExporterTimeOutInMills(), TimeUnit.MILLISECONDS)
                .setAggregationTemporalitySelector(type -> {
                    if (proxyConfig.isMetricsInDelta() &&
                        (type == InstrumentType.COUNTER || type == InstrumentType.OBSERVABLE_COUNTER || type == InstrumentType.HISTOGRAM)) {
                        return AggregationTemporality.DELTA;
                    }
                    return AggregationTemporality.CUMULATIVE;
                });

            String headers = proxyConfig.getMetricsGrpcExporterHeader();
            if (StringUtils.isNotBlank(headers)) {
                Map<String, String> headerMap = new HashMap<>();
                List<String> kvPairs = Splitter.on(',').omitEmptyStrings().splitToList(headers);
                for (String item : kvPairs) {
                    String[] split = item.split(":");
                    if (split.length != 2) {
                        log.warn("metricsGrpcExporterHeader is not valid: {}", headers);
                        continue;
                    }
                    headerMap.put(split[0], split[1]);
                }
                headerMap.forEach(metricExporterBuilder::addHeader);
            }

            metricExporter = metricExporterBuilder.build();

            periodicMetricReader = PeriodicMetricReader.builder(metricExporter)
                .setInterval(proxyConfig.getMetricGrpcExporterIntervalInMills(), TimeUnit.MILLISECONDS)
                .build();

            providerBuilder.registerMetricReader(periodicMetricReader);
        }

        if (metricsExporterType == BrokerConfig.MetricsExporterType.PROM) {
            String promExporterHost = proxyConfig.getMetricsPromExporterHost();
            if (StringUtils.isBlank(promExporterHost)) {
                promExporterHost = "0.0.0.0";
            }
            prometheusHttpServer = PrometheusHttpServer.builder()
                .setHost(promExporterHost)
                .setPort(proxyConfig.getMetricsPromExporterPort())
                .build();
            providerBuilder.registerMetricReader(prometheusHttpServer);
        }

        if (metricsExporterType == BrokerConfig.MetricsExporterType.LOG) {
            SLF4JBridgeHandler.removeHandlersForRootLogger();
            SLF4JBridgeHandler.install();
            loggingMetricExporter = LoggingMetricExporter.create(proxyConfig.isMetricsInDelta() ? AggregationTemporality.DELTA : AggregationTemporality.CUMULATIVE);
            java.util.logging.Logger.getLogger(LoggingMetricExporter.class.getName()).setLevel(java.util.logging.Level.FINEST);
            periodicMetricReader = PeriodicMetricReader.builder(loggingMetricExporter)
                .setInterval(proxyConfig.getMetricLoggingExporterIntervalInMills(), TimeUnit.MILLISECONDS)
                .build();
            providerBuilder.registerMetricReader(periodicMetricReader);
        }

        // register remoting rpc metrics
        for (Pair<InstrumentSelector, View> selectorViewPair : RemotingMetricsManager.getMetricsView()) {
            providerBuilder.registerView(selectorViewPair.getObject1(), selectorViewPair.getObject2());
        }

        Meter proxyMeter = OpenTelemetrySdk.builder()
            .setMeterProvider(providerBuilder.build())
            .build()
            .getMeter(OPEN_TELEMETRY_METER_NAME);

        initMetrics(proxyMeter, null);

        // init other metrics
        initRemotingRpcMetrics(proxyMeter);
        initConnectionMetrics(proxyMeter);
        initStatsMetrics(proxyMeter);
    }

    private void initConnectionMetrics(Meter proxyMeter) {
        producerConnection = proxyMeter.gaugeBuilder(GAUGE_PRODUCER_CONNECTIONS)
                .setDescription("Producer connections")
                .ofLongs()
                .buildWithCallback(measurement -> {
                    Map<ProducerAttr, Integer> metricsMap = new HashMap<>();
                    serviceManager.getProducerManager()
                            .getGroupChannelTable()
                            .forEach((group, groupChannelTable) -> {
                                if (groupChannelTable != null) {
                                    groupChannelTable.values().forEach(info -> {
                                        ProducerAttr attr = new ProducerAttr(group, info.getLanguage(), info.getVersion());
                                        Integer count = metricsMap.computeIfAbsent(attr, k -> 0);
                                        metricsMap.put(attr, count + 1);
                                    });
                                }
                            });
                    metricsMap.forEach((attr, count) -> {
                        Attributes attributes = newAttributesBuilder()
                                .put("producer_group", attr.getGroup())
                                .put(LABEL_LANGUAGE, attr.getLanguage().name().toLowerCase())
                                .put(LABEL_VERSION, MQVersion.getVersionDesc(attr.getVersion()).toLowerCase())
                                .put(LABEL_PROTOCOL_TYPE, PROTOCOL_TYPE_REMOTING)
                                .build();
                        measurement.record(count, attributes);
                    });
                });

        consumerConnection = proxyMeter.gaugeBuilder(GAUGE_CONSUMER_CONNECTIONS)
                .setDescription("Consumer connections")
                .ofLongs()
                .buildWithCallback(measurement -> {
                    Map<ConsumerAttr, Integer> metricsMap = new HashMap<>();
                    ConsumerManager consumerManager = serviceManager.getConsumerManager();
                    consumerManager.getConsumerTable()
                            .forEach((group, groupInfo) -> {
                                if (groupInfo != null) {
                                    groupInfo.getChannelInfoTable().values().forEach(info -> {
                                        ConsumerAttr attr = new ConsumerAttr(group, info.getLanguage(), info.getVersion(), groupInfo.getConsumeType());
                                        Integer count = metricsMap.computeIfAbsent(attr, k -> 0);
                                        metricsMap.put(attr, count + 1);
                                    });
                                }
                            });
                    metricsMap.forEach((attr, count) -> {
                        Attributes attributes = newAttributesBuilder()
                                .put(LABEL_CONSUMER_GROUP, attr.getGroup())
                                .put(LABEL_LANGUAGE, attr.getLanguage().name().toLowerCase())
                                .put(LABEL_VERSION, MQVersion.getVersionDesc(attr.getVersion()).toLowerCase())
                                .put(LABEL_CONSUME_MODE, attr.getConsumeMode().getTypeCN().toLowerCase())
                                .put(LABEL_PROTOCOL_TYPE, PROTOCOL_TYPE_REMOTING)
                                .put(LABEL_IS_SYSTEM, BrokerMetricsManager.isSystemGroup(attr.getGroup()))
                                .build();
                        measurement.record(count, attributes);
                    });
                });
    }

    private void initRemotingRpcMetrics(Meter proxyMeter) {
        RemotingMetricsManager.initMetrics(proxyMeter, null);
    }

    private void initStatsMetrics(Meter proxyMeter) {
        processorWatermark = proxyMeter.gaugeBuilder(GAUGE_PROCESSOR_WATERMARK)
                .setDescription("Request processor watermark")
                .ofLongs()
                .buildWithCallback(measurement -> {
                    ThreadPoolMonitor.MONITOR_EXECUTOR.forEach(threadPoolWrapper -> {
                        ThreadPoolExecutor executor = threadPoolWrapper.getThreadPoolExecutor();
                        if (executor != null) {
                            measurement.record(executor.getQueue().size(), newAttributesBuilder().put(LABEL_PROCESSOR, threadPoolWrapper.getName()).build());
                        }
                    });
                });
    }

    @Override
    public void shutdown() throws Exception {
        if (proxyConfig.getMetricsExporterType() == BrokerConfig.MetricsExporterType.OTLP_GRPC) {
            periodicMetricReader.forceFlush();
            periodicMetricReader.shutdown();
            metricExporter.shutdown();
        }
        if (proxyConfig.getMetricsExporterType() == BrokerConfig.MetricsExporterType.PROM) {
            prometheusHttpServer.forceFlush();
            prometheusHttpServer.shutdown();
        }
        if (proxyConfig.getMetricsExporterType() == BrokerConfig.MetricsExporterType.LOG) {
            periodicMetricReader.forceFlush();
            periodicMetricReader.shutdown();
            loggingMetricExporter.shutdown();
        }
    }
}
