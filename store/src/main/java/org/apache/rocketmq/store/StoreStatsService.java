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
package org.apache.rocketmq.store;

import org.apache.rocketmq.common.BrokerIdentity;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.body.PercentileStat;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStoreStatManager;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;

public class StoreStatsService extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private static final int FREQUENCY_OF_SAMPLING = 1000;

    private static final int MAX_RECORDS_OF_SAMPLING = 60 * 10;

    private static int printTPSInterval = 60 * 1;

    private final LongAdder putMessageFailedTimes = new LongAdder();

    private final ConcurrentMap<String, LongAdder> putMessageTopicTimesTotal =
        new ConcurrentHashMap<>(128);
    private final ConcurrentMap<String, LongAdder> putMessageTopicSizeTotal =
        new ConcurrentHashMap<>(128);

    private final LongAdder getMessageTimesTotalFound = new LongAdder();
    private final LongAdder getMessageTransferredMsgCount = new LongAdder();
    private final LongAdder getMessageTimesTotalMiss = new LongAdder();
    private final LinkedList<CallSnapshot> putTimesList = new LinkedList<>();

    private final LinkedList<CallSnapshot> getTimesFoundList = new LinkedList<>();
    private final LinkedList<CallSnapshot> getTimesMissList = new LinkedList<>();
    private final LinkedList<CallSnapshot> transferredMsgCountList = new LinkedList<>();
    private long messageStoreBootTimestamp = System.currentTimeMillis();
    private volatile long getMessageEntireTimeMax = 0;
    // for getMessageEntireTimeMax
    private ReentrantLock getLock = new ReentrantLock();

    private volatile long dispatchMaxBuffer = 0;

    private ReentrantLock samplingLock = new ReentrantLock();
    private long lastPrintTimestamp = System.currentTimeMillis();

    private BrokerIdentity brokerIdentity;

    // broker存储统计管理
    private BrokerStoreStatManager brokerStoreStatManager;

    public StoreStatsService() {
    }

    public StoreStatsService(BrokerIdentity brokerIdentity, MessageStoreConfig messageStoreConfig) {
        if (BrokerRole.SLAVE != messageStoreConfig.getBrokerRole()) {
            brokerStoreStatManager = new BrokerStoreStatManager();
        }
        this.brokerIdentity = brokerIdentity;
    }

    public void setPutMessageEntireTimeMax(long value) {
        if (brokerStoreStatManager == null) {
            return;
        }
        brokerStoreStatManager.increment((int) value);
    }

    public PercentileStat getBrokerStoreStat() {
        if (brokerStoreStatManager == null) {
            return null;
        }
        return brokerStoreStatManager.getBrokerStoreStat();
    }

    public long getGetMessageEntireTimeMax() {
        return getMessageEntireTimeMax;
    }

    public void setGetMessageEntireTimeMax(long value) {
        if (value > this.getMessageEntireTimeMax) {
            this.getLock.lock();
            this.getMessageEntireTimeMax =
                value > this.getMessageEntireTimeMax ? value : this.getMessageEntireTimeMax;
            this.getLock.unlock();
        }
    }

    public long getDispatchMaxBuffer() {
        return dispatchMaxBuffer;
    }

    public void setDispatchMaxBuffer(long value) {
        this.dispatchMaxBuffer = value > this.dispatchMaxBuffer ? value : this.dispatchMaxBuffer;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(1024);
        Long totalTimes = getPutMessageTimesTotal();
        if (0 == totalTimes) {
            totalTimes = 1L;
        }

        sb.append("\truntime: " + this.getFormatRuntime() + "\r\n");
        sb.append("\tputMessageTimesTotal: " + totalTimes + "\r\n");
        sb.append("\tgetPutMessageFailedTimes: " + this.getPutMessageFailedTimes() + "\r\n");
        sb.append("\tputMessageSizeTotal: " + this.getPutMessageSizeTotal() + "\r\n");
        sb.append("\tputMessageAverageSize: " + (this.getPutMessageSizeTotal() / totalTimes.doubleValue())
            + "\r\n");
        sb.append("\tdispatchMaxBuffer: " + this.dispatchMaxBuffer + "\r\n");
        sb.append("\tgetMessageEntireTimeMax: " + this.getMessageEntireTimeMax + "\r\n");
        sb.append("\tputTps: " + this.getPutTps() + "\r\n");
        sb.append("\tgetFoundTps: " + this.getGetFoundTps() + "\r\n");
        sb.append("\tgetMissTps: " + this.getGetMissTps() + "\r\n");
        sb.append("\tgetTotalTps: " + this.getGetTotalTps() + "\r\n");
        sb.append("\tgetTransferredTps: " + this.getGetTransferredTps() + "\r\n");
        return sb.toString();
    }

    public long getPutMessageTimesTotal() {
        Map<String, LongAdder> map = putMessageTopicTimesTotal;
        return map.values()
                .parallelStream()
                .mapToLong(LongAdder::longValue)
                .sum();
    }

    private String getFormatRuntime() {
        final long millisecond = 1;
        final long second = 1000 * millisecond;
        final long minute = 60 * second;
        final long hour = 60 * minute;
        final long day = 24 * hour;
        final MessageFormat messageFormat = new MessageFormat("[ {0} days, {1} hours, {2} minutes, {3} seconds ]");

        long time = System.currentTimeMillis() - this.messageStoreBootTimestamp;
        long days = time / day;
        long hours = (time % day) / hour;
        long minutes = (time % hour) / minute;
        long seconds = (time % minute) / second;
        return messageFormat.format(new Long[] {days, hours, minutes, seconds});
    }

    public long getPutMessageSizeTotal() {
        Map<String, LongAdder> map = putMessageTopicSizeTotal;
        return map.values()
                .parallelStream()
                .mapToLong(LongAdder::longValue)
                .sum();
    }

    private String getPutTps() {
        StringBuilder sb = new StringBuilder();

        sb.append(this.getPutTps(10));
        sb.append(" ");

        sb.append(this.getPutTps(60));
        sb.append(" ");

        sb.append(this.getPutTps(600));

        return sb.toString();
    }

    private String getGetFoundTps() {
        StringBuilder sb = new StringBuilder();

        sb.append(this.getGetFoundTps(10));
        sb.append(" ");

        sb.append(this.getGetFoundTps(60));
        sb.append(" ");

        sb.append(this.getGetFoundTps(600));

        return sb.toString();
    }

    private String getGetMissTps() {
        StringBuilder sb = new StringBuilder();

        sb.append(this.getGetMissTps(10));
        sb.append(" ");

        sb.append(this.getGetMissTps(60));
        sb.append(" ");

        sb.append(this.getGetMissTps(600));

        return sb.toString();
    }

    private String getGetTotalTps() {
        StringBuilder sb = new StringBuilder();

        sb.append(this.getGetTotalTps(10));
        sb.append(" ");

        sb.append(this.getGetTotalTps(60));
        sb.append(" ");

        sb.append(this.getGetTotalTps(600));

        return sb.toString();
    }

    private String getGetTransferredTps() {
        StringBuilder sb = new StringBuilder();

        sb.append(this.getGetTransferredTps(10));
        sb.append(" ");

        sb.append(this.getGetTransferredTps(60));
        sb.append(" ");

        sb.append(this.getGetTransferredTps(600));

        return sb.toString();
    }

    private String getPutTps(int time) {
        String result = "";
        this.samplingLock.lock();
        try {
            CallSnapshot last = this.putTimesList.getLast();

            if (this.putTimesList.size() > time) {
                CallSnapshot lastBefore = this.putTimesList.get(this.putTimesList.size() - (time + 1));
                result += CallSnapshot.getTPS(lastBefore, last);
            }

        } finally {
            this.samplingLock.unlock();
        }
        return result;
    }

    private String getGetFoundTps(int time) {
        String result = "";
        this.samplingLock.lock();
        try {
            CallSnapshot last = this.getTimesFoundList.getLast();

            if (this.getTimesFoundList.size() > time) {
                CallSnapshot lastBefore =
                    this.getTimesFoundList.get(this.getTimesFoundList.size() - (time + 1));
                result += CallSnapshot.getTPS(lastBefore, last);
            }
        } finally {
            this.samplingLock.unlock();
        }

        return result;
    }

    private String getGetMissTps(int time) {
        String result = "";
        this.samplingLock.lock();
        try {
            CallSnapshot last = this.getTimesMissList.getLast();

            if (this.getTimesMissList.size() > time) {
                CallSnapshot lastBefore =
                    this.getTimesMissList.get(this.getTimesMissList.size() - (time + 1));
                result += CallSnapshot.getTPS(lastBefore, last);
            }

        } finally {
            this.samplingLock.unlock();
        }

        return result;
    }

    private String getGetTotalTps(int time) {
        this.samplingLock.lock();
        double found = 0;
        double miss = 0;
        try {
            {
                CallSnapshot last = this.getTimesFoundList.getLast();

                if (this.getTimesFoundList.size() > time) {
                    CallSnapshot lastBefore =
                        this.getTimesFoundList.get(this.getTimesFoundList.size() - (time + 1));
                    found = CallSnapshot.getTPS(lastBefore, last);
                }
            }
            {
                CallSnapshot last = this.getTimesMissList.getLast();

                if (this.getTimesMissList.size() > time) {
                    CallSnapshot lastBefore =
                        this.getTimesMissList.get(this.getTimesMissList.size() - (time + 1));
                    miss = CallSnapshot.getTPS(lastBefore, last);
                }
            }

        } finally {
            this.samplingLock.unlock();
        }

        return Double.toString(found + miss);
    }

    private String getGetTransferredTps(int time) {
        String result = "";
        this.samplingLock.lock();
        try {
            CallSnapshot last = this.transferredMsgCountList.getLast();

            if (this.transferredMsgCountList.size() > time) {
                CallSnapshot lastBefore =
                    this.transferredMsgCountList.get(this.transferredMsgCountList.size() - (time + 1));
                result += CallSnapshot.getTPS(lastBefore, last);
            }

        } finally {
            this.samplingLock.unlock();
        }

        return result;
    }

    public HashMap<String, String> getRuntimeInfo() {
        HashMap<String, String> result = new HashMap<>(64);

        Long totalTimes = getPutMessageTimesTotal();
        if (0 == totalTimes) {
            totalTimes = 1L;
        }

        result.put("bootTimestamp", String.valueOf(this.messageStoreBootTimestamp));
        result.put("runtime", this.getFormatRuntime());
        result.put("putMessageTimesTotal", String.valueOf(totalTimes));
        result.put("putMessageFailedTimes", String.valueOf(this.putMessageFailedTimes));
        result.put("putMessageSizeTotal", String.valueOf(this.getPutMessageSizeTotal()));
        result.put("putMessageAverageSize",
            String.valueOf(this.getPutMessageSizeTotal() / totalTimes.doubleValue()));
        result.put("dispatchMaxBuffer", String.valueOf(this.dispatchMaxBuffer));
        result.put("getMessageEntireTimeMax", String.valueOf(this.getMessageEntireTimeMax));
        result.put("putTps", this.getPutTps());
        result.put("getFoundTps", this.getGetFoundTps());
        result.put("getMissTps", this.getGetMissTps());
        result.put("getTotalTps", this.getGetTotalTps());
        result.put("getTransferredTps", this.getGetTransferredTps());

        return result;
    }

    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                this.waitForRunning(FREQUENCY_OF_SAMPLING);

                this.sampling();

                this.printTps();
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
        if (this.brokerIdentity != null && this.brokerIdentity.isInBrokerContainer()) {
            return brokerIdentity.getIdentifier() + StoreStatsService.class.getSimpleName();
        }
        return StoreStatsService.class.getSimpleName();
    }

    private void sampling() {
        this.samplingLock.lock();
        try {
            this.putTimesList.add(new CallSnapshot(System.currentTimeMillis(), getPutMessageTimesTotal()));
            if (this.putTimesList.size() > (MAX_RECORDS_OF_SAMPLING + 1)) {
                this.putTimesList.removeFirst();
            }

            this.getTimesFoundList.add(new CallSnapshot(System.currentTimeMillis(),
                this.getMessageTimesTotalFound.longValue()));
            if (this.getTimesFoundList.size() > (MAX_RECORDS_OF_SAMPLING + 1)) {
                this.getTimesFoundList.removeFirst();
            }

            this.getTimesMissList.add(new CallSnapshot(System.currentTimeMillis(),
                this.getMessageTimesTotalMiss.longValue()));
            if (this.getTimesMissList.size() > (MAX_RECORDS_OF_SAMPLING + 1)) {
                this.getTimesMissList.removeFirst();
            }

            this.transferredMsgCountList.add(new CallSnapshot(System.currentTimeMillis(),
                this.getMessageTransferredMsgCount.longValue()));
            if (this.transferredMsgCountList.size() > (MAX_RECORDS_OF_SAMPLING + 1)) {
                this.transferredMsgCountList.removeFirst();
            }

        } finally {
            this.samplingLock.unlock();
        }
    }

    private void printTps() {
        if (System.currentTimeMillis() > (this.lastPrintTimestamp + printTPSInterval * 1000)) {
            this.lastPrintTimestamp = System.currentTimeMillis();

            log.info("[STORETPS] put_tps {} get_found_tps {} get_miss_tps {} get_transferred_tps {}",
                this.getPutTps(printTPSInterval),
                this.getGetFoundTps(printTPSInterval),
                this.getGetMissTps(printTPSInterval),
                this.getGetTransferredTps(printTPSInterval)
            );
        }
    }

    public LongAdder getGetMessageTimesTotalFound() {
        return getMessageTimesTotalFound;
    }

    public LongAdder getGetMessageTimesTotalMiss() {
        return getMessageTimesTotalMiss;
    }

    public LongAdder getGetMessageTransferredMsgCount() {
        return getMessageTransferredMsgCount;
    }

    public LongAdder getPutMessageFailedTimes() {
        return putMessageFailedTimes;
    }

    public LongAdder getSinglePutMessageTopicSizeTotal(String topic) {
        LongAdder rs = putMessageTopicSizeTotal.get(topic);
        if (null == rs) {
            rs = new LongAdder();
            LongAdder previous = putMessageTopicSizeTotal.putIfAbsent(topic, rs);
            if (previous != null) {
                rs = previous;
            }
        }
        return rs;
    }

    public LongAdder getSinglePutMessageTopicTimesTotal(String topic) {
        LongAdder rs = putMessageTopicTimesTotal.get(topic);
        if (null == rs) {
            rs = new LongAdder();
            LongAdder previous = putMessageTopicTimesTotal.putIfAbsent(topic, rs);
            if (previous != null) {
                rs = previous;
            }
        }
        return rs;
    }

    public Map<String, LongAdder> getPutMessageTopicTimesTotal() {
        return putMessageTopicTimesTotal;
    }

    public Map<String, LongAdder> getPutMessageTopicSizeTotal() {
        return putMessageTopicSizeTotal;
    }

    static class CallSnapshot {
        public final long timestamp;
        public final long callTimesTotal;

        public CallSnapshot(long timestamp, long callTimesTotal) {
            this.timestamp = timestamp;
            this.callTimesTotal = callTimesTotal;
        }

        public static double getTPS(final CallSnapshot begin, final CallSnapshot end) {
            long total = end.callTimesTotal - begin.callTimesTotal;
            Long time = end.timestamp - begin.timestamp;

            double tps = total / time.doubleValue();

            return tps * 1000;
        }
    }
}
