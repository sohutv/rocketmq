package org.apache.rocketmq.broker.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class TokenBucketRateLimiterTest {
    int threadNO = 3;
    double qps = 1000;
    ExecutorService es = Executors.newFixedThreadPool(threadNO);
    TokenBucketRateLimiter rateLimiter = new TokenBucketRateLimiter(qps);

    @Test
    public void test() throws InterruptedException {
        double qps = 1000;
        TokenBucketRateLimiter rateLimiter = new TokenBucketRateLimiter(qps);
        int passedCount = 0;
        for (int i = 0; i < 5000; ++i) {
            boolean acquired = rateLimiter.acquire();
            if (!acquired) {
                Assert.assertTrue(passedCount >= qps);
                passedCount = 0;
                System.out.println(i + ",needWaitMicrosecs=" + rateLimiter.getLastNeedWaitMicrosecs() + "," +
                        "rateLimitTimestamp=" + rateLimiter.getLastRateLimitTimestamp());
                Thread.sleep(1000);
            } else {
                ++passedCount;
            }
        }
    }

    @Test
    public void testThread() throws InterruptedException {
        for (int i = 0; i < 3; ++i) {
            System.out.println("===" + i);
            testTask();
            Thread.sleep(threadNO * 1000);
        }
    }

    private void testTask() throws InterruptedException {
        List<Callable<Integer>> taskList = new ArrayList<>(threadNO);
        int tasks = (int) qps;
        for (int k = 0; k < threadNO; ++k) {
            taskList.add(new Callable<Integer>() {
                public Integer call() throws Exception {
                    int pass = 0;
                    for (int i = 0; i < tasks; ++i) {
                        boolean acquired = rateLimiter.acquire();
                        if (acquired) {
                            ++pass;
                        }
                    }
                    System.out.println(Thread.currentThread().getName() + " passed:" + pass);
                    return pass;
                }
            });
        }
        AtomicInteger passedCount = new AtomicInteger();
        es.invokeAll(taskList).forEach(f -> {
            try {
                passedCount.addAndGet(f.get());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        System.out.println("passedCount:" + passedCount.get());
        Assert.assertTrue(Math.abs(passedCount.get() - qps) <= 10);
    }

    @Test
    public void testThroughput() throws InterruptedException {
        long start = System.currentTimeMillis();
        AtomicLong counter = new AtomicLong();
        for (int i = 0; i < 8; ++i) {
            new Thread(() -> {
                while (true) {
                    rateLimiter.acquire();
                    counter.incrementAndGet();
                }
            }).start();
        }
        for (int i = 0; i < 10; ++i) {
            Thread.sleep(5 * 1000);
            System.out.println("qps=" + counter.longValue() / (System.currentTimeMillis() - start) * 1000);
            start = System.currentTimeMillis();
            counter.set(0);
        }
    }
}
