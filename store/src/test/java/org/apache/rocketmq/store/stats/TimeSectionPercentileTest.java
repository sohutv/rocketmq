package org.apache.rocketmq.store.stats;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class TimeSectionPercentileTest {
    public static final int MAX_TIME = 10000;

    private TimeSectionPercentile timeSectionPercentile = new TimeSectionPercentile(MAX_TIME);

    @Test
    public void testMax() {
        for (int i = 0; i < MAX_TIME * 2; ++i) {
            timeSectionPercentile.increment(i);
        }
        timeSectionPercentile.sample();
        Assert.assertEquals(MAX_TIME, timeSectionPercentile.max());
    }

    @Test
    public void testTotal() {
        for (int i = 0; i < MAX_TIME * 2; ++i) {
            timeSectionPercentile.increment(i);
        }
        timeSectionPercentile.sample();
        Assert.assertEquals(MAX_TIME * 2, timeSectionPercentile.getTotalCount());
    }

    @Test
    public void testAvg() {
        double statTotal = 0;
        for (int i = 0; i < MAX_TIME; ++i) {
            timeSectionPercentile.increment(i);
            statTotal += timeSectionPercentile.time(timeSectionPercentile.index(i));
        }
        timeSectionPercentile.sample();
        Assert.assertEquals(statTotal / timeSectionPercentile.getTotalCount(), timeSectionPercentile.avg(), 0.001);
    }

    @Test
    public void test() {
        List<Integer> timeSection = timeSectionPercentile.getTimeSection();
        for (int i = 0; i < timeSection.size(); ++i) {
            System.out.println(i + "=" + timeSection.get(i));
        }
        for (int i = 0; i <= MAX_TIME * 10; ++i) {
            test(i);
        }
    }

    private void test(int time) {
        int index = getIndex(time);
        int dest = getTime(index);
        int minus = dest - time;
        System.out.println(time + ":" + dest + ":" + index);
        if (time <= 10) {
            Assert.assertEquals(time, dest);
        } else if (time <= 100) {
            Assert.assertTrue(minus >= 0 && minus < 5);
        } else if (time <= 10000) {
            Assert.assertTrue(minus >= 0 && minus < 50);
        } else {
            Assert.assertTrue(minus < 0);
        }
    }

    private int getTime(int index) {
        return timeSectionPercentile.time(index);
    }

    private int getIndex(int time) {
        return timeSectionPercentile.index(time);
    }

}
