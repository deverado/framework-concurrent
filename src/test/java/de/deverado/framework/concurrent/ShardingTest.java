package de.deverado.framework.concurrent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Date;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import shardableobjectids.ShardableObjectId;
import de.deverado.framework.concurrent.Sharding.ShardingStrategy;
import de.deverado.framework.core.Utils;

public class ShardingTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void shouldShard16Evenly() {
        testSharding(10000, 16, 8, new Sharding.ShardByArbitraryStrategy(16),
                20);
    }

    @Test
    public void shouldShard256Evenly() {
        testSharding(50000, 256, 10,
                new Sharding.ShardByArbitraryStrategy(256), 25);
    }

    /**
     * 512 is not power of 2, better adapter than completely arbitrary, but max
     * diff bias stronger
     */
    @Test
    public void shouldShard512Evenly() {
        testSharding(100000, 512, 10,
                new Sharding.ShardByArbitraryStrategy(512), 35);
    }

    @Test
    public void shouldShard10Evenly() {
        // non-2 power creates strong bias, example:
        // 1236 diff: 236
        // 1270 diff: 270
        // 1289 diff: 289
        // 1234 diff: 234
        // 1227 diff: 227
        // 1244 diff: 244
        // 635 diff: -365
        // 630 diff: -370
        // 630 diff: -370
        // 605 diff: -395
        testSharding(10000, 10, 40, new Sharding.ShardByArbitraryStrategy(10),
                50);
    }

    @Test
    public void shouldShard2Evenly() {
        testSharding(10000, 2, 5, new Sharding.ShardByArbitraryStrategy(2), 5);
    }

    @Test
    public void shouldShard8Evenly() {
        testSharding(10000, 8, 5, new Sharding.ShardByArbitraryStrategy(8), 10);
    }

    @Test
    public void shouldShard32Evenly() {
        testSharding(10000, 32, 7, new Sharding.ShardByArbitraryStrategy(32),
                20);
    }

    @Test
    public void shouldShard128Evenly() {
        testSharding(50000, 128, 7, new Sharding.ShardByArbitraryStrategy(128),
                20);
    }

    public static void testSharding(int count, int shards,
            int maxStddevMeanPct, ShardingStrategy shardingStrategy,
            int maxDiffMeanPct) {
        int[] counters = new int[shards];
        Arrays.fill(counters, 0);
        int total = count;
        Random rand = new Random();
        long twoYears = 2L * 84600 * 365 * 1000;
        long twoYearsAgo = System.currentTimeMillis() - twoYears;
        for (int i = 0; i < total; i++) {
            // random ids of 10 machines in last two years:

            String toHash = new ShardableObjectId(new Date(twoYearsAgo
                    + (long) (rand.nextDouble() * twoYears)), rand.nextInt(10),
                    0).toStringSortableBase64URLSafe();
            // to check generation en masse on same machine:
            // int hashCode = ShardableObjectIdCreator.INSTANCE.apply(null)
            // .hashCode();
            counters[shardingStrategy.shard(toHash)]++;
        }
        // ArrayList<String> later = new ArrayList<String>();

        int avg = total / counters.length;
        double stddev = Utils.stddev(counters);
        double stddevMeanPct = stddev / avg * 100;
        assertTrue("Stddev too big: " + stddev + ", of mean: " + stddevMeanPct
                + " %", stddev < (maxStddevMeanPct * 0.01 * avg));
        for (int i = 0; i < counters.length; i++) {
            int iVal = counters[i];
            int compVal = iVal - avg;

            // System.out.println("" + iVal + " diff: " + compVal);

            // String o = "" + Utils.toBinaryString(i);
            //
            // if (iVal == 0) {
            // System.out.println(o);
            // } else {
            // later.add(o);
            // }

            assertTrue("Bias should not be this strong (stddev of mean:"
                    + stddevMeanPct + " %): " + iVal + " diff: " + compVal,
                    compVal < (maxDiffMeanPct * 0.01 * avg)
                            && compVal > -(maxDiffMeanPct * 0.01 * avg));
        }
        // System.out.println("haveVal:");
        // for (String s : later) {
        // System.out.println(s);
        // }
    }

    @Test
    public void shouldDoLeveledSharding() {
        String[] result = Sharding.shardToMultiLevelHexStrings(1, "asdf");
        assertEquals("3d", result[0]);

        result = Sharding.shardToMultiLevelHexStrings(3, "asdf");
        assertEquals("3d", result[0]);
        assertEquals("a5", result[1]);
        assertEquals("41", result[2]);

        result = Sharding.shardToMultiLevelHexStrings(0, "asdf");
        assertEquals(0, result.length);

        result = Sharding.shardToMultiLevelHexStrings(-1, "asdf");
        assertEquals(0, result.length);
    }

    @Test
    public void shouldExtractMultiLeveledHexStringFiles() throws Exception {
        assertEquals(
                "22/2a/DIR/file.012341.flv",
                Sharding.extractMultiLevelHexStringStoragePath("http://asdf/jklsdf/asdf/22/2a/DIR/file.012341.flv"));
        assertEquals(
                "33/22/2a/DIR/file.012341.flv",
                Sharding.extractMultiLevelHexStringStoragePath("http://asdf/jklsdf/asdf/33/22/2a/DIR/file.012341.flv"));
        assertEquals(
                "22/2a/DIR",
                Sharding.extractMultiLevelHexStringStoragePath("http://asdf/jklsdf/asdf/22/2a/DIR"));
        assertEquals("22/2a/DIR",
                Sharding.extractMultiLevelHexStringStoragePath("22/2a/DIR"));
        assertEquals("/DIR",
                Sharding.extractMultiLevelHexStringStoragePath("/DIR"));
    }

}
