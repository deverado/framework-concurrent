package de.deverado.framework.concurrent.consistenthashing;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConsistentHashRingsTest {

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testCreateEvenSplitVNodeHashMappingLong() {
        int[] counts = new int[]{2, 3, 12, 13, 32, 64, 47, 128, 256, 512, 1024, 1024 * 128};
        for (int vnodeCount : counts) {

            long correctCalc = new BigInteger(Long.toString(Long.MAX_VALUE)).add(BigInteger.ONE)
                    .subtract(new BigInteger(Long.toString(Long.MIN_VALUE)))
                    .divide(new BigInteger(Long.toString(vnodeCount))).longValue();
            long[] longs = ConsistentHashRings.createEvenSplitVNodeHashMappingLong(vnodeCount);
            assertEquals(vnodeCount, longs.length);
            long spacing = longs[1] - longs[0];
            assertTrue("count: " + vnodeCount + ", correct: " + correctCalc + " incorrect: " + spacing,
                    Math.abs(correctCalc - spacing) / (1.0 * correctCalc) < 0.01);
            long lastSpacing = Long.MAX_VALUE - longs[vnodeCount - 1];
            long diffLastToNormal = Math.abs(spacing - lastSpacing);
            double relative = 1.0 * diffLastToNormal / spacing;
            if (relative > 0.01) {
                fail("Diff too big: " + vnodeCount + " spacing: " + spacing + " diff: " + diffLastToNormal + " " +
                        "relative diff: " + relative);
            }
        }

    }
}
