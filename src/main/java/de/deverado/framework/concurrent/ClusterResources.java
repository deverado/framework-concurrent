package de.deverado.framework.concurrent;/*
 * Copyright Georg Koester 2012-2015. All rights reserved.
 */

import java.util.Random;

public class ClusterResources {

    /**
     * Distributed recreation of time-limited resources: Recreate randomly over
     * last minutes of life, with increasing probability over the last minutes
     * of resource life.
     * <p>
     * Two problems: high rate, low but bursty. High rate is no problem with
     * geometric prob. But burst then remains a problem. For this local sync
     * helps a bit, hoping that the burst is localized.
     * </p>
     *
     * @param validityAtLeastMs
     * @return -1 not in check range, 0 in check range, no renew, 1 should renew
     */
    public static int shouldRenew(long resourceValidityEnd, long now,
                                  long validityAtLeastMs, long partOfLifeWhereRenewalShouldStart,
                                  Random normalRandom) {
        double timeToRenewal = resourceValidityEnd - now - validityAtLeastMs;
        // checks for example during the last x mins - getting closer chances
        // get higher.
        // built to work with renewals taking only milliseconds and thousands of
        // requests per second.
        // For example for 100 seconds (see
        // distributed_creation_probabilities.ods):
        // At 1.000.000 requests/sec renewal should be triggered right during
        // the
        // first second of 100 secs square(1/1.000.000 chance), but with less
        // than 1 % probability during the first 100 ms. With 20.000 it should
        // happen during the first 8 secs after the 100 secs border.
        // With small req numbers it will happen with the first request after
        // validityAtLeastMs was passed.
        if (timeToRenewal < partOfLifeWhereRenewalShouldStart) {
            // if timeToRenewal shrinks target increases
            double target = 1.0 - (timeToRenewal / partOfLifeWhereRenewalShouldStart);
            double geometricTarget = target * target;
            if (normalRandom.nextDouble() <= geometricTarget) {
                return 1;
            } else {
                return 0;
            }
        }
        return -1;
    }
}
