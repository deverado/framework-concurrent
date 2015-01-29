package de.deverado.framework.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class ConcurrencyHelper {

    private static final Logger log = LoggerFactory
            .getLogger(ConcurrencyHelper.class);

    /**
     * Retries depending on start times of tries, so if retry takes
     * 
     * @param executor
     * @param toRetry
     *            must return non-null - abort by returning a TRUE future
     * @param totalTimeoutMillis
     *            must be > 0
     * @param retryIntervalMillis
     *            may be < 0, then retry instantly
     * @param successCB
     * @param failureCB
     */
    public static void retryUntil(
            final ListeningScheduledExecutorService executor,
            final Function<RetryStatus, ListenableFuture<Boolean>> toRetry,
            final long totalTimeoutMillis, final long retryIntervalMillis,
            @Nullable final Runnable successCB,
            @Nullable final Runnable failureCB) {

        final RetryStatus status = new RetryStatus(totalTimeoutMillis);

        final AtomicReference<Runnable> resolver = new AtomicReference<Runnable>();
        resolver.set(new Runnable() {
            private ListenableFuture<Boolean> refFuture = null;

            @Override
            public void run() {
                boolean success = false;
                try {
                    if (refFuture != null) {
                        Boolean res = refFuture.get();
                        if (Boolean.TRUE.equals(res)) {

                            success = true;
                            try {
                                if (successCB != null) {
                                    successCB.run();
                                }
                            } catch (Exception e) {
                                log.warn("Problem in successCB", e);
                            }
                        }
                    }
                } catch (Exception e) {
                    log.trace("Could not exec {} because: {}, "
                            + "retrying if time left", toRetry, e.getMessage());
                }

                if (!success) {
                    final long totalMillisLeft = status.getTotalMillisLeft();
                    if (totalMillisLeft <= 0) {
                        log.debug(
                                "Total timeout of {} expired for executing {}",
                                totalTimeoutMillis, toRetry);
                        if (failureCB != null) {
                            failureCB.run();
                        }
                    } else {
                        long retryIntervalLeft;
                        // be careful here, arithmetics can loop
                        if (retryIntervalMillis <= 0) {
                            retryIntervalLeft = 0;
                        } else {
                            retryIntervalLeft = Math.max(0, retryIntervalMillis
                                    - status.getMillisSinceLastTry());
                            if (retryIntervalLeft > totalMillisLeft) {
                                retryIntervalLeft = totalMillisLeft;
                            }
                        }

                        executor.schedule(new Runnable() {
                            @Override
                            public void run() {
                                status.setLastTryStartedAt(System
                                        .currentTimeMillis());
                                refFuture = toRetry.apply(status);
                                if (refFuture == null) {
                                    log.error(
                                            "Function to retry must return a future, "
                                                    + "but returned null: {}, no retrying",
                                            toRetry);
                                } else {
                                    refFuture.addListener(resolver.get(),
                                            MoreExecutors.sameThreadExecutor());
                                }
                            }
                        }, retryIntervalLeft, TimeUnit.MILLISECONDS);

                    }
                }
            }
        });
        resolver.get().run();
    }

    public static class RetryStatus {
        private final long start = System.currentTimeMillis();
        private long lastTryStartedAt = Long.MIN_VALUE;
        private final long totalTimeoutMillis;

        public RetryStatus(long totalTimeoutMillis) {
            this.totalTimeoutMillis = totalTimeoutMillis;
        }

        public long getStart() {
            return start;
        }

        public long getMillisElapsed() {
            return System.currentTimeMillis() - start;
        }

        public long getLastTryStartedAt() {
            return lastTryStartedAt;
        }

        void setLastTryStartedAt(long lastTryStartedAt) {
            this.lastTryStartedAt = lastTryStartedAt;
        }

        public long getMillisSinceLastTry() {
            if (lastTryStartedAt == Long.MIN_VALUE) {
                return Long.MAX_VALUE;
            }
            return System.currentTimeMillis() - lastTryStartedAt;
        }

        public long getTotalMillisLeft() {
            return totalTimeoutMillis - getMillisElapsed();
        }
    }

}
