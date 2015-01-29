package de.deverado.framework.concurrent;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.ParametersAreNonnullByDefault;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * If you are using {@link ShutdownListener}s you have to call this on shutdown of your node.
 *
 * See also framework-guice's support for startup and shutdown listeners if you are using guice.
 *
 * @see RunlevelShutdownListenerRunner
 */
@ParametersAreNonnullByDefault
public class ShutdownListenerRunner implements Callable<Void> {

    private static final Logger log = LoggerFactory
            .getLogger(ShutdownListenerRunner.class);

    private Collection<ShutdownListener> shutdownListeners;
    private int executionTimeout;

    /**
     * @param executionTimeoutMs Timeout after which the the runner will stop waiting for the (parallel) execution
     *                           of listeners to finish.
     * @return initialized instance
     */
    public static ShutdownListenerRunner create(Collection<ShutdownListener> listeners, int executionTimeoutMs) {

        ShutdownListenerRunner result = new ShutdownListenerRunner();
        result.shutdownListeners = listeners;
        result.executionTimeout = executionTimeoutMs;
        return result;
    }

    @Override
    public Void call() throws Exception {
        if (shutdownListeners != null) {
            log.info("Starting shutdown listeners...");
            Map<ListenableFuture<?>, ShutdownListener> futures = Maps
                    .newHashMap();
            for (ShutdownListener sl : shutdownListeners) {
                try {
                    ListenableFuture<?> future = sl.shutdown();
                    if (future != null) {
                        futures.put(future, sl);
                    }
                } catch (Exception e) {
                    log.warn("Shutdown listener failure: {}", sl, e);
                }
            }
            log.info("Starting shutdown listeners... Done");
            executionTimeout = 500;
            int waitMs = executionTimeout;
            log.info("Waiting " + waitMs + " ms for everything to clear up...");
            Stopwatch timer = Stopwatch.createStarted();
            Set<ListenableFuture<?>> finished = Sets
                    .newHashSet();
            for (ListenableFuture<?> f : futures.keySet()) {
                try {
                    Object result = f.get(
                            Math.max(0, waitMs - timer.elapsed(TimeUnit.MILLISECONDS)),
                            TimeUnit.MILLISECONDS);
                    log.trace("Shutdown listener {}: {}", futures.get(f),
                            result);
                    finished.add(f);

                } catch (TimeoutException te) {
                    log.trace("Could not finish ShutdownListener: {}",
                            futures.get(f));
                } catch (InterruptedException ie) {
                    log.info("Interrupted, abort waiting for ShutdownListeners");
                    break;

                } catch (Exception e) {
                    log.info("Exception in ShutdownListener: {}",
                            futures.get(f), e);
                }
            }
            Map<ListenableFuture<?>, ShutdownListener> failed = Maps
                    .newHashMap(futures);
            for (ListenableFuture<?> o : finished) {
                failed.remove(o);
            }
            if (!failed.isEmpty()) {
                log.warn("{} ShutdownListener failed to finish: {}",
                        failed.size(), failed.values());
            }
            log.info("Waited "
                    + timer.elapsed(TimeUnit.MILLISECONDS)
                    + " ms for everything to clear up... done. Running of ShutdownListeners ending.");
        } else {
            log.debug("No ShutdownListeners to run.");
        }
        return null;
    }

}
