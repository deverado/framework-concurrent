package de.deverado.framework.concurrent;/*
 * Copyright Georg Koester 2012-15. All rights reserved.
 */

import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import de.deverado.framework.core.Multimaps2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

@ParametersAreNonnullByDefault
public class RunlevelShutdownListenerRunner implements Callable<Void> {

    private static final Logger LOG = LoggerFactory.getLogger(RunlevelShutdownListenerRunner.class);

    private Multimap<String, ShutdownListener> listenersByRunlevel;

    private int executionTimeoutMs;

    /**
     * @param executionTimeoutMs Timeout after which the the runner will stop waiting for the (parallel) execution
     *                           of listeners to finish.
     * @return initialized instance
     */
    public static RunlevelShutdownListenerRunner create(Multimap<String, ShutdownListener> listenersByRunlevel,
                                                        int executionTimeoutMs) {

        RunlevelShutdownListenerRunner result = new RunlevelShutdownListenerRunner();
        result.listenersByRunlevel = listenersByRunlevel;
        result.executionTimeoutMs = executionTimeoutMs;
        return result;
    }

    @Override
    public Void call() throws Exception {
        List<String> runlevels = new ArrayList<>(listenersByRunlevel.keySet());

        // shutdown high to low
        Collections.sort(runlevels, Collections.reverseOrder());

        for (String runlevel : runlevels) {
            Collection<ShutdownListener> listeners = listenersByRunlevel.get(runlevel);
            if (listeners != null && !listeners.isEmpty()) {
                LOG.debug("Running {} shutdown listeners for level {}", listeners.size(), runlevel);

                ShutdownListenerRunner.create(listeners, executionTimeoutMs).call();
            }
        }
        return null;
    }

    /**
     * Map any unmapped listeners that implement {@link RunlevelProvider} into the existing mapped structure.
     * @param unmappedListeners to be sorted. If they are not instanceof {@link RunlevelProvider} they are
     *                          mapped with defaultRunlevel
     * @param mappedListeners not modified.
     * @param defaultRunlevel with which listeners that are not instances of {@link RunlevelProvider} are mapped
     *                        or which return null on getRunlevel (which shouldn't happen of course).
     * @param <L> please
     * @return new (set) multimap
     */
    public static <L> Multimap<String, L> createRunlevelMap(
            @Nullable Collection<L> unmappedListeners, @Nullable Multimap<String ,L> mappedListeners,
            String defaultRunlevel) {
        SetMultimap<String, L> result = Multimaps2.<String, L>newHashSetMultimap();

        if (mappedListeners != null) {
            result.putAll(mappedListeners);
        }

        if (unmappedListeners != null) {
            for (L l : unmappedListeners) {
                String level = defaultRunlevel;
                if (l instanceof RunlevelProvider) {
                    RunlevelProvider rp = (RunlevelProvider) l;
                    if (rp.getRunlevel() != null) {
                        level = rp.getRunlevel();
                    }
                }
                result.put(level, l);
            }
        }

        return result;
    }
}
