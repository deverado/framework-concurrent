package de.deverado.framework.concurrent;/*
 * Copyright Georg Koester 2012-15. All rights reserved.
 */

import com.google.common.base.Stopwatch;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.ParametersAreNonnullByDefault;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

@ParametersAreNonnullByDefault
public class RunlevelStartupListenerRunner implements Callable<Void> {

    private static final Logger LOG = LoggerFactory.getLogger(RunlevelStartupListenerRunner.class);

    private Multimap<String, StartupListener> listenersByRunlevel;

    public static RunlevelStartupListenerRunner create(Multimap<String, StartupListener> listenersByRunlevel) {

        RunlevelStartupListenerRunner result = new RunlevelStartupListenerRunner();
        result.listenersByRunlevel = listenersByRunlevel;
        return result;
    }

    @Override
    public Void call() throws Exception {
        List<String> runlevels = new ArrayList<>(listenersByRunlevel.keySet());

        // startup low to high
        Collections.sort(runlevels);

        Stopwatch allWatch = Stopwatch.createStarted();
        for (String runlevel : runlevels) {
            Collection<StartupListener> listeners = listenersByRunlevel.get(runlevel);
            if (listeners != null && !listeners.isEmpty()) {
                LOG.debug("Running {} startup listeners for level {}", listeners.size(), runlevel);

                for (StartupListener l : listeners) {
                    Stopwatch singleWatch = Stopwatch.createStarted();
                    try {

                        l.startup().get();
                    } catch (Exception e) {
                        LOG.error("StartupListenerFailed={}", l, e);
                        throw e;
                    }
                    LOG.debug("Ran startupListener={} in singleStartupListenerMs={}",
                            l, allWatch.elapsed(TimeUnit.MILLISECONDS));
                }
            }
        }
        LOG.debug("Ran startup listeners in startupListenersMs={}", allWatch.elapsed(TimeUnit.MILLISECONDS));
        return null;
    }
}
