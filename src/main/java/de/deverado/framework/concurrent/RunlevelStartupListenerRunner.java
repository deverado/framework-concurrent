package de.deverado.framework.concurrent;/*
 * Copyright Georg Koester 2012-15. All rights reserved.
 */

import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.tuple.Pair;
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

    private List<Function<Pair<String, Collection<StartupListener>>, Void>> preRunListeners = new ArrayList<>();

    public static RunlevelStartupListenerRunner create(Multimap<String, StartupListener> listenersByRunlevel) {

        RunlevelStartupListenerRunner result = new RunlevelStartupListenerRunner();
        result.listenersByRunlevel = listenersByRunlevel;
        return result;
    }

    public void addPreRunListener(Function<Pair<String, Collection<StartupListener>>, Void> preRunListener) {
        preRunListeners.add(preRunListener);
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
                if (preRunListeners.size()> 0) {
                    LOG.debug("Running {} pre-run listeners for level {}", preRunListeners.size(),
                            runlevel);
                    for (Function<Pair<String, Collection<StartupListener>>, Void> l : preRunListeners) {
                        l.apply(Pair.of(runlevel, listeners));
                    }
                }

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
