package de.deverado.framework.concurrent.consistenthashing;/*
 * Copyright Georg Koester 2012-15. All rights reserved.
 */

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * Aggregates state information from different guice-registered
 * {@link de.deverado.framework.concurrent.consistenthashing.ServiceStateProvider}.
 * Must have at least one listener set to get updates!
 */
@ParametersAreNonnullByDefault
public class MachineInRingStateMgr {

    private static final Logger LOG = LoggerFactory.getLogger(MachineInRingStateMgr.class);
    private static final long DEFAULT_UPDATE_RETRY_DELAY_MS = 500;
    private static final long DEFAULT_UPDATER_DELAY_MS = 2000;

    private Collection<ServiceStateProvider> serviceStateProviders;

    private final List<MachineInRingStateListener> stateListeners = new CopyOnWriteArrayList<>();

    private ListeningScheduledExecutorService scheduledExecutor;

    private volatile MachineInRingState currentState = null;

    private volatile ListenableScheduledFuture<?> updaterCancelFuture;

    private long updateRetryDelayMs = DEFAULT_UPDATE_RETRY_DELAY_MS;

    private long updaterDelayMs = DEFAULT_UPDATER_DELAY_MS;

    /**
     * State updating will only start after initial delay - if you want direct updates, call {@link #updateState()}.
     */
    public void init(Collection<? extends ServiceStateProvider> providers,
                     ListeningScheduledExecutorService scheduledExecutor) {
        serviceStateProviders = new ArrayList<>(providers);
        this.scheduledExecutor = scheduledExecutor;
        updaterJobCheckup();
    }

    /**
     *
     * @return null if no listeners are set, no service is set or no state was ever set.
     */
    @Nullable
    public MachineInRingState getCurrentState() {
        return currentState;
    }

    /**
     * This sets a given state - but cannot override all state management. So cannot go OUT_OF_RING -> BOOTSTRAPPING.
     * Neither can this be used to go from INTEGRATED back to BOOTSTRAPPING - that will only log an error.
     * BUT you can force a machine to go to INTEGRATED from BOOTSTRAPPING when its services still report BOOTSTRAPPING.
     *
     * @param newState new state
     */
    public void setCurrentState(MachineInRingState newState) {
        boolean changed = false;
        MachineInRingState oldState;
        MachineInRingState newDerivedState;
        synchronized (this) {
            oldState = currentState;
            newDerivedState = MachineInRingState.deriveNewState(LOG,
                    Collections.singletonList(newState), currentState);
            if (!Objects.equals(currentState, newDerivedState)) {
                this.currentState = newDerivedState;
                changed = true;
            }
        }
        if (changed) {
            notifyListeners(oldState, newDerivedState);
        }
    }

    /**
     * Notify manager that a state update might be due. If a change is detected the listeners will be notified.
     */
    public void updateState() {
        MachineInRingState nextState;
        MachineInRingState currentStateCache;


        int rounds = 3;
        boolean finished = false;
        do {
            synchronized (this) {
                nextState = currentState;
                currentStateCache = currentState;
            }

            List<MachineInRingState> statesRead = new ArrayList<>();
            for (ServiceStateProvider p : serviceStateProviders) {
                try {
                    statesRead.add(p.getCurrentState().getState());
                } catch (ServiceStateNotYetAvailableException stateNotAvailableEx) {
                    // retry later
                    scheduleUpdateStateRetry();
                    nextState = currentStateCache;
                    finished = true;
                    break;
                } catch (Exception e) {
                    LOG.error("Cannot read state for service {}", p, e);
                }
            }

            nextState = MachineInRingState.deriveNewState(LOG, statesRead, currentStateCache);

            synchronized (this) {
                if (Objects.equals(currentStateCache, currentState)) {
                    if (!Objects.equals(currentStateCache, nextState)) {
                        currentState = nextState;
                    }
                    finished = true;
                    break;
                }
            }
        } while (--rounds > 0);

        if (!finished) {
            throw new ConcurrentModificationException("Could not update state - concurrent modification?");
        }

        if (!Objects.equals(nextState, currentStateCache)) {
            notifyListeners(currentStateCache, nextState);
        }
    }

    private void scheduleUpdateStateRetry() {
        scheduledExecutor.schedule(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                updateState();
                return Boolean.TRUE;
            }
        }, updateRetryDelayMs, TimeUnit.MILLISECONDS);
    }

    private void notifyListeners(@Nullable MachineInRingState oldState, @Nullable MachineInRingState nextState) {
        for (MachineInRingStateListener l : stateListeners) {
            try {
                l.changed(oldState, nextState);
            } catch (Exception e) {
                LOG.error("Listener threw: {}", l);
            }
        }
    }

    public void addListener(MachineInRingStateListener listener) {
        stateListeners.add(listener);
        updaterJobCheckup();
    }

    private void updaterJobCheckup() {
        synchronized (this) {
            if (stateListeners.isEmpty()) {
                if (updaterCancelFuture != null) {
                    updaterCancelFuture.cancel(false);
                    updaterCancelFuture = null;
                }
            } else {
                if (updaterCancelFuture == null || updaterCancelFuture.isDone()) {
                    if (scheduledExecutor == null) {
                        return; // only after init!
                    }
                    updaterCancelFuture = startUpdaterJob();
                }
            }
        }
    }

    private ListenableScheduledFuture<?> startUpdaterJob() {
        return scheduledExecutor.scheduleWithFixedDelay(
                new Runnable() {

                    @Override
                    public void run() {
                        try {
                            updateState();
                        } catch (Exception e) {
                            LOG.error("Exception in updater job", e);
                        }
                    }
                }, updaterDelayMs, updaterDelayMs, TimeUnit.MILLISECONDS);
    }

    public void removeListener(MachineInRingStateListener listener) {
        stateListeners.remove(listener);
        updaterJobCheckup();
    }

    public void setUpdateRetryDelayMs(int updateRetryDelayMs) {
        Preconditions.checkArgument(updateRetryDelayMs >= 1, "Delay must at least be 1 (ms)");
        this.updateRetryDelayMs = updateRetryDelayMs;
    }

    public long getUpdateRetryDelayMs() {
        return updateRetryDelayMs;
    }

    public long getUpdaterDelayMs() {
        return updaterDelayMs;
    }

    public void setUpdaterDelayMs(long updaterDelayMs) {
        Preconditions.checkArgument(updaterDelayMs >= 1, "Delay must at least be 1 (ms)");
        this.updaterDelayMs = updaterDelayMs;
    }
}
