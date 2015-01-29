package de.deverado.framework.concurrent.consistenthashing;/*
 * Copyright Georg Koester 2012-15. All rights reserved.
 */

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;

@ParametersAreNonnullByDefault
public class VNodeConsistentHashRingMgr {

    private static final Logger LOG = LoggerFactory.getLogger(VNodeConsistentHashRingMgr.class);

    private final ConcurrentLinkedDeque<Boolean> ringUpdateRequests = new ConcurrentLinkedDeque<>();

    private ListeningExecutorService executor;

    private Callable<ListenableFuture<? extends VNodeConsistentHashRing>> ringCreator;

    private volatile VNodeConsistentHashRing ring;

    private final List<VNodeConsistentHashRingChangeListener> ringChangeListeners = new CopyOnWriteArrayList<>();

    /**
     *
     * @param executor for future listening
     */
    public void init(Callable<ListenableFuture<? extends VNodeConsistentHashRing>> ringCreator,
                     ListeningExecutorService executor) {
        this.ringCreator = ringCreator;
        this.executor = executor;
    }

    /**
     * ring will be created - listeners notified on change.
     */
    public void startRing() {
        ringChangedPossibly();
    }

    public VNodeConsistentHashRing getRing() {
        return ring;
    }

    public void addRingListener(VNodeConsistentHashRingChangeListener listener) {
        if (!ringChangeListeners.contains(listener)) {
            ringChangeListeners.add(listener);
        }
    }

    public void removeRingListener(VNodeConsistentHashRingChangeListener listener) {
        ringChangeListeners.remove(listener);
    }

    /**
     * Checks for changes (with id and state) and updates ring if necessary.
     */
    public void ringChangedPossibly() {
        synchronized (ringUpdateRequests) {
            ringUpdateRequests.addLast(Boolean.TRUE);
            if (ringUpdateRequests.size() == 1) {
                runRingUpdate(); // starts it - if there are more it is already running
            }
        }
    }

    private void continueRingUpdatingIfNecessary() {
        boolean finished = false;
        synchronized (ringUpdateRequests) {
            ringUpdateRequests.removeFirst();
            if (ringUpdateRequests.size() == 0) {
                finished = true;
            }
        }
        if (!finished) {
            runRingUpdate();
        }
    }

    private void runRingUpdate() {

        // TODO decide if intermediate updates should be dropped - is that a problem?
        try {
            ringUpdateRequests.getFirst();
        } catch (NoSuchElementException nse) {
            throw new IllegalStateException("Threading problem - it should be impossible to have an updater run when " +
                    "no updates are queued.", nse);
        }

        try {
            final ListenableFuture<? extends VNodeConsistentHashRing> future = ringCreator.call();
            future.addListener(new Runnable() {
                @Override
                public void run() {
                    try {
                        VNodeConsistentHashRing newRing = future.get();
                        RingDifferenceImpl ringDifferenceAfterUpdate = RingDifferenceImpl.create(getRing(),
                                Machine.asMachineInfo(newRing.getMachines()));

                        if (ringDifferenceAfterUpdate.isHavingDifference()) {
                            updateRing(newRing, ringDifferenceAfterUpdate);
                        }
                    } catch (Exception e) {
                        LOG.warn("Ring change failed due to creator failure, trying again on next update", e);
                    }
                    continueRingUpdatingIfNecessary();
                }
            }, executor);

        } catch (Exception e) {
            LOG.warn("Ring change failed due to creator failure, trying again on next udpate", e);
            continueRingUpdatingIfNecessary();
        }

    }

    private void updateRing(VNodeConsistentHashRing newRing, RingDifferenceImpl ringDifferenceAfterUpdate) {
        VNodeConsistentHashRing old;
        synchronized (this) {
            old = ring;
            ring = newRing;
        }
        notifyRingChangeListeners(old, newRing, ringDifferenceAfterUpdate);
    }

    private void notifyRingChangeListeners(@Nullable VNodeConsistentHashRing old, VNodeConsistentHashRing newRing,
                                           RingDifference diff) {
        for (VNodeConsistentHashRingChangeListener l : ringChangeListeners) {
            try {
                l.changed(old, newRing, diff);
            } catch (Exception e) {
                LOG.warn("Exception in ring change listener: {}, ignoring", l, e);
            }
        }
    }


}
