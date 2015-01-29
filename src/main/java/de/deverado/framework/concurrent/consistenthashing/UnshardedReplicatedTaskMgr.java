package de.deverado.framework.concurrent.consistenthashing;/*
 * Copyright Georg Koester 2012-15. All rights reserved.
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import java.util.Objects;

@ParametersAreNonnullByDefault
public class UnshardedReplicatedTaskMgr implements VNodeConsistentHashRingChangeListener {

    private static final Logger LOG = LoggerFactory.getLogger(UnshardedReplicatedTaskMgr.class);

    private VNodeConsistentHashRingMgr ringMgr;
    private UnshardedReplicatedTask task;

    private AssignmentState assignmentState = AssignmentState.UNRELATED;
    private String executingMachineId;

    private int replicaCount = -1;

    public void init(String executingMachineId, VNodeConsistentHashRingMgr ringMgr,
                     UnshardedReplicatedTask managedTask) {

        this.executingMachineId = executingMachineId;
        this.ringMgr = ringMgr;
        this.task = managedTask;

        ringMgr.addRingListener(this);
        if (ringMgr.getRing() != null) {
            changed(null, ringMgr.getRing(), RingDifferenceImpl.create(null, ringMgr.getRing().getMachineInfos()));
        }
    }

    /**
     * @param replicaCount less than 1: default , else the replicaCount to use. If ring doesn't support it (too high)
     *                     then the highest possible is used.
     */
    public UnshardedReplicatedTaskMgr setReplicaCount(int replicaCount) {
        this.replicaCount = replicaCount;
        return this;
    }

    public int getReplicaCount() {
        return replicaCount;
    }

    /**
     * To shut the mgr's task down in an orderly fashion.
     */
    public void shutdown() {
        ringMgr.removeRingListener(this);
        synchronized (this) {
            assignmentState = AssignmentState.UNRELATED;
            task.becomeUnrelated();
        }
    }

    @Override
    public void changed(@Nullable VNodeConsistentHashRing old, VNodeConsistentHashRing newRing,
                        RingDifference difference) {

        AssignmentState newState = getStateInRing(newRing);
        synchronized (this) {
            if (!Objects.equals(newState, getAssignmentState())) {
                if (newState == null) {
                    throw new NullPointerException("AssignmentState must not be null");
                }
                assignmentState = newState;
                switch (newState) {
                    case UNRELATED:
                        task.becomeUnrelated();
                        break;
                    case READ_REPLICA:
                    case WRITE_REPLICA:
                    case READ_WRITE_REPLICA:
                        task.becomeReplica(newState);
                        break;
                    default:
                        String msg = "Unknown state! Fix " + getClass() + " !";
                        LOG.error(msg);
                        throw new IllegalStateException(msg);
                }
            }
        }
    }

    private AssignmentState getStateInRing(VNodeConsistentHashRing newRing) {
        VNodeImpl node = newRing.getNode(task.getName());
        if (node != null) {
            return node.getAssignmentStateForMachine(getExecutingMachineId(), getReplicaCount());
        } else {
            LOG.warn("Ring seems down: {}. Could not get a node for '{}', got null. Continuing as before.", newRing,
                    task.getName());
            return getAssignmentState();
        }
    }

    public AssignmentState getAssignmentState() {
        return assignmentState;
    }

    public String getExecutingMachineId() {
        return executingMachineId;
    }
}
