package de.deverado.framework.concurrent.consistenthashing;/*
 * Copyright Georg Koester 2012-15. All rights reserved.
 */

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@ParametersAreNonnullByDefault
public class FixedShardCountReplicatedTaskMgr implements VNodeConsistentHashRingChangeListener {

    private static final Logger LOG = LoggerFactory.getLogger(FixedShardCountReplicatedTaskMgr.class);

    private VNodeConsistentHashRingMgr ringMgr;

    private UnshardedReplicatedTaskFactory factory;

    private Map<String, AssignmentState> assignmentStates = Collections.emptyMap();

    private String executingMachineId;

    private int replicaCount = -1;

    private final Map<String, TaskAssignment> createdTasks = new HashMap<>();
    private Iterable<String> shardIds;

    public void init(String executingMachineId, VNodeConsistentHashRingMgr ringMgr,
                     Iterable<String> shardIds,
                     UnshardedReplicatedTaskFactory factory) {

        this.executingMachineId = executingMachineId;
        this.shardIds = shardIds;
        this.ringMgr = ringMgr;
        this.factory = factory;

        ringMgr.addRingListener(this);
        if (ringMgr.getRing() != null) {
            changed(null, ringMgr.getRing(), RingDifferenceImpl.create(null, ringMgr.getRing().getMachineInfos()));
        }
    }

    /**
     * To shut the mgr's tasks down in an orderly fashion.
     */
    public void shutdown() {
        ringMgr.removeRingListener(this);
        synchronized (createdTasks) {
            for (String shardId : shardIds) {

                changeTaskAssignmentStateIfNecessary(shardId, AssignmentState.UNRELATED);
            }
            assignmentStates = Collections.emptyMap();
        }
    }

    /**
     * @param replicaCount less than 1: default , else the replicaCount to use. If ring doesn't support it (too high)
     *                     then the highest possible is used.
     */
    public FixedShardCountReplicatedTaskMgr setReplicaCount(int replicaCount) {
        this.replicaCount = replicaCount;
        return this;
    }

    public int getReplicaCount() {
        return replicaCount;
    }

    @Override
    public void changed(@Nullable VNodeConsistentHashRing old, VNodeConsistentHashRing newRing,
                        RingDifference difference) {

        synchronized (createdTasks) {
            Map<String, AssignmentState> assignments = new HashMap<>();

            for (String shardId : shardIds) {
                AssignmentState newState = getStateInRing(newRing, shardId);
                if (newState == null) {
                    LOG.warn("Ring seems down: {}. Could not get a node for '{}', got null. Continuing " +
                                    "with old assignments.", newRing,
                            shardId);
                    return;
                }

                changeTaskAssignmentStateIfNecessary(shardId, newState);

                if (newState.isReplicaState()) {
                    assignments.put(shardId, newState);
                }
            }

            assignmentStates = ImmutableMap.copyOf(assignments);
        }
    }

    protected void changeTaskAssignmentStateIfNecessary(String shardId, AssignmentState newState) {
        synchronized (createdTasks) {

            TaskAssignment assignment = createdTasks.get(shardId);

            switch (newState) {
                case UNRELATED:
                    try {
                        if (assignment != null) {
                            assignment.becomeUnrelated();
                            createdTasks.remove(shardId); // no (current) need to keep deassigned tasks
                        }
                    } catch (Exception e) {
                        LOG.error("Could not unrelate task {}, dropping it.", shardId, e);
                    }
                    break;
                case READ_REPLICA:
                case WRITE_REPLICA:
                case READ_WRITE_REPLICA:
                    // catch here so that all successfully started tasks are taken over into the mgr
                    try {
                        if (assignment == null) {
                            assignment = TaskAssignment.create(factory, shardId);
                            assignment.becomeReplica(newState); // ensure this is only added if successful
                            createdTasks.put(shardId, assignment);
                        } else {
                            assignment.becomeReplica(newState);
                        }
                    } catch (Exception e) {
                        LOG.error("Could not change state of task {}, ignored.", shardId, e);
                    }
                    break;
                default:
                    String msg = "Unknown state! Fix " + getClass() + " !";
                    LOG.error(msg);
                    throw new IllegalStateException(msg);
            }
        }

    }

    @Nullable
    private AssignmentState getStateInRing(VNodeConsistentHashRing newRing, String shardId) {
        VNodeImpl node = newRing.getNode(shardId);
        if (node != null) {
            return node.getAssignmentStateForMachine(getExecutingMachineId(), getReplicaCount());
        }

        return null;
    }

    /**
     * @return shards that this machine is a replica for
     */
    public Map<String, AssignmentState> getAssignmentStates() {

        return assignmentStates;
    }

    public Iterable<String> getShardIds() {
        return shardIds;
    }

    public String getExecutingMachineId() {
        return executingMachineId;
    }

    protected static class TaskAssignment {
        private AssignmentState assignmentState;
        private UnshardedReplicatedTask task;
        private String shardId;

        public static TaskAssignment create(UnshardedReplicatedTaskFactory factory, String shardId) {
            TaskAssignment result = new TaskAssignment();
            result.task = factory.create(shardId);
            result.shardId = shardId;
            return result;
        }

        public AssignmentState getAssignmentState() {
            return assignmentState;
        }

        public UnshardedReplicatedTask getTask() {
            return task;
        }

        public String getShardId() {
            return shardId;
        }

        @Override
        public String toString() {
            return "TaskAssignment{" +
                    "shardId='" + shardId + '\'' +
                    ", assignmentState=" + assignmentState +
                    '}';
        }

        public void becomeUnrelated() {
            if (!Objects.equals(getAssignmentState(), AssignmentState.UNRELATED)) {
                getTask().becomeUnrelated();
                assignmentState = AssignmentState.UNRELATED;
            }
        }

        public void becomeReplica(AssignmentState newState) {
            if (!Objects.equals(getAssignmentState(), newState)) {
                getTask().becomeReplica(newState);
                assignmentState = newState;
            }
        }
    }

    @Override
    public String toString() {
        return "FixedShardCountReplicatedTaskMgr{" +
                "executingMachineId='" + executingMachineId + '\'' +
                ", factory=" + factory +
                ", assignedShardIdCount=" + getAssignmentStates().size() +
                '}';
    }
}
