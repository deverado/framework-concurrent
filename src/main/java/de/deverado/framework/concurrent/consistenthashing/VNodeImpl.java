package de.deverado.framework.concurrent.consistenthashing;/*
 * Copyright Georg Koester 2012-15. All rights reserved.
 */

import com.google.common.collect.ImmutableList;

import javax.annotation.ParametersAreNonnullByDefault;

import java.util.List;
import java.util.Objects;

@ParametersAreNonnullByDefault
public class VNodeImpl implements VNode {

    private Long hash;
    private Machine leader;
    private List<Machine> readReplicas;
    private List<Machine> writeReplicas;

    void init(Long hash) {
        this.hash = hash;
    }

    void setLeader(Machine leader) {
        this.leader = leader;
    }

    void finishSetup() {
        readReplicas = ImmutableList.copyOf(readReplicas);
        writeReplicas = ImmutableList.copyOf(writeReplicas);
    }

    @Override
    public String getLeaderId() {
        return this.getLeader().getMachineId();
    }

    /**
     * Replica order is important - should be in ring-order!
     */
    void setReadReplicas(List<Machine> readReplicas) {
        this.readReplicas = readReplicas;
    }

    /**
     * Replica order is important - should be in ring-order!
     */
    void setWriteReplicas(List<Machine> writeReplicas) {
        this.writeReplicas = writeReplicas;
    }

    public Long getHash() {
        return hash;
    }

    public Machine getLeader() {
        return leader;
    }

    /**
     * @return a list containing also the leader if that is a read replica for this vnode
     */
    public List<Machine> getReadReplicas() {
        return readReplicas;
    }

    public AssignmentState getAssignmentStateForMachine(String machineId) {
        return getAssignmentStateForMachine(machineId, -1);
    }

    public AssignmentState getAssignmentStateForMachine(String machineId, int replicaCount) {
        boolean readReplica = isReadReplica(machineId, replicaCount);
        boolean writeReplica = isWriteReplica(machineId, replicaCount);
        if (readReplica && writeReplica) {
            return AssignmentState.READ_WRITE_REPLICA;
        }
        if (readReplica) {
            return AssignmentState.READ_REPLICA;
        }
        if (writeReplica) {
            return AssignmentState.WRITE_REPLICA;
        }
        return AssignmentState.UNRELATED;
    }

    public boolean isReadReplica(String machineId) {
        for (Machine m : getReadReplicas()) {
            if (Objects.equals(m.getMachineId(), machineId)) {
                return true;
            }
        }
        return false;
    }

    /**
     * checks with the replicaCount limit that was specified upon ring creation.
     */
    public boolean isReadReplica(String machineId, int replicaCount) {
        if (replicaCount <= 0) {
            replicaCount = Integer.MAX_VALUE;
        }

        int foundIntegratedOrLeaving = 0;
        outer:
        for (Machine m : getReadReplicas()) {
            if (Objects.equals(m.getMachineId(), machineId)) {
                return true;
            }
            switch (m.getState()) {
                case INTEGRATED:
                case LEAVING: // need to count leaving so as to avoid returning non-initialized INTEGRATED nodes.
                    foundIntegratedOrLeaving++;
                    if (foundIntegratedOrLeaving >= replicaCount) {
                        break outer; // machineId not within the first replicaCount INTEGRATED or LEAVING read replicas,
                        // so not a replica under the given limit
                    }
                    break;
                case BOOTSTRAPPING:
                case OUT_OF_RING:
                    // ignore
                    break;
                default:
                    throw new IllegalStateException("Missing a state value in " + getClass());

            }
        }
        return false;
    }

    /**
     * The length of the list is determined by the replicaCount setting on the ring builder. Every builder is
     * different but in general it is important to write to all targetReplicaCount INTEGRATED nodes and the other
     * nodes between them to ensure that the LEAVING/BOOTSTRAPPING algo works (if you target a smaller replicaCount
     * than what was given in the builder and you want to shrink this list).
     * @return a list containing also the leader if that is a write replica for this vnode
     */
    public List<Machine> getWriteReplicas() {
        return writeReplicas;
    }

    /**
     * checks with the replicaCount limit that was specified upon ring creation.
     */
    public boolean isWriteReplica(String machineId) {
        for (Machine m : getWriteReplicas()) {
            if (Objects.equals(m.getMachineId(), machineId)) {
                return true;
            }
        }
        return false;
    }

    public boolean isWriteReplica(String machineId, int replicaCount) {
        if (replicaCount <= 0) {
            replicaCount = Integer.MAX_VALUE;
        }

        int foundIntegrated = 0;
        boolean seenLeader = false;
        for (Machine m : getWriteReplicas()) {
            if (Objects.equals(m.getMachineId(), machineId)) {
                return true;
            }

            if (!seenLeader && Objects.equals(getLeaderId(), m.getMachineId())) {
                seenLeader = true;
            }
            if (MachineInRingState.INTEGRATED.equals(m.getState())) {
                // writers might contain an INTEGRATED machine from a node before the lead node which won't count
                // towards the replicaCount: When the node is led by a LEAVING machine. Then the first integrated
                // machine found before it is a write replica in prep of becoming the new boss/owner of this node's
                // token range. But it's not a real replica yet, only bootstrapping this range. So don't count it or
                // running replicas will be removed from the (shortened) list of replicas too soon.
                if (seenLeader) {
                    foundIntegrated++;
                    if (foundIntegrated >= replicaCount) {
                        break; // machineId not within the first replicaCount INTEGRATED read replicas, so not a
                        // replica under the given limit
                    }
                }
            }
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof VNodeImpl)) return false;

        VNodeImpl vNode = (VNodeImpl) o;

        if (!hash.equals(vNode.hash)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return hash.hashCode();
    }

    @Override
    public String toString() {
        return "VNodeImpl{" +
                "hash=" + hash +
                ", leader=" + leader +
                ", readReplicas=" + readReplicas +
                ", writeReplicas=" + writeReplicas +
                '}';
    }
}
