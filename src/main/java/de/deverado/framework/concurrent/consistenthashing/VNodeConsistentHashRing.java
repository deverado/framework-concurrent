package de.deverado.framework.concurrent.consistenthashing;/*
 * Copyright Georg Koester 2012-15. All rights reserved.
 */

import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Collections;
import java.util.List;

/**
 * This class provides a usable interface to the complexity of ring management and usage. For a set of machines
 * with states and vnodes this ring ensures that hash->vnode mapping is possible. The vnodes have readReplicas and
 * writeReplicas set. The exact semantics depend on the builder used for creating the ring:
 * <ul>
 *     <li>Replica count is limited by: Maximum of x (5, see Builder) active replicas for one vnode. If there is a
 *     {@link de.deverado.framework.concurrent.consistenthashing.MachineInRingState#BOOTSTRAPPING} or
 *     {@link de.deverado.framework.concurrent.consistenthashing.MachineInRingState#LEAVING} machine leading a vnode,
 *     the list of writeReplicas can be longer than 5. </li>
 *     <li>If there are not enough machines to satisfy the replication count condition the lists will be shorter. A ring
 *     with only 1 machine will have a max replica count of 1.</li>
 *     <li>leader is the owner of the vnode, the machine that has the lowest order id and had the hash assigned when
 *     building the ring. The machine must be of status
 *     {@link de.deverado.framework.concurrent.consistenthashing.MachineInRingState#BOOTSTRAPPING},
 *     {@link de.deverado.framework.concurrent.consistenthashing.MachineInRingState#INTEGRATED}, or
 *     {@link de.deverado.framework.concurrent.consistenthashing.MachineInRingState#LEAVING}.
 *     This means that not all machines given to the builder might be in the ring.</li>
 *     <li>readReplicas are the leader of the nearest vnode that the hash can be assigned to with state
 *     {@link de.deverado.framework.concurrent.consistenthashing.MachineInRingState#INTEGRATED or
 *     {@link de.deverado.framework.concurrent.consistenthashing.MachineInRingState#LEAVING} and their replicas.</li>
 *     <li>writeReplicas are in short the new owners AND the old owners, so that takeover is seamless. Writes can
 *     go to a
 *     {@link de.deverado.framework.concurrent.consistenthashing.MachineInRingState#BOOTSTRAPPING} machine taking over
 *     the node or to a {@link de.deverado.framework.concurrent.consistenthashing.MachineInRingState#LEAVING} machine
 *     that surrenders ownership (but still serves reads).</li>
 *
 * </ul>
 *
 */
@ParametersAreNonnullByDefault
public class VNodeConsistentHashRing {

    private ConsistentHashRingHoldingResourceEntries<?, VNodeImpl, String, String> ring;
    private List<Machine> allMachineList;
    private int machineInfos;

    public static VNodeConsistentHashRing create(List<Machine> allMachines,
                                                 ConsistentHashRingHoldingResourceEntries<?, VNodeImpl, String,
                                                         String> ring) {
        VNodeConsistentHashRing result = new VNodeConsistentHashRing();
        result.ring = ring;
        result.allMachineList = ImmutableList.copyOf(allMachines);
        return result;
    }

    /**
     * @return check machine's vnode list to know if it leads vnodes in the ring.
     */
    public List<Machine> getMachines() {
        return allMachineList;
    }

    public List<MachineInfo> getMachineInfos() {
        return Machine.asMachineInfo(getMachines());
    }

    @Nullable
    public VNodeImpl getNodeForHash(Long hash) {
        return ring.getNodeForEntry(hash);
    }

    @Nullable
    public VNodeImpl getNode(String key) {
        return ring.getNodeForEntry(key);
    }

    /**
     * @return the machines that should have a clear picture of the data: leaving machines (still getting writes) or
     * integrated machines. The list might not contain the leader of the vnode to which the hash maps, because if that
     * leader is a bootstrapping machine it will not be in the list, because its data is not yet consistent.
     */
    public List<Machine> getReadReplicasForHash(Long hash) {
        VNodeImpl nodeForHash = getNodeForHash(hash);
        if (nodeForHash != null) {
            return nodeForHash.getReadReplicas();
        }
        return Collections.emptyList();
    }

    /**
     * @return a list of machines that should all receive writes for the given hash. They are the main replicas
     * for the machine owning/leading the vnode as well as possible bootstrapping machines that will take over the area
     * from an integrated machine, or integrated or bootstrapping machines that will take over an area from a leaving
     * machine.
     */
    public List<Machine> getWriteReplicasForHash(Long hash) {
        VNodeImpl nodeForHash = getNodeForHash(hash);
        if (nodeForHash != null) {
            return nodeForHash.getWriteReplicas();
        }
        return Collections.emptyList();
    }

}
