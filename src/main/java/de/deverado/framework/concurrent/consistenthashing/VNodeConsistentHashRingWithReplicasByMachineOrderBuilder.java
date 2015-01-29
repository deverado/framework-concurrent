package de.deverado.framework.concurrent.consistenthashing;/*
 * Copyright Georg Koester 2012-15. All rights reserved.
 */

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.hash.Hashing;
import de.deverado.framework.core.AssertionFailure;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * This builder determines replicas by (possibly hashed) machine order. This requires taking care that machines are in
 * 'leaving' state for a while before being completely removed from the ring (the machine doesn't have to be turned on,
 * only in the state so that data ownership is transferred).
 *
 * For these the following holds when using this builder:
 * <ul>
 *      <li>Replicas for a node are selected by order of machines, not by looking left and/or right of the vnode in the
 *     ring. Georg created this because at the beginning due to a misunderstanding he thought that vnodes increase the
 *     likeliness for multiple downed machines taking down at least one keyrange in the ring.
 *     </li>
 *     <li>If you want to avoid letting the machines hide data you must ensure that before retiring
 *     {@link de.deverado.framework.concurrent.consistenthashing.MachineInRingState#LEAVING} machines from the ring on
 *     a ring update the next vnode owning the data is not lead by a
 *     {@link de.deverado.framework.concurrent.consistenthashing.MachineInRingState#BOOTSTRAPPING} machine, because
 *     the {@link de.deverado.framework.concurrent.consistenthashing.MachineInRingState#BOOTSTRAPPING} machine will not
 *     be a readReplica for the data. That will be the machines of the next
 *     {@link de.deverado.framework.concurrent.consistenthashing.MachineInRingState#LEAVING} or
 *     {@link de.deverado.framework.concurrent.consistenthashing.MachineInRingState#INTEGRATED} machine-lead vnode.
 *     </li>
 * </ul>
 */
public class VNodeConsistentHashRingWithReplicasByMachineOrderBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(VNodeConsistentHashRingWithReplicasByMachineOrderBuilder.class);

    private static final int DEFAULT_REPLICA_COUNT = 5;

    private SortedMap<String, Pair<MachineInRingState, long[]>> machines = new TreeMap<>();

    private boolean hashMachineOrder = true;

    private int replicaCount = DEFAULT_REPLICA_COUNT;

    /**
     *
     * @param id collisions not allowed
     * @param state user has to determine the state the machine should be in. This is not managed by this API. This
     *              API will only ensure that readReplicas and writeReplicas will be determined correctly.
     */
    public VNodeConsistentHashRingWithReplicasByMachineOrderBuilder addMachine(String id, MachineInRingState state, long... vnodes) {
        machines.put(id, Pair.of(state, vnodes));
        return this;
    }

    /**
     * Defaults to true, set this to false for simpler testing for example. Otherwise it might make the ring less
     * resilient.
     */
    public VNodeConsistentHashRingWithReplicasByMachineOrderBuilder setHashMachineOrder(boolean hashMachineOrder) {
        this.hashMachineOrder = hashMachineOrder;
        return this;
    }

    public VNodeConsistentHashRingWithReplicasByMachineOrderBuilder setReplicaCount(int replicaCount) {
        if (replicaCount < 1) {
            throw new IllegalArgumentException("Replica count must be 1 at least, got: " + replicaCount);
        }
        this.replicaCount = replicaCount;
        return this;
    }

    public VNodeConsistentHashRing build() {
        return build(null);
    }

    public VNodeConsistentHashRing build(@Nullable ConsistentHashRingHoldingResourceEntries<?, VNodeImpl, String, String> ring) {

        if (ring == null) {
            ring = ConsistentHashRings.newMurmur3Ring64Bit();
        }

        List<Machine> allMachines = new ArrayList<>(machines.size());
        for (Map.Entry<String, Pair<MachineInRingState, long[]>> entry : machines.entrySet()) {
            Machine m = new Machine();
            allMachines.add(m);
            m.setMachineId(entry.getKey());
            m.setState(entry.getValue().getLeft());
        }

        allMachines = ImmutableList.copyOf(allMachines);

        if (hashMachineOrder) {
            allMachines = consistentHashOrderForMachinesById(allMachines);
            int i = 0;
            for (Machine m : allMachines) {
                m.init(allMachines, i);
                i++;
            }
        } else {
            int i = 0;
            for (Machine m : allMachines) {
                m.init(allMachines, i);
                i++;
            }
        }

        // ring building:
        // first add all vnodes,
        // then update read and write replica lists by checking neighboring vnodes.
        // optional: Remove uninteresting (non-INTEGRATED) nodes.
        for (final Machine m : allMachines) {

            final List<Machine> readReplicas = determineReadReplicasFromMachineOrder(m);
            final List<Machine> writeReplicas = determineWriteReplicasFromMachineOrder(m);

            ConsistentHashRings.addVNodeMachineToRing(ring, m.getMachineId(),
                    new Function<Pair<String, Long>, VNodeImpl>() {
                        @Nullable
                        @Override
                        public VNodeImpl apply(Pair<String, Long> input) {
                            VNodeImpl result = new VNodeImpl();
                            result.init(input.getRight());
                            if (!input.getLeft().equals(m.getMachineId())) {
                                throw new AssertionFailure("This shouldn't happen");
                            }
                            result.setLeader(m);
                            // new lists because per-vnode modifications later required
                            result.setReadReplicas(new ArrayList<Machine>(readReplicas));
                            result.setWriteReplicas(new ArrayList<Machine>(writeReplicas));
                            return result;
                        }
                    },
                    machines.get(m.getMachineId()).getRight());
        }

        // circle left through the ring, must start at an integrated-led vnode:
        final VNodeImpl startNode = findOneIntegratedLedVNodeOrNull(ring);
        VNodeImpl currNode = startNode;
        while (currNode != null) {
            // only look at nodes that are able to serve reads - nodes led by integrated or leaving machines
            if (MachineInRingState.LEAVING.equals(currNode.getLeader().getState())
                    || MachineInRingState.INTEGRATED.equals(currNode.getLeader().getState())) {

                // reminder: a node owns data before its hash; -2 owned by 0. (see ConsistentHashRingImpl)

                // DIFFICULT: check for possible former owners of the key range. Reading should rather be done by
                // checking on all nodes for data belonging to some region - or by retiring LEAVING nodes only
                // after data was completely taken over.


                // What's going on? Make a drawing to understand: b left of b then leaving or integrated, l left of i or
                // l. Going through the ring leftwards, so when checking a node left of l for example the l will already
                // have an extended write list with replicas of a preceeding integrated or bootstrapping-led vnode...

                VNodeImpl previous = ring.getNodeBeforeNode(currNode.getHash());
                boolean finished = false;
                while (previous != null && !finished) {
                    MachineInRingState state = previous.getLeader().getState();
                    switch (state) {
                        case BOOTSTRAPPING:
                            // we need to get writes for this new node, too - as long as we still serve the reads
                            previous.getWriteReplicas().addAll(currNode.getWriteReplicas());
                            // read from current owner - currNode/us - synchronize replica list (bootstrapping don't have all their data yet)
                            previous.setReadReplicas(currNode.getReadReplicas());
                            break;
                        case INTEGRATED:
                            finished = true; // new read owner starting
                            break;
                        case LEAVING:
                            // we will take over from this node, we want writes (too) and let the leaver continue to serve reads
                            previous.getWriteReplicas().addAll(currNode.getWriteReplicas());
                            finished = true; // going to continue there. The leaving node will set its write list to following leaving nodes
                            break;
                        case OUT_OF_RING:
                            // ignore
                            break;
                        default:
                            throw new AssertionFailure("Missing state: " + state);
                    }
                    previous = ring.getNodeBeforeNode(previous.getHash());
                }
            }

            currNode = ring.getNodeBeforeNode(currNode.getHash()); // left circling
            if (currNode == startNode) {
                break;
            }
        }


        // finish replica lists, drop duplicates,
        // remove out-of-ring nodes
        finishNodesByAddingToMachineAndRemovingDuplicatesFromReplicaListsMaintainingOrderAndRemoveOldNodes(ring);
        for (Machine m : allMachines) {
            m.finishSetup();
        }

        return VNodeConsistentHashRing.create(allMachines, ring);
    }

    private List<Machine> consistentHashOrderForMachinesById(List<Machine> allMachines) {
        Map<String, Machine> machineMapping = new HashMap<>(allMachines.size());
        for (Machine m : allMachines) {
            String hash = Hashing.sha1().newHasher().putString(m.getMachineId(), Charsets.UTF_8).hash().toString();
            Machine collision = machineMapping.put(hash, m);
            if (collision != null) {
                Machine drop;

                // don't drop a live machine without need:
                if (MachineInRingState.LEAVING.equals(collision.getState())
                        || MachineInRingState.INTEGRATED.equals(collision.getState())) {
                    drop = m;
                    machineMapping.put(hash, collision); // replace
                } else {
                    drop = collision;
                }
                LOG.error("Dropping machine {} from ring due to SHA1 hash collision.", drop);
            }
        }

        List<String> hashOrder = new ArrayList<>(machineMapping.keySet());
        Collections.sort(hashOrder);

        List<Machine> machinesInHashOrder = new ArrayList<>(hashOrder.size());
        for (String hash : hashOrder) {
            machinesInHashOrder.add(machineMapping.get(hash));
        }
        return machinesInHashOrder;
    }

    private void finishNodesByAddingToMachineAndRemovingDuplicatesFromReplicaListsMaintainingOrderAndRemoveOldNodes(
            ConsistentHashRingHoldingResourceEntries<?, VNodeImpl, String, String> ring) {

        Collection<VNodeImpl> toRemove = new ArrayList<>();

        for (VNodeImpl currNode : ring.getNodes()) {
            if (MachineInRingState.OUT_OF_RING.equals(currNode.getLeader().getState())) {
                toRemove.add(currNode);
            } else {
                currNode.getLeader().getVNodes().add(currNode);

                ConsistentHashRings.removeDuplicatesMaintainingOrder(currNode.getReadReplicas());
                ConsistentHashRings.removeDuplicatesMaintainingOrder(currNode.getWriteReplicas());
            }
            currNode.finishSetup();
        }
        for (VNodeImpl n : toRemove) {
            ring.removeNodeByHash(n.getHash());
        }
    }

    @Nullable
    private VNodeImpl findOneIntegratedLedVNodeOrNull(
            ConsistentHashRingHoldingResourceEntries<?, VNodeImpl, String, String> ring) {
        for (VNodeImpl n : ring.getNodes()) {
            if (MachineInRingState.INTEGRATED.equals(n.getLeader().getState())) {
                return n;
            }
        }
        return null;
    }

    protected List<Machine> determineReadReplicasFromMachineOrder(Machine m) {
        List<Machine> result = new ArrayList<>();

        int count = replicaCount;
        for (Machine possibleReplica : Iterables.concat(Collections.singleton(m), m.getReplicaIterable())) {
            if (count <= 0) break;
            // basic idea here: only return machines that should have a complete data set
            switch (possibleReplica.getState()) {
                case BOOTSTRAPPING:
                    // write-replica
                    break;
                case INTEGRATED:
                    // full replica
                    count--;
                    result.add(possibleReplica);
                    break;
                case LEAVING:
                    // still reading from this - while the following integrated or bootstrapping machine is taking over.
                    // leaving can disappear when this process has finished.
                    result.add(possibleReplica);
                    count--; // counting down, because should still be a full replica with all data (writes and
                    // replicated existing data) - don't want to include a new read replica before it took over
                    // from this leaving replica
                    break;
                case OUT_OF_RING:
                    // ignore
                    break;
                default:
                    throw new AssertionFailure("Missing state: " + possibleReplica.getState());

            }
        }
        return result;
    }

    protected List<Machine> determineWriteReplicasFromMachineOrder(Machine m) {
        List<Machine> result = new ArrayList<>();

        int count = replicaCount;
        for (Machine possibleReplica : Iterables.concat(Collections.singleton(m), m.getReplicaIterable())) {
            if (count <= 0) break;
            // basic idea here: return machines that need or might need the data, such as machines that might become
            // replicas in the future such as bootstrapping machines, or integrated machines that follow leaving
            // machines.
            switch (possibleReplica.getState()) {
                case BOOTSTRAPPING:
                    result.add(possibleReplica);
                    // but not counting down, because not a full replica - doesn't have full data set, can disappear without warning
                    break;
                case INTEGRATED:
                    // full replica
                    count--;
                    result.add(possibleReplica);
                    break;
                case LEAVING:
                    // still receives writes, but not counting down, to send new writes to replacement replicas
                    result.add(possibleReplica);
                    break;
                case OUT_OF_RING:
                    // ignore
                    break;
                default:
                    throw new AssertionFailure("Missing state: " + possibleReplica.getState());
            }
        }
        return result;
    }
}
