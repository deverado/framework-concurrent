package de.deverado.framework.concurrent.consistenthashing;/*
 * Copyright Georg Koester 2012-15. All rights reserved.
 */

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import de.deverado.framework.core.AssertionFailure;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * This builder determines replicas by token order. Reminder: In {@link ConsistentHashRingImpl} a node owns the entries
 * that have the same hash or a hash smaller than the node's hash - the keys before it. So if a vnode disappears from
 * the ring the next bigger vnode will take ownership of the keyrange (too). The replicas stay the same: Replicas are
 * found by walking the ring backwards and finding nodes belonging to a different machine.
 * <p>
 *     Reminder: Holding data on machines with this algorithm requires more logic for the setup and especially for the
 *     initialization of the data - ensuring that machines don't get removed from the ring before their data
 *     responsibilities have been taken over by adequate new replicas/bosses.
 * </p>
 *
 * For these the following holds when using this builder:
 * <ul>
 *     <li>
 *         Assignment of read replicas to nodes follows the node order: Ring towards the left: Nodes which are led by
 *         a not-yet-replica in state LEAVING or INTEGRATED are added as read replicas. Replica count determines how
 *         many INTEGRATED replicas are being searched. Leaving are added to to allow a phase of transition.
 *     </li>
 *     <li>
 *         Write replicas follows the read replica determination, but adds intermittent BOOTSTRAPPING nodes, too, as
 *         they when entering INTEGRATED state will be replacing a replica after them. Furthermore if the node
 *         is LEAVING an INTEGRATED node to the right of it will be looked for and added as a write replica, as will
 *         BOOTSTRAPPING machines leading nodes encountered while looking for the INTEGRATED to replace the LEAVING.
 *     </li>
 *     <li>
 *         This has the advantages that 1. leaving the ring a node will have it's vnodes removed and therefore affect a
 *         random sample of machines from the total set, if #perMachineVnodes >> machineCount possibly all machines,
 *         distributing the load of the ring change. 2. the same is true for a joining machine, whose randomly inserted
 *         vnodes will also affect a random sample of other vnodes. And 3. machine load can be tuned by changing the
 *         number of vnodes depending on the capabilities of a machine.
 *     </li>
 *     <li>
 *         Machines can determine which nodes they are replicas for by using the vnodes list in their machine entry in
 *         the built ring. They have to filter by position to match replica count. Machines should not really
 *         distinguish the cases of being a read replica for a node they lead or for a node they don't lead - otherwise
 *         hash ring concept doesn't work.
 *     </li>
 *     <li>
 *         Disadvantage is of course that more bookkeeping has to be done: A huge cluster will have considerable
 *         memory resources reserved only for bookkeeping of the ring: 10k machines with 256 nodes assigned each will
 *         have 2.5m nodes, each with 2 lists of replicas ... using O(2.5m * replicaCount * 2) memory, easily 200Mb per
 *         ring instance (on every machine and every client). Alternatives: 1. You could lower the vnodePerMachine count
 *         in this case to
 *         lower the memory consumption. But you'll have to monitor the load per machine as this might adversely affect
 *         the effectiveness of the random distribution.
 *         Or 2. the per-node lists can be removed and calculated on-demand. This will remove the 2*replicaCount factor,
 *         and some constant factors.
 *     </li>
 *     <li>
 *         Only LEAVING or INTEGRATED-led nodes will be visible in the built ring. The other vnodes will get removed.
 *         This might become changeable in the future - an alternative being to copy replicas from the LEAVING or
 *         INTEGRATED-led nodes to the other
 *         non-(INTEGRATED|LEAVING)-led nodes. The machines-list will not contain the removed vnodes in the list of
 *         vnodes per-machine. But the machines that have had their nodes removed from the ring contain the vnodes for
 *         which they were determined a replica (write usually) in their list of vnodes.
 *     </li>
 *     <li>
 *         The LEAVING state is still useful for bootstrapping the next replicas in a ring change situation.
 *     </li>
 *     <li>
 *         Some other interesting facts:
 *         <ul>
 *             <li>
 *                 A new ring with only BOOTSTRAPPING machines will not have any active node - ring.getNodeForHash will
 *                 return null.
 *             </li>
 *             <li>
 *                 You can just drop machines instead of adding them as OUT_OF_RING. You can use that state for
 *                 documentation purposes.
 *             </li>
 *         </ul>
 *     </li>
 *     <li>At the beginning Georg thought this will increase the probability that a machine failure after a first
 *     machine has failed will affect availability for at least one key range.
 *     But that's not the case, see example further down.
 *     </li>
 * </ul>
 * <p>
 *     Example calculation for availability impact of vnodes:
 15 nodes, replication factor 3, 256 vnodes, one datacenter, one rack
 </p>
 <p>
 Old single-token case:
 1 node down: 100 % available (even for QUORUM requests)
 2 nodes down: QUORUM requests impacted if second down node owns a replica of the first or vice-versa (2 behind or 2
 before down node): 4/14 = 28 % probability
 3 nodes down: ... more math
 </p>
 <p>
 With 256 vnodes:
 1 node down: 100 % available, 256 tokens have 1 replica down (even for QUORUM requests)
 2 nodes down:  QUORUM requests impacted if second down node owns a replica of first or vice-versa, total token count
 = 15 * 256. Now 2*256 tokens are down. Due to random generation of vnodes it is very likely that for 1 or more
 tokens 256 * 4/((256*14)) = 4/14 = 28 % probability. This is the same probability of the second node impacting
 availability of a keyrange as in the single-token case. So statistically the vnode case isn't worse than the single-
 token case.
 * </p>
 */
@ParametersAreNonnullByDefault
public class VNodeConsistentHashRingBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(VNodeConsistentHashRingBuilder.class);

    private static final int DEFAULT_REPLICA_COUNT = 5;

    private SortedMap<String, Pair<MachineInRingState, long[]>> machines = new TreeMap<>();

    private int replicaCount = DEFAULT_REPLICA_COUNT;

    /**
     *
     * @param id collisions not allowed
     * @param state user has to determine the state the machine should be in. This is not managed by this API. This
     *              API will only ensure that readReplicas and writeReplicas will be determined correctly.
     */
    public VNodeConsistentHashRingBuilder addMachine(String id, MachineInRingState state, long... vnodes) {
        machines.put(id, Pair.of(state, vnodes));
        return this;
    }

    /**
     * How many replicas to set up for every vnode.
     * This also determines how many bootstrapping/leaving etc nodes are
     * added as write targets.
     */
    public VNodeConsistentHashRingBuilder setReplicaCount(int replicaCount) {
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
            // keep them out - their nodes don't own anything any more and they should not serve as replicas
            if (Objects.equals(MachineInRingState.OUT_OF_RING, entry.getKey())) {
                continue;
            }

            Machine m = new Machine();
            allMachines.add(m);
            m.setMachineId(entry.getKey());
            m.setState(entry.getValue().getLeft());
        }
        allMachines = ImmutableList.copyOf(allMachines);

        int i = 0;
        for (Machine m : allMachines) {
            m.init(allMachines, i);
            i++;
        }

        // ring building:
        // first add all vnodes,
        // then update read and write replica lists by checking neighboring vnodes.
        // optional: Remove uninteresting (non-INTEGRATED) nodes.
        for (final Machine m : allMachines) {

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
                            return result;
                        }
                    },
                    machines.get(m.getMachineId()).getRight());
        }

        // then find replicas for every boss - need to do this in an additional iteration because need to look forward
        // for next bosses of LEAVING nodes:
        for (final VNodeImpl n : ring.getNodes()) {
            switch (n.getLeader().getState()) {
                case INTEGRATED:
                    addReplicasForIntegratedLedNode(ring, n);
                    break;
                case LEAVING:
                    addReplicasForLeavingLedNode(ring, n);
                    break;
                case OUT_OF_RING:
                case BOOTSTRAPPING:
                    break; // ok, ignore this kind of node. Will be removed later. Still here for determining replicas:
                    // a bootstrapping node is important as a write replica for its initialization/bootstrapping
                default:
                    throw new IllegalStateException(
                            "Unknown MachineInRingState, please fix (in " + VNodeConsistentHashRingBuilder.class + ")");
            }
        }

        removeNonIntegratedOrLeavingLedNodesAndAddNodesToMachines(allMachines, ring); // they don't own anything, only for replica determination.

        ensureNoOutOfRingNodesAndDuplicates(ring);


        for (Machine m : allMachines) {
            m.finishSetup();
        }

        return VNodeConsistentHashRing.create(allMachines, ring);
    }

    private void removeNonIntegratedOrLeavingLedNodesAndAddNodesToMachines(
            List<Machine> allMachines, ConsistentHashRingHoldingResourceEntries<?, VNodeImpl, String, String> ring) {

        Multimap<String, VNodeImpl> vnodesForMachines = Multimaps.newSetMultimap(
                new HashMap<String, Collection<VNodeImpl>>(), new Supplier<Set<VNodeImpl>>() {
                    @Override
                    public Set<VNodeImpl> get() {
                        return new LinkedHashSet<VNodeImpl>();
                    }
                });

        List<VNodeImpl> toRemove = new ArrayList<>();
        for (VNodeImpl n : ring.getNodes()) {
            boolean remove = true;
            if (Objects.equals(n.getLeader().getState(), MachineInRingState.INTEGRATED)) {
                remove = false;
            }
            if (Objects.equals(n.getLeader().getState(), MachineInRingState.LEAVING)) {
                remove = false;
            }
            if (remove) {
                toRemove.add(n);
            } else {
                addNodeToMachines(vnodesForMachines, n.getReadReplicas(), n);
                addNodeToMachines(vnodesForMachines, n.getWriteReplicas(), n);
            }
        }

        for (Machine m : allMachines) {
            m.getVNodes().addAll(vnodesForMachines.get(m.getMachineId()));
        }

        for (VNodeImpl n : toRemove) {
            ring.removeNodeByHash(n.getHash());
        }
    }

    private void addNodeToMachines(Multimap<String, VNodeImpl> vnodesForMachines, List<Machine> replicas, VNodeImpl n) {
        for (Machine m : replicas) {
            vnodesForMachines.put(m.getMachineId(), n);
        }
    }

    private void ensureNoOutOfRingNodesAndDuplicates(
            ConsistentHashRingHoldingResourceEntries<?, VNodeImpl, String, String> ring) {

        IdentityHashMap<List, Object> seen = new IdentityHashMap<>();
        for (VNodeImpl node : ring.getNodes()) {
            if (!seen.containsKey(node.getReadReplicas())) {
                ensureNoOutOfRingNodesAndDuplicates(node, node.getReadReplicas());
                seen.put(node.getReadReplicas(), Boolean.TRUE);
            }
            if (!seen.containsKey(node.getWriteReplicas())) {
                ensureNoOutOfRingNodesAndDuplicates(node, node.getWriteReplicas());
                seen.put(node.getWriteReplicas(), Boolean.TRUE);
            }
        }

    }

    private void ensureNoOutOfRingNodesAndDuplicates(VNodeImpl node, List<Machine> toModify) {
        HashSet<Machine> seen = new HashSet<>(toModify.size());
        for (Machine m : toModify) {
            if (!seen.add(m)) {
                throw new IllegalArgumentException(
                        "Dpulicate entry in replica list of node " + node + ": " + m + ". List: " + toModify);
            }
        }
    }

    private void addReplicasForLeavingLedNode(
            ConsistentHashRingHoldingResourceEntries<?, VNodeImpl, String, String> ring, VNodeImpl n) {

        addReplicasForIntegratedLedNode(ring, n);

        List<Machine> additionalWriters = getPossibleNewOwnersForLeavingNode(ring, n, n.getWriteReplicas());

        additionalWriters.addAll(n.getWriteReplicas()); // order determines prio - and future owner nodes are very
        // important

        n.setWriteReplicas(additionalWriters);
    }

    private List<Machine> getPossibleNewOwnersForLeavingNode(
            ConsistentHashRingHoldingResourceEntries<?, VNodeImpl, String, String> ring, VNodeImpl n,
            List<Machine> existingWriteReplicas) {

        LinkedHashSet<Machine> existingWriteReplicasSet = new LinkedHashSet<>(existingWriteReplicas);
        VNodeImpl current = ring.getNodeAfterNode(n.getHash());
        List<Machine> additionalWriters = new ArrayList<>();
        outer:
        while (true) {
            if (current == null || Objects.equals(current.getHash(), n.getHash())) {
                break; // could not finish
            }

            switch (current.getLeader().getState()) {
                case BOOTSTRAPPING:
                    if (!existingWriteReplicasSet.contains(current.getLeader())) {
                        additionalWriters.add(current.getLeader());
                        existingWriteReplicasSet.add(current.getLeader()); // adding at wrong position! Should be front
                    }
                    break;
                case INTEGRATED:
                    if (!existingWriteReplicasSet.contains(current.getLeader())) {
                        additionalWriters.add(current.getLeader());
                        existingWriteReplicasSet.add(current.getLeader()); // adding at wrong position! Should be front
                    } else {
                        // here an additional INTEGRATED replica to start setting up should be added on the left
                        // because this new owner/boss is already a replica - so taking the current node out will leave
                        // the node one replica short of the target replicaCount.
                        Machine additionalReplica = getAnotherIntegratedWriteReplicaBeforeLast(ring, n,
                                existingWriteReplicasSet);
                        // TODO research if it's necessary to add the in-between bootstrapping etc nodes, too. Mind
                        // that especially for low replicaCounts this could influence a lot.
                        if (additionalReplica != null) { // otherwise none found
                            additionalWriters.add(additionalReplica);
                        }
                    }
                    break outer; // done, new boss/owner found.
                case LEAVING:
                    // safety, if the current node leaves before the other leaving node, the other node needs to take
                    // over
                    if (!existingWriteReplicasSet.contains(current.getLeader())) {
                        additionalWriters.add(current.getLeader());
                        existingWriteReplicasSet.add(current.getLeader()); // wrong position, but doesn't matter because
                        // getAnotherIntegratedWriteReplicaBeforeLast will look for INTEGRATED
                    }
                    break;
                case OUT_OF_RING:
                    break; // ignore
                default:
                    throw new IllegalStateException("Unknown state in " + getClass());
            }

            current = ring.getNodeAfterNode(current.getHash());
        }
        return additionalWriters;
    }

    private Machine getAnotherIntegratedWriteReplicaBeforeLast(
            ConsistentHashRingHoldingResourceEntries<?, VNodeImpl, String, String> ring,
            VNodeImpl n, LinkedHashSet<Machine> existingWriteReplicas /* order important! */) {
        Machine lastIntegrated = null;
        for (Machine m : existingWriteReplicas) {
            if (MachineInRingState.INTEGRATED.equals(m.getState())) {
                lastIntegrated = m;
            }
        }
        if (lastIntegrated == null) {
            // could already not find integrated machines, so no use searching again
            return null;
        }

        VNodeImpl current = ring.getNodeBeforeNode(n.getHash()); // look left for lastIntegrated-led node
        while (current != null && !Objects.equals(lastIntegrated, current.getLeader()) && n != current) {
            current = ring.getNodeBeforeNode(current.getHash());
        }

        if (current != null && Objects.equals(lastIntegrated, current.getLeader())) {
            // found it, now look for new integrated machine before it
            VNodeImpl lastIntegratedLedNode = current;
            do {
                current = ring.getNodeBeforeNode(current.getHash());
                if (current == null || current.equals(lastIntegratedLedNode)) {
                    break; // search failed/looped
                }
                if (MachineInRingState.INTEGRATED.equals(current.getLeader().getState())
                        && !existingWriteReplicas.contains(current.getLeader())) {
                    return current.getLeader();
                }
            } while (true);

        }
        return null;
    }

    private void addReplicasForIntegratedLedNode(
            ConsistentHashRingHoldingResourceEntries<?, VNodeImpl, String, String> ring, VNodeImpl n) {
        List<Machine> readReplicas = findReadReplicas(ring, n);
        List<Machine> writeReplicas = new ArrayList<>(findDefaultWriteReplicas(ring, n));
        n.setReadReplicas(readReplicas);
        n.setWriteReplicas(writeReplicas);
    }

    /**
     * Find the write replicas left of the node - the replicas in the original sense.
     */
    private LinkedHashSet<Machine> findDefaultWriteReplicas(
            ConsistentHashRingHoldingResourceEntries<?, VNodeImpl, String, String> ring,
            VNodeImpl n) {

        LinkedHashSet<Machine> defaultWriters = new LinkedHashSet<>();
        defaultWriters.add(n.getLeader());
        int realReplicasAdded = 1;

        VNodeImpl current = ring.getNodeBeforeNode(n.getHash());

        while (realReplicasAdded < replicaCount) {
            if (current == null || Objects.equals(current.getHash(), n.getHash())) {
                break; // could not finish
            }

            Machine currentMachine = current.getLeader();
            MachineInRingState currentState = currentMachine.getState();
            switch (currentState) {
                case BOOTSTRAPPING:
                    defaultWriters.add(currentMachine); // these might take over
                    break;
                case INTEGRATED:
                    if (!defaultWriters.contains(currentMachine)) {
                        defaultWriters.add(currentMachine);
                        realReplicasAdded++;
                    }
                    break;
                case LEAVING:
                    // leaving machine still serves as read replica, needs updates
                    defaultWriters.add(currentMachine);
                    // don't count this - to start bootstrapping a replacement INTEGRATED behind it
                    break;
                case OUT_OF_RING:
                    break; // ignore
                default:
                    throw new IllegalStateException("Unknown state " + currentState + " in " + getClass());
            }

            current = ring.getNodeBeforeNode(current.getHash());
        }
        return defaultWriters;
    }

    private List<Machine> findReadReplicas(ConsistentHashRingHoldingResourceEntries<?, VNodeImpl, String, String> ring,
                                           VNodeImpl n) {

        LinkedHashSet<Machine> defaultReaders = new LinkedHashSet<>();
        Preconditions.checkState(
                MachineInRingState.INTEGRATED.equals(n.getLeader().getState())
                        || MachineInRingState.LEAVING.equals(n.getLeader().getState()),
                "Assumed that only INTEGRATED and LEAVING can lead nodes for " +
                        "which replicas are determined. This impacts writeReplicas, too ");
        defaultReaders.add(n.getLeader());
        int replicasAdded = 1; // counting a LEAVING node here - don't switch to (maybe still booting) integrated(!) too
        // early

        VNodeImpl current = ring.getNodeBeforeNode(n.getHash());

        while (replicasAdded < replicaCount) {
            if (current == null || Objects.equals(current.getHash(), n.getHash())) {
                break; // could not finish
            }

            // ?maybe determine read replicas from the write replicas - maybe ignore the write replicas.?
            // currently ignoring them. Write replicas are more far-reaching and will include the read replicas.

            Machine currentMachine = current.getLeader();
            MachineInRingState currentState = currentMachine.getState();
            switch (currentState) {

                case INTEGRATED:
                case LEAVING: // see above why counting leaving
                    if (!defaultReaders.contains(currentMachine)) {
                        defaultReaders.add(currentMachine);
                        replicasAdded++;
                    }
                    break;

                case BOOTSTRAPPING:
                case OUT_OF_RING:
                    break; // ignore
                default:
                    throw new IllegalStateException("Unknown state " + currentState + " in " + getClass());
            }

            current = ring.getNodeBeforeNode(current.getHash());
        }
        return new ArrayList<>(defaultReaders);
    }

}
