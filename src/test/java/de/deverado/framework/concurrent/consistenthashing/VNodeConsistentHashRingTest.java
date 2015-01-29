package de.deverado.framework.concurrent.consistenthashing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class VNodeConsistentHashRingTest {

    private VNodeConsistentHashRingBuilder builder;

    @Before
    public void setUp() throws Exception {
        builder = new VNodeConsistentHashRingBuilder();
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testVerySimpleOneIntegratedMachineSetup() throws Exception {

        VNodeConsistentHashRing ring = builder.addMachine("1", MachineInRingState.INTEGRATED, 1).build();
        assertEquals(1, ring.getMachines().size());
        assertEquals("1", ring.getMachines().get(0).getMachineId());
        assertEquals(1, ring.getMachines().get(0).getVNodes().size());
        assertEquals(Long.valueOf(1), ring.getMachines().get(0).getVNodes().get(0).getHash());
        assertSame(ring.getMachines().get(0).getVNodes().get(0), ring.getNodeForHash(0l));
        assertEquals(ring.getMachines().get(0), ring.getNodeForHash(0l).getLeader());

        List<Machine> singleMachineList = Collections.singletonList(ring.getMachines().get(0));
        assertEquals(singleMachineList, ring.getReadReplicasForHash(1l));
        assertEquals(singleMachineList, ring.getWriteReplicasForHash(1l));
    }

    @Test
    public void testVerySimpleOneBootstrappingMachineSetup() throws Exception {

        VNodeConsistentHashRing ring = builder.addMachine("1", MachineInRingState.BOOTSTRAPPING, 1).build();
        assertEquals(1, ring.getMachines().size());
        assertEquals("1", ring.getMachines().get(0).getMachineId());
        assertEquals(0, ring.getMachines().get(0).getVNodes().size());
        assertNull(ring.getNodeForHash(1l)); // no owning nodes -> no nodes at all

    }

    @Test
    public void testVerySimpleOneLeavingMachineSetup() throws Exception {

        VNodeConsistentHashRing ring = builder.addMachine("1", MachineInRingState.LEAVING, 1).build();
        assertEquals(1, ring.getMachines().size());
        assertEquals("1", ring.getMachines().get(0).getMachineId());
        assertEquals(1, ring.getMachines().get(0).getVNodes().size());
        assertEquals(Long.valueOf(1), ring.getMachines().get(0).getVNodes().get(0).getHash());
        assertSame(ring.getMachines().get(0).getVNodes().get(0), ring.getNodeForHash(0l));
        assertEquals(ring.getMachines().get(0), ring.getNodeForHash(0l).getLeader());

        List<Machine> singleMachineList = Collections.singletonList(ring.getMachines().get(0));
        assertEquals(singleMachineList, ring.getReadReplicasForHash(1l));
        assertEquals(singleMachineList, ring.getWriteReplicasForHash(1l));
    }

    @Test
    public void testVerySimpleOneOutOfRingMachineSetup() throws Exception {

        VNodeConsistentHashRing ring = builder.addMachine("1", MachineInRingState.OUT_OF_RING, 1).build();
        assertEquals(1, ring.getMachines().size());
        assertEquals("1", ring.getMachines().get(0).getMachineId());
        assertEquals(0, ring.getMachines().get(0).getVNodes().size());
        assertNull(ring.getNodeForHash(1l));
    }

    @Test
    public void testVerySimpleTwoMachineBootstrappingIntegratedSetup() throws Exception {

        VNodeConsistentHashRing ring = builder.addMachine("1", MachineInRingState.BOOTSTRAPPING, 1)
                .addMachine("2", MachineInRingState.INTEGRATED, 5).build();
        assertEquals(2, ring.getMachines().size());
        assertEquals("1", ring.getMachines().get(0).getMachineId());
        assertEquals("2", ring.getMachines().get(1).getMachineId());

        assertEquals(1, ring.getMachines().get(0).getVNodes().size());
        assertEquals(1, ring.getMachines().get(1).getVNodes().size());

        VNodeImpl firstMachineNode = ring.getMachines().get(0).getVNodes().get(0);
        VNodeImpl secondMachineNode = ring.getMachines().get(1).getVNodes().get(0);
        assertEquals(firstMachineNode, secondMachineNode);
        assertEquals(Long.valueOf(5), secondMachineNode.getHash());

        assertEquals(secondMachineNode, ring.getNodeForHash(0l));
        assertEquals(secondMachineNode, ring.getNodeForHash(2l));

        List<Machine> integratedList = Collections.singletonList(ring.getMachines().get(1));
        Set<Machine> both = new HashSet<>(ring.getMachines());
        assertEquals(integratedList, ring.getReadReplicasForHash(0l));
        assertEquals(integratedList, ring.getReadReplicasForHash(2l));

        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(0l)));
        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(2l)));
    }

    @Test
    public void testVerySimpleTwoMachineIntegratedIntegratedOneReplicaCountSetup() throws Exception {

        VNodeConsistentHashRing ring = builder.addMachine("1", MachineInRingState.INTEGRATED, 1)
                .addMachine("2", MachineInRingState.INTEGRATED, 5).setReplicaCount(1).build();
        assertEquals(2, ring.getMachines().size());
        assertEquals("1", ring.getMachines().get(0).getMachineId());
        assertEquals("2", ring.getMachines().get(1).getMachineId());

        assertEquals(1, ring.getMachines().get(0).getVNodes().size());
        assertEquals(1, ring.getMachines().get(1).getVNodes().size());

        VNodeImpl firstMachineNode = ring.getMachines().get(0).getVNodes().get(0);
        VNodeImpl secondMachineNode = ring.getMachines().get(1).getVNodes().get(0);
        assertNotEquals(firstMachineNode, secondMachineNode);
        assertEquals(Long.valueOf(1), firstMachineNode.getHash());
        assertEquals(Long.valueOf(5), secondMachineNode.getHash());

        assertEquals(firstMachineNode, ring.getNodeForHash(0l));
        assertEquals(secondMachineNode, ring.getNodeForHash(2l));

        List<Machine> firstList = Collections.singletonList(ring.getMachines().get(0));
        List<Machine> secondList = Collections.singletonList(ring.getMachines().get(1));
        assertEquals(firstList, ring.getReadReplicasForHash(0l));
        assertEquals(secondList, ring.getReadReplicasForHash(2l));

        assertEquals(firstList, ring.getWriteReplicasForHash(0l));
        assertEquals(secondList, ring.getWriteReplicasForHash(2l));
    }

    @Test
    public void testVerySimpleTwoMachineLeavingIntegratedSetup() throws Exception {

        VNodeConsistentHashRing ring = builder.addMachine("1", MachineInRingState.LEAVING, 1)
                .addMachine("2", MachineInRingState.INTEGRATED, 5).build();
        assertEquals(2, ring.getMachines().size());
        assertEquals("1", ring.getMachines().get(0).getMachineId());
        assertEquals("2", ring.getMachines().get(1).getMachineId());

        assertEquals(2, ring.getMachines().get(0).getVNodes().size()); // contains replica nodes, too
        assertEquals(2, ring.getMachines().get(1).getVNodes().size()); // contains replica nodes, too

        VNodeImpl firstMachineNode = ring.getMachines().get(0).getVNodes().get(0);
        VNodeImpl secondMachineNode = ring.getMachines().get(0).getVNodes().get(1);
        assertNotEquals(firstMachineNode, ring.getMachines().get(1).getVNodes().get(1));

        assertEquals(Long.valueOf(1), firstMachineNode.getHash());
        assertEquals(Long.valueOf(5), secondMachineNode.getHash());

        assertEquals(firstMachineNode, ring.getNodeForHash(0l));
        assertEquals(secondMachineNode, ring.getNodeForHash(2l));

        Set<Machine> both = new HashSet<>(ring.getMachines());
        assertEquals(both, new HashSet<>(ring.getReadReplicasForHash(0l)));
        assertEquals(both, new HashSet<Machine>(ring.getReadReplicasForHash(2l)));

        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(0l)));
        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(2l)));
    }

    @Test
    public void testVerySimpleTwoMachineBootstrappingBootstrappingSetup() throws Exception {

        VNodeConsistentHashRing ring = builder.addMachine("1", MachineInRingState.BOOTSTRAPPING, 1)
                .addMachine("2", MachineInRingState.BOOTSTRAPPING, 5).build();
        assertEquals(2, ring.getMachines().size());
        assertEquals("1", ring.getMachines().get(0).getMachineId());
        assertEquals("2", ring.getMachines().get(1).getMachineId());

        assertEquals(0, ring.getMachines().get(0).getVNodes().size());
        assertEquals(0, ring.getMachines().get(1).getVNodes().size());

        assertNull(ring.getNodeForHash(0l));
    }

    @Test
    public void testVerySimpleTwoMachineBootstrappingLeavingSetup() throws Exception {

        VNodeConsistentHashRing ring = builder.addMachine("1", MachineInRingState.BOOTSTRAPPING, 1)
                .addMachine("2", MachineInRingState.LEAVING, 5).build();
        assertEquals(2, ring.getMachines().size());
        assertEquals("1", ring.getMachines().get(0).getMachineId());
        assertEquals("2", ring.getMachines().get(1).getMachineId());

        assertEquals(1, ring.getMachines().get(0).getVNodes().size());
        assertEquals(1, ring.getMachines().get(1).getVNodes().size());

        VNodeImpl firstMachineNode = ring.getMachines().get(0).getVNodes().get(0);
        VNodeImpl secondMachineNode = ring.getMachines().get(1).getVNodes().get(0);
        assertEquals(firstMachineNode, secondMachineNode);
        assertEquals(Long.valueOf(5), secondMachineNode.getHash());

        assertEquals(secondMachineNode, ring.getNodeForHash(0l));
        assertEquals(secondMachineNode, ring.getNodeForHash(2l));

        List<Machine> leavingList = Collections.singletonList(ring.getMachines().get(1));
        Set<Machine> both = new HashSet<>(ring.getMachines());
        assertEquals(leavingList, ring.getReadReplicasForHash(0l));
        assertEquals(leavingList, ring.getReadReplicasForHash(2l));

        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(0l)));
        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(2l)));
    }

    @Test
    public void testVerySimpleTwoMachineLeavingLeavingSetup() throws Exception {

        VNodeConsistentHashRing ring = builder.addMachine("1", MachineInRingState.LEAVING, 1)
                .addMachine("2", MachineInRingState.LEAVING, 5).build();
        assertEquals(2, ring.getMachines().size());
        assertEquals("1", ring.getMachines().get(0).getMachineId());
        assertEquals("2", ring.getMachines().get(1).getMachineId());

        assertEquals(2, ring.getMachines().get(0).getVNodes().size());
        assertEquals(2, ring.getMachines().get(1).getVNodes().size());

        Map<Long, VNodeImpl> firstMachineNodes = makeNodesByHash(ring.getMachines().get(0).getVNodes());
        assertEquals(firstMachineNodes,
                makeNodesByHash(ring.getMachines().get(1).getVNodes()));
        VNodeImpl firstMachineNode = firstMachineNodes.get(1l);
        VNodeImpl secondMachineNode = firstMachineNodes.get(5l);
        assertNotEquals(firstMachineNode, secondMachineNode);
        assertEquals(Long.valueOf(1), firstMachineNode.getHash());
        assertEquals(Long.valueOf(5), secondMachineNode.getHash());

        assertEquals(firstMachineNode, ring.getNodeForHash(0l));
        assertEquals(secondMachineNode, ring.getNodeForHash(2l));

        Set<Machine> both = new HashSet<>(ring.getMachines());

        assertEquals(both, new HashSet<Machine>(ring.getReadReplicasForHash(0l)));
        assertEquals(both, new HashSet<Machine>(ring.getReadReplicasForHash(2l)));

        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(0l)));
        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(2l)));
    }

    @Test
    public void testBoostrappingMachinesWillGetVnodesForWhichTheyAreReplicas() throws Exception {

        VNodeConsistentHashRing ring = builder.setReplicaCount(2).addMachine("1", MachineInRingState.BOOTSTRAPPING, 1)
                .addMachine("2", MachineInRingState.INTEGRATED, 5).build();
        assertEquals(2, ring.getMachines().size());
        assertEquals("1", ring.getMachines().get(0).getMachineId());
        assertEquals("2", ring.getMachines().get(1).getMachineId());

        assertEquals(1, ring.getMachines().get(0).getVNodes().size());
        VNodeImpl firstMachineVnode = ring.getMachines().get(0).getVNodes().get(0);
        assertEquals("first machine should be replica to the INTEGRATED machine's vnode", 5l,
                firstMachineVnode.getHash().longValue());
        assertEquals(Collections.singletonList(ring.getMachines().get(1)), firstMachineVnode.getReadReplicas());

        Set<Machine> both = new HashSet<>(ring.getMachines());
        assertEquals(both, new HashSet<>(firstMachineVnode.getWriteReplicas()));
    }

    /**
     * The integrated machine to the right of the leaving node may not be initialized yet and so cannot serve as a
     * read replica.
     */
    @Test
    public void testNodesLedByLeavingMachinesWillNotHaveTheNewBossInListOfReadReplicas() throws Exception {

        VNodeConsistentHashRing ring = builder.setReplicaCount(2).addMachine("1", MachineInRingState.INTEGRATED, 1)
                .addMachine("2", MachineInRingState.INTEGRATED, 5).addMachine("3", MachineInRingState.LEAVING, 7)
                .addMachine("4", MachineInRingState.INTEGRATED, 10).build();
        assertEquals(4, ring.getMachines().size());
        assertEquals("1", ring.getMachines().get(0).getMachineId());
        assertEquals("2", ring.getMachines().get(1).getMachineId());
        assertEquals("3", ring.getMachines().get(2).getMachineId());
        assertEquals("4", ring.getMachines().get(3).getMachineId());

        VNodeImpl leavingLedNode = ring.getNodeForHash(6l);
        Set<Machine> leavingReadReplicas = new HashSet<>();
        leavingReadReplicas.add(ring.getMachines().get(2));
        leavingReadReplicas.add(ring.getMachines().get(1));

        assertNotEquals("4", leavingLedNode.getReadReplicas().get(0).getMachineId());
        assertEquals(leavingReadReplicas, new HashSet<>(leavingLedNode.getReadReplicas()));

    }

    @Test
    public void testNoReplicationTwoMachineBootstrappingIntegratedSetup() throws Exception {

        VNodeConsistentHashRing ring = builder.setReplicaCount(1).addMachine("1", MachineInRingState.BOOTSTRAPPING, 1)
                .addMachine("2", MachineInRingState.INTEGRATED, 5).build();
        assertEquals(2, ring.getMachines().size());
        assertEquals("1", ring.getMachines().get(0).getMachineId());
        assertEquals("2", ring.getMachines().get(1).getMachineId());

        assertEquals("with replicaCount 1 an integrated node will not need/look for replicas, only LEAVING will", 0,
                ring.getMachines().get(0).getVNodes().size());
        assertEquals(1, ring.getMachines().get(1).getVNodes().size());

        VNodeImpl secondMachineNode = ring.getMachines().get(1).getVNodes().get(0);
        assertEquals(Long.valueOf(5), secondMachineNode.getHash());

        assertEquals(secondMachineNode, ring.getNodeForHash(0l));
        assertEquals(secondMachineNode, ring.getNodeForHash(2l));

        List<Machine> integratedList = Collections.singletonList(ring.getMachines().get(1));
        Set<Machine> both = new HashSet<>(ring.getMachines());
        assertEquals(integratedList, ring.getReadReplicasForHash(0l));
        assertEquals(integratedList, ring.getReadReplicasForHash(2l));

        assertEquals(integratedList, ring.getWriteReplicasForHash(0l));
        assertEquals(integratedList, ring.getWriteReplicasForHash(2l)); // but same node anyways
    }

    @Test
    public void testReplicationTwoMachineBootstrappingIntegratedSetup() throws Exception {

        VNodeConsistentHashRing ring = builder.setReplicaCount(2).addMachine("1", MachineInRingState.BOOTSTRAPPING, 1)
                .addMachine("2", MachineInRingState.INTEGRATED, 5).build();
        assertEquals(2, ring.getMachines().size());
        assertEquals("1", ring.getMachines().get(0).getMachineId());
        assertEquals("2", ring.getMachines().get(1).getMachineId());

        assertEquals(1, ring.getMachines().get(0).getVNodes().size());
        assertEquals(1, ring.getMachines().get(1).getVNodes().size());

        VNodeImpl firstMachineNode = ring.getMachines().get(0).getVNodes().get(0);
        VNodeImpl secondMachineNode = ring.getMachines().get(1).getVNodes().get(0);
        assertEquals(Long.valueOf(5), secondMachineNode.getHash());
        assertSame("first machine is a replica candidate", firstMachineNode, secondMachineNode);

        assertEquals(secondMachineNode, ring.getNodeForHash(0l));
        assertEquals(secondMachineNode, ring.getNodeForHash(2l));

        List<Machine> integratedList = Collections.singletonList(ring.getMachines().get(1));
        Set<Machine> both = new HashSet<>(ring.getMachines());
        assertEquals(integratedList, ring.getReadReplicasForHash(0l));
        assertEquals(integratedList, ring.getReadReplicasForHash(2l));

        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(0l)));
        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(2l))); // but same node anyways
    }

    @Test
    public void testNoReplicationTwoMachineLeavingIntegratedSetup() throws Exception {

        VNodeConsistentHashRing ring = builder.setReplicaCount(1).addMachine("1", MachineInRingState.LEAVING, 1)
                .addMachine("2", MachineInRingState.INTEGRATED, 5).build();
        assertEquals(2, ring.getMachines().size());
        assertEquals("1", ring.getMachines().get(0).getMachineId());
        assertEquals("2", ring.getMachines().get(1).getMachineId());

        assertEquals("leaving should stay with its node", 1, ring.getMachines().get(0).getVNodes().size());
        assertEquals("integrated prepares to take over for leaving", 2, ring.getMachines().get(1).getVNodes().size());

        VNodeImpl firstMachineNode = ring.getMachines().get(0).getVNodes().get(0);
        int posOfFirstMachineNodeInSecondsList = ring.getMachines().get(1).getVNodes().indexOf(
                ring.getMachines().get(0).getVNodes().get(0));
        assertTrue(posOfFirstMachineNodeInSecondsList >= 0);
        VNodeImpl secondMachineNode = ring.getMachines().get(1).getVNodes().get(
                posOfFirstMachineNodeInSecondsList == 0 ? 1 : 0);


        assertEquals(Long.valueOf(1), firstMachineNode.getHash());
        assertEquals(Long.valueOf(5), secondMachineNode.getHash());

        assertEquals(firstMachineNode, ring.getNodeForHash(0l));
        assertEquals(secondMachineNode, ring.getNodeForHash(2l));

        List<Machine> integratedList = Collections.singletonList(ring.getMachines().get(1));
        List<Machine> leavingList = Collections.singletonList(ring.getMachines().get(0));
        Set<Machine> both = new HashSet<>(ring.getMachines());
        assertEquals(leavingList, ring.getReadReplicasForHash(0l));
        assertEquals(integratedList, ring.getReadReplicasForHash(2l));

        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(0l)));
        assertEquals(integratedList, ring.getWriteReplicasForHash(2l));
    }

    @Test
    public void testNoReplicationTwoMachineBootstrappingBootstrappingSetup() throws Exception {

        VNodeConsistentHashRing ring = builder.setReplicaCount(1).addMachine("1", MachineInRingState.BOOTSTRAPPING, 1)
                .addMachine("2", MachineInRingState.BOOTSTRAPPING, 5).build();
        assertEquals(2, ring.getMachines().size());
        assertEquals("1", ring.getMachines().get(0).getMachineId());
        assertEquals("2", ring.getMachines().get(1).getMachineId());

        assertEquals(0, ring.getMachines().get(0).getVNodes().size());
        assertEquals(0, ring.getMachines().get(1).getVNodes().size());

        assertNull(ring.getNodeForHash(0l));
    }

    @Test
    public void testNoReplicationTwoMachineBootstrappingLeavingSetup() throws Exception {

        VNodeConsistentHashRing ring = builder.setReplicaCount(1).addMachine("1", MachineInRingState.BOOTSTRAPPING, 1)
                .addMachine("2", MachineInRingState.LEAVING, 5).build();
        assertEquals(2, ring.getMachines().size());
        assertEquals("1", ring.getMachines().get(0).getMachineId());
        assertEquals("2", ring.getMachines().get(1).getMachineId());

        assertEquals(1, ring.getMachines().get(0).getVNodes().size());
        assertEquals(1, ring.getMachines().get(1).getVNodes().size());

        VNodeImpl firstMachineNode = ring.getMachines().get(0).getVNodes().get(0);
        VNodeImpl secondMachineNode = ring.getMachines().get(1).getVNodes().get(0);
        assertEquals("bootstrapping-led nodes should be gone, " +
                        "first machine (replacement) write replica for leaving-led node",
                firstMachineNode, secondMachineNode);
        assertEquals(Long.valueOf(5), secondMachineNode.getHash());

        assertEquals(secondMachineNode, ring.getNodeForHash(0l));
        assertEquals(secondMachineNode, ring.getNodeForHash(2l));

        List<Machine> leavingList = Collections.singletonList(ring.getMachines().get(1));
        Set<Machine> both = new HashSet<>(ring.getMachines());
        assertEquals(leavingList, ring.getReadReplicasForHash(0l));
        assertEquals(leavingList, ring.getReadReplicasForHash(2l)); // same node, somewhat useless

        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(0l)));
        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(2l))); // same node, somewhat useless
    }

    @Test
    public void testNoReplicationTwoMachineLeavingLeavingSetup() throws Exception {

        VNodeConsistentHashRing ring = builder.setReplicaCount(1).addMachine("1", MachineInRingState.LEAVING, 1)
                .addMachine("2", MachineInRingState.LEAVING, 5).build();
        assertEquals(2, ring.getMachines().size());
        assertEquals("1", ring.getMachines().get(0).getMachineId());
        assertEquals("2", ring.getMachines().get(1).getMachineId());

        assertEquals(2, ring.getMachines().get(0).getVNodes().size());
        assertEquals(2, ring.getMachines().get(1).getVNodes().size());

        assertEquals("leaving-led nodes add machines of leaving nodes as replicas",
                new HashSet<>(ring.getMachines().get(0).getVNodes()),
                new HashSet<>(ring.getMachines().get(1).getVNodes()));
        VNodeImpl firstNode = ring.getMachines().get(0).getVNodes().get(0);
        VNodeImpl secondNode = ring.getMachines().get(0).getVNodes().get(1);
        assertNotEquals(firstNode, secondNode);
        assertEquals(Long.valueOf(1), firstNode.getHash()); // this is dependent on implementation! Order no important
        assertEquals(Long.valueOf(5), secondNode.getHash());

        assertEquals(firstNode, ring.getNodeForHash(0l));
        assertEquals(secondNode, ring.getNodeForHash(2l));

        Set<Machine> both = new HashSet<>(ring.getMachines());

        assertEquals(Collections.singletonList(firstNode.getLeader()), ring.getReadReplicasForHash(0l));
        assertEquals(Collections.singletonList(secondNode.getLeader()), ring.getReadReplicasForHash(2l));

        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(0l)));
        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(2l)));
    }

    @Test
    public void testNoReplicationTwoBootstrappingOfSameMachineFollowingWithLeavingBehind() throws Exception {
        VNodeConsistentHashRing ring = builder.setReplicaCount(1)//
                .addMachine("1", MachineInRingState.BOOTSTRAPPING, 1, 5)//
                .addMachine("2", MachineInRingState.LEAVING, 10, 15)//
                .build();
        assertEquals(2, ring.getMachines().size());
        assertEquals("1", ring.getMachines().get(0).getMachineId());
        assertEquals("2", ring.getMachines().get(1).getMachineId());

        assertEquals(2, ring.getMachines().get(0).getVNodes().size());
        assertEquals(2, ring.getMachines().get(1).getVNodes().size());

        VNodeImpl firstMachineFirstNode = ring.getMachines().get(0).getVNodes().get(0);
        VNodeImpl firstMachineSecondNode = ring.getMachines().get(0).getVNodes().get(1);
        assertEquals("first machine is write replica for INTEGRATED nodes",
                Long.valueOf(10), firstMachineFirstNode.getHash());
        assertEquals(Long.valueOf(15), firstMachineSecondNode.getHash());

        VNodeImpl secondMachineNode = ring.getMachines().get(1).getVNodes().get(0);
        assertEquals(Long.valueOf(10), secondMachineNode.getHash());

        List<Machine> integratedList = Collections.singletonList(ring.getMachines().get(1));
        Set<Machine> both = new HashSet<>(ring.getMachines());

        assertEquals(integratedList, ring.getReadReplicasForHash(0l));
        assertEquals(integratedList, ring.getReadReplicasForHash(2l));
        assertEquals(integratedList, ring.getReadReplicasForHash(11l));

        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(0l)));
        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(2l)));
        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(11l)));
    }

    @Test
    public void testNoReplicationTwoBootstrappingOfSameMachineFollowingWithIntegratedBehind() throws Exception {
        VNodeConsistentHashRing ring = builder.setReplicaCount(1)//
                .addMachine("1", MachineInRingState.BOOTSTRAPPING, 1, 5)//
                .addMachine("2", MachineInRingState.INTEGRATED, 10, 15)//
                .build();
        assertEquals(2, ring.getMachines().size());
        assertEquals("1", ring.getMachines().get(0).getMachineId());
        assertEquals("2", ring.getMachines().get(1).getMachineId());

        assertEquals(0, ring.getMachines().get(0).getVNodes().size());
        assertEquals(2, ring.getMachines().get(1).getVNodes().size());

        VNodeImpl secondMachineNode = ring.getMachines().get(1).getVNodes().get(0);
        assertEquals(Long.valueOf(10), secondMachineNode.getHash());

        List<Machine> integratedList = Collections.singletonList(ring.getMachines().get(1));

        assertEquals(integratedList, ring.getReadReplicasForHash(0l));
        assertEquals(integratedList, ring.getReadReplicasForHash(2l));
        assertEquals(integratedList, ring.getReadReplicasForHash(11l));

        assertEquals(integratedList, ring.getWriteReplicasForHash(0l));
        assertEquals(integratedList, ring.getWriteReplicasForHash(2l));
        assertEquals(integratedList, ring.getWriteReplicasForHash(11l));
    }

    @Test
    public void testReplicationTwoBootstrappingOfSameMachineFollowingWithIntegratedBehind() throws Exception {
        VNodeConsistentHashRing ring = builder.setReplicaCount(2)//
                .addMachine("1", MachineInRingState.BOOTSTRAPPING, 1, 5)//
                .addMachine("2", MachineInRingState.INTEGRATED, 10, 15)//
                .build();
        assertEquals(2, ring.getMachines().size());
        assertEquals("1", ring.getMachines().get(0).getMachineId());
        assertEquals("2", ring.getMachines().get(1).getMachineId());

        assertEquals(2, ring.getMachines().get(0).getVNodes().size());
        assertEquals(2, ring.getMachines().get(1).getVNodes().size());

        VNodeImpl firstMachineFirstNode = ring.getMachines().get(0).getVNodes().get(0);
        VNodeImpl firstMachineSecondNode = ring.getMachines().get(0).getVNodes().get(1);
        assertEquals("first machine is write replica for INTEGRATED nodes",
                Long.valueOf(10), firstMachineFirstNode.getHash());
        assertEquals(Long.valueOf(15), firstMachineSecondNode.getHash());

        VNodeImpl secondMachineNode = ring.getMachines().get(1).getVNodes().get(0);
        assertEquals(Long.valueOf(10), secondMachineNode.getHash());

        List<Machine> integratedList = Collections.singletonList(ring.getMachines().get(1));
        Set<Machine> both = new HashSet<>(ring.getMachines());

        assertEquals(integratedList, ring.getReadReplicasForHash(0l));
        assertEquals(integratedList, ring.getReadReplicasForHash(2l));
        assertEquals(integratedList, ring.getReadReplicasForHash(11l));

        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(0l)));
        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(2l)));
        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(11l)));
    }

    @Test
    public void testTwoLeavingOfSameMachineFollowing() throws Exception {
        VNodeConsistentHashRing ring = builder.setReplicaCount(1)//
                .addMachine("1", MachineInRingState.LEAVING, 1, 5)//
                .addMachine("2", MachineInRingState.INTEGRATED, 10, 15)//
                .build();
        assertEquals(2, ring.getMachines().size());
        assertEquals("1", ring.getMachines().get(0).getMachineId());
        assertEquals("2", ring.getMachines().get(1).getMachineId());

        assertEquals(2, ring.getMachines().get(0).getVNodes().size());
        assertEquals("INTEGRATED becoming new boss of leaving",
                4, ring.getMachines().get(1).getVNodes().size());

        VNodeImpl firstMachineFirstNode = ring.getMachines().get(0).getVNodes().get(0);
        VNodeImpl firstMachineSecondNode = ring.getMachines().get(0).getVNodes().get(1);
        assertEquals(Long.valueOf(1), firstMachineFirstNode.getHash());
        assertEquals(firstMachineFirstNode, ring.getNodeForHash(0l));
        assertEquals(Long.valueOf(5), firstMachineSecondNode.getHash());
        assertEquals(firstMachineSecondNode, ring.getNodeForHash(2l));

        assertEquals(4, ring.getMachines().get(1).getVNodes().size());
        Map<Long, VNodeImpl> nodesByHash = makeNodesByHash(ring.getMachines().get(1).getVNodes());
        assertNotNull(nodesByHash.get(1l));
        assertNotNull(nodesByHash.get(5l));
        assertNotNull(nodesByHash.get(10l));
        assertNotNull(nodesByHash.get(15l));

        List<Machine> leavingList = Collections.singletonList(ring.getMachines().get(0));
        List<Machine> integratedList = Collections.singletonList(ring.getMachines().get(1));
        Set<Machine> both = new HashSet<>(ring.getMachines());

        assertEquals(leavingList, ring.getReadReplicasForHash(0l));
        assertEquals(leavingList, ring.getReadReplicasForHash(2l));
        assertEquals(integratedList, ring.getReadReplicasForHash(7l));
        assertEquals(integratedList, ring.getReadReplicasForHash(11l));

        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(0l)));
        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(2l)));
        assertEquals(integratedList, ring.getWriteReplicasForHash(11l));
    }

    private Map<Long, VNodeImpl> makeNodesByHash(List<VNodeImpl> vNodes) {
        HashMap<Long, VNodeImpl> result = new HashMap<>();
        for (VNodeImpl n : vNodes) {
            result.put(n.getHash(), n);
        }
        return result;
    }
}