package de.deverado.framework.concurrent.consistenthashing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class VNodeConsistentHashRingWithMachineOrderBuilderTest {

    private VNodeConsistentHashRingWithReplicasByMachineOrderBuilder builder;

    @Before
    public void setUp() throws Exception {
        builder = new VNodeConsistentHashRingWithReplicasByMachineOrderBuilder().setHashMachineOrder(false);
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
        assertEquals(1, ring.getMachines().get(0).getVNodes().size());
        assertEquals(Long.valueOf(1), ring.getMachines().get(0).getVNodes().get(0).getHash());
        assertSame(ring.getMachines().get(0).getVNodes().get(0), ring.getNodeForHash(0l));
        assertEquals(ring.getMachines().get(0), ring.getNodeForHash(0l).getLeader());

        List<Machine> singleMachineList = Collections.singletonList(ring.getMachines().get(0));
        assertEquals(0, ring.getReadReplicasForHash(1l).size());
        assertEquals(singleMachineList, ring.getWriteReplicasForHash(1l));
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
        assertNotEquals(firstMachineNode, secondMachineNode);
        assertEquals(Long.valueOf(1), firstMachineNode.getHash());
        assertEquals(Long.valueOf(5), secondMachineNode.getHash());

        assertEquals(firstMachineNode, ring.getNodeForHash(0l));
        assertEquals(secondMachineNode, ring.getNodeForHash(2l));

        List<Machine> integratedList = Collections.singletonList(ring.getMachines().get(1));
        Set<Machine> both = new HashSet<>(ring.getMachines());
        assertEquals(integratedList, ring.getReadReplicasForHash(0l));
        assertEquals(integratedList, ring.getReadReplicasForHash(2l));

        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(0l)));
        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(2l)));
    }

    @Test
    public void testVerySimpleTwoMachineLeavingIntegratedSetup() throws Exception {

        VNodeConsistentHashRing ring = builder.addMachine("1", MachineInRingState.LEAVING, 1)
                .addMachine("2", MachineInRingState.INTEGRATED, 5).build();
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

        List<Machine> integratedList = Collections.singletonList(ring.getMachines().get(1));
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

        assertEquals(1, ring.getMachines().get(0).getVNodes().size());
        assertEquals(1, ring.getMachines().get(1).getVNodes().size());

        VNodeImpl firstMachineNode = ring.getMachines().get(0).getVNodes().get(0);
        VNodeImpl secondMachineNode = ring.getMachines().get(1).getVNodes().get(0);
        assertNotEquals(firstMachineNode, secondMachineNode);
        assertEquals(Long.valueOf(1), firstMachineNode.getHash());
        assertEquals(Long.valueOf(5), secondMachineNode.getHash());

        assertEquals(firstMachineNode, ring.getNodeForHash(0l));
        assertEquals(secondMachineNode, ring.getNodeForHash(2l));

        Set<Machine> both = new HashSet<>(ring.getMachines());
        assertEquals(Collections.<Machine>emptyList(), ring.getReadReplicasForHash(0l));
        assertEquals(Collections.<Machine>emptyList(), ring.getReadReplicasForHash(2l));

        // both are write replicas of both, because of machine-order replication
        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(0l)));
        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(2l)));
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
        assertNotEquals(firstMachineNode, secondMachineNode);
        assertEquals(Long.valueOf(1), firstMachineNode.getHash());
        assertEquals(Long.valueOf(5), secondMachineNode.getHash());

        assertEquals(firstMachineNode, ring.getNodeForHash(0l));
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

        assertEquals(1, ring.getMachines().get(0).getVNodes().size());
        assertEquals(1, ring.getMachines().get(1).getVNodes().size());

        VNodeImpl firstMachineNode = ring.getMachines().get(0).getVNodes().get(0);
        VNodeImpl secondMachineNode = ring.getMachines().get(1).getVNodes().get(0);
        assertNotEquals(firstMachineNode, secondMachineNode);
        assertEquals(Long.valueOf(1), firstMachineNode.getHash());
        assertEquals(Long.valueOf(5), secondMachineNode.getHash());

        assertEquals(firstMachineNode, ring.getNodeForHash(0l));
        assertEquals(secondMachineNode, ring.getNodeForHash(2l));

        List<Machine> leavingList = Collections.singletonList(ring.getMachines().get(1));
        Set<Machine> both = new HashSet<>(ring.getMachines());

        assertEquals(both, new HashSet<Machine>(ring.getReadReplicasForHash(0l)));
        assertEquals(both, new HashSet<Machine>(ring.getReadReplicasForHash(2l)));

        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(0l)));
        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(2l)));
    }

    @Test
    public void testNoReplicationTwoMachineBootstrappingIntegratedSetup() throws Exception {

        VNodeConsistentHashRing ring = builder.setReplicaCount(1).addMachine("1", MachineInRingState.BOOTSTRAPPING, 1)
                .addMachine("2", MachineInRingState.INTEGRATED, 5).build();
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

        List<Machine> integratedList = Collections.singletonList(ring.getMachines().get(1));
        Set<Machine> both = new HashSet<>(ring.getMachines());
        assertEquals(integratedList, ring.getReadReplicasForHash(0l));
        assertEquals(integratedList, ring.getReadReplicasForHash(2l));

        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(0l)));
        assertEquals(integratedList, ring.getWriteReplicasForHash(2l));
    }

    @Test
    public void testNoReplicationTwoMachineLeavingIntegratedSetup() throws Exception {

        VNodeConsistentHashRing ring = builder.setReplicaCount(1).addMachine("1", MachineInRingState.LEAVING, 1)
                .addMachine("2", MachineInRingState.INTEGRATED, 5).build();
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

        assertEquals(1, ring.getMachines().get(0).getVNodes().size());
        assertEquals(1, ring.getMachines().get(1).getVNodes().size());

        VNodeImpl firstMachineNode = ring.getMachines().get(0).getVNodes().get(0);
        VNodeImpl secondMachineNode = ring.getMachines().get(1).getVNodes().get(0);
        assertNotEquals(firstMachineNode, secondMachineNode);
        assertEquals(Long.valueOf(1), firstMachineNode.getHash());
        assertEquals(Long.valueOf(5), secondMachineNode.getHash());

        assertEquals(firstMachineNode, ring.getNodeForHash(0l));
        assertEquals(secondMachineNode, ring.getNodeForHash(2l));

        Set<Machine> both = new HashSet<>(ring.getMachines());
        assertEquals(Collections.<Machine>emptyList(), ring.getReadReplicasForHash(0l));
        assertEquals(Collections.<Machine>emptyList(), ring.getReadReplicasForHash(2l));

        // because of safety approach all following bootstrapping and leaving-led machines are always included in
        // core replica list. TODO not super-sure about this safety-first-include-all-bootstrapping-leaving decision,
        // but is another guarantee that even with replicaCount=1 data isn't lost - as is the ring-walking resolution
        // of state-dependent replica relationships
        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(0l)));
        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(2l)));
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
        assertNotEquals(firstMachineNode, secondMachineNode);
        assertEquals(Long.valueOf(1), firstMachineNode.getHash());
        assertEquals(Long.valueOf(5), secondMachineNode.getHash());

        assertEquals(firstMachineNode, ring.getNodeForHash(0l));
        assertEquals(secondMachineNode, ring.getNodeForHash(2l));

        List<Machine> leavingList = Collections.singletonList(ring.getMachines().get(1));
        Set<Machine> both = new HashSet<>(ring.getMachines());
        assertEquals(leavingList, ring.getReadReplicasForHash(0l));
        assertEquals(leavingList, ring.getReadReplicasForHash(2l));

        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(0l)));
        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(2l)));
    }

    @Test
    public void testNoReplicationTwoMachineLeavingLeavingSetup() throws Exception {

        VNodeConsistentHashRing ring = builder.setReplicaCount(1).addMachine("1", MachineInRingState.LEAVING, 1)
                .addMachine("2", MachineInRingState.LEAVING, 5).build();
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

        List<Machine> leavingList = Collections.singletonList(ring.getMachines().get(1));
        Set<Machine> both = new HashSet<>(ring.getMachines());

        assertEquals(Collections.singletonList(firstMachineNode.getLeader()), ring.getReadReplicasForHash(0l));
        assertEquals(Collections.singletonList(secondMachineNode.getLeader()), ring.getReadReplicasForHash(2l));

        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(0l)));
        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(2l)));
    }

    @Test
    public void testTwoBootstrappingOfSameMachineFollowing() throws Exception {
        VNodeConsistentHashRing ring = builder.setReplicaCount(1)//
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
        assertEquals(Long.valueOf(1), firstMachineFirstNode.getHash());
        assertEquals(Long.valueOf(5), firstMachineSecondNode.getHash());

        VNodeImpl secondMachineNode = ring.getMachines().get(1).getVNodes().get(0);
        assertEquals(Long.valueOf(10), secondMachineNode.getHash());

        List<Machine> integratedList = Collections.singletonList(ring.getMachines().get(1));
        Set<Machine> both = new HashSet<>(ring.getMachines());

        assertEquals(integratedList, ring.getReadReplicasForHash(0l));
        assertEquals(integratedList, ring.getReadReplicasForHash(2l));
        assertEquals(integratedList, ring.getReadReplicasForHash(11l));

        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(0l)));
        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(2l)));
        assertEquals(integratedList, ring.getWriteReplicasForHash(11l));
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
        assertEquals(2, ring.getMachines().get(1).getVNodes().size());

        VNodeImpl firstMachineFirstNode = ring.getMachines().get(0).getVNodes().get(0);
        VNodeImpl firstMachineSecondNode = ring.getMachines().get(0).getVNodes().get(1);
        assertEquals(Long.valueOf(1), firstMachineFirstNode.getHash());
        assertEquals(firstMachineFirstNode, ring.getNodeForHash(0l));
        assertEquals(Long.valueOf(5), firstMachineSecondNode.getHash());
        assertEquals(firstMachineSecondNode, ring.getNodeForHash(2l));

        VNodeImpl secondMachineNode = ring.getMachines().get(1).getVNodes().get(0);
        assertEquals(Long.valueOf(10), secondMachineNode.getHash());

        List<Machine> leavingList = Collections.singletonList(ring.getMachines().get(0));
        List<Machine> integratedList = Collections.singletonList(ring.getMachines().get(1));
        Set<Machine> both = new HashSet<>(ring.getMachines());

        assertEquals(leavingList, ring.getReadReplicasForHash(0l));
        assertEquals(leavingList, ring.getReadReplicasForHash(2l));
        assertEquals(integratedList, ring.getReadReplicasForHash(11l));

        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(0l)));
        assertEquals(both, new HashSet<Machine>(ring.getWriteReplicasForHash(2l)));
        assertEquals(integratedList, ring.getWriteReplicasForHash(11l));
    }
}