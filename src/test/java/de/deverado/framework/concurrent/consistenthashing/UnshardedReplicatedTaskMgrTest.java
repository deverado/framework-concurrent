package de.deverado.framework.concurrent.consistenthashing;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayDeque;
import java.util.Deque;

public class UnshardedReplicatedTaskMgrTest {

    @Test
    public void testChanged() throws Exception {
        final Deque<AssignmentState> stateChanges = new ArrayDeque<>();

        VNodeConsistentHashRingMgr mgr = Mockito.mock(VNodeConsistentHashRingMgr.class);
        VNodeConsistentHashRing ring = Mockito.mock(VNodeConsistentHashRing.class);
        Mockito.when(mgr.getRing()).thenReturn(ring);

        VNodeImpl node = Mockito.mock(VNodeImpl.class);
        Mockito.when(ring.getNode("test")).thenReturn(node);
        Mockito.when(node.getAssignmentStateForMachine("a", -1)).thenReturn(AssignmentState.UNRELATED,
                AssignmentState.READ_REPLICA,
                AssignmentState.WRITE_REPLICA, AssignmentState.READ_WRITE_REPLICA, AssignmentState.UNRELATED);

        UnshardedReplicatedTaskMgr cut = new UnshardedReplicatedTaskMgr();
        cut.init("a", mgr,
                new UnshardedReplicatedTask() {
                    @Override
                    public String getName() {
                        return "test";
                    }

                    @Override
                    public void becomeReplica(AssignmentState newState) {
                        stateChanges.add(newState);
                    }

                    @Override
                    public void becomeUnrelated() {
                        stateChanges.add(AssignmentState.UNRELATED);
                    }
                });

        assertEquals(AssignmentState.UNRELATED, cut.getAssignmentState());

        // init calls first change if required
        assertEquals(0, stateChanges.size());

        cut.changed(null, ring, null);

        assertEquals(1, stateChanges.size());
        assertEquals(AssignmentState.READ_REPLICA, stateChanges.getLast());

        cut.changed(null, ring, null);

        assertEquals(2, stateChanges.size());
        assertEquals(AssignmentState.WRITE_REPLICA, stateChanges.getLast());

        cut.changed(null, ring, null);

        assertEquals(3, stateChanges.size());
        assertEquals(AssignmentState.READ_WRITE_REPLICA, stateChanges.getLast());

        cut.changed(null, ring, null);

        assertEquals(4, stateChanges.size());
        assertEquals(AssignmentState.UNRELATED, stateChanges.getLast());
    }

    @Test
    public void testChangedWorksOnReplicaAssignmentDuringInit() throws Exception {
        final Deque<AssignmentState> stateChanges = new ArrayDeque<>();

        VNodeConsistentHashRingMgr mgr = Mockito.mock(VNodeConsistentHashRingMgr.class);
        VNodeConsistentHashRing ring = Mockito.mock(VNodeConsistentHashRing.class);
        Mockito.when(mgr.getRing()).thenReturn(ring);

        VNodeImpl node = Mockito.mock(VNodeImpl.class);
        Mockito.when(ring.getNode("test")).thenReturn(node);
        Mockito.when(node.getAssignmentStateForMachine("a", 3)).thenReturn(AssignmentState.READ_REPLICA);

        UnshardedReplicatedTaskMgr cut = new UnshardedReplicatedTaskMgr().setReplicaCount(3);
        cut.init("a", mgr,
                new UnshardedReplicatedTask() {
                    @Override
                    public String getName() {
                        return "test";
                    }

                    @Override
                    public void becomeReplica(AssignmentState newState) {
                        stateChanges.add(newState);
                    }

                    @Override
                    public void becomeUnrelated() {
                        stateChanges.add(AssignmentState.UNRELATED);
                    }
                });

        assertEquals(AssignmentState.READ_REPLICA, cut.getAssignmentState());

        // init calls first change if required
        assertEquals(1, stateChanges.size());
        assertEquals(AssignmentState.READ_REPLICA, stateChanges.getLast());
    }
}
