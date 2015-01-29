package de.deverado.framework.concurrent.consistenthashing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class VNodeConsistentHashRingMgrTest {

    private VNodeConsistentHashRingMgr cut;
    private ListeningExecutorService executor;

    @Before
    public void setUp() throws Exception {
        cut = new VNodeConsistentHashRingMgr();
        executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
    }

    @After
    public void tearDown() throws Exception {
        executor.shutdownNow();
    }

    @Test
    public void testRingChangedPossibly() throws Exception {

        final Deque<Object[][]> ringUpdates = new LinkedBlockingDeque<>();
        ringUpdates.add(new Object[][] {
                new Object[] { "1", MachineInRingState.INTEGRATED, new long[] { 1 } },
                new Object[] { "2", MachineInRingState.INTEGRATED, new long[] { 51 } },
                new Object[] { "3", MachineInRingState.INTEGRATED, new long[] { 101 } },
        });
        ringUpdates.add(new Object[][] {
                // 1 gone
                new Object[] { "2", MachineInRingState.INTEGRATED, new long[] { 51 } }, // 2 same
                new Object[] { "3", MachineInRingState.LEAVING, new long[] { 101 } }, // state change
                new Object[] { "4", MachineInRingState.BOOTSTRAPPING, new long[] { 201 } }, // new
        });
        ringUpdates.add(new Object[][] {
                // SAME AS BEFORE
                new Object[] { "2", MachineInRingState.INTEGRATED, new long[] { 51 } }, // same
                new Object[] { "3", MachineInRingState.LEAVING, new long[] { 101 } }, // same
                new Object[] { "4", MachineInRingState.BOOTSTRAPPING, new long[] { 201 } }, // same
        });

        cut.init(new Callable<ListenableFuture<? extends VNodeConsistentHashRing>>() {
            @Override
            public ListenableFuture<? extends VNodeConsistentHashRing> call() throws Exception {
                Object[][] machines = ringUpdates.removeFirst();
                VNodeConsistentHashRingBuilder builder = new VNodeConsistentHashRingBuilder();
                for (Object[] m : machines) {
                    builder.addMachine((String)m[0], (MachineInRingState) m[1], (long[]) m[2]);
                }
                return Futures.immediateFuture(builder.build());
            }
        }, executor);


        final AtomicReference<Object> problemRef = new AtomicReference<>();
        final AtomicInteger listenerCallCount = new AtomicInteger(0);

        VNodeConsistentHashRingChangeListener firstListener = new VNodeConsistentHashRingChangeListener() {

            @Override
            public void changed(@Nullable VNodeConsistentHashRing old, VNodeConsistentHashRing newRing,
                                RingDifference difference) {
                listenerCallCount.incrementAndGet();
                try {
                    assertNull(old);
                    assertEquals(3, difference.getChanged().size());
                    assertEquals(3, newRing.getMachines().size());

                } catch (Exception e) {
                    problemRef.set(e);
                }
            }
        };

        cut.addRingListener(firstListener);

        cut.startRing();
        Thread.sleep(20);

        assertNull("" + problemRef.get(), problemRef.get());
        assertEquals(1, listenerCallCount.get());

        assertNotNull(cut.getRing());
        assertEquals(3, cut.getRing().getMachines().size());
        Machine leader1 = cut.getRing().getNodeForHash(1l).getLeader();
        assertEquals("1", leader1.getMachineId());
        assertEquals(MachineInRingState.INTEGRATED, leader1.getState());

        Machine leader51 = cut.getRing().getNodeForHash(51l).getLeader();
        assertEquals("2", leader51.getMachineId());
        assertEquals(MachineInRingState.INTEGRATED, leader51.getState());

        Machine leader101 = cut.getRing().getNodeForHash(101l).getLeader();
        assertEquals("3", leader101.getMachineId());
        assertEquals(MachineInRingState.INTEGRATED, leader101.getState());

        cut.removeRingListener(firstListener);


        VNodeConsistentHashRingChangeListener secondListener = new VNodeConsistentHashRingChangeListener() {

            @Override
            public void changed(@Nullable VNodeConsistentHashRing old, VNodeConsistentHashRing newRing,
                                RingDifference difference) {
                listenerCallCount.incrementAndGet();
                try {
                    assertNotNull(old);
                    assertEquals(3, newRing.getMachines().size());
                    assertEquals(true, difference.isHavingDifference());
                    assertEquals(3, difference.getChanged().size());

                    Map<String, MachineInfo> changedMachines = new HashMap<>();
                    for (MachineInfo i : difference.getChanged()) {
                        changedMachines.put(i.getId(), i);
                    }

                    assertEquals(MachineInRingState.INTEGRATED, changedMachines.get("1").getState());
                    assertEquals(MachineInRingState.LEAVING, changedMachines.get("3").getState());
                    assertEquals(MachineInRingState.BOOTSTRAPPING, changedMachines.get("4").getState());

                } catch (Exception e) {
                    problemRef.set(e);
                }
            }
        };

        cut.addRingListener(secondListener);
        
        cut.ringChangedPossibly();
        Thread.sleep(20);

        assertNull("" + problemRef.get(), problemRef.get());
        assertEquals(2, listenerCallCount.get());

        assertEquals(3, cut.getRing().getMachines().size());

        leader51 = cut.getRing().getNodeForHash(51l).getLeader();
        assertEquals("2", leader51.getMachineId());
        assertEquals(MachineInRingState.INTEGRATED, leader51.getState());

        leader101 = cut.getRing().getNodeForHash(101l).getLeader();
        assertEquals("3", leader101.getMachineId());
        assertEquals(MachineInRingState.LEAVING, leader101.getState());

        // new machine is bootstrapping and will take over from 3 machine owning node at 51
        assertTrue(cut.getRing().getNodeForHash(51l).isWriteReplica("4"));


        cut.ringChangedPossibly();
        Thread.sleep(20);
        assertEquals("Listener should not have been called again - no changes", 2, listenerCallCount.get());
    }
}
