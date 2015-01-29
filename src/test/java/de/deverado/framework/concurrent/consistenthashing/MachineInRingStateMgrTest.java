package de.deverado.framework.concurrent.consistenthashing;

import static org.junit.Assert.assertEquals;

import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;

public class MachineInRingStateMgrTest {
    private static final Logger LOG = LoggerFactory.getLogger(MachineInRingStateMgrTest.class);

    private MachineInRingStateMgr cut;
    private ArrayList<MachineInRingState> stateUpdates;
    private ListeningScheduledExecutorService exec;

    @Before
    public void setUp() throws Exception {
        cut = new MachineInRingStateMgr();
        stateUpdates = new ArrayList<MachineInRingState>();
        exec = MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor());
        cut.addListener(new MachineInRingStateListener() {
            @Override
            public void changed(@Nullable MachineInRingState previous, @Nullable MachineInRingState newState) {
                stateUpdates.add(newState);
            }
        });
    }

    @After
    public void tearDown() throws Exception {
        if (exec != null) {
            exec.shutdownNow();
        }
        exec = null;
    }

    @Test
    public void testSimpleStateUpdates() throws Exception {

        ServiceStateProvider mock = Mockito.mock(ServiceStateProvider.class);
        Mockito.when(mock.getCurrentState()).thenReturn(//
                ServiceStateImpl.create("test", MachineInRingState.BOOTSTRAPPING),
                ServiceStateImpl.create("test", MachineInRingState.INTEGRATED),
                ServiceStateImpl.create("test", MachineInRingState.LEAVING),
                ServiceStateImpl.create("test", MachineInRingState.INTEGRATED));
        cut.init(Collections.<ServiceStateProvider>singleton(mock), exec);
        cut.updateState();
        assertEquals(Collections.singletonList(MachineInRingState.BOOTSTRAPPING), stateUpdates);

        stateUpdates.clear();
        cut.updateState();
        assertEquals(Collections.singletonList(MachineInRingState.INTEGRATED), stateUpdates);

        stateUpdates.clear();
        cut.updateState();
        assertEquals(Collections.singletonList(MachineInRingState.LEAVING), stateUpdates);

        stateUpdates.clear();
        cut.updateState();
        assertEquals(0, stateUpdates.size()); // state stayed at LEAVING, not notifications to listeners

        cut.setCurrentState(MachineInRingState.OUT_OF_RING);
        assertEquals(Collections.singletonList(MachineInRingState.OUT_OF_RING), stateUpdates);
    }

    @Test
    public void testSimpleStateUpdatesWithUpdaterJob() throws Exception {

        ServiceStateProvider mock = Mockito.mock(ServiceStateProvider.class);
        Mockito.when(mock.getCurrentState()).thenReturn(//
                ServiceStateImpl.create("test", MachineInRingState.BOOTSTRAPPING),
                ServiceStateImpl.create("test", MachineInRingState.INTEGRATED));
        cut.setUpdaterDelayMs(50);
        cut.init(Collections.<ServiceStateProvider>singleton(mock), exec);
        Thread.sleep(60);
        assertEquals(Collections.singletonList(MachineInRingState.BOOTSTRAPPING), stateUpdates);

        stateUpdates.clear();
        Thread.sleep(60);
        assertEquals(Collections.singletonList(MachineInRingState.INTEGRATED), stateUpdates);

        stateUpdates.clear();
        cut.setCurrentState(MachineInRingState.OUT_OF_RING);
        assertEquals(Collections.singletonList(MachineInRingState.OUT_OF_RING), stateUpdates);
    }

    @Test
    public void testCannotGoBackToBootstrappingFromIntegrated() throws Exception {

        ServiceStateProvider mock = Mockito.mock(ServiceStateProvider.class);
        Mockito.when(mock.getCurrentState()).thenReturn(ServiceStateImpl.create("test", MachineInRingState.INTEGRATED),
                ServiceStateImpl.create("test", MachineInRingState.BOOTSTRAPPING));
        cut.init(Collections.<ServiceStateProvider>singleton(mock), exec);
        cut.updateState();
        assertEquals(Collections.singletonList(MachineInRingState.INTEGRATED), stateUpdates);

        stateUpdates.clear();
        cut.updateState();
        assertEquals(0, stateUpdates.size()); // state stayed at INTEGRATED, not notifications to listeners

    }

    @Test
    public void testMultipleServicesStateUpdates() throws Exception {

        ServiceStateProvider mock1 = Mockito.mock(ServiceStateProvider.class);
        Mockito.when(mock1.getCurrentState()).thenReturn(//
                ServiceStateImpl.create("test1", MachineInRingState.BOOTSTRAPPING),
                ServiceStateImpl.create("test1", MachineInRingState.INTEGRATED),
                ServiceStateImpl.create("test1", MachineInRingState.INTEGRATED),
                ServiceStateImpl.create("test1", MachineInRingState.INTEGRATED));
        ServiceStateProvider mock2 = Mockito.mock(ServiceStateProvider.class);
        Mockito.when(mock2.getCurrentState()).thenReturn(//
                ServiceStateImpl.create("test2", MachineInRingState.BOOTSTRAPPING),
                ServiceStateImpl.create("test2", MachineInRingState.BOOTSTRAPPING),
                ServiceStateImpl.create("test2", MachineInRingState.INTEGRATED),
                ServiceStateImpl.create("test2", MachineInRingState.LEAVING));

        cut.init(Arrays.asList(mock1, mock2), exec);
        cut.updateState();
        assertEquals(Collections.singletonList(MachineInRingState.BOOTSTRAPPING), stateUpdates);

        stateUpdates.clear();
        cut.updateState();
        assertEquals(0, stateUpdates.size()); // no update of state

        stateUpdates.clear();
        cut.updateState();
        assertEquals(Collections.singletonList(MachineInRingState.INTEGRATED), stateUpdates);

        stateUpdates.clear();
        cut.updateState();
        assertEquals(Collections.singletonList(MachineInRingState.LEAVING), stateUpdates);

        stateUpdates.clear();
        cut.setCurrentState(MachineInRingState.OUT_OF_RING);
        assertEquals(Collections.singletonList(MachineInRingState.OUT_OF_RING), stateUpdates);
    }

    @Test
    public void testRetryStateUpdates() throws Exception {

        ServiceStateProvider mock1 = Mockito.mock(ServiceStateProvider.class);
        Mockito.when(mock1.getCurrentState()).thenAnswer(new Answer<ServiceStateImpl>() {
            int i = 0;
            List<ServiceStateImpl> retvals = Arrays.asList(
                    ServiceStateImpl.create("test1", MachineInRingState.BOOTSTRAPPING),
                    null,
                    ServiceStateImpl.create("test1", MachineInRingState.INTEGRATED),
                    ServiceStateImpl.create("test1", MachineInRingState.INTEGRATED)
            );
            @Override
            public ServiceStateImpl answer(InvocationOnMock invocation) throws Throwable {
                ServiceStateImpl currAnswer = retvals.get(i++);
                if (currAnswer == null) {
                    throw new ServiceStateNotYetAvailableException();
                }
                return currAnswer;
            }
        });
        ServiceStateProvider mock2 = Mockito.mock(ServiceStateProvider.class);
        Mockito.when(mock2.getCurrentState()).thenReturn(//
                ServiceStateImpl.create("test2", MachineInRingState.INTEGRATED),
                ServiceStateImpl.create("test2", MachineInRingState.INTEGRATED),
                ServiceStateImpl.create("test2", MachineInRingState.INTEGRATED),
                ServiceStateImpl.create("test2", MachineInRingState.INTEGRATED));

        cut.setUpdateRetryDelayMs(10);
        cut.init(Arrays.asList(mock1, mock2), exec);
        cut.updateState();
        assertEquals(Collections.singletonList(MachineInRingState.BOOTSTRAPPING), stateUpdates);

        stateUpdates.clear();
        cut.updateState();

        assertEquals(0, stateUpdates.size()); // mock1 threw - no update of state
        // wait for retry
        Thread.sleep(50);
        assertEquals("" + stateUpdates, Collections.singletonList(MachineInRingState.INTEGRATED), stateUpdates);

        stateUpdates.clear();
        cut.updateState();
        assertEquals(0, stateUpdates.size()); // all integrated: no update of state
    }


}
