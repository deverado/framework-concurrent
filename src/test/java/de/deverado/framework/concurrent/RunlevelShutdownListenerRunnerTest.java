package de.deverado.framework.concurrent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import de.deverado.framework.core.Multimaps2;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RunlevelShutdownListenerRunnerTest {


    private Map<String, ShutdownListener> createdListeners;
    private List<ShutdownListener> executionList;

    @Before
    public void setup() {
        createdListeners = new HashMap<>();
        executionList = new ArrayList<>();
    }

    @Test
    public void testCreateRunlevelMap() {
        Multimap<String, ShutdownListener> original = Multimaps2.newHashSetMultimap();

        original.put("rl0", createListener("rl0_0"));
        original.put("rl", createListener("rl_0"));

        List<ShutdownListener> otherListeners = new ArrayList<>();
        otherListeners.add(createListener("blah"));
        otherListeners.add(createListenerWithRl("blahrl", "rl"));

        Multimap<String, ShutdownListener> result = RunlevelShutdownListenerRunner.createRunlevelMap(otherListeners,
                original, "defaultrl");

        Collection<ShutdownListener> defaultrl = result.get("defaultrl");
        assertEquals(1, defaultrl.size());
        assertTrue(defaultrl.contains(createdListeners.get("blah")));

        Collection<ShutdownListener> rl = result.get("rl");
        assertEquals(2, rl.size());
        assertTrue(rl.contains(createdListeners.get("blahrl")));
        assertTrue(rl.contains(createdListeners.get("rl_0")));

        Collection<ShutdownListener> rl0 = result.get("rl0");
        assertEquals(1, rl0.size());
        assertTrue(rl0.contains(createdListeners.get("rl0_0")));

        assertEquals(createdListeners.size(), result.size());
    }

    private ShutdownListener createListener(String label) {
        ShutdownListener sl = new ShutdownListener() {
            @Override
            public ListenableFuture<?> shutdown() {
                return null;
            }
        };

        createdListeners.put(label, sl);
        return sl;
    }

    private ShutdownListener createListenerWithRl(String label, final String rl) {
        ShutdownListenerWithRunlevel sl = new ShutdownListenerWithRunlevel() {
            @Override
            public String getRunlevel() {
                return rl;
            }

            @Override
            public ListenableFuture<?> shutdown() {
                executionList.add(this);
                return null;
            }
        };

        createdListeners.put(label, sl);
        return sl;
    }

    @Test
    public void testCall() throws Exception {
        ShutdownListener rl0 = createListenerWithRl("first_rl0", "rl0");
        ShutdownListener rl2 = createListenerWithRl("first_rl0", "rl2");

        Multimap<String, ShutdownListener> listeners = RunlevelShutdownListenerRunner.createRunlevelMap(
                Arrays.asList(rl0, rl2), null, "def");
        RunlevelShutdownListenerRunner runlevelShutdownListenerRunner = RunlevelShutdownListenerRunner.create(listeners,
                50);

        runlevelShutdownListenerRunner.call();

        assertEquals(2, executionList.size());
        assertEquals(rl2, executionList.get(0));
        assertEquals(rl0, executionList.get(1));
    }
}