package de.deverado.framework.concurrent.consistenthashing;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.Hasher;
import com.google.common.primitives.Ints;
import de.deverado.framework.concurrent.consistenthashing.ConsistentHashRingHoldingResourceEntries;
import de.deverado.framework.concurrent.consistenthashing.ConsistentHashRingImpl;
import de.deverado.framework.concurrent.consistenthashing.ConsistentHashRings;
import de.deverado.framework.concurrent.consistenthashing.ConsistentHashRings.RingHasher;
import de.deverado.framework.core.RandomHelpers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class ConsistentHashRingImplTest {

    private ConsistentHashRingHoldingResourceEntries<String, String, String, String> o;
    private TreeMap<String, String> nodesAdded;
    private TreeMap<String, String> entriesAdded;

    @Before
    public void setUp() throws Exception {
        o = new ConsistentHashRingImpl<String, String, String, String>(ConsistentHashRings.DEFAULT_64_BIT_HASH_FUNC,
                ConsistentHashRings.DEFAULT_HASHER, ConsistentHashRings.DEFAULT_HASHER);

    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void shouldInstantiate() {
        assertNotNull(o);
    }

    @Test
    public void shouldWorkWithCollisionsInEntries() {
        RingHasher<String> h = new RingHasher<String>() {
            @Override
            public long hash(Hasher h, String n) {
                return 123;
            }
        };
        o = new ConsistentHashRingImpl<String, String, String, String>(ConsistentHashRings.DEFAULT_64_BIT_HASH_FUNC,
                ConsistentHashRings.DEFAULT_HASHER, h);
        addAFewEntries(o);
        shouldIterateAllEntriesViaNodesAndCheckNoDups(o);
    }

    @Test
    public void shouldAddAllNodes() {
        addAFewEntries(o);

        List<String> nodeList = Lists.newArrayList();
        Iterables.addAll(nodeList, o.getNodes());
        Collections.sort(nodeList);
        List<String> origNodes = Lists.newArrayList(nodesAdded.values());
        Collections.sort(origNodes);
        assertEquals(origNodes, nodeList);
    }

    @Test
    public void shouldIterateAllEntriesViaNodes() throws Exception {
        for (int i = 0; i < 3; i++) {
            setUp();
            addAFewEntries(o);
            shouldIterateAllEntriesViaNodesAndCheckNoDups(o);
        }
    }

    @Test
    public void shouldReturnNullIfNoNodeWhenGetNodeForEntry() {
        assertEquals(false, o.isHavingNodes());
        assertNull(o.getNodeForEntry("anEntry"));
    }

    @Test
    public void shouldGetNodeForEntryWithLooping() {
        addAFewEntries(o, 1, 0); // test looping
        for (int i = 0; i < 100; i++) {
            String entryKey = RandomHelpers.randIntStr();
            assertEquals(Iterables.getFirst(nodesAdded.values(), null),
                    o.getNodeForEntry(entryKey));

        }
    }

    @Test
    public void shouldGetNodeForEntryForLoadedMap() {
        addAFewEntries(o, 10, 100);
        for (String n : o.getNodes()) {
            for (String entry : o.getEntriesForNode(makeNodeKey(n), true)) {
                String entryKey = makeEntryKey(entry);
                String nodeForEntry = o.getNodeForEntry(entryKey);
                // System.out.println("" + entryKey + " h: "
                // + o.hashEntryKey(entryKey) + " node: " + n + " nodeh: "
                // + o.hashNodeKey(makeNodeKey(n)) + " nodefore: "
                // + nodeForEntry + " h: "
                // + o.hashNodeKey(makeNodeKey(nodeForEntry)) + " entry: "
                // + entry);

                assertEquals(n, nodeForEntry);
            }
        }
    }

    @Test
    public void shouldCheckIsNodeForEntryWell() {
        addAFewEntries(o, 3, 25);
        for (String node : o.getNodes()) {
            String nodeKey = makeNodeKey(node);
            for (String entry : o.getEntriesForNode(nodeKey, false)) {
                String entryKey = makeEntryKey(entry);
                assertTrue(o.isNodeForEntry(nodeKey, entryKey, false));
            }
        }
    }

    @Test
    public void shouldRemoveNode() {
        addAFewEntries(o);
        while (nodesAdded.size() > 0) {
            double randPos = Math.random() * nodesAdded.size();
            String nodeKey = Iterables.get(nodesAdded.keySet(),
                    Ints.checkedCast((long) randPos));
            String node = "node" + nodeKey;
            // String message = "" + Iterables.size(o.getEntries()) + " "
            // + Iterables.toString(o.getEntries());
            assertEquals(node, o.removeNode(nodeKey));
            nodesAdded.remove(nodeKey);
            assertEquals(nodesAdded.size(), o.getNodeCount());
            assertEquals(entriesAdded.size(), Iterables.size(o.getEntries()));
            if (nodesAdded.size() > 0) { // last entry gone iteration by nodes will yield nothing, see below
                shouldIterateAllEntriesViaNodesAndCheckNoDups(o);
            }
        }
        // the entries should still be contained
        shouldIterateAllEntriesViaGetEntriesForOtherTests();
        // and be reavailable after node gets added:
        o.addNode("1231", "node1231");
        shouldIterateAllEntriesViaNodesAndCheckNoDups(o);
    }

    @Test
    public void shouldRemoveEntry() {
        addAFewEntries(o);

        while (entriesAdded.size() > 0) {
            double randPos = Math.random() * entriesAdded.size();
            String entryKey = Iterables.get(entriesAdded.keySet(),
                    Ints.checkedCast((long) randPos));
            String entry = "entry" + entryKey;
            // String message = "" + Iterables.size(o.getEntries()) + " "
            // + Iterables.toString(o.getEntries());
            assertEquals(/* message, */entry, o.removeEntry(entryKey));
            entriesAdded.remove(entryKey);
            assertEquals(entriesAdded.size(), Iterables.size(o.getEntries()));
            shouldIterateAllEntriesViaNodesAndCheckNoDups(o);
        }
    }

    public String makeEntryKey(String entry) {
        return entry.substring("entry".length());
    }

    public String makeNodeKey(String node) {
        return node.substring("node".length());
    }

    @Test
    public void shouldShowNodeIn() {
        String randIntStr = RandomHelpers.randIntStr();
        long hash = o.hashNodeKey(randIntStr);
        assertFalse(o.isHavingNode(randIntStr));
        o.addNode(randIntStr, "node" + randIntStr);
        assertFalse(o.isHavingNode(randIntStr + "1"));
        assertTrue(o.isHavingNode(randIntStr));
        assertTrue(o.isHavingNode(hash));
    }

    @Test
    public void shouldReturnNullOnGetNodeAfterNodeForEmptyRing() {

        assertNull(o.getNodeAfterNode((long)123421839));
    }

    @Test
    public void shouldHaveWorkingGetNodeAfterNode() {
        String[] threeStrings = RandomHelpers.randIntStrings(3, true);

        o.addNode(threeStrings[0], "node" + threeStrings[0]);
        assertNull("must be null for itself because there is no next",
                o.getNodeAfterNode(threeStrings[0]));
        assertEquals("must return only member for any other but that member",
                "node" + threeStrings[0], o.getNodeAfterNode(threeStrings[2]));

        o.addNode(threeStrings[1], "node" + threeStrings[1]);
        o.addNode(threeStrings[2], "node" + threeStrings[2]);

        // must work like a ring:
        String[] res = new String[3];
        res[0] = threeStrings[RandomHelpers.randPosInt(3)];
        res[1] = makeNodeKey(o.getNodeAfterNode(res[0]));
        res[2] = makeNodeKey(o.getNodeAfterNode(res[1]));
        assertEquals(res[0], makeNodeKey(o.getNodeAfterNode(res[2])));

        Arrays.sort(threeStrings);
        Arrays.sort(res);
        assertEquals(Arrays.asList(threeStrings), Arrays.asList(res));
    }

    private void shouldIterateAllEntriesViaNodesAndCheckNoDups(
            ConsistentHashRingHoldingResourceEntries<String, String, String, String> o2) {
        TreeMap<String, String> result = new TreeMap<String, String>();
        for (String node : o2.getNodes()) {
            for (String entry : o2.getEntriesForNode(makeNodeKey(node), true)) {
                String previous = result.put(entry, null);
                assertNull("Duplicate entry for node " + node + ": " + previous, previous);
            }
        }
        compareAllEntryIterationWithExpected(result);

    }

    @Test
    public void shouldIterateAllEntriesViaGetEntries() {
        addAFewEntries(o);
        shouldIterateAllEntriesViaGetEntriesForOtherTests();
    }

    public void shouldIterateAllEntriesViaGetEntriesForOtherTests() {
        TreeMap<String, String> result = new TreeMap<String, String>();
        for (String entry : o.getEntries()) {
            result.put(entry, null);
        }
        compareAllEntryIterationWithExpected(result);
    }

    public void compareAllEntryIterationWithExpected(
            TreeMap<String, String> result) {

        // this shows better comparison results reflecting the ring but
        // skips dups

        TreeMap<Long, String> origEntriesByHash = Maps.newTreeMap();
        for (String k : entriesAdded.keySet()) {
            origEntriesByHash.put(o.hashEntryKey(k), entriesAdded.get(k));
        }
        TreeMap<Long, String> actualEntriesByHash = Maps.newTreeMap();
        for (String entry : result.keySet()) {
            String entryKey = makeEntryKey(entry);
            String value = entriesAdded.get(entryKey);
            if (value == null) {
                System.err.println("found no entry for key " + entryKey
                        + " and hash " + o.hashEntryKey(entryKey));
            }
            actualEntriesByHash.put(o.hashEntryKey(entryKey), value);
        }
        // System.out.println("Actual by hash: " + actualEntriesByHash);

        assertEquals(origEntriesByHash, actualEntriesByHash);

        ArrayList<String> addedVals = Lists.newArrayList(entriesAdded
                .values());
        Collections.sort(addedVals);
        ArrayList<String> actualVals = Lists.newArrayList(result.keySet());
        Collections.sort(actualVals);
        Assert.assertEquals(addedVals, actualVals);
    }

    private void addAFewEntries(
            ConsistentHashRingHoldingResourceEntries<String, String, String, String> o2) {
        addAFewEntries(o2, 10, 100);
    }

    private void addAFewEntries(
            ConsistentHashRingHoldingResourceEntries<String, String, String, String> o2,
            int nodes, int entries) {
        nodesAdded = new TreeMap<String, String>();
        for (int i = 0; i < nodes; i++) {
            String val = RandomHelpers.randIntStr();
            if (!nodesAdded.containsKey(val)) {
                nodesAdded.put(val, "node" + val);
                o2.addNode(val, "node" + val);
            }
        }
        entriesAdded = new TreeMap<String, String>();
        for (int i = 0; i < entries; i++) {
            String val = RandomHelpers.randIntStr();
            if (!entriesAdded.containsKey(val)) {
                entriesAdded.put(val, "entry" + val);
                o2.putEntry(val, "entry" + val);
            }
        }

    }

    @Test
    public void throwsOnCollisionsInNodes() {
        RingHasher<String> h = new RingHasher<String>() {
            @Override
            public long hash(Hasher h, String n) {
                return 123;
            }
        };
        o = new ConsistentHashRingImpl<String, String, String, String>(ConsistentHashRings.DEFAULT_64_BIT_HASH_FUNC, h,
                ConsistentHashRings.DEFAULT_HASHER);
        o.addNode("1", "node1");
        try {
            o.addNode("2", "node2");
            fail();
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

}
