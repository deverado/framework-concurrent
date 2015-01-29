package de.deverado.framework.concurrent;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import javax.annotation.Nullable;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.hash.HashFunction;

import de.deverado.framework.concurrent.ConsistentHashRings.RingHasher;

/**
 * Goals:
 * <ul>
 * <li>
 * 1. Find node for resource
 * <li>
 * 2. Find resources for node (eg. affected by move)
 * </ul>
 * Assumptions
 * <ul>
 * <li>
 * a1. Keys are unique - BUT nodes and entries can share keys.
 * <li>a2. Each set, stored nodes and stored entries
 * <li>a3. Node keys don't collide.
 * </ul>
 * 
 * Implementation: node A has all entries that have the same hash as its key's
 * hash, or are less than A's key's hash and greater than the hash of the first
 * node B with a hash B.hash less than A.hash.
 * 
 * @param <T>
 */
public class ConsistentHashRingImpl<NodeKeyT, NodeT, EntryKeyT, EntryT>
        implements
        ConsistentHashRingHoldingResourceEntries<NodeKeyT, NodeT, EntryKeyT, EntryT> {

    /**
     * @see vcode.framework.ConsistentHashRingHoldingResourceEntries#hashEntryKey(EntryKeyT)
     */
    @Override
    public long hashEntryKey(EntryKeyT key) {
        return ConsistentHashRings.hashKey(hashFunction, entryHasher, key);
    }

    /**
     * @see vcode.framework.ConsistentHashRingHoldingResourceEntries#hashNodeKey(NodeKeyT)
     */
    @Override
    public long hashNodeKey(NodeKeyT key) {
        return ConsistentHashRings.hashKey(hashFunction, nodeHasher, key);
    }

    private final HashFunction hashFunction;
    private final RingHasher<Object> nodeHasher;
    private final RingHasher<Object> entryHasher;
    private final TreeMap<Long, NodeT> nodeRing = Maps.newTreeMap();
    private final TreeMap<Long, Map<EntryKeyT, EntryT>> entryRing = Maps
            .newTreeMap();

    public ConsistentHashRingImpl() {
        this(null, null, null);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public ConsistentHashRingImpl(@Nullable HashFunction hashFunction,
            @Nullable RingHasher<? super NodeKeyT> nodeHasher,
            @Nullable RingHasher<? super EntryKeyT> entryHasher) {
        this.hashFunction = hashFunction;
        this.nodeHasher = nodeHasher == null ? (RingHasher) ConsistentHashRings.DEFAULT_HASHER
                : nodeHasher;
        this.entryHasher = entryHasher == null ? (RingHasher) ConsistentHashRings.DEFAULT_HASHER
                : entryHasher;
    }

    // public void addNodes(Collection<NodeT> nodes) {
    // for (NodeT node : nodes) {
    // addNode(node);
    // }
    // }

    /**
     * @see vcode.framework.ConsistentHashRingHoldingResourceEntries#addNode(NodeKeyT,
     *      NodeT)
     */
    @Override
    public Object addNode(NodeKeyT key, NodeT node) {

        long hashNodeKey = hashNodeKey(key);
        return addNodeByHash(hashNodeKey, node);
    }

    /**
     * @see vcode.framework.ConsistentHashRingHoldingResourceEntries#addNodeByHash(java.lang.Long,
     *      NodeT)
     */
    @Override
    public NodeT addNodeByHash(Long hashNodeKey, NodeT node) {
        NodeT previous = nodeRing.get(hashNodeKey);
        if (previous != null) {
            throw new IllegalArgumentException("Collision of node key hashes: "
                    + previous + ".hashEquals(" + node + ")");
        }
        return nodeRing.put(hashNodeKey, node);
    }

    /**
     * @see vcode.framework.ConsistentHashRingHoldingResourceEntries#removeNode(NodeKeyT)
     */
    @Override
    public NodeT removeNode(NodeKeyT key) {
        long hashNodeKey = hashNodeKey(key);
        return removeNodeByHash(hashNodeKey);
    }

    /**
     * @see vcode.framework.ConsistentHashRingHoldingResourceEntries#removeNodeByHash(java.lang.Long)
     */
    @Override
    public NodeT removeNodeByHash(Long hashNodeKey) {
        return nodeRing.remove(hashNodeKey);
    }

    /**
     * @see vcode.framework.ConsistentHashRingHoldingResourceEntries#getEntriesForNode(NodeKeyT,
     *      boolean)
     */
    @Override
    public Iterable<EntryT> getEntriesForNode(NodeKeyT nodeKey,
            boolean checkNodeInRing) {
        Long nodeHash = hashNodeKey(nodeKey);
        if (checkNodeInRing) {
            if (!isHavingNode(nodeHash)) {
                throw new IllegalArgumentException(
                        "Given node key is not used in ring: " + nodeKey);
            }
        }
        Long previousNodeInRing = previousKeyInRingNonInclusive(nodeRing,
                nodeHash);
        if (null == previousNodeInRing) {
            // either no other nodes in ring or only current node in ring,
            // return all
            return iterateEntries(entryRing.values());
        } else {
            if (previousNodeInRing > nodeHash) {
                // need to loop
                return Iterables.concat(
                        iterateEntries(entryRing.tailMap(previousNodeInRing,
                                false).values()), //
                        iterateEntries(entryRing.headMap(nodeHash, true)
                                .values()));
            } else {
                // from previous
                return iterateEntries(entryRing.subMap(previousNodeInRing,
                        false, nodeHash, true).values());
            }
        }
    }

    /**
     * @see vcode.framework.ConsistentHashRingHoldingResourceEntries#getNodes()
     */
    @Override
    public Iterable<NodeT> getNodes() {
        return nodeRing.values();
    }

    public NodeT getNode(NodeKeyT nodeKey) {
        Long nodeKeyHash = hashNodeKey(nodeKey);
        return getNode(nodeKeyHash);
    }

    public NodeT getNode(Long nodeKeyHash) {
        return nodeRing.get(nodeKeyHash);
    }

    /**
     * @see vcode.framework.ConsistentHashRingHoldingResourceEntries#getEntries()
     */
    @Override
    public Iterable<EntryT> getEntries() {
        return iterateEntries(entryRing.values());
    }

    private Iterable<EntryT> iterateEntries(
            final Collection<Map<EntryKeyT, EntryT>> collection) {

        return new Iterable<EntryT>() {
            @Override
            public Iterator<EntryT> iterator() {
                return new Iterator<EntryT>() {

                    private Iterator<Map<EntryKeyT, EntryT>> outer = collection
                            .iterator();
                    private Iterator<EntryT> inner;

                    @Override
                    public boolean hasNext() {
                        boolean innerHasNext = false;
                        while (!innerHasNext) {
                            if (inner == null) {
                                if (outer.hasNext()) {
                                    inner = outer.next().values().iterator();
                                } else {
                                    break;
                                }
                            }

                            innerHasNext = inner.hasNext();
                            if (!innerHasNext) {
                                inner = null;
                            }
                        }
                        return innerHasNext;
                    }

                    @Override
                    public EntryT next() {
                        if (inner == null) {
                            if (!hasNext()) {
                                throw new IllegalStateException();
                            }
                        }
                        EntryT next = inner.next();
                        return next;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }

                };
            }
        };
    }

    /**
     * @see vcode.framework.ConsistentHashRingHoldingResourceEntries#getEntry(EntryKeyT)
     */
    @Override
    public EntryT getEntry(EntryKeyT key) {

        Long hash = hashEntryKey(key);
        Map<EntryKeyT, EntryT> inner = entryRing.get(hash);
        if (inner != null) {
            return inner.get(key);
        }

        return null;
    }

    /**
     * @see vcode.framework.ConsistentHashRingHoldingResourceEntries#putEntry(EntryKeyT,
     *      EntryT)
     */
    @Override
    public EntryT putEntry(EntryKeyT key, EntryT entry) {
        Long hash = hashEntryKey(key);
        Map<EntryKeyT, EntryT> inner = entryRing.get(hash);
        EntryT retval = null;
        if (inner != null) {
            if (inner instanceof TreeMap) {
                retval = inner.put(key, entry);
            } else {
                inner = new TreeMap<EntryKeyT, EntryT>(inner);
                retval = inner.put(key, entry);
                if (inner.size() == 1) {
                    entryRing.put(hash, Collections.singletonMap(key, entry));
                } else {
                    entryRing.put(hash, inner);
                }
            }
        } else {
            inner = Collections.singletonMap(key, entry);
            entryRing.put(hash, inner);
        }
        return retval;
    }

    /**
     * @see vcode.framework.ConsistentHashRingHoldingResourceEntries#removeEntry(EntryKeyT)
     */
    @Override
    public EntryT removeEntry(EntryKeyT key) {
        Long hash = hashEntryKey(key);
        Map<EntryKeyT, EntryT> inner = entryRing.get(hash);
        EntryT retval = null;
        if (inner != null) {
            if (inner.size() == 1) {
                retval = inner.get(key);
                if (retval != null) {
                    entryRing.remove(hash);
                }
            } else {
                retval = inner.remove(key);
            }
        }
        return retval;
    }

    /**
     * @see vcode.framework.ConsistentHashRingHoldingResourceEntries#getNodeCount()
     */
    @Override
    public int getNodeCount() {
        return nodeRing.size();
    }

    /**
     * @see vcode.framework.ConsistentHashRingHoldingResourceEntries#isHavingNodes()
     */
    @Override
    public boolean isHavingNodes() {
        return nodeRing.size() > 0;
    }

    @Override
    public boolean isWithoutNodes() {
        return nodeRing.size() == 0;
    }

    @Override
    public boolean isHavingNode(Long nodeKeyHash) {
        return nodeRing.containsKey(nodeKeyHash);
    }

    public boolean isHavingNode(NodeKeyT key) {
        return isHavingNode(hashNodeKey(key));
    }

    /**
     * @see vcode.framework.ConsistentHashRingHoldingResourceEntries#getNodeForEntry(EntryKeyT)
     */
    @Override
    public NodeT getNodeForEntry(EntryKeyT key) {
        Long entryKeyHash = hashEntryKey(key);
        return getNodeForEntry(entryKeyHash);
    }

    /**
     * @see vcode.framework.ConsistentHashRingHoldingResourceEntries#getNodeForEntry(java.lang.Long)
     */
    @Override
    public NodeT getNodeForEntry(Long entryKeyHash) {

        NodeT retval = nextEntryInRing(nodeRing, entryKeyHash);
        return retval;
    }

    @Override
    public NodeT getNodeAfterNode(Long nodeKeyHash) {
        // +1 because nextInner.. is inclusive and long arith loops MIN_VAL at
        // end
        Entry<Long, NodeT> nextEntryInRing = nextInnerEntryInRing(nodeRing,
                nodeKeyHash + 1);
        if (nextEntryInRing == null
                || nextEntryInRing.getKey().equals(nodeKeyHash)) {
            return null;
        }
        return nextEntryInRing.getValue();
    }

    public NodeT getNodeAfterNode(NodeKeyT key) {
        return getNodeAfterNode(hashNodeKey(key));
    }

    public boolean isNodeForEntry(NodeKeyT nodeKey, EntryKeyT entryKey,
            boolean ensureNodeIsInRing) {
        Long nodeKeyHash = hashNodeKey(nodeKey);
        if (ensureNodeIsInRing && !isHavingNode(nodeKeyHash)) {
            return false;
        }

        long testHash = nodeKeyHash;
        long entryHash = hashEntryKey(entryKey);
        Long previous = previousKeyInRingNonInclusive(nodeRing, testHash);
        if (previous == null) {
            // ring empty or given node is same as only node in ring:
            return true;
        }

        return ConsistentHashRings.testBelongsToNode(entryHash, testHash,
                previous);
    }

    /**
     * 
     * @param ring
     * @param entryKeyHash
     * @return <code>null</code> for an empty ring. the entry with the given
     *         entryKeyHash or the next bigger key (or after a loop next bigger
     *         starting with the smallest key)
     */
    public static <T> T nextEntryInRing(TreeMap<Long, T> ring, Long entryKeyHash) {

        Entry<Long, T> retval = nextInnerEntryInRing(ring, entryKeyHash);
        return retval != null ? retval.getValue() : null;
    }

    public static <T> T previousEntryInRingNonInclusive(TreeMap<Long, T> ring,
            Long entryKeyHash) {
        Entry<Long, T> retval = previousInnerEntryInRingNonInclusive(ring,
                entryKeyHash);
        return retval != null ? retval.getValue() : null;
    }

    public static <T> Entry<Long, T> previousInnerEntryInRingNonInclusive(
            TreeMap<Long, T> ring, Long entryKeyHash) {
        if (ring.isEmpty()) {
            return null;
        }

        // floor is the next smaller or the value for key itself
        Entry<Long, T> retval = ring.floorEntry(entryKeyHash - 1);
        if (retval == null) {
            // loop to end
            Entry<Long, T> lastEntry = ring.lastEntry();
            if (lastEntry != null) {
                if (lastEntry.getKey().longValue() == entryKeyHash) {
                    return null; // non-incl
                }
                return lastEntry;
            }
        }
        return retval;
    }

    public static <T> Long previousKeyInRingNonInclusive(TreeMap<Long, T> ring,
            long entryKeyHash) {
        Entry<Long, T> retval = previousInnerEntryInRingNonInclusive(ring,
                entryKeyHash);
        return retval != null ? retval.getKey() : null;
    }

    /**
     * 
     * @param ring
     * @param entryKeyHash
     * @return <code>null</code> if ring is empty. the key with the given
     *         entryKeyHash or the next bigger (or after a loop next bigger
     *         starting with the smallest key)
     */
    public static <T> Long nextKeyInRing(TreeMap<Long, T> ring,
            Long entryKeyHash) {
        Entry<Long, T> retval = nextInnerEntryInRing(ring, entryKeyHash);
        return retval != null ? retval.getKey() : null;
    }

    public static <T> Entry<Long, T> nextInnerEntryInRing(
            TreeMap<Long, T> ring, Long entryKeyHash) {
        if (ring.isEmpty()) {
            return null;
        }

        // ceiling is the next bigger or the value for key itself
        Entry<Long, T> retval = ring.ceilingEntry(entryKeyHash);
        if (retval == null) {
            // loop to beginning
            retval = ring.firstEntry();
        }
        return retval;
    }

    // public interface Node<NodeKeyT2> {
    // NodeKeyT2 getKey();
    // }

}
