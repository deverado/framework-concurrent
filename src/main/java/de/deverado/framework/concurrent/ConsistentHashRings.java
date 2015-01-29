package de.deverado.framework.concurrent;

import java.util.Collections;

import javax.annotation.Nullable;

import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

/**
 * <p>
 * Example for my shardableobjectid project:
 * 
 * <pre>
 * public static final RingHasher&lt;ShardableObjectId&gt; SHARDABLE_OID_HASHER = new RingHasher&lt;ShardableObjectId&gt;() {
 *     &#064;Override
 *     public long hash(Hasher h, ShardableObjectId n) {
 *         if (h == null) {
 *             // GTODO validate cross-platform handling of byte order of
 *             // murmur3
 *             h = ConsistentHashRings.DEFAULT_HASH_FUNC.newHasher(3);
 *         }
 * 
 *         return h.putInt(n.getMachine()).putInt(n.getTimeSecond())
 *                 .putInt(n.getInc()).hash().padToLong();
 * 
 *     }
 * };
 * </pre>
 * 
 * </p>
 */
public class ConsistentHashRings {

    /**
     * Handles String well, others fall back on hashCode(), which is than hashed
     * again.
     */
    public static final RingHasher<Object> DEFAULT_HASHER = new RingHasher<Object>() {
        @Override
        public long hash(Hasher h, Object n) {
            if (h == null) {
                if (n instanceof CharSequence) {
                    return DEFAULT_HASH_FUNC.hashString((CharSequence) n,
                            Charsets.UTF_8).asInt();
                } else {
                    return DEFAULT_HASH_FUNC.hashInt(
                            n == null ? null : n.hashCode()).asInt();
                }
            } else {
                if (n instanceof CharSequence) {
                    return h.putString((CharSequence) n, Charsets.UTF_8).hash()
                            .padToLong();
                } else {
                    return h.putInt(n == null ? 0 : n.hashCode()).hash()
                            .padToLong();
                }
            }
        }
    };

    /**
     * murmur3 32 is thread safe and size should be sufficient for most
     * applications. GTODO validate cross-platform handling of byte order of
     * murmur3
     */
    public static final HashFunction DEFAULT_HASH_FUNC = Hashing
            .murmur3_32(0 /* seed must be same on all nodes */);

    public static <KeyT> long hashKey(@Nullable HashFunction hashFunction,
            RingHasher<KeyT> hasher, KeyT key) {
        return hasher.hash(
                hashFunction == null ? null : hashFunction.newHasher(), key);
    }

    /**
     * Creates a new ring hashing {@link CharSequence} well and everything else
     * by using the hashCode as source data for it's murmur3_32 hash.
     * 
     * 
     * @return
     */
    public static <NodeKeyT, NodeT, EntryKeyT, EntryT> ConsistentHashRingHoldingResourceEntries<NodeKeyT, NodeT, EntryKeyT, EntryT> newRing() {
        return new ConsistentHashRingImpl<NodeKeyT, NodeT, EntryKeyT, EntryT>(
                null, null, null);
    }

    public static <NodeKeyT, NodeT, EntryKeyT, EntryT> ConsistentHashRingHoldingResourceEntries<NodeKeyT, NodeT, EntryKeyT, EntryT> singletonNodeRing(
            final HashFunction func, final RingHasher<NodeKeyT> nodeHasher,
            final RingHasher<EntryKeyT> entryHasher, final NodeKeyT key,
            final NodeT node) {
        final Long hash = key == null ? 0 : ConsistentHashRings.hashKey(func,
                nodeHasher, key);
        return new ConsistentHashRingHoldingResourceEntries<NodeKeyT, NodeT, EntryKeyT, EntryT>() {

            @Override
            public long hashEntryKey(EntryKeyT key) {
                return ConsistentHashRings.hashKey(func, entryHasher, key);
            }

            @Override
            public long hashNodeKey(NodeKeyT key) {
                return ConsistentHashRings.hashKey(func, nodeHasher, key);
            }

            @Override
            public Object addNode(NodeKeyT key, NodeT node) {
                throw new UnsupportedOperationException(
                        "only a singletonNodeRing");
            }

            @Override
            public NodeT addNodeByHash(Long hashNodeKey, NodeT node) {
                throw new UnsupportedOperationException(
                        "only a singletonNodeRing");
            }

            @Override
            public NodeT removeNode(NodeKeyT key) {
                throw new UnsupportedOperationException(
                        "only a singletonNodeRing");
            }

            @Override
            public NodeT removeNodeByHash(Long hashNodeKey) {
                throw new UnsupportedOperationException(
                        "only a singletonNodeRing");
            }

            @Override
            public boolean isHavingNode(Long nodeKeyHash) {
                return hash != null
                        && hash.longValue() == nodeKeyHash.longValue();
            }

            public boolean isHavingNode(NodeKeyT keyParam) {
                return Objects.equal(key, keyParam);
            }

            @Override
            public Iterable<EntryT> getEntriesForNode(NodeKeyT nodeKey,
                    boolean checkNodeInRing) {
                return Collections.emptyList();
            }

            @Override
            public Iterable<NodeT> getNodes() {
                return node != null ? Collections.<NodeT> singleton(node)
                        : Collections.<NodeT> emptyList();
            }

            @Override
            public Iterable<EntryT> getEntries() {
                return Collections.emptyList();
            }

            @Override
            public NodeT getNode(NodeKeyT keyParam) {
                if (keyParam != null && keyParam.equals(key)) {
                    return node;
                }
                return null;
            }

            @Override
            public EntryT getEntry(EntryKeyT key) {
                return null;
            }

            @Override
            public EntryT putEntry(EntryKeyT key, EntryT entry) {
                throw new UnsupportedOperationException(
                        "only a singletonNodeRing");
            }

            @Override
            public EntryT removeEntry(EntryKeyT key) {
                return null;
            }

            @Override
            public int getNodeCount() {
                return node != null ? 1 : 0;
            }

            @Override
            public boolean isHavingNodes() {
                return getNodeCount() != 0;
            }

            @Override
            public boolean isWithoutNodes() {
                return getNodeCount() == 0;
            }

            @Override
            public NodeT getNodeAfterNode(Long nodeKeyHash) {
                if (hash != null && nodeKeyHash != null
                        && hash.longValue() != nodeKeyHash.longValue()) {
                    return node;
                }
                return null;
            }

            public NodeT getNodeAfterNode(NodeKeyT keyParam) {
                if (Objects.equal(keyParam, key)) {
                    return null;
                }
                return node;
            }

            @Override
            public NodeT getNodeForEntry(EntryKeyT key) {
                // all entries go to one node, null is also possible
                return node;
            }

            @Override
            public NodeT getNodeForEntry(Long entryKeyHash) {
                // all entries go to one node, null is also possible
                return node;
            }

            public boolean isNodeForEntry(NodeKeyT nodeKey, EntryKeyT entryKey,
                    boolean ensureNodeIsInRing) {
                boolean testedEqualsMyNodeKey = Objects.equal(key, nodeKey);
                if (ensureNodeIsInRing && !testedEqualsMyNodeKey) {
                    return false;
                } else {
                    if (testedEqualsMyNodeKey) {
                        return true;
                    }
                    long testHash = ConsistentHashRings.hashKey(func,
                            nodeHasher, nodeKey);
                    long entryHash = ConsistentHashRings.hashKey(func,
                            entryHasher, entryKey);
                    long previous = hash;

                    return ConsistentHashRings.testBelongsToNode(entryHash,
                            testHash, previous);

                }
            }

        };

    }

    public static boolean testBelongsToNode(long entryHash, long testHash,
            long previousHash) {

        if (entryHash <= testHash) {
            if (previousHash >= entryHash && previousHash < testHash) {
                // previous lies between entry and test, takes
                // entry
                return false;
            } else {
                // also true if previous is same as testHash
                return true;
            }
        } else {
            // entryHash > testHash
            if (previousHash < testHash) {
                return false;
            }
            if (entryHash <= previousHash) {
                // the following never happens, only here for
                // comment:
                // if (previous == testHash) {
                // return true;
                // }
                return false;
            }
            // entryHash > previous, so between previous and testHash
            return true;
        }
    }

    public interface RingHasher<N> {
        long hash(Hasher h, N n);
    }

}
