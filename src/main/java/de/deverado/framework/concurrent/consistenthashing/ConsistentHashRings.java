package de.deverado.framework.concurrent.consistenthashing;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Multimap;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import de.deverado.framework.core.Multimaps2;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Sharding via consistent hashing.
 *
 */
@ParametersAreNonnullByDefault
public class ConsistentHashRings {

    /**
     * Handles String well, others fall back on hashCode(), which is then hashed
     * again.
     */
    public static final RingHasher<Object> DEFAULT_HASHER = new RingHasher<Object>() {
        @Override
        public long hash(@Nullable Hasher h, @Nullable Object n) {
            if (h == null) {
                h = DEFAULT_64_BIT_HASH_FUNC.newHasher();
            }
            if (n instanceof CharSequence) {
                HashCode hash = h.putString((CharSequence) n, Charsets.UTF_8).hash();
                return de.deverado.framework.concurrent.Hashing.extract64Bits(hash);
            } else {
                HashCode hash = h.putInt(n == null ? 0 : n.hashCode()).hash();
                return de.deverado.framework.concurrent.Hashing.extract64Bits(hash);
            }
        }
    };

    public static final RingHasher<Object> DEFAULT_32_BIT_HASHER = new RingHasher<Object>() {
        @Override
        public long hash(@Nullable Hasher h, @Nullable Object n) {
            if (h == null) {
                h = DEFAULT_32_BIT_HASH_FUNC.newHasher();
            }
            if (n instanceof CharSequence) {
                HashCode hash = h.putString((CharSequence) n, Charsets.UTF_8).hash();
                return de.deverado.framework.concurrent.Hashing.extract32Bits(hash);
            } else {
                HashCode hash = h.putInt(n == null ? 0 : n.hashCode()).hash();
                return de.deverado.framework.concurrent.Hashing.extract32Bits(hash);
            }
        }
    };

    /**
     * murmur3 32 is thread safe and size should be sufficient for most
     * applications. GTODO validate cross-platform handling of byte order of
     * murmur3
     */
    public static final HashFunction DEFAULT_32_BIT_HASH_FUNC = Hashing
            .murmur3_32(0 /* seed must be same on all nodes */);

    public static final HashFunction DEFAULT_64_BIT_HASH_FUNC = Hashing
            .murmur3_128(0 /* seed must be same on all nodes */);

    public static final HashFunction SHA1_HASH_FUNC = Hashing
            .sha1();

    public static <KeyT> long hashKey(@Nullable HashFunction hashFunction,
            RingHasher<KeyT> hasher, @Nullable KeyT key) {
        return hasher.hash(
                hashFunction == null ? null : hashFunction.newHasher(), key);
    }

    /**
     * Creates a new 32bit ring hashing {@link CharSequence} well and everything else
     * by using the hashCode as source data for it's murmur3_32 hash.
     * 
     *
     */
    public static <NodeKeyT, NodeT, EntryKeyT, EntryT> ConsistentHashRingHoldingResourceEntries<NodeKeyT, NodeT, EntryKeyT, EntryT> new32BitRing() {
        return new ConsistentHashRingImpl<NodeKeyT, NodeT, EntryKeyT, EntryT>(
                DEFAULT_32_BIT_HASH_FUNC, DEFAULT_32_BIT_HASHER, DEFAULT_32_BIT_HASHER);
    }

    public static <NodeKeyT, NodeT, EntryKeyT, EntryT> ConsistentHashRingHoldingResourceEntries<NodeKeyT, NodeT, EntryKeyT, EntryT> newMurmur3Ring64Bit() {
        return new ConsistentHashRingImpl<NodeKeyT, NodeT, EntryKeyT, EntryT>(
                DEFAULT_64_BIT_HASH_FUNC, DEFAULT_HASHER, DEFAULT_HASHER);
    }

    /**
     * Creates a new ring hashing {@link CharSequence} well and everything else
     * by using the hashCode as source data for it's sha1 hash.
     *
     *
     */
    public static <NodeKeyT, NodeT, EntryKeyT, EntryT> ConsistentHashRingHoldingResourceEntries<NodeKeyT, NodeT, EntryKeyT, EntryT> newSha1Ring64Bit() {
        return new ConsistentHashRingImpl<NodeKeyT, NodeT, EntryKeyT, EntryT>(
                SHA1_HASH_FUNC, DEFAULT_HASHER, DEFAULT_HASHER);
    }

    public static <NodeKeyT, NodeT, EntryKeyT, EntryT> ConsistentHashRingHoldingResourceEntries<NodeKeyT, NodeT, EntryKeyT, EntryT> newSha1Ring32Bit() {
        return new ConsistentHashRingImpl<NodeKeyT, NodeT, EntryKeyT, EntryT>(
                SHA1_HASH_FUNC, DEFAULT_32_BIT_HASHER, DEFAULT_32_BIT_HASHER);
    }

    public static <NodeKeyT, NodeT, EntryKeyT, EntryT> ConsistentHashRingHoldingResourceEntries<NodeKeyT, NodeT, EntryKeyT, EntryT> singletonNodeRing(
            final @Nullable HashFunction func, final RingHasher<NodeKeyT> nodeHasher,
            final RingHasher<EntryKeyT> entryHasher, @Nullable final NodeKeyT key,
            @Nullable final NodeT node) {
        final Long hash = key == null ? 0 : ConsistentHashRings.hashKey(func,
                nodeHasher, key);
        return new ConsistentHashRingHoldingResourceEntries<NodeKeyT, NodeT, EntryKeyT, EntryT>() {

            @Override
            public long hashEntryKey(@Nullable EntryKeyT key) {
                return ConsistentHashRings.hashKey(func, entryHasher, key);
            }

            @Override
            public long hashNodeKey(@Nullable NodeKeyT key) {
                return ConsistentHashRings.hashKey(func, nodeHasher, key);
            }

            @Override
            public NodeT addNode(NodeKeyT key, NodeT node) {
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
            public boolean isHavingNode(@Nullable Long nodeKeyHash) {
                if (nodeKeyHash == null) {
                    return false;
                }
                return hash.longValue() == nodeKeyHash.longValue();
            }

            public boolean isHavingNode(@Nullable NodeKeyT keyParam) {
                if (keyParam == null) {
                    return false;
                }
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
            public Iterable<Map.Entry<Long, NodeT>> getNodeEntries() {
                return Collections.singletonMap(hash.longValue(), node).entrySet();
            }

            @Override
            public Iterable<EntryT> getEntries() {
                return Collections.emptyList();
            }

            @Override
            public NodeT getNode(@Nullable NodeKeyT keyParam) {
                if (keyParam != null && keyParam.equals(key)) {
                    return node;
                }
                return null;
            }

            @Override
            public NodeT getNodeByHash(@Nullable Long nodeKey) {
                if (nodeKey != null && nodeKey.equals(hash)) {
                    return node;
                }
                return null;
            }

            @Override
            public EntryT getEntry(@Nullable EntryKeyT key) {
                return null;
            }

            @Override
            public EntryT putEntry(EntryKeyT key, EntryT entry) {
                throw new UnsupportedOperationException(
                        "only a singletonNodeRing");
            }

            @Override
            public EntryT removeEntry(@Nullable EntryKeyT key) {
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
            public NodeT getNodeBeforeNode(Long nodeKeyHash) {
                if (hash.longValue() != nodeKeyHash.longValue()) {
                    return node;
                }
                return null;
            }

            @Override
            public NodeT getNodeAfterNode(Long nodeKeyHash) {
                if (hash.longValue() != nodeKeyHash.longValue()) {
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

            public boolean isNodeForEntry(@Nullable NodeKeyT nodeKey, @Nullable EntryKeyT entryKey,
                    boolean ensureNodeIsInRing) {
                if (nodeKey == null || entryKey == null) {
                    return false;
                }
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


    /**
     * @deprecated for documentation purposes - better to use random vnodes.
     */
    public static int[] createEvenSplitVNodeHashMapping(int vnodeCount) {
        long spacing = (1l + Integer.MAX_VALUE - (long)Integer.MIN_VALUE) / vnodeCount;
        int current = Integer.MIN_VALUE;
        int[] result = new int[vnodeCount];
        for (int i = 0; i < vnodeCount; i++) {
            result[i] = current;
            current += spacing;
        }
        return result;
    }

    /**
     * @deprecated for documentation purposes - better to use random vnodes.
     */
    public static long[] createEvenSplitVNodeHashMappingLong(int vnodeCount) {

        if (vnodeCount < 2) {
            throw new IllegalArgumentException("min 2");
        }
        long spacing = Long.MAX_VALUE / vnodeCount * 2;
        long current = Long.MIN_VALUE;
        long[] result = new long[vnodeCount];
        for (int i = 0; i < vnodeCount; i++) {
            result[i] = current;
            current += spacing;
        }
        return result;
    }

    public static long[] createRandomVnodes(Random random, String additionalRandData, int vnodeCount) {
        long[] result = new long[vnodeCount];
        for (int i = 0; i < vnodeCount; i++) {
            HashCode hash = Hashing.md5().newHasher().putString(additionalRandData, Charsets.UTF_8)//
                    .putLong(random.nextLong()).hash();
            result[i] = de.deverado.framework.concurrent.Hashing.extract64Bits(hash);
        }
        return result;
    }

    /**
     * Create a multi-machine ring with given (random) keys per-machine. Key collisions are resolved bei letting the
     * machine with the smallest id win.
     * The keys must be the same for a machine on all nodes distributing work - so they must be in the config DB.
     */
    public static <VNodeType extends VNode> void addVNodeMachinesToRing(
            ConsistentHashRingHoldingResourceEntries<?, VNodeType, ?, ?> ring,
            Function<Pair<String, Long>, VNodeType> nodeCreator, Multimap<String, Long> machines) {

        for (Map.Entry<String, Long> m : machines.entries()) {
            VNodeType curr = ring.getNodeByHash(m.getValue());
            if (curr != null) {
                if (curr.getLeaderId().compareTo(m.getKey()) < 0) {
                    continue;
                }
                ring.removeNodeByHash(m.getValue());
            }
            ring.addNodeByHash(m.getValue(), nodeCreator.apply(Pair.of(m.getKey(), m.getValue())));
        }
    }

    public static <VNodeType extends VNode> void addVNodeMachineToRing(
            ConsistentHashRingHoldingResourceEntries<?, VNodeType, ?, ?> ring,
            String machineId, Function<Pair<String, Long>, VNodeType> nodeCreator, long... machineVNodes) {

        Multimap<String, Long> map = Multimaps2.newHashSetMultimap();
        for (long l : machineVNodes) {
            map.put(machineId, l);
        }
        addVNodeMachinesToRing(ring, nodeCreator, map);
    }

    /**
     * Shuffles the (contiguous) vnodes of {@link #createEvenSplitVNodeHashMapping } deterministically so that the new
     * contiguous regions are non-contiguous for a certain number (depending on subdivisor) entries.
     * <p>
     *     Careful, to multi-machine failure resilience it is important that replicas for all ranges that a machine
     *     is responsible for are assigned not in a permutated way different for each range, but to the next nodes
     *     in the ring. Otherwise the chances for a single range to go down increase with vnode count.
     * </p>
     * Could be used to map contiguous regions of non-contiguous hashes to machines (not multiple vnodes per machine,
     * but one id per machine in ring, taking ownership of all following contiguous ranges.
     * @deprecated for documentation purposes - better to use random vnodes.
     */
    public static int[] createVNodeNonContiguousPermutation(Integer vnodeCount, Integer subdivisor,
                                                            Boolean checkMappingCorrectness) {

        int[] result = new int[vnodeCount];
        int[] checkArr = null;
        if (checkMappingCorrectness) {
            checkArr = new int[vnodeCount];
            Arrays.fill(checkArr, -1);
        }

        for (int i = 0; i < vnodeCount; i++) {
            int curr = (i + i * vnodeCount/subdivisor) % vnodeCount;
            result[i] = curr;
            if (checkArr != null) {
                checkArr[curr] = i;
            }
        }

        if (checkArr != null) {
            for (int i = 0; i < checkArr.length; i++) {
                if (checkArr[i] < 0) {
                    throw new IllegalArgumentException("Mapping erroneous, missing at least: " + i);
                }
            }
        }
        return result;
    }

    public static <O> void removeDuplicatesMaintainingOrder(List<O> toModify) {
        Set<O> seen = new HashSet<>(toModify.size());
        List<O> newContents = new ArrayList<>(toModify.size());
        for (O m : toModify) {
            if (seen.add(m)) {
                newContents.add(m);
            }
        }
        toModify.clear();
        toModify.addAll(newContents);
    }

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
     *             h = ConsistentHashRings.DEFAULT_32_BIT_HASH_FUNC.newHasher(3);
     *         }
     *
     *         if (n == null) h.putInt(0);
     *         else h.putInt(n.getMachine()).putInt(n.getTimeSecond())
     *                 .putInt(n.getInc());
     *         return h.hash().asInt();
     *
     *     }
     * };
     * </pre>
     *
     * </p>
     * @param <N>
     */
    public interface RingHasher<N> {
        long hash(@Nullable Hasher h, @Nullable N n);
    }

}
