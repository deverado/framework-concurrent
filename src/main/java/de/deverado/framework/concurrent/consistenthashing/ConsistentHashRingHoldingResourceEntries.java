package de.deverado.framework.concurrent.consistenthashing;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Map;

@ParametersAreNonnullByDefault
public interface ConsistentHashRingHoldingResourceEntries<NodeKeyT, NodeT, EntryKeyT, EntryT> {

    public long hashEntryKey(@Nullable EntryKeyT key);

    public long hashNodeKey(@Nullable NodeKeyT key);

    public NodeT addNode(NodeKeyT key, NodeT node);

    public NodeT addNodeByHash(Long hashNodeKey, NodeT node);

    public NodeT removeNode(NodeKeyT key);

    public NodeT removeNodeByHash(Long hashNodeKey);

    public boolean isHavingNode(@Nullable NodeKeyT key);

    public boolean isHavingNode(@Nullable Long nodeKeyHash);

    public Iterable<EntryT> getEntriesForNode(NodeKeyT nodeKey,
            boolean checkNodeInRing);

    public Iterable<NodeT> getNodes();

    public Iterable<Map.Entry<Long, NodeT>> getNodeEntries();

    public NodeT getNode(@Nullable NodeKeyT nodeKey);

    public NodeT getNodeByHash(@Nullable Long nodeKey);

    public Iterable<EntryT> getEntries();

    public EntryT getEntry(@Nullable EntryKeyT key);

    public EntryT putEntry(EntryKeyT key, EntryT entry);

    public EntryT removeEntry(@Nullable EntryKeyT key);

    public int getNodeCount();

    public boolean isHavingNodes();

    public boolean isWithoutNodes();

    public NodeT getNodeBeforeNode(Long nodeKeyHash);

    public NodeT getNodeAfterNode(NodeKeyT key);

    public NodeT getNodeAfterNode(Long nodeKeyHash);

    public NodeT getNodeForEntry(EntryKeyT key);

    public NodeT getNodeForEntry(Long entryKeyHash);

    public boolean isNodeForEntry(@Nullable NodeKeyT nodeKey, @Nullable EntryKeyT entryKey,
            boolean ensureNodeIsInRing);

}
