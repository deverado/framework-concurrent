package de.deverado.framework.concurrent;

public interface ConsistentHashRingHoldingResourceEntries<NodeKeyT, NodeT, EntryKeyT, EntryT> {

    public long hashEntryKey(EntryKeyT key);

    public long hashNodeKey(NodeKeyT key);

    public Object addNode(NodeKeyT key, NodeT node);

    public NodeT addNodeByHash(Long hashNodeKey, NodeT node);

    public NodeT removeNode(NodeKeyT key);

    public NodeT removeNodeByHash(Long hashNodeKey);

    public boolean isHavingNode(NodeKeyT key);

    public boolean isHavingNode(Long nodeKeyHash);

    public Iterable<EntryT> getEntriesForNode(NodeKeyT nodeKey,
            boolean checkNodeInRing);

    public Iterable<NodeT> getNodes();

    public NodeT getNode(NodeKeyT nodeKey);

    public Iterable<EntryT> getEntries();

    public EntryT getEntry(EntryKeyT key);

    public EntryT putEntry(EntryKeyT key, EntryT entry);

    public EntryT removeEntry(EntryKeyT key);

    public int getNodeCount();

    public boolean isHavingNodes();

    public boolean isWithoutNodes();

    public NodeT getNodeAfterNode(NodeKeyT key);

    public NodeT getNodeAfterNode(Long nodeKeyHash);

    public NodeT getNodeForEntry(EntryKeyT key);

    public NodeT getNodeForEntry(Long entryKeyHash);

    public boolean isNodeForEntry(NodeKeyT nodeKey, EntryKeyT entryKey,
            boolean ensureNodeIsInRing);

}
