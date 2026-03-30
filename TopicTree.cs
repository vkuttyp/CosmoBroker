using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System;
using System.Threading;

namespace CosmoBroker;

public class TopicTree
{
    private class TopicNode : IDisposable
    {
        // Individual subscribers: SID -> Connection Set
        public ConcurrentDictionary<string, ConcurrentDictionary<BrokerConnection, byte>> Subscribers { get; } = new();

        // Queue Groups: GroupName -> { SID -> Connection Set }
        public ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<BrokerConnection, byte>>> QueueGroups { get; } = new();

        // Queue group cursors for round-robin selection.
        public ConcurrentDictionary<string, int> QueueGroupCursors { get; } = new();

        // Child nodes (e.g., "foo" -> "bar" for "foo.bar")
        public Dictionary<byte[], TopicNode> Children { get; } = new(new Sublist.ReadOnlySpanByteComparer());
        public ReaderWriterLockSlim ChildrenLock { get; } = new();

        public void Dispose()
        {
            ChildrenLock.Dispose();
        }
    }

    private readonly TopicNode _root = new();
    private static readonly byte[] WildcardStar = [(byte)'*'];
    private static readonly byte[] WildcardChevron = [(byte)'>'];

    public void Subscribe(string subject, BrokerConnection connection, string sid, string? queueGroup = null)
    {
        var current = _root;
        int start = 0;
        int dotIdx;

        // stackalloc avoids heap allocation for the UTF-8 encoding (matches Publish path).
        Span<byte> subjectBuf = stackalloc byte[subject.Length * 3];
        int subjectLen = System.Text.Encoding.UTF8.GetBytes(subject, subjectBuf);
        ReadOnlySpan<byte> subjectBytes = subjectBuf.Slice(0, subjectLen);

        while ((dotIdx = subjectBytes.Slice(start).IndexOf((byte)'.')) != -1)
        {
            var part = subjectBytes.Slice(start, dotIdx);
            current = GetOrAddChild(current, part);
            start += dotIdx + 1;
        }
        current = GetOrAddChild(current, subjectBytes.Slice(start));

        if (string.IsNullOrEmpty(queueGroup))
        {
            var set = current.Subscribers.GetOrAdd(sid, _ => new ConcurrentDictionary<BrokerConnection, byte>());
            set[connection] = 0;
        }
        else
        {
            var group = current.QueueGroups.GetOrAdd(queueGroup, _ => new ConcurrentDictionary<string, ConcurrentDictionary<BrokerConnection, byte>>());
            var set = group.GetOrAdd(sid, _ => new ConcurrentDictionary<BrokerConnection, byte>());
            set[connection] = 0;
        }
    }

    private TopicNode GetOrAddChild(TopicNode node, ReadOnlySpan<byte> part)
    {
        var lookup = node.Children.GetAlternateLookup<ReadOnlySpan<byte>>();
        node.ChildrenLock.EnterReadLock();
        try
        {
            if (lookup.TryGetValue(part, out var child)) return child;
        }
        finally { node.ChildrenLock.ExitReadLock(); }

        node.ChildrenLock.EnterWriteLock();
        try
        {
            if (lookup.TryGetValue(part, out var child)) return child;
            var newNode = new TopicNode();
            node.Children.Add(part.ToArray(), newNode);
            return newNode;
        }
        finally { node.ChildrenLock.ExitWriteLock(); }
    }

    public void Unsubscribe(string subject, BrokerConnection connection, string sid, string? queueGroup = null)
    {
        // stackalloc avoids heap allocation for the UTF-8 encoding.
        Span<byte> subjectBuf = stackalloc byte[subject.Length * 3];
        int subjectLen = System.Text.Encoding.UTF8.GetBytes(subject, subjectBuf);
        ReadOnlySpan<byte> subjectBytes = subjectBuf.Slice(0, subjectLen);

        // Track path so we can prune empty nodes bottom-up after unsubscribing.
        var path = new List<(TopicNode Parent, byte[] Key, TopicNode Child)>(4);

        var current = _root;
        int start = 0;
        int dotIdx;

        while ((dotIdx = subjectBytes.Slice(start).IndexOf((byte)'.')) != -1)
        {
            var part = subjectBytes.Slice(start, dotIdx);
            if (!TryGetChild(current, part, out var next)) return;
            path.Add((current, part.ToArray(), next!));
            current = next!;
            start += dotIdx + 1;
        }
        {
            var lastPart = subjectBytes.Slice(start);
            if (!TryGetChild(current, lastPart, out var leaf)) return;
            path.Add((current, lastPart.ToArray(), leaf!));
            current = leaf!;
        }

        if (string.IsNullOrEmpty(queueGroup))
        {
            if (current.Subscribers.TryGetValue(sid, out var set))
            {
                set.TryRemove(connection, out _);
                if (set.IsEmpty) current.Subscribers.TryRemove(sid, out _);
            }
        }
        else
        {
            if (current.QueueGroups.TryGetValue(queueGroup, out var group))
            {
                if (group.TryGetValue(sid, out var set))
                {
                    set.TryRemove(connection, out _);
                    if (set.IsEmpty) group.TryRemove(sid, out _);
                }
                if (group.IsEmpty) current.QueueGroups.TryRemove(queueGroup, out _);
            }
        }

        // Prune empty nodes from leaf toward root. Stop as soon as a non-empty node is found
        // since its ancestors must also be non-empty (they have at least this child).
        for (int i = path.Count - 1; i >= 0; i--)
        {
            var (parent, key, child) = path[i];
            if (!child.Subscribers.IsEmpty || !child.QueueGroups.IsEmpty || child.Children.Count != 0)
                break;

            parent.ChildrenLock.EnterWriteLock();
            try
            {
                // Re-check under write lock: another thread may have subscribed to this node
                // between our check above and acquiring the lock.
                if (child.Subscribers.IsEmpty && child.QueueGroups.IsEmpty && child.Children.Count == 0)
                {
                    parent.Children.Remove(key);
                    child.Dispose();
                }
            }
            finally { parent.ChildrenLock.ExitWriteLock(); }
        }
    }

    private bool TryGetChild(TopicNode node, ReadOnlySpan<byte> part, out TopicNode? child)
    {
        var lookup = node.Children.GetAlternateLookup<ReadOnlySpan<byte>>();
        node.ChildrenLock.EnterReadLock();
        try
        {
            return lookup.TryGetValue(part, out child);
        }
        finally { node.ChildrenLock.ExitReadLock(); }
    }

    public bool Publish(string subject, System.Buffers.ReadOnlySequence<byte> payload, string? replyTo = null, BrokerConnection? source = null)
    {
        return PublishWithTTL(subject, payload, replyTo, null, source);
    }

    public bool PublishWithTTL(string subject, System.Buffers.ReadOnlySequence<byte> payload, string? replyTo = null, TimeSpan? ttl = null, BrokerConnection? source = null)
    {
        // Fallback for string-based calls
        Span<byte> subjectBytes = stackalloc byte[subject.Length * 3]; // UTF8 worst case
        int len = System.Text.Encoding.UTF8.GetBytes(subject, subjectBytes);
        var subSpan = subjectBytes.Slice(0, len);
        return MatchAndPublish(_root, subSpan, subSpan, subject, payload, replyTo, ttl, source);
    }

    public bool PublishWithTTL(ReadOnlySpan<byte> subject, System.Buffers.ReadOnlySequence<byte> payload, string? replyTo = null, TimeSpan? ttl = null, BrokerConnection? source = null)
    {
        // Primary zero-allocation path - pass full subject span for correct delivery
        return MatchAndPublish(_root, subject, subject, null, payload, replyTo, ttl, source);
    }

    private bool MatchAndPublish(TopicNode node, ReadOnlySpan<byte> fullSubject, ReadOnlySpan<byte> remaining, string? originalSubject, System.Buffers.ReadOnlySequence<byte> payload, string? replyTo = null, TimeSpan? ttl = null, BrokerConnection? source = null)
    {
        // Collect matching child node references under the read lock, then deliver outside it.
        // Holding the lock during Send calls (which can block on backpressure) would stall all
        // concurrent subscribe/unsubscribe and publish operations at this tree level.
        TopicNode? chevronNode = null;
        TopicNode? literalNode = null;
        TopicNode? starNode = null;
        TopicNode? nextLiteral = null;
        TopicNode? nextStar = null;
        bool isLeafLevel;
        ReadOnlySpan<byte> nextRemaining = default;

        var lookup = node.Children.GetAlternateLookup<ReadOnlySpan<byte>>();
        node.ChildrenLock.EnterReadLock();
        try
        {
            lookup.TryGetValue(WildcardChevron, out chevronNode);

            int dotIdx = remaining.IndexOf((byte)'.');
            isLeafLevel = dotIdx == -1;
            if (isLeafLevel)
            {
                lookup.TryGetValue(remaining, out literalNode);
                lookup.TryGetValue(WildcardStar, out starNode);
            }
            else
            {
                var part = remaining.Slice(0, dotIdx);
                nextRemaining = remaining.Slice(dotIdx + 1);
                lookup.TryGetValue(part, out nextLiteral);
                lookup.TryGetValue(WildcardStar, out nextStar);
            }
        }
        finally { node.ChildrenLock.ExitReadLock(); }

        // Deliver / recurse outside the lock.
        bool accepted = true;

        if (chevronNode != null && !DeliverToNode(chevronNode, originalSubject, fullSubject, payload, replyTo, ttl, source))
            accepted = false;

        if (isLeafLevel)
        {
            if (literalNode != null && !DeliverToNode(literalNode, originalSubject, fullSubject, payload, replyTo, ttl, source))
                accepted = false;
            if (starNode != null && !DeliverToNode(starNode, originalSubject, fullSubject, payload, replyTo, ttl, source))
                accepted = false;
            return accepted;
        }

        if (nextLiteral != null && !MatchAndPublish(nextLiteral, fullSubject, nextRemaining, originalSubject, payload, replyTo, ttl, source))
            accepted = false;
        if (nextStar != null && !MatchAndPublish(nextStar, fullSubject, nextRemaining, originalSubject, payload, replyTo, ttl, source))
            accepted = false;

        return accepted;
    }

    private bool DeliverToNode(TopicNode node, string? originalSubject, ReadOnlySpan<byte> fullSubject, System.Buffers.ReadOnlySequence<byte> payload, string? replyTo, TimeSpan? ttl, BrokerConnection? source)
    {
        bool accepted = true;
        string? resolvedSubject = originalSubject;

        if (node.Subscribers.Count > 0)
        {
            foreach (var sub in node.Subscribers)
            {
                foreach (var conn in sub.Value.Keys)
                {
                    if (conn == source && source?.NoEcho == true) continue;
                    if (source != null && (source.IsRoute || source.IsLeaf) && (conn.IsRoute || conn.IsLeaf)) continue;

                    bool res;
                    if (resolvedSubject != null)
                        res = conn.SendMessageWithTTL(resolvedSubject, sub.Key, payload, replyTo, ttl);
                    else
                        res = conn.SendMessageWithTTL(fullSubject, sub.Key, payload, replyTo, ttl);

                    if (!res) accepted = false;
                }
            }
        }

        if (node.QueueGroups.Count > 0)
        {
            foreach (var groupEntry in node.QueueGroups)
            {
                var group = groupEntry.Value;
                if (group.IsEmpty) continue;

                // Round-robin selection without renting arrays
                int totalCount = 0;
                foreach (var sidEntry in group) totalCount += sidEntry.Value.Count;
                if (totalCount == 0) continue;

                int cursor = node.QueueGroupCursors.AddOrUpdate(groupEntry.Key, 0, (k, v) => (v + 1) % 1000000);
                int target = cursor % totalCount;

                (string Sid, BrokerConnection Conn)? selected = null;
                bool found = false;
                int current = 0;

                foreach (var sidEntry in group)
                {
                    foreach (var conn in sidEntry.Value.Keys)
                    {
                        if (current == target)
                        {
                            // Validate constraints
                            if ((conn == source && source?.NoEcho == true) ||
                                (source != null && (source.IsRoute || source.IsLeaf) && (conn.IsRoute || conn.IsLeaf)))
                            {
                                // If skipped, just take the next available one in the same pass or fallback
                                target = (target + 1) % totalCount;
                            }
                            else
                            {
                                selected = (sidEntry.Key, conn);
                                found = true;
                                break;
                            }
                        }
                        current++;
                    }
                    if (found) break;
                }

                // Simple fallback if the specific target was excluded
                if (!found)
                {
                    foreach (var sidEntry in group)
                    {
                        foreach (var conn in sidEntry.Value.Keys)
                        {
                            if (conn == source && source?.NoEcho == true) continue;
                            if (source != null && (source.IsRoute || source.IsLeaf) && (conn.IsRoute || conn.IsLeaf)) continue;
                            selected = (sidEntry.Key, conn);
                            found = true;
                            break;
                        }
                        if (found) break;
                    }
                }

                if (selected != null)
                {
                    bool res;
                    var pick = selected.Value;
                    if (resolvedSubject != null)
                        res = pick.Conn.SendMessageWithTTL(resolvedSubject, pick.Sid, payload, replyTo, ttl);
                    else
                        res = pick.Conn.SendMessageWithTTL(fullSubject, pick.Sid, payload, replyTo, ttl);

                    if (!res) accepted = false;
                }
            }
        }
        return accepted;
    }

    public bool HasSubscribers(string subject)
    {
        var current = _root;
        int start = 0;
        int dotIdx;

        Span<byte> subjectBuf = stackalloc byte[subject.Length * 3];
        int subjectLen = System.Text.Encoding.UTF8.GetBytes(subject, subjectBuf);
        ReadOnlySpan<byte> subjectBytes = subjectBuf.Slice(0, subjectLen);

        while ((dotIdx = subjectBytes.Slice(start).IndexOf((byte)'.')) != -1)
        {
            var part = subjectBytes.Slice(start, dotIdx);
            if (!TryGetChild(current, part, out current!)) return false;
            start += dotIdx + 1;
        }
        if (!TryGetChild(current, subjectBytes.Slice(start), out current!)) return false;

        return current.Subscribers.Count > 0 || current.QueueGroups.Count > 0;
    }
}
