using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System;
using System.Threading;

namespace CosmoBroker;

public class ReadOnlySpanByteComparer : IEqualityComparer<byte[]>, IAlternateEqualityComparer<ReadOnlySpan<byte>, byte[]>
{
    public bool Equals(byte[]? x, byte[]? y) => (x == null && y == null) || (x != null && y != null && x.AsSpan().SequenceEqual(y));
    public int GetHashCode(byte[] obj) => GetHashCode(obj.AsSpan());
    public byte[] Create(ReadOnlySpan<byte> alternate) => alternate.ToArray();
    public bool Equals(ReadOnlySpan<byte> alternate, byte[] other) => alternate.SequenceEqual(other);
    public int GetHashCode(ReadOnlySpan<byte> alternate)
    {
        var hash = new HashCode();
        hash.AddBytes(alternate);
        return hash.ToHashCode();
    }
}

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
        public Dictionary<byte[], TopicNode> Children { get; } = new(new ReadOnlySpanByteComparer());
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

        ReadOnlySpan<byte> subjectBytes = System.Text.Encoding.UTF8.GetBytes(subject);

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
        var current = _root;
        int start = 0;
        int dotIdx;
        ReadOnlySpan<byte> subjectBytes = System.Text.Encoding.UTF8.GetBytes(subject);

        while ((dotIdx = subjectBytes.Slice(start).IndexOf((byte)'.')) != -1)
        {
            var part = subjectBytes.Slice(start, dotIdx);
            if (!TryGetChild(current, part, out current!)) return;
            start += dotIdx + 1;
        }
        if (!TryGetChild(current, subjectBytes.Slice(start), out current!)) return;

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

    public void Publish(string subject, System.Buffers.ReadOnlySequence<byte> payload, string? replyTo = null, BrokerConnection? source = null)
    {
        PublishWithTTL(subject, payload, replyTo, null, source);
    }

    public void PublishWithTTL(string subject, System.Buffers.ReadOnlySequence<byte> payload, string? replyTo = null, TimeSpan? ttl = null, BrokerConnection? source = null)
    {
        // Fallback for string-based calls
        Span<byte> subjectBytes = stackalloc byte[subject.Length * 3]; // UTF8 worst case
        int len = System.Text.Encoding.UTF8.GetBytes(subject, subjectBytes);
        MatchAndPublish(_root, subjectBytes.Slice(0, len), subject, payload, replyTo, ttl, source);
    }

    public void Publish(ReadOnlySpan<byte> subject, System.Buffers.ReadOnlySequence<byte> payload, string? replyTo = null, BrokerConnection? source = null)
    {
        // Primary zero-allocation path
        MatchAndPublish(_root, subject, null, payload, replyTo, ttl: null, source);
    }

    private void MatchAndPublish(TopicNode node, ReadOnlySpan<byte> remaining, string? originalSubject, System.Buffers.ReadOnlySequence<byte> payload, string? replyTo = null, TimeSpan? ttl = null, BrokerConnection? source = null)
    {
        var lookup = node.Children.GetAlternateLookup<ReadOnlySpan<byte>>();
        node.ChildrenLock.EnterReadLock();
        try
        {
            if (lookup.TryGetValue(WildcardChevron, out var chevronNode))
            {
                DeliverToNode(chevronNode, originalSubject, remaining, payload, replyTo, ttl, source);
            }

            int dotIdx = remaining.IndexOf((byte)'.');
            if (dotIdx == -1)
            {
                if (lookup.TryGetValue(remaining, out var literalNode))
                    DeliverToNode(literalNode, originalSubject, remaining, payload, replyTo, ttl, source);
                if (lookup.TryGetValue(WildcardStar, out var starNode))
                    DeliverToNode(starNode, originalSubject, remaining, payload, replyTo, ttl, source);
                return;
            }

            var part = remaining.Slice(0, dotIdx);
            var nextRemaining = remaining.Slice(dotIdx + 1);

            if (lookup.TryGetValue(part, out var nextLiteral))
                MatchAndPublish(nextLiteral, nextRemaining, originalSubject, payload, replyTo, ttl, source);

            if (lookup.TryGetValue(WildcardStar, out var nextStar))
                MatchAndPublish(nextStar, nextRemaining, originalSubject, payload, replyTo, ttl, source);
        }
        finally { node.ChildrenLock.ExitReadLock(); }
    }

    private void DeliverToNode(TopicNode node, string? originalSubject, ReadOnlySpan<byte> remaining, System.Buffers.ReadOnlySequence<byte> payload, string? replyTo, TimeSpan? ttl, BrokerConnection? source)
    {
        string? resolvedSubject = originalSubject;

        foreach (var sub in node.Subscribers)
        {
            foreach (var conn in sub.Value.Keys)
            {
                if (conn == source && source?.NoEcho == true) continue;
                if (source != null && (source.IsRoute || source.IsLeaf) && (conn.IsRoute || conn.IsLeaf)) continue;
                
                if (resolvedSubject == null) resolvedSubject = System.Text.Encoding.UTF8.GetString(remaining);
                conn.SendMessageWithTTL(resolvedSubject, sub.Key, payload, replyTo, ttl);
            }
        }

        foreach (var groupEntry in node.QueueGroups)
        {
            var group = groupEntry.Value;
            if (group.IsEmpty) continue;

            int total = 0;
            foreach (var sidEntry in group) total += sidEntry.Value.Count;
            if (total == 0) continue;

            var entries = System.Buffers.ArrayPool<(string Sid, BrokerConnection Conn)>.Shared.Rent(total);
            try
            {
                int count = 0;
                foreach (var sidEntry in group)
                {
                    foreach (var conn in sidEntry.Value.Keys)
                    {
                        entries[count++] = (sidEntry.Key, conn);
                    }
                }

                if (count == 0) continue;

                int start = node.QueueGroupCursors.AddOrUpdate(
                    groupEntry.Key,
                    _ => 0,
                    (_, v) => (v + 1) % count);

                (string Sid, BrokerConnection Conn)? selected = null;
                bool sourcePresent = false;

                for (int i = 0; i < count; i++)
                {
                    var entry = entries[(start + i) % count];
                    if (entry.Conn == source)
                    {
                        sourcePresent = true;
                        if (source?.NoEcho == true) continue;
                    }
                    if (source != null && (source.IsRoute || source.IsLeaf) && (entry.Conn.IsRoute || entry.Conn.IsLeaf)) continue;
                    selected = entry;
                    break;
                }

                if (selected == null && sourcePresent && source?.NoEcho != true)
                {
                    for (int i = 0; i < count; i++)
                    {
                        if (entries[i].Conn == source)
                        {
                            selected = entries[i];
                            break;
                        }
                    }
                }

                if (selected != null)
                {
                    if (resolvedSubject == null) resolvedSubject = System.Text.Encoding.UTF8.GetString(remaining);
                    var pick = selected.Value;
                    pick.Conn.SendMessageWithTTL(resolvedSubject, pick.Sid, payload, replyTo, ttl);
                }
            }
            finally
            {
                System.Buffers.ArrayPool<(string Sid, BrokerConnection Conn)>.Shared.Return(entries);
            }
        }
    }

    public bool HasSubscribers(string subject)
    {
        var current = _root;
        int start = 0;
        int dotIdx;
        ReadOnlySpan<byte> subjectBytes = System.Text.Encoding.UTF8.GetBytes(subject);

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
