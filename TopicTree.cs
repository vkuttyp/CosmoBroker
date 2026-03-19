using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System;

namespace CosmoBroker;

public class TopicTree
{
    private class TopicNode
    {
        // Individual subscribers: SID -> Connection Set
        public ConcurrentDictionary<string, ConcurrentDictionary<BrokerConnection, byte>> Subscribers { get; } = new();
        
        // Queue Groups: GroupName -> { SID -> Connection Set }
        public ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<BrokerConnection, byte>>> QueueGroups { get; } = new();

        // Queue group cursors for round-robin selection.
        public ConcurrentDictionary<string, int> QueueGroupCursors { get; } = new();

        // Child nodes (e.g., "foo" -> "bar" for "foo.bar")
        public ConcurrentDictionary<string, TopicNode> Children { get; } = new();
    }

    private readonly TopicNode _root = new();

    public void Subscribe(string subject, BrokerConnection connection, string sid, string? queueGroup = null)
    {
        var parts = subject.Split('.');
        var current = _root;

        foreach (var part in parts)
        {
            current = current.Children.GetOrAdd(part, _ => new TopicNode());
        }

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

    public void Unsubscribe(string subject, BrokerConnection connection, string sid, string? queueGroup = null)
    {
        var parts = subject.Split('.');
        var current = _root;

        foreach (var part in parts)
        {
            if (!current.Children.TryGetValue(part, out current)) return;
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
    }

    public void Publish(string subject, System.Buffers.ReadOnlySequence<byte> payload, string? replyTo = null, BrokerConnection? source = null)
    {
        PublishWithTTL(subject, payload, replyTo, null, source);
    }

    public void PublishWithTTL(string subject, System.Buffers.ReadOnlySequence<byte> payload, string? replyTo = null, TimeSpan? ttl = null, BrokerConnection? source = null)
    {
        ReadOnlySpan<char> span = subject.AsSpan();
        MatchAndPublish(_root, span, subject, payload, replyTo, ttl, source);
    }

    private void MatchAndPublish(TopicNode node, ReadOnlySpan<char> remaining, string originalSubject, System.Buffers.ReadOnlySequence<byte> payload, string? replyTo = null, TimeSpan? ttl = null, BrokerConnection? source = null)
    {
        if (node.Children.TryGetValue(">", out var chevronNode))
        {
            DeliverToNode(chevronNode, originalSubject, payload, replyTo, ttl, source);
        }

        int dotIdx = remaining.IndexOf('.');
        if (dotIdx == -1)
        {
            string lastPart = remaining.ToString();
            if (node.Children.TryGetValue(lastPart, out var literalNode))
                DeliverToNode(literalNode, originalSubject, payload, replyTo, ttl, source);
            if (node.Children.TryGetValue("*", out var starNode))
                DeliverToNode(starNode, originalSubject, payload, replyTo, ttl, source);
            return;
        }

        string part = remaining.Slice(0, dotIdx).ToString();
        ReadOnlySpan<char> nextRemaining = remaining.Slice(dotIdx + 1);

        if (node.Children.TryGetValue(part, out var nextLiteral))
            MatchAndPublish(nextLiteral, nextRemaining, originalSubject, payload, replyTo, ttl, source);

        if (node.Children.TryGetValue("*", out var nextStar))
            MatchAndPublish(nextStar, nextRemaining, originalSubject, payload, replyTo, ttl, source);
    }

    private void DeliverToNode(TopicNode node, string originalSubject, System.Buffers.ReadOnlySequence<byte> payload, string? replyTo, TimeSpan? ttl, BrokerConnection? source)
    {
        foreach (var sub in node.Subscribers)
        {
            foreach (var conn in sub.Value.Keys)
            {
                if (conn == source && source?.NoEcho == true) continue;
                if (source != null && (source.IsRoute || source.IsLeaf) && (conn.IsRoute || conn.IsLeaf)) continue;
                conn.SendMessageWithTTL(originalSubject, sub.Key, payload, replyTo, ttl);
            }
        }

        foreach (var groupEntry in node.QueueGroups)
        {
            var group = groupEntry.Value;
            if (group.IsEmpty) continue;

            // Flatten to (sid, conn) list for proper queue-group selection.
            var entries = new List<(string Sid, BrokerConnection Conn)>();
            foreach (var sidEntry in group)
            {
                foreach (var conn in sidEntry.Value.Keys)
                {
                    entries.Add((sidEntry.Key, conn));
                }
            }
            if (entries.Count == 0) continue;

            int start = node.QueueGroupCursors.AddOrUpdate(
                groupEntry.Key,
                _ => 0,
                (_, v) => (v + 1) % entries.Count);
            (string Sid, BrokerConnection Conn)? selected = null;
            bool sourcePresent = false;

            for (int i = 0; i < entries.Count; i++)
            {
                var entry = entries[(start + i) % entries.Count];
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
                selected = entries.First(e => e.Conn == source);
            }

            if (selected != null)
            {
                var pick = selected.Value;
                pick.Conn.SendMessageWithTTL(originalSubject, pick.Sid, payload, replyTo, ttl);
            }
        }
    }

    public bool HasSubscribers(string subject)
    {
        var parts = subject.Split('.');
        var current = _root;
        foreach (var part in parts)
        {
            if (!current.Children.TryGetValue(part, out current)) return false;
        }
        return current.Subscribers.Count > 0 || current.QueueGroups.Count > 0;
    }
}
