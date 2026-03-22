using System;
using System.Collections.Generic;

namespace CosmoBroker;

public sealed class Sublist
{
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

    public sealed class QueueGroup
    {
        public string Name { get; init; } = string.Empty;
        public readonly List<SubEntry> Members = new();
        private int _cursor = -1;
        public int NextIndex() => System.Threading.Interlocked.Increment(ref _cursor);
    }

    private sealed class Node
    {
        public readonly Dictionary<byte[], Node> Nodes = new(new ReadOnlySpanByteComparer());
        public readonly List<SubEntry> Psubs = new();
        public readonly Dictionary<string, QueueGroup> Qsubs = new(StringComparer.Ordinal);
        public Node? Star;
        public Node? Chevron;
    }

    public sealed record SubEntry(BrokerConnection Conn, string Sid, string? Queue);

    public sealed class SublistResult
    {
        public List<SubEntry> Psubs { get; } = new();
        public List<QueueGroup> Qsubs { get; } = new();
        public long Version { get; set; }
        public bool IsEmpty => Psubs.Count == 0 && Qsubs.Count == 0;
    }

    private readonly Node _root = new();
    private readonly System.Collections.Concurrent.ConcurrentDictionary<byte[], SublistResult> _cache = new(new ReadOnlySpanByteComparer());
    private readonly Dictionary<byte[], Node> _literal = new(new ReadOnlySpanByteComparer());
    private readonly Dictionary<string, int> _wildcardSubjects = new(StringComparer.Ordinal);
    private readonly System.Threading.ReaderWriterLockSlim _mu = new();
    private static long _globalVersion = 0;
    private long _version;
    public long Version => System.Threading.Volatile.Read(ref _version);

    public Sublist()
    {
        // Start each instance at a unique version so ThreadLocal MatchCaches
        // from previous instances (e.g. in tests) never produce stale hits.
        _version = System.Threading.Interlocked.Add(ref _globalVersion, 1024);
    }
    private int _wildcardCount = 0;

    private const int CacheMax = 4096;
    private static readonly byte[] WildcardStar = [(byte)'*'];
    private static readonly byte[] WildcardChevron = [(byte)'>'];

    public bool HasWildcards => Volatile.Read(ref _wildcardCount) > 0;

    public void Add(string subject, BrokerConnection conn, string sid, string? queueGroup)
    {
        bool hasWildcard = HasWildcard(subject);
        var subjectBytes = System.Text.Encoding.UTF8.GetBytes(subject);
        
        _mu.EnterWriteLock();
        try
        {
            var node = _root;
            int start = 0;
            int dotIdx;
            while ((dotIdx = subjectBytes.AsSpan(start).IndexOf((byte)'.')) != -1)
            {
                var tok = subjectBytes.AsSpan(start, dotIdx);
                node = GetOrAddNode(node, tok);
                start += dotIdx + 1;
            }
            node = GetOrAddNode(node, subjectBytes.AsSpan(start));

            var entry = new SubEntry(conn, sid, queueGroup);
            if (string.IsNullOrEmpty(queueGroup))
            {
                node.Psubs.Add(entry);
            }
            else
            {
                if (!node.Qsubs.TryGetValue(queueGroup, out var group))
                {
                    group = new QueueGroup { Name = queueGroup };
                    node.Qsubs[queueGroup] = group;
                }
                group.Members.Add(entry);
            }

            if (hasWildcard)
            {
                if (_wildcardSubjects.TryGetValue(subject, out var c)) _wildcardSubjects[subject] = c + 1;
                else
                {
                    _wildcardSubjects[subject] = 1;
                    Interlocked.Increment(ref _wildcardCount);
                }
            }
            else
            {
                _literal[subjectBytes] = node;
            }

            System.Threading.Interlocked.Increment(ref _version);
        }
        finally { _mu.ExitWriteLock(); }
    }

    private Node GetOrAddNode(Node node, ReadOnlySpan<byte> tok)
    {
        if (tok.SequenceEqual(WildcardStar))
        {
            node.Star ??= new Node();
            return node.Star;
        }
        if (tok.SequenceEqual(WildcardChevron))
        {
            node.Chevron ??= new Node();
            return node.Chevron;
        }
        
        var lookup = node.Nodes.GetAlternateLookup<ReadOnlySpan<byte>>();
        if (!lookup.TryGetValue(tok, out var next))
        {
            next = new Node();
            node.Nodes.Add(tok.ToArray(), next);
        }
        return next;
    }

    public void Remove(string subject, BrokerConnection conn, string sid, string? queueGroup)
    {
        var subjectBytes = System.Text.Encoding.UTF8.GetBytes(subject);
        bool hasWildcard = HasWildcard(subject);
        
        _mu.EnterWriteLock();
        try
        {
            var node = _root;
            int start = 0;
            int dotIdx;
            while ((dotIdx = subjectBytes.AsSpan(start).IndexOf((byte)'.')) != -1)
            {
                var tok = subjectBytes.AsSpan(start, dotIdx);
                if (!TryGetNode(node, tok, out node!)) return;
                start += dotIdx + 1;
            }
            if (!TryGetNode(node, subjectBytes.AsSpan(start), out node!)) return;

            if (string.IsNullOrEmpty(queueGroup))
            {
                node.Psubs.RemoveAll(s => s.Conn == conn && s.Sid == sid);
            }
            else if (node.Qsubs.TryGetValue(queueGroup, out var group))
            {
                group.Members.RemoveAll(s => s.Conn == conn && s.Sid == sid);
                if (group.Members.Count == 0) node.Qsubs.Remove(queueGroup);
            }

            if (hasWildcard)
            {
                if (_wildcardSubjects.TryGetValue(subject, out var c))
                {
                    c--;
                    if (c <= 0)
                    {
                        _wildcardSubjects.Remove(subject);
                        Interlocked.Decrement(ref _wildcardCount);
                    }
                    else _wildcardSubjects[subject] = c;
                }
            }
            else
            {
                if (node.Psubs.Count == 0 && node.Qsubs.Count == 0)
                {
                    var lookup = _literal.GetAlternateLookup<ReadOnlySpan<byte>>();
                    lookup.Remove(subjectBytes);
                }
            }

            System.Threading.Interlocked.Increment(ref _version);
        }
        finally { _mu.ExitWriteLock(); }
    }

    private bool TryGetNode(Node node, ReadOnlySpan<byte> tok, out Node? next)
    {
        if (tok.SequenceEqual(WildcardStar))
        {
            next = node.Star;
            return next != null;
        }
        if (tok.SequenceEqual(WildcardChevron))
        {
            next = node.Chevron;
            return next != null;
        }
        return node.Nodes.GetAlternateLookup<ReadOnlySpan<byte>>().TryGetValue(tok, out next);
    }

    public bool TryMatchLiteral(ReadOnlySpan<byte> subject, out List<SubEntry> psubs, out Dictionary<string, QueueGroup> qsubs)
    {
        _mu.EnterReadLock();
        try
        {
            var lookup = _literal.GetAlternateLookup<ReadOnlySpan<byte>>();
            if (lookup.TryGetValue(subject, out var node))
            {
                psubs = node.Psubs;
                qsubs = node.Qsubs;
                return true;
            }
        }
        finally { _mu.ExitReadLock(); }
        
        psubs = null!;
        qsubs = null!;
        return false;
    }

    public SublistResult Match(ReadOnlySpan<byte> subject)
    {
        long currentVersion = System.Threading.Volatile.Read(ref _version);
        var lookup = _cache.GetAlternateLookup<ReadOnlySpan<byte>>();
        if (lookup.TryGetValue(subject, out var cached) && cached.Version == currentVersion)
            return cached;

        var result = new SublistResult { Version = currentVersion };
        
        _mu.EnterReadLock();
        try
        {
            // Re-check version under lock to be sure, although not strictly necessary for correctness
            currentVersion = System.Threading.Volatile.Read(ref _version);
            result.Version = currentVersion;
            MatchLevel(_root, subject, result);
        }
        finally { _mu.ExitReadLock(); }

        if (_cache.Count >= CacheMax) _cache.Clear();
        lookup.TryAdd(subject, result);
        return result;
    }

    private static void MatchLevel(Node node, ReadOnlySpan<byte> subject, SublistResult result)
    {
        if (node.Chevron != null)
        {
            Append(node.Chevron, result);
        }

        int dotIdx = subject.IndexOf((byte)'.');
        if (dotIdx == -1)
        {
            if (node.Nodes.GetAlternateLookup<ReadOnlySpan<byte>>().TryGetValue(subject, out var next))
                Append(next, result);
            if (node.Star != null)
                Append(node.Star, result);
            return;
        }

        var tok = subject.Slice(0, dotIdx);
        var rest = subject.Slice(dotIdx + 1);

        if (node.Nodes.GetAlternateLookup<ReadOnlySpan<byte>>().TryGetValue(tok, out var n))
            MatchLevel(n, rest, result);

        if (node.Star != null)
            MatchLevel(node.Star, rest, result);
    }

    private static void Append(Node node, SublistResult result)
    {
        if (node.Psubs.Count > 0) result.Psubs.AddRange(node.Psubs);
        if (node.Qsubs.Count > 0)
        {
            foreach (var group in node.Qsubs.Values)
                result.Qsubs.Add(group);
        }
    }

    private static bool HasWildcard(string subject)
    {
        return subject.Contains('*') || subject.Contains('>');
    }
}
