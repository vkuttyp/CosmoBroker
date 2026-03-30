using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace CosmoBroker.RabbitMQ;

public enum ExchangeType { Direct, Fanout, Topic, Headers, SuperStream }

/// <summary>
/// Represents a RabbitMQ-style exchange. Routes incoming messages to bound queues
/// based on exchange type and routing key / header rules.
/// </summary>
public sealed class Exchange
{
    public string Vhost { get; }
    public string Name { get; }
    public ExchangeType Type { get; }
    public bool Durable { get; }
    public bool AutoDelete { get; }
    public int? SuperStreamPartitions { get; }

    // Binding table: routing key → set of bound queues (byte value unused).
    // ConcurrentDictionary<string,byte> gives atomic TryAdd/TryRemove, eliminating the
    // TOCTOU race that ConcurrentBag.Contains+Add had, and the rebuild-swap race in Unbind.
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, byte>> _bindings = new(StringComparer.OrdinalIgnoreCase);

    // Headers exchange: queue name → required header pairs
    private readonly ConcurrentDictionary<string, Dictionary<string, string>> _headerArgs = new();

    // Topic exchange: cache pattern.Split('.') result per pattern to avoid per-publish allocations.
    private readonly ConcurrentDictionary<string, string[]> _topicPatternCache = new(StringComparer.OrdinalIgnoreCase);

    public Exchange(string name, ExchangeType type, bool durable = true, bool autoDelete = false, int? superStreamPartitions = null)
        : this("/", name, type, durable, autoDelete, superStreamPartitions)
    {
    }

    public Exchange(string vhost, string name, ExchangeType type, bool durable = true, bool autoDelete = false, int? superStreamPartitions = null)
    {
        Vhost = string.IsNullOrWhiteSpace(vhost) ? "/" : vhost;
        Name = name;
        Type = type;
        Durable = durable;
        AutoDelete = autoDelete;
        SuperStreamPartitions = type == ExchangeType.SuperStream ? superStreamPartitions : null;
    }

    // --- Binding management --------------------------------------------------

    public void Bind(string queueName, string routingKey, Dictionary<string, string>? headerArgs = null)
    {
        string key = NormaliseKey(routingKey);
        var set = _bindings.GetOrAdd(key, _ => new ConcurrentDictionary<string, byte>(StringComparer.OrdinalIgnoreCase));
        // TryAdd is atomic — no TOCTOU between existence check and insert.
        set.TryAdd(queueName, 0);

        if (Type == ExchangeType.Headers && headerArgs != null)
            _headerArgs[queueName] = headerArgs;

        // Pre-cache the split pattern for topic exchanges so RouteTopic pays no per-publish alloc.
        if (Type == ExchangeType.Topic)
            _topicPatternCache.TryAdd(key, key.Split('.'));
    }

    public bool Unbind(string queueName, string routingKey)
    {
        string key = NormaliseKey(routingKey);
        bool removed = false;
        if (_bindings.TryGetValue(key, out var set))
        {
            // TryRemove is atomic — no rebuild-swap race between concurrent unbinds.
            removed = set.TryRemove(queueName, out _);
            if (set.IsEmpty)
            {
                _bindings.TryRemove(key, out _);
                _topicPatternCache.TryRemove(key, out _);
            }
        }
        _headerArgs.TryRemove(queueName, out _);
        return removed;
    }

    // --- Routing -------------------------------------------------------------

    /// <summary>Returns the names of queues that should receive the message.</summary>
    public IEnumerable<string> Route(string routingKey, Dictionary<string, string>? headers = null)
    {
        return Type switch
        {
            ExchangeType.Direct   => RouteDirect(routingKey),
            ExchangeType.Fanout   => RouteFanout(),
            ExchangeType.Topic    => RouteTopic(routingKey),
            ExchangeType.Headers  => RouteHeaders(headers),
            ExchangeType.SuperStream => RouteSuperStream(routingKey),
            _                     => Enumerable.Empty<string>()
        };
    }

    private IEnumerable<string> RouteDirect(string routingKey)
    {
        if (_bindings.TryGetValue(routingKey, out var set))
            foreach (var q in set.Keys) yield return q;
    }

    private IEnumerable<string> RouteFanout()
    {
        foreach (var set in _bindings.Values)
            foreach (var q in set.Keys)
                yield return q;
    }

    private IEnumerable<string> RouteTopic(string routingKey)
    {
        var rParts = routingKey.Split('.');
        foreach (var (pattern, set) in _bindings)
        {
            // Use cached split — avoids per-publish heap allocation of pattern parts.
            var pParts = _topicPatternCache.GetOrAdd(pattern, static p => p.Split('.'));
            if (MatchParts(pParts, 0, rParts, 0))
                foreach (var q in set.Keys)
                    yield return q;
        }
    }

    private IEnumerable<string> RouteHeaders(Dictionary<string, string>? headers)
    {
        if (headers == null) yield break;

        foreach (var (queueName, required) in _headerArgs)
        {
            bool xMatch = required.TryGetValue("x-match", out var matchMode) && matchMode == "any";
            bool matched = xMatch
                ? required.Any(kv => kv.Key != "x-match" && headers.TryGetValue(kv.Key, out var v) && v == kv.Value)
                : required.All(kv => kv.Key == "x-match" || (headers.TryGetValue(kv.Key, out var v) && v == kv.Value));
            if (matched) yield return queueName;
        }
    }

    private IEnumerable<string> RouteSuperStream(string routingKey)
    {
        var partition = ResolveSuperStreamPartition(routingKey);
        if (!string.IsNullOrWhiteSpace(partition))
            yield return partition;
    }

    // --- Topic pattern matching (RabbitMQ semantics: * = one word, # = zero or more) ---

    public static bool TopicMatches(string pattern, string routingKey)
    {
        var pParts = pattern.Split('.');
        var rParts = routingKey.Split('.');
        return MatchParts(pParts, 0, rParts, 0);
    }

    private static bool MatchParts(string[] p, int pi, string[] r, int ri)
    {
        while (pi < p.Length && ri < r.Length)
        {
            if (p[pi] == "#")
            {
                // # matches zero or more words
                for (int skip = 0; skip <= r.Length - ri; skip++)
                    if (MatchParts(p, pi + 1, r, ri + skip)) return true;
                return false;
            }
            if (p[pi] != "*" && !string.Equals(p[pi], r[ri], StringComparison.OrdinalIgnoreCase))
                return false;
            pi++; ri++;
        }
        // Consume trailing # wildcards
        while (pi < p.Length && p[pi] == "#") pi++;
        return pi == p.Length && ri == r.Length;
    }

    private string NormaliseKey(string key) => Type == ExchangeType.Fanout ? "" : key;

    private static int GetStableHash(string value)
    {
        unchecked
        {
            uint hash = 2166136261;
            foreach (char ch in value ?? string.Empty)
            {
                hash ^= ch;
                hash *= 16777619;
            }

            return (int)(hash & 0x7fffffff);
        }
    }

    public IReadOnlyDictionary<string, IReadOnlyList<string>> GetBindings()
    {
        var result = new Dictionary<string, IReadOnlyList<string>>();
        foreach (var (k, set) in _bindings)
            result[k] = set.Keys.ToList().AsReadOnly();
        return result;
    }

    public IReadOnlyList<string> GetSuperStreamPartitions()
    {
        if (Type != ExchangeType.SuperStream)
            return Array.Empty<string>();

        return _bindings.Values
            .SelectMany(static set => set.Keys)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(static x => x, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    public string? ResolveSuperStreamPartition(string routingKey)
    {
        if (Type != ExchangeType.SuperStream)
            return null;

        var partitions = GetSuperStreamPartitions();
        if (partitions.Count == 0)
            return null;

        int index = GetStableHash(routingKey) % partitions.Count;
        return partitions[index];
    }

    public bool HasBindings()
        => _bindings.Values.Any(set => !set.IsEmpty);
}
