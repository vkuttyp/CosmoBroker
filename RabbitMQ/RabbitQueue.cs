using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;

namespace CosmoBroker.RabbitMQ;

/// <summary>
/// A named, optionally durable NATS-backed queue with RabbitMQ semantics.
/// Supports priority, TTL, DLX, prefetch, and ack/nack.
/// </summary>
public sealed class RabbitQueue
{
    private static readonly int InitialRetryDelayMs = ResolveRetryDelayMilliseconds("COSMOBROKER_RMQ_RETRY_DELAY_MS", 10);
    private static readonly int MaxRetryDelayMs = ResolveRetryDelayMilliseconds("COSMOBROKER_RMQ_RETRY_MAX_DELAY_MS", 250);

    // --- Configuration -------------------------------------------------------

    public string Vhost { get; }
    public string Name { get; }
    public RabbitQueueType Type { get; init; } = RabbitQueueType.Classic;
    public bool Durable { get; init; }
    public bool Exclusive { get; init; }
    public bool AutoDelete { get; init; }

    /// <summary>x-max-priority (0 = no priority, >0 enables priority queue)</summary>
    public byte MaxPriority { get; init; } = 0;

    /// <summary>x-dead-letter-exchange — exchange to route dead-lettered messages to.</summary>
    public string? DeadLetterExchange { get; init; }

    /// <summary>x-dead-letter-routing-key — overrides original routing key when dead-lettering.</summary>
    public string? DeadLetterRoutingKey { get; init; }

    /// <summary>x-message-ttl — per-message TTL in milliseconds (individual message TTL wins).</summary>
    public int? MessageTtlMs { get; init; }

    /// <summary>x-expires — queue TTL in milliseconds (queue auto-deletes when unused).</summary>
    public int? QueueTtlMs { get; init; }
    public long? StreamMaxLengthBytes { get; set; }
    public long? StreamMaxLengthMessages { get; set; }
    public long? StreamMaxAgeMs { get; set; }

    // --- State ---------------------------------------------------------------

    // For priority queues we use a sorted structure; otherwise a simple queue.
    private readonly SortedSet<RabbitMessage>? _priorityQueue;
    private readonly Queue<RabbitMessage>? _queue;
    private readonly List<RabbitMessage>? _stream;
    private readonly Queue<RabbitMessage> _requeued = new();
    private readonly object _dequeueLock = new();
    private int _retryScheduled;
    private int _drainActive;
    private int _retryDelayMs = InitialRetryDelayMs;
    private long _streamTailOffset;
    private long _streamBytes;

    // delivery-tag → unacked messages per consumer
    public ConcurrentDictionary<ulong, (RabbitMessage Msg, string ConsumerTag)> Unacked { get; } = new();
    private readonly ConcurrentDictionary<string, int> _inFlightByConsumer = new();
    private readonly ConcurrentDictionary<string, long> _nextOffsetByConsumer = new();

    // prefetch_count per consumer tag (0 = unlimited)
    public ConcurrentDictionary<string, int> PrefetchCount { get; } = new();

    // Push consumers currently subscribed: consumerTag → callback
    public ConcurrentDictionary<string, ConsumerRegistration> Consumers { get; } = new();

    // Publisher confirm callbacks: exchange publish sequence → TCS
    private long _publishSeq = 0;
    public ConcurrentDictionary<long, Action<bool>> ConfirmCallbacks { get; } = new();

    public DateTime LastUsed { get; private set; } = DateTime.UtcNow;
    public string? ExclusiveOwner { get; private set; }
    public string? ExclusiveConsumerTag { get; private set; }
    public Action<RabbitMessage>? RetentionDropHandler { get; set; }

    public readonly record struct ConsumerRegistration(
        Func<RabbitMessage, bool> Callback,
        bool AutoAck,
        Action<RabbitMessage>? AutoAckHandler,
        string? OwnerId,
        Action<string, string>? CancelHandler,
        bool ExclusiveConsumer);

    // --- Constructor ---------------------------------------------------------

    public RabbitQueue(string name)
        : this("/", name)
    {
    }

    public RabbitQueue(string vhost, string name)
    {
        Vhost = string.IsNullOrWhiteSpace(vhost) ? "/" : vhost;
        Name = name;
        _queue = new Queue<RabbitMessage>();
    }

    public RabbitQueue(string name, RabbitQueueArgs args)
        : this("/", name, args)
    {
    }

    public RabbitQueue(string vhost, string name, RabbitQueueArgs args)
    {
        Vhost = string.IsNullOrWhiteSpace(vhost) ? "/" : vhost;
        Name = name;
        Durable = args.Durable;
        Type = args.Type;
        Exclusive = args.Exclusive;
        AutoDelete = args.AutoDelete;
        MaxPriority = args.MaxPriority;
        DeadLetterExchange = args.DeadLetterExchange;
        DeadLetterRoutingKey = args.DeadLetterRoutingKey;
        MessageTtlMs = args.MessageTtlMs;
        QueueTtlMs = args.QueueTtlMs;
        StreamMaxLengthBytes = args.StreamMaxLengthBytes;
        StreamMaxLengthMessages = args.StreamMaxLengthMessages;
        StreamMaxAgeMs = args.StreamMaxAgeMs;

        if (Type == RabbitQueueType.Stream)
            _stream = new List<RabbitMessage>();
        else if (MaxPriority > 0)
            _priorityQueue = new SortedSet<RabbitMessage>(PriorityMessageComparer.Instance);
        else
            _queue = new Queue<RabbitMessage>();
    }

    // --- Enqueue / Dequeue ---------------------------------------------------

    public void Enqueue(RabbitMessage message)
        => EnqueueInternal(message, dispatch: true);

    public void Restore(RabbitMessage message)
        => EnqueueInternal(message, dispatch: false);

    private void EnqueueInternal(RabbitMessage message, bool dispatch)
    {
        // Apply queue-level message TTL if message has none.
        RabbitMessage msg = message;
        if (MessageTtlMs.HasValue && !msg.ExpiresAt.HasValue)
        {
            // Create a copy with TTL set (record not supported, use new message)
            msg = new RabbitMessage
            {
                RoutingKey = message.RoutingKey,
                Payload = message.Payload,
                Headers = message.Headers,
                Priority = message.Priority,
                ExpiresAt = DateTime.UtcNow.AddMilliseconds(MessageTtlMs.Value),
                DeathCount = message.DeathCount,
                Redelivered = message.Redelivered,
                OriginalExchange = message.OriginalExchange,
                OriginalRoutingKey = message.OriginalRoutingKey,
                PersistedId = message.PersistedId,
                StreamOffset = message.StreamOffset,
                CreatedAt = message.CreatedAt,
                Properties = message.Properties
            };
        }

        List<RabbitMessage>? trimmed = null;
        lock (_dequeueLock)
        {
            if (_stream != null)
            {
                if (msg.StreamOffset <= 0)
                    msg.StreamOffset = message.PersistedId > 0 ? message.PersistedId : Interlocked.Increment(ref _streamTailOffset);
                _streamTailOffset = Math.Max(_streamTailOffset, msg.StreamOffset);
                _stream.Add(msg);
                _streamBytes += msg.Payload.LongLength;
                trimmed = TrimStreamRetentionLocked();
            }
            else if (_priorityQueue != null) _priorityQueue.Add(msg);
            else _queue!.Enqueue(msg);
        }

        if (trimmed != null)
        {
            foreach (var trimmedMessage in trimmed)
                RetentionDropHandler?.Invoke(trimmedMessage);
        }

        // Dispatch immediately to push consumers respecting prefetch.
        if (dispatch)
            DispatchToConsumers();
        LastUsed = DateTime.UtcNow;

        // Fire publisher confirm (best-effort, synchronous)
        var seq = Interlocked.Increment(ref _publishSeq);
        if (ConfirmCallbacks.TryRemove(seq, out var cb))
            cb(true);
    }

    public bool TryDequeue(out RabbitMessage? message)
    {
        if (_stream != null)
        {
            message = null;
            return false;
        }

        lock (_dequeueLock)
        {
            if (_requeued.Count > 0)
            {
                message = _requeued.Dequeue();
                LastUsed = DateTime.UtcNow;
                return !message.IsExpired || TryDequeue(out message);
            }
            if (_priorityQueue != null && _priorityQueue.Count > 0)
            {
                var first = _priorityQueue.Min; // comparer puts highest-priority first = Min in sorted order
                if (first == null)
                {
                    message = null;
                    return false;
                }
                _priorityQueue.Remove(first!);
                message = first;
                LastUsed = DateTime.UtcNow;
                return !message.IsExpired || TryDequeue(out message);
            }
            if (_queue != null && _queue.Count > 0)
            {
                message = _queue.Dequeue();
                LastUsed = DateTime.UtcNow;
                return !message.IsExpired || TryDequeue(out message);
            }
        }
        message = null;
        return false;
    }

    /// <summary>Requeue a nacked message at the head of the queue (prepend hack via priority).</summary>
    public void Requeue(RabbitMessage message)
    {
        lock (_dequeueLock)
        {
            message.Redelivered = true;
            RequeueCore(message);
        }
        DispatchToConsumers();
    }

    public int Count
    {
        get
        {
            lock (_dequeueLock)
                return (_stream?.Count ?? 0) + _requeued.Count + (_priorityQueue?.Count ?? 0) + (_queue?.Count ?? 0);
        }
    }

    public long NextPublishSeq() => Interlocked.Increment(ref _publishSeq);
    public long StreamBytes
    {
        get
        {
            lock (_dequeueLock)
                return _streamBytes;
        }
    }

    public long StreamHeadOffset
    {
        get
        {
            lock (_dequeueLock)
                return _stream is { Count: > 0 } ? _stream[0].StreamOffset : _streamTailOffset + 1;
        }
    }

    public long StreamTailOffset
    {
        get
        {
            lock (_dequeueLock)
                return _streamTailOffset;
        }
    }

    public bool TryClaimExclusiveOwner(string? ownerId, out string? error)
    {
        error = null;
        if (!Exclusive)
            return true;

        string resolvedOwner = ownerId ?? string.Empty;
        lock (_dequeueLock)
        {
            if (ExclusiveOwner == null)
            {
                ExclusiveOwner = resolvedOwner;
                return true;
            }

            if (!string.Equals(ExclusiveOwner, resolvedOwner, StringComparison.Ordinal))
            {
                error = $"Queue '{Name}' is exclusive and owned by another connection.";
                return false;
            }
        }

        return true;
    }

    public bool IsAccessibleBy(string? ownerId)
    {
        if (!Exclusive)
            return true;

        lock (_dequeueLock)
            return ExclusiveOwner == null || string.Equals(ExclusiveOwner, ownerId ?? string.Empty, StringComparison.Ordinal);
    }

    public bool TryRegisterConsumer(string consumerTag, ConsumerRegistration registration, int prefetchCount, out string? error)
    {
        error = null;

        if (Exclusive)
        {
            if (!TryClaimExclusiveOwner(registration.OwnerId ?? consumerTag, out error))
                return false;
        }

        lock (_dequeueLock)
        {
            if (registration.ExclusiveConsumer)
            {
                if (!Consumers.IsEmpty)
                {
                    error = $"Queue '{Name}' already has active consumers.";
                    return false;
                }

                ExclusiveConsumerTag = consumerTag;
            }
            else if (ExclusiveConsumerTag != null)
            {
                error = $"Queue '{Name}' already has an exclusive consumer.";
                return false;
            }
        }

        if (prefetchCount > 0)
            PrefetchCount[consumerTag] = prefetchCount;
        else
            PrefetchCount.TryRemove(consumerTag, out _);

        Consumers[consumerTag] = registration;
        _inFlightByConsumer.TryAdd(consumerTag, 0);
        if (_stream != null)
            _nextOffsetByConsumer.TryAdd(consumerTag, _streamTailOffset + 1);
        LastUsed = DateTime.UtcNow;
        return true;
    }

    public ConsumerRegistration? RemoveConsumer(string consumerTag)
    {
        if (!Consumers.TryRemove(consumerTag, out var registration))
            return null;
        PrefetchCount.TryRemove(consumerTag, out _);
        _inFlightByConsumer.TryRemove(consumerTag, out _);
        LastUsed = DateTime.UtcNow;

        var requeue = Unacked
            .Where(x => x.Value.ConsumerTag == consumerTag)
            .Select(x => (x.Key, x.Value.Msg))
            .ToList();
        foreach (var (tag, msg) in requeue)
        {
            TryRemoveUnacked(tag, out _);
            if (_stream != null)
            {
                msg.Redelivered = true;
                _nextOffsetByConsumer.AddOrUpdate(consumerTag, _ => msg.StreamOffset, (_, current) => Math.Min(current, msg.StreamOffset));
            }
            else
            {
                lock (_dequeueLock)
                {
                    msg.Redelivered = true;
                    RequeueCore(msg);
                }
            }
        }

        if (Exclusive)
        {
            lock (_dequeueLock)
            {
                if (Consumers.IsEmpty)
                    ExclusiveOwner = null;
            }
        }

        lock (_dequeueLock)
        {
            if (string.Equals(ExclusiveConsumerTag, consumerTag, StringComparison.Ordinal))
                ExclusiveConsumerTag = null;
        }

        _nextOffsetByConsumer.TryRemove(consumerTag, out _);

        if (_stream == null && requeue.Count > 0)
            ScheduleRetry();

        return registration;
    }

    // --- Consumer Dispatch ---------------------------------------------------

    private void DispatchToConsumers()
    {
        if (_stream != null)
        {
            DispatchStreamToConsumers();
            return;
        }

        if (Interlocked.CompareExchange(ref _drainActive, 1, 0) != 0)
            return;

        try
        {
            while (true)
            {
                bool deliveredAny = false;

                foreach (var (tag, registration) in Consumers)
                {
                    int limit = registration.AutoAck ? 0 : PrefetchCount.GetValueOrDefault(tag, 0);
                    // Drain until queue empty or prefetch limit reached
                    while (true)
                    {
                        int inFlight = registration.AutoAck ? 0 : GetInFlightCount(tag);
                        if (limit > 0 && inFlight >= limit) break;

                        if (!TryDequeue(out var msg) || msg == null) break;

                        bool tracked = false;
                        if (!registration.AutoAck)
                        {
                            TrackUnacked(msg, tag);
                            tracked = true;
                        }

                        bool accepted;
                        try
                        {
                            accepted = registration.Callback(msg);
                        }
                        catch
                        {
                            accepted = false;
                        }

                        if (!accepted)
                        {
                            if (tracked)
                                TryRemoveUnacked(msg.DeliveryTag, out _);

                            lock (_dequeueLock)
                            {
                                RequeueCore(msg);
                            }
                            ScheduleRetry();
                            break;
                        }

                        deliveredAny = true;
                        ResetRetryBackoff();

                        if (registration.AutoAck)
                        {
                            try
                            {
                                registration.AutoAckHandler?.Invoke(msg);
                            }
                            catch
                            {
                                lock (_dequeueLock)
                                {
                                    RequeueCore(msg);
                                }
                                ScheduleRetry();
                                break;
                            }
                        }
                    }
                }

                if (!deliveredAny)
                    break;
            }
        }
        finally
        {
            Interlocked.Exchange(ref _drainActive, 0);
        }

        if (!HasQueuedMessages())
            ResetRetryBackoff();

        if (!Consumers.IsEmpty && HasQueuedMessages())
            ScheduleRetry();
    }

    private void DispatchStreamToConsumers()
    {
        if (Interlocked.CompareExchange(ref _drainActive, 1, 0) != 0)
            return;

        try
        {
            while (true)
            {
                bool deliveredAny = false;

                foreach (var (tag, registration) in Consumers)
                {
                    int limit = registration.AutoAck ? 0 : PrefetchCount.GetValueOrDefault(tag, 0);
                    while (true)
                    {
                        int inFlight = registration.AutoAck ? 0 : GetInFlightCount(tag);
                        if (limit > 0 && inFlight >= limit)
                            break;

                        if (!TryPeekNextStreamMessage(tag, out var msg) || msg == null)
                            break;

                        bool tracked = false;
                        if (!registration.AutoAck)
                        {
                            TrackUnacked(msg, tag);
                            tracked = true;
                        }

                        bool accepted;
                        try
                        {
                            accepted = registration.Callback(msg);
                        }
                        catch
                        {
                            accepted = false;
                        }

                        if (!accepted)
                        {
                            if (tracked)
                                TryRemoveUnacked(msg.DeliveryTag, out _);

                            msg.Redelivered = true;
                            _nextOffsetByConsumer[tag] = msg.StreamOffset;
                            ScheduleRetry();
                            break;
                        }

                        _nextOffsetByConsumer[tag] = msg.StreamOffset + 1;
                        deliveredAny = true;
                        ResetRetryBackoff();
                    }
                }

                if (!deliveredAny)
                    break;
            }
        }
        finally
        {
            Interlocked.Exchange(ref _drainActive, 0);
        }

        if (!HasQueuedMessages())
            ResetRetryBackoff();
    }

    // Trigger dispatch externally (e.g. after ack frees a prefetch slot)
    public void DrainToConsumers() => DispatchToConsumers();

    // Purge expired messages periodically
    public int PurgeExpired(Action<RabbitMessage>? onExpired = null)
    {
        int purged = 0;
        lock (_dequeueLock)
        {
            if (_stream != null)
            {
                for (int i = _stream.Count - 1; i >= 0; i--)
                {
                    var msg = _stream[i];
                    if (!msg.IsExpired)
                        continue;

                    _stream.RemoveAt(i);
                    _streamBytes -= msg.Payload.LongLength;
                    purged++;
                    onExpired?.Invoke(msg);
                }

                return purged;
            }

            int requeuedCount = _requeued.Count;
            for (int i = 0; i < requeuedCount; i++)
            {
                var msg = _requeued.Dequeue();
                if (msg.IsExpired) { purged++; onExpired?.Invoke(msg); }
                else _requeued.Enqueue(msg);
            }
            if (_queue != null)
            {
                int count = _queue.Count;
                for (int i = 0; i < count; i++)
                {
                    var msg = _queue.Dequeue();
                    if (msg.IsExpired) { purged++; onExpired?.Invoke(msg); }
                    else _queue.Enqueue(msg);
                }
            }
            if (_priorityQueue != null)
            {
                var expired = new List<RabbitMessage>();
                foreach (var msg in _priorityQueue)
                    if (msg.IsExpired) { expired.Add(msg); onExpired?.Invoke(msg); }
                foreach (var msg in expired) { _priorityQueue.Remove(msg!); purged++; }
            }
        }
        return purged;
    }

    public int Purge(Func<RabbitMessage, bool>? onPurged = null)
    {
        int purged = 0;
        lock (_dequeueLock)
        {
            if (_stream != null)
            {
                foreach (var msg in _stream)
                {
                    if (onPurged?.Invoke(msg) ?? true) { }
                    purged++;
                }

                _stream.Clear();
                _streamBytes = 0;
                return purged;
            }

            while (_requeued.Count > 0)
            {
                var msg = _requeued.Dequeue();
                if (onPurged?.Invoke(msg) ?? true) { }
                purged++;
            }

            if (_queue != null)
            {
                while (_queue.Count > 0)
                {
                    var msg = _queue.Dequeue();
                    if (onPurged?.Invoke(msg) ?? true) { }
                    purged++;
                }
            }

            if (_priorityQueue != null && _priorityQueue.Count > 0)
            {
                var remaining = _priorityQueue.ToArray();
                _priorityQueue.Clear();
                foreach (var msg in remaining)
                {
                    if (onPurged?.Invoke(msg) ?? true) { }
                    purged++;
                }
            }
        }

        LastUsed = DateTime.UtcNow;
        return purged;
    }

    private void RequeueCore(RabbitMessage message)
    {
        if (_priorityQueue != null)
        {
            _priorityQueue.Add(message);
            return;
        }

        _requeued.Enqueue(message);
    }

    private void ScheduleRetry()
    {
        if (Interlocked.CompareExchange(ref _retryScheduled, 1, 0) != 0)
            return;

        var delayMs = GetAndIncreaseRetryDelay();

        _ = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(delayMs).ConfigureAwait(false);
            }
            finally
            {
                Interlocked.Exchange(ref _retryScheduled, 0);
            }

            DrainToConsumers();
        });
    }

    private static int ResolveRetryDelayMilliseconds(string envName, int fallback)
    {
        var raw = Environment.GetEnvironmentVariable(envName);
        return int.TryParse(raw, out var parsed) && parsed > 0 ? parsed : fallback;
    }

    private int GetAndIncreaseRetryDelay()
    {
        while (true)
        {
            var current = Volatile.Read(ref _retryDelayMs);
            var next = Math.Min(current * 2, MaxRetryDelayMs);
            if (Interlocked.CompareExchange(ref _retryDelayMs, next, current) == current)
                return current;
        }
    }

    private void ResetRetryBackoff()
    {
        Volatile.Write(ref _retryDelayMs, InitialRetryDelayMs);
    }

    private bool HasQueuedMessages()
    {
        lock (_dequeueLock)
            return (_stream?.Count ?? 0) > 0 || _requeued.Count > 0 || (_priorityQueue?.Count ?? 0) > 0 || (_queue?.Count ?? 0) > 0;
    }

    public void TrackUnacked(RabbitMessage msg, string consumerTag)
    {
        Unacked[msg.DeliveryTag] = (msg, consumerTag);
        _inFlightByConsumer.AddOrUpdate(consumerTag, 1, static (_, current) => current + 1);
    }

    public bool TryRemoveUnacked(ulong deliveryTag, out (RabbitMessage Msg, string ConsumerTag) entry)
    {
        if (!Unacked.TryRemove(deliveryTag, out entry))
            return false;

        _inFlightByConsumer.AddOrUpdate(
            entry.ConsumerTag,
            0,
            static (_, current) => current > 0 ? current - 1 : 0);
        return true;
    }

    public int GetInFlightCount(string consumerTag)
        => _inFlightByConsumer.TryGetValue(consumerTag, out var count) ? count : 0;

    public IReadOnlyDictionary<string, long> GetStreamOffsetsSnapshot()
    {
        if (_stream == null)
            return new Dictionary<string, long>();

        lock (_dequeueLock)
            return new Dictionary<string, long>(_nextOffsetByConsumer);
    }

    public IReadOnlyDictionary<string, long> GetStreamConsumerLagSnapshot()
    {
        if (_stream == null)
            return new Dictionary<string, long>();

        lock (_dequeueLock)
        {
            var snapshot = new Dictionary<string, long>(_nextOffsetByConsumer.Count, StringComparer.Ordinal);
            foreach (var (consumerTag, nextOffset) in _nextOffsetByConsumer)
            {
                long lag = _streamTailOffset >= nextOffset
                    ? (_streamTailOffset - nextOffset) + 1
                    : 0;
                snapshot[consumerTag] = lag;
            }

            return snapshot;
        }
    }

    public void SetStreamOffset(string consumerTag, RabbitStreamOffsetSpec? offset)
    {
        if (_stream == null)
            return;

        _nextOffsetByConsumer[consumerTag] = ResolveStreamOffset(offset);
    }

    public long ResolveRequestedStreamOffset(RabbitStreamOffsetSpec? offset)
    {
        if (_stream == null)
            return 0;

        return ResolveStreamOffset(offset);
    }

    public bool TryGetStreamMessage(string consumerTag, out RabbitMessage? message)
    {
        if (_stream == null)
        {
            message = null;
            return false;
        }

        if (!TryPeekNextStreamMessage(consumerTag, out message) || message == null)
            return false;

        _nextOffsetByConsumer[consumerTag] = message.StreamOffset + 1;
        LastUsed = DateTime.UtcNow;
        return true;
    }

    public void ResetStreamOffset(string consumerTag, long streamOffset)
    {
        if (_stream == null)
            return;

        _nextOffsetByConsumer.AddOrUpdate(consumerTag, _ => streamOffset, (_, current) => Math.Min(current, streamOffset));
    }

    private bool TryPeekNextStreamMessage(string consumerTag, out RabbitMessage? message)
    {
        message = null;
        if (_stream == null)
            return false;

        lock (_dequeueLock)
        {
            var nextOffset = _nextOffsetByConsumer.TryGetValue(consumerTag, out var currentOffset)
                ? currentOffset
                : _streamTailOffset + 1;

            for (int i = 0; i < _stream.Count; i++)
            {
                var candidate = _stream[i];
                if (candidate.StreamOffset < nextOffset)
                    continue;

                if (candidate.IsExpired)
                {
                    nextOffset = candidate.StreamOffset + 1;
                    _nextOffsetByConsumer[consumerTag] = nextOffset;
                    continue;
                }

                message = candidate;
                LastUsed = DateTime.UtcNow;
                return true;
            }
        }

        return false;
    }

    private long ResolveStreamOffset(RabbitStreamOffsetSpec? spec)
    {
        lock (_dequeueLock)
        {
            if (_stream == null)
                return 0;

            if (spec?.Kind == RabbitStreamOffsetKind.Offset && spec.Offset.HasValue)
                return spec.Offset.Value;

            if (spec?.Kind == RabbitStreamOffsetKind.Timestamp && spec.TimestampUtc.HasValue)
            {
                var ts = spec.TimestampUtc.Value.UtcDateTime;
                foreach (var msg in _stream)
                    if (msg.CreatedAt >= ts)
                        return msg.StreamOffset;
                return _streamTailOffset + 1;
            }

            if (_stream.Count == 0)
                return _streamTailOffset + 1;

            return spec?.Kind switch
            {
                RabbitStreamOffsetKind.First => _stream[0].StreamOffset,
                RabbitStreamOffsetKind.Last => _stream[^1].StreamOffset,
                RabbitStreamOffsetKind.Next => _streamTailOffset + 1,
                _ => _streamTailOffset + 1
            };
        }
    }

    private List<RabbitMessage>? TrimStreamRetentionLocked()
    {
        if (_stream == null || _stream.Count == 0)
            return null;

        List<RabbitMessage>? removed = null;
        while (_stream.Count > 0)
        {
            bool remove = false;
            var candidate = _stream[0];

            if (StreamMaxAgeMs.HasValue)
            {
                var ageMs = (DateTime.UtcNow - candidate.CreatedAt).TotalMilliseconds;
                if (ageMs > StreamMaxAgeMs.Value)
                    remove = true;
            }

            if (!remove && StreamMaxLengthMessages.HasValue && _stream.Count > StreamMaxLengthMessages.Value)
                remove = true;

            if (!remove && StreamMaxLengthBytes.HasValue && _streamBytes > StreamMaxLengthBytes.Value)
                remove = true;

            if (!remove)
                break;

            _stream.RemoveAt(0);
            _streamBytes -= candidate.Payload.LongLength;
            removed ??= new List<RabbitMessage>();
            removed.Add(candidate);
        }

        return removed;
    }

    // --- Comparers -----------------------------------------------------------

    private sealed class PriorityMessageComparer : IComparer<RabbitMessage>
    {
        public static readonly PriorityMessageComparer Instance = new();
        public int Compare(RabbitMessage? x, RabbitMessage? y)
        {
            if (x == null && y == null) return 0;
            if (x == null) return -1;
            if (y == null) return 1;
            // Higher priority first; older messages first within same priority
            int cmp = y.Priority.CompareTo(x.Priority);
            if (cmp != 0) return cmp;
            cmp = x.EnqueueTicks.CompareTo(y.EnqueueTicks);
            if (cmp != 0) return cmp;
            return x.DeliveryTag.CompareTo(y.DeliveryTag);
        }
    }

    public void UpdateStreamRetention(long? maxLengthMessages, long? maxLengthBytes, long? maxAgeMs)
    {
        StreamMaxLengthMessages = maxLengthMessages;
        StreamMaxLengthBytes = maxLengthBytes;
        StreamMaxAgeMs = maxAgeMs;

        if (Type != RabbitQueueType.Stream)
            return;

        List<RabbitMessage>? removed;
        lock (_dequeueLock)
        {
            removed = TrimStreamRetentionLocked();
        }

        if (removed == null || removed.Count == 0)
            return;

        foreach (var message in removed)
            RetentionDropHandler?.Invoke(message);
    }

    public RabbitQueueArgs ToArgs()
        => new()
        {
            Type = Type,
            Durable = Durable,
            Exclusive = Exclusive,
            AutoDelete = AutoDelete,
            MaxPriority = MaxPriority,
            DeadLetterExchange = DeadLetterExchange,
            DeadLetterRoutingKey = DeadLetterRoutingKey,
            MessageTtlMs = MessageTtlMs,
            QueueTtlMs = QueueTtlMs,
            StreamMaxLengthBytes = StreamMaxLengthBytes,
            StreamMaxLengthMessages = StreamMaxLengthMessages,
            StreamMaxAgeMs = StreamMaxAgeMs
        };
}

public enum RabbitQueueType
{
    Classic = 0,
    Stream = 1
}

public enum RabbitStreamOffsetKind
{
    Next = 0,
    First = 1,
    Last = 2,
    Offset = 3,
    Timestamp = 4
}

public sealed class RabbitStreamOffsetSpec
{
    public RabbitStreamOffsetKind Kind { get; set; } = RabbitStreamOffsetKind.Next;
    public long? Offset { get; set; }
    public DateTimeOffset? TimestampUtc { get; set; }
}

/// <summary>Arguments bag for queue declaration.</summary>
public sealed class RabbitQueueArgs
{
    public RabbitQueueType Type { get; set; } = RabbitQueueType.Classic;
    public bool Durable { get; set; }
    public bool Exclusive { get; set; }
    public bool AutoDelete { get; set; }
    public byte MaxPriority { get; set; }
    public string? DeadLetterExchange { get; set; }
    public string? DeadLetterRoutingKey { get; set; }
    public int? MessageTtlMs { get; set; }
    public int? QueueTtlMs { get; set; }
    public long? StreamMaxLengthBytes { get; set; }
    public long? StreamMaxLengthMessages { get; set; }
    public long? StreamMaxAgeMs { get; set; }
}
