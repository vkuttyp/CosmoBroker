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
    // --- Configuration -------------------------------------------------------

    public string Vhost { get; }
    public string Name { get; }
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

    // --- State ---------------------------------------------------------------

    // For priority queues we use a sorted structure; otherwise a simple queue.
    private readonly SortedSet<RabbitMessage>? _priorityQueue;
    private readonly Queue<RabbitMessage>? _queue;
    private readonly Queue<RabbitMessage> _requeued = new();
    private readonly object _dequeueLock = new();
    private int _retryScheduled;
    private int _drainActive;

    // delivery-tag → unacked messages per consumer
    public ConcurrentDictionary<ulong, (RabbitMessage Msg, string ConsumerTag)> Unacked { get; } = new();
    private readonly ConcurrentDictionary<string, int> _inFlightByConsumer = new();

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
        Exclusive = args.Exclusive;
        AutoDelete = args.AutoDelete;
        MaxPriority = args.MaxPriority;
        DeadLetterExchange = args.DeadLetterExchange;
        DeadLetterRoutingKey = args.DeadLetterRoutingKey;
        MessageTtlMs = args.MessageTtlMs;
        QueueTtlMs = args.QueueTtlMs;

        if (MaxPriority > 0)
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
                Properties = message.Properties
            };
        }

        lock (_dequeueLock)
        {
            if (_priorityQueue != null) _priorityQueue.Add(msg);
            else _queue!.Enqueue(msg);
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
                return _requeued.Count + (_priorityQueue?.Count ?? 0) + (_queue?.Count ?? 0);
        }
    }

    public long NextPublishSeq() => Interlocked.Increment(ref _publishSeq);

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
            lock (_dequeueLock)
            {
                msg.Redelivered = true;
                RequeueCore(msg);
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

        if (requeue.Count > 0)
            ScheduleRetry();

        return registration;
    }

    // --- Consumer Dispatch ---------------------------------------------------

    private void DispatchToConsumers()
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

        if (!Consumers.IsEmpty && HasQueuedMessages())
            ScheduleRetry();
    }

    // Trigger dispatch externally (e.g. after ack frees a prefetch slot)
    public void DrainToConsumers() => DispatchToConsumers();

    // Purge expired messages periodically
    public int PurgeExpired(Action<RabbitMessage>? onExpired = null)
    {
        int purged = 0;
        lock (_dequeueLock)
        {
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

        _ = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(1).ConfigureAwait(false);
            }
            finally
            {
                Interlocked.Exchange(ref _retryScheduled, 0);
            }

            DrainToConsumers();
        });
    }

    private bool HasQueuedMessages()
    {
        lock (_dequeueLock)
            return _requeued.Count > 0 || (_priorityQueue?.Count ?? 0) > 0 || (_queue?.Count ?? 0) > 0;
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
}

/// <summary>Arguments bag for queue declaration.</summary>
public sealed class RabbitQueueArgs
{
    public bool Durable { get; set; }
    public bool Exclusive { get; set; }
    public bool AutoDelete { get; set; }
    public byte MaxPriority { get; set; }
    public string? DeadLetterExchange { get; set; }
    public string? DeadLetterRoutingKey { get; set; }
    public int? MessageTtlMs { get; set; }
    public int? QueueTtlMs { get; set; }
}
