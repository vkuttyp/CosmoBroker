using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CosmoBroker.Persistence;

namespace CosmoBroker.RabbitMQ;

/// <summary>
/// Central registry for RabbitMQ exchanges and queues.
/// Thread-safe; backed by ConcurrentDictionary for all hot paths.
/// </summary>
public sealed class ExchangeManager
{
    private const string DefaultVhost = "/";
    private const string SuperStreamPartitionKeyHeader = "x-super-stream-partition-key";

    private readonly MessageRepository? _repo;
    private readonly ConcurrentDictionary<string, ulong> _streamPublisherSequences = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, string> _streamActiveConsumers = new(StringComparer.OrdinalIgnoreCase);
    public ConcurrentDictionary<string, Exchange> Exchanges { get; } = new(StringComparer.OrdinalIgnoreCase);
    public ConcurrentDictionary<string, RabbitQueue> Queues { get; } = new(StringComparer.OrdinalIgnoreCase);
    public event Action<string, string>? StreamMessageAppended;

    public ExchangeManager(MessageRepository? repo = null)
    {
        _repo = repo;
        EnsureBuiltInExchanges(DefaultVhost);
    }

    // --- Exchange lifecycle --------------------------------------------------

    public Exchange DeclareExchange(string name, ExchangeType type, bool durable = true, bool autoDelete = false, int? superStreamPartitions = null)
        => DeclareExchange(DefaultVhost, name, type, durable, autoDelete, superStreamPartitions);

    public Exchange DeclareExchange(string vhost, string name, ExchangeType type, bool durable = true, bool autoDelete = false, int? superStreamPartitions = null)
    {
        var resolvedVhost = NormalizeVhost(vhost);
        EnsureBuiltInExchanges(resolvedVhost);
        var key = ExchangeKey(resolvedVhost, name);
        if (name.StartsWith("amq.", StringComparison.OrdinalIgnoreCase) && !Exchanges.ContainsKey(key))
            throw new InvalidOperationException($"Exchange name '{name}' is reserved.");
        var exchange = Exchanges.GetOrAdd(key, _ => new Exchange(resolvedVhost, name, type, durable, autoDelete, superStreamPartitions));
        if (_repo != null && durable && !IsBuiltInExchange(name))
            _repo.SaveRabbitExchangeAsync(resolvedVhost, name, type.ToString(), durable, autoDelete, superStreamPartitions).GetAwaiter().GetResult();
        return exchange;
    }

    public Exchange DeclareSuperStream(string vhost, string name, int partitions, bool durable = true, bool autoDelete = false, RabbitQueueArgs? partitionArgs = null)
    {
        if (partitions <= 0)
            throw new InvalidOperationException("Super stream partitions must be greater than zero.");

        var exchange = DeclareExchange(vhost, name, ExchangeType.SuperStream, durable, autoDelete, partitions);
        var resolvedVhost = NormalizeVhost(vhost);
        var effectivePartitionArgs = partitionArgs ?? new RabbitQueueArgs();

        for (int i = 0; i < partitions; i++)
        {
            string queueName = $"{name}-{i}";
            var queue = DeclareQueue(resolvedVhost, queueName, new RabbitQueueArgs
            {
                Type = RabbitQueueType.Stream,
                Durable = durable,
                Exclusive = false,
                AutoDelete = autoDelete,
                MaxPriority = effectivePartitionArgs.MaxPriority,
                DeadLetterExchange = effectivePartitionArgs.DeadLetterExchange,
                DeadLetterRoutingKey = effectivePartitionArgs.DeadLetterRoutingKey,
                MessageTtlMs = effectivePartitionArgs.MessageTtlMs,
                QueueTtlMs = effectivePartitionArgs.QueueTtlMs,
                StreamMaxLengthMessages = effectivePartitionArgs.StreamMaxLengthMessages,
                StreamMaxLengthBytes = effectivePartitionArgs.StreamMaxLengthBytes,
                StreamMaxAgeMs = effectivePartitionArgs.StreamMaxAgeMs
            });

            Bind(resolvedVhost, name, queue.Name, i.ToString());
        }

        return exchange;
    }

    public bool DeleteExchange(string name)
        => DeleteExchange(DefaultVhost, name);

    public bool DeleteExchange(string vhost, string name)
        => DeleteExchange(vhost, name, ifUnused: false);

    public bool DeleteExchange(string vhost, string name, bool ifUnused)
    {
        if (IsBuiltInExchange(name))
            return false;

        var resolvedVhost = NormalizeVhost(vhost);
        if (ifUnused &&
            Exchanges.TryGetValue(ExchangeKey(resolvedVhost, name), out var existing) &&
            existing.HasBindings())
        {
            return false;
        }
        var deleted = Exchanges.TryRemove(ExchangeKey(resolvedVhost, name), out _);
        if (deleted && _repo != null)
            _repo.DeleteRabbitExchangeAsync(resolvedVhost, name).GetAwaiter().GetResult();
        return deleted;
    }

    // --- Queue lifecycle -----------------------------------------------------

    public RabbitQueue DeclareQueue(string name, RabbitQueueArgs? args = null)
        => DeclareQueue(DefaultVhost, name, args);

    public RabbitQueue DeclareQueue(string vhost, string name, RabbitQueueArgs? args = null)
        => DeclareQueue(vhost, name, args, ownerId: null);

    public RabbitQueue DeclareQueue(string vhost, string name, RabbitQueueArgs? args, string? ownerId)
    {
        var resolvedVhost = NormalizeVhost(vhost);
        EnsureBuiltInExchanges(resolvedVhost);
        var resolvedArgs = args ?? new RabbitQueueArgs { Durable = true };
        var key = QueueKey(resolvedVhost, name);
        var queue = Queues.GetOrAdd(key, _ => new RabbitQueue(resolvedVhost, name, resolvedArgs));
        queue.RetentionDropHandler = DeletePersistedMessage;
        if (!string.IsNullOrEmpty(ownerId) && !queue.TryClaimExclusiveOwner(ownerId, out var error))
            throw new InvalidOperationException(error);
        if (_repo != null && queue.Durable)
            _repo.SaveRabbitQueueAsync(resolvedVhost, name, resolvedArgs).GetAwaiter().GetResult();
        return queue;
    }

    public bool DeleteQueue(string name)
        => DeleteQueue(DefaultVhost, name);

    public bool DeleteQueue(string vhost, string name)
        => DeleteQueue(vhost, name, ifUnused: true, ifEmpty: false);

    public bool DeleteQueue(string vhost, string name, bool ifUnused, bool ifEmpty)
        => TryDeleteQueue(vhost, name, ifUnused, ifEmpty, out _);

    public bool TryDeleteQueue(string vhost, string name, bool ifUnused, bool ifEmpty, out uint messageCount)
    {
        messageCount = 0;
        var resolvedVhost = NormalizeVhost(vhost);
        var key = QueueKey(resolvedVhost, name);

        if (Queues.TryGetValue(key, out var existing))
        {
            messageCount = (uint)existing.Count;
            if (ifUnused && !existing.Consumers.IsEmpty)
                return false;
            if (ifEmpty && (existing.Count > 0 || !existing.Unacked.IsEmpty))
                return false;

            if (!existing.Consumers.IsEmpty)
            {
                foreach (var consumerTag in existing.Consumers.Keys.ToArray())
                    CancelConsumer(resolvedVhost, name, consumerTag);
            }

            if (!existing.Unacked.IsEmpty)
                return false;
        }

        var deleted = Queues.TryRemove(key, out _);
        if (deleted)
        {
            RemoveQueueBindings(resolvedVhost, name);
            if (_repo != null)
                _repo.DeleteRabbitQueueAsync(resolvedVhost, name).GetAwaiter().GetResult();
        }
        else
        {
            messageCount = 0;
        }
        return deleted;
    }

    public uint PurgeQueue(string name)
        => PurgeQueue(DefaultVhost, name);

    public uint PurgeQueue(string vhost, string name)
    {
        var resolvedVhost = NormalizeVhost(vhost);
        if (!Queues.TryGetValue(QueueKey(resolvedVhost, name), out var queue))
            return 0;

        return (uint)queue.Purge(msg =>
        {
            DeletePersistedMessage(msg);
            return true;
        });
    }

    // --- Bindings ------------------------------------------------------------

    public void Bind(string exchangeName, string queueName, string routingKey, Dictionary<string, string>? headerArgs = null)
        => Bind(DefaultVhost, exchangeName, queueName, routingKey, headerArgs);

    public void Bind(string vhost, string exchangeName, string queueName, string routingKey, Dictionary<string, string>? headerArgs = null)
    {
        var resolvedVhost = NormalizeVhost(vhost);
        EnsureBuiltInExchanges(resolvedVhost);
        if (!Exchanges.TryGetValue(ExchangeKey(resolvedVhost, exchangeName), out var exchange))
            throw new InvalidOperationException($"Exchange '{exchangeName}' does not exist.");
        var queueKey = QueueKey(resolvedVhost, queueName);
        if (!Queues.ContainsKey(queueKey))
            throw new InvalidOperationException($"Queue '{queueName}' does not exist.");

        exchange.Bind(queueKey, routingKey, headerArgs);
        if (_repo != null && exchange.Durable && Queues.TryGetValue(queueKey, out var queue) && queue.Durable)
            _repo.SaveRabbitBindingAsync(resolvedVhost, exchangeName, queueName, routingKey, headerArgs, BindingDestinationKind.Queue).GetAwaiter().GetResult();
    }

    public void BindExchange(string sourceExchange, string destinationExchange, string routingKey, Dictionary<string, string>? headerArgs = null)
        => BindExchange(DefaultVhost, sourceExchange, destinationExchange, routingKey, headerArgs);

    public void BindExchange(string vhost, string sourceExchange, string destinationExchange, string routingKey, Dictionary<string, string>? headerArgs = null)
    {
        var resolvedVhost = NormalizeVhost(vhost);
        EnsureBuiltInExchanges(resolvedVhost);
        if (!Exchanges.TryGetValue(ExchangeKey(resolvedVhost, sourceExchange), out var source))
            throw new InvalidOperationException($"Exchange '{sourceExchange}' does not exist.");

        var destinationKey = ExchangeKey(resolvedVhost, destinationExchange);
        if (!Exchanges.ContainsKey(destinationKey))
            throw new InvalidOperationException($"Exchange '{destinationExchange}' does not exist.");

        source.Bind(destinationKey, routingKey, headerArgs);
        if (_repo != null && source.Durable && Exchanges.TryGetValue(destinationKey, out var destination) && destination.Durable)
            _repo.SaveRabbitBindingAsync(resolvedVhost, sourceExchange, destinationExchange, routingKey, headerArgs, BindingDestinationKind.Exchange).GetAwaiter().GetResult();
    }

    public void Unbind(string exchangeName, string queueName, string routingKey)
        => Unbind(DefaultVhost, exchangeName, queueName, routingKey);

    public void Unbind(string vhost, string exchangeName, string queueName, string routingKey)
    {
        var resolvedVhost = NormalizeVhost(vhost);
        if (Exchanges.TryGetValue(ExchangeKey(resolvedVhost, exchangeName), out var exchange))
        {
            exchange.Unbind(QueueKey(resolvedVhost, queueName), routingKey);
            TryDeleteAutoDeleteExchange(resolvedVhost, exchangeName, exchange);
        }
        if (_repo != null)
            _repo.DeleteRabbitBindingAsync(resolvedVhost, exchangeName, queueName, routingKey, BindingDestinationKind.Queue).GetAwaiter().GetResult();
    }

    public void UnbindExchange(string sourceExchange, string destinationExchange, string routingKey)
        => UnbindExchange(DefaultVhost, sourceExchange, destinationExchange, routingKey);

    public void UnbindExchange(string vhost, string sourceExchange, string destinationExchange, string routingKey)
    {
        var resolvedVhost = NormalizeVhost(vhost);
        if (Exchanges.TryGetValue(ExchangeKey(resolvedVhost, sourceExchange), out var exchange))
        {
            exchange.Unbind(ExchangeKey(resolvedVhost, destinationExchange), routingKey);
            TryDeleteAutoDeleteExchange(resolvedVhost, sourceExchange, exchange);
        }
        if (_repo != null)
            _repo.DeleteRabbitBindingAsync(resolvedVhost, sourceExchange, destinationExchange, routingKey, BindingDestinationKind.Exchange).GetAwaiter().GetResult();
    }

    // --- Publishing ----------------------------------------------------------

    /// <summary>
    /// Routes a message through the exchange to all matching bound queues.
    /// Returns the number of queues the message was enqueued to.
    /// </summary>
    public int Publish(string exchangeName, string routingKey, byte[] payload,
        Dictionary<string, string>? headers = null, byte priority = 0,
        int? messageTtlMs = null, string? correlationId = null, RabbitMessageProperties? properties = null, bool immediatePersist = false)
        => Publish(DefaultVhost, exchangeName, routingKey, payload, headers, priority, messageTtlMs, correlationId, properties, immediatePersist);

    public int Publish(string vhost, string exchangeName, string routingKey, byte[] payload,
        Dictionary<string, string>? headers = null, byte priority = 0,
        int? messageTtlMs = null, string? correlationId = null, RabbitMessageProperties? properties = null, bool immediatePersist = false)
    {
        var resolvedVhost = NormalizeVhost(vhost);
        EnsureBuiltInExchanges(resolvedVhost);
        return RoutePublishToExchange(
            resolvedVhost,
            exchangeName,
            routingKey,
            payload,
            headers,
            priority,
            messageTtlMs,
            exchangeName,
            properties,
            immediatePersist,
            new HashSet<string>(StringComparer.OrdinalIgnoreCase),
            new HashSet<string>(StringComparer.OrdinalIgnoreCase));
    }

    private void EnqueueToQueue(RabbitQueue queue, string routingKey, byte[] payload,
        Dictionary<string, string>? headers, byte priority, int? messageTtlMs, string fromExchange, RabbitMessageProperties? properties, bool immediatePersist)
    {
        DateTime? expiresAt = messageTtlMs.HasValue
            ? DateTime.UtcNow.AddMilliseconds(messageTtlMs.Value)
            : (DateTime?)null;

        var message = new RabbitMessage
        {
            RoutingKey = routingKey,
            Payload = payload,
            Headers = headers,
            Priority = priority,
            ExpiresAt = expiresAt,
            OriginalExchange = fromExchange,
            OriginalRoutingKey = routingKey,
            Properties = properties ?? new RabbitMessageProperties()
        };

        if (_repo != null && queue.Durable)
            message.PersistedId = _repo.EnqueueRabbitMessageAsync(queue.Vhost, queue.Name, message, immediatePersist).GetAwaiter().GetResult();

        queue.Enqueue(message);
        if (queue.Type == RabbitQueueType.Stream)
            StreamMessageAppended?.Invoke(queue.Vhost, queue.Name);
    }

    public bool TryAppendToStream(string vhost, string streamName, byte[] payload, out string? error, RabbitMessageProperties? properties = null, bool immediatePersist = false)
    {
        error = null;
        var resolvedVhost = NormalizeVhost(vhost);
        if (!Queues.TryGetValue(QueueKey(resolvedVhost, streamName), out var queue))
        {
            error = $"Stream '{streamName}' does not exist.";
            return false;
        }

        if (queue.Type != RabbitQueueType.Stream)
        {
            error = $"Queue '{streamName}' is not a stream queue.";
            return false;
        }

        EnqueueToQueue(queue, streamName, payload, headers: null, priority: 0, messageTtlMs: null, fromExchange: streamName, properties, immediatePersist);
        return true;
    }

    // --- Consumer registration -----------------------------------------------

    public void Consume(string queueName, string consumerTag, Action<RabbitMessage> onMessage, int prefetchCount = 0, string? ownerId = null)
        => Consume(DefaultVhost, queueName, consumerTag, onMessage, prefetchCount, ownerId);

    public void Consume(string vhost, string queueName, string consumerTag, Action<RabbitMessage> onMessage, int prefetchCount = 0, string? ownerId = null)
        => Consume(vhost, queueName, consumerTag, msg => { onMessage(msg); return true; }, prefetchCount, autoAck: false, ownerId: ownerId);

    public void Consume(string queueName, string consumerTag, Func<RabbitMessage, bool> onMessage, int prefetchCount = 0, bool autoAck = false, string? ownerId = null, Action<string, string>? cancelHandler = null, bool exclusiveConsumer = false)
        => Consume(DefaultVhost, queueName, consumerTag, onMessage, prefetchCount, autoAck, ownerId, cancelHandler, exclusiveConsumer);

    public void Consume(string vhost, string queueName, string consumerTag, Func<RabbitMessage, bool> onMessage, int prefetchCount = 0, bool autoAck = false, string? ownerId = null, Action<string, string>? cancelHandler = null, bool exclusiveConsumer = false, bool drainImmediately = true, RabbitStreamOffsetSpec? streamOffset = null)
    {
        var resolvedVhost = NormalizeVhost(vhost);
        if (!Queues.TryGetValue(QueueKey(resolvedVhost, queueName), out var queue))
            throw new InvalidOperationException($"Queue '{queueName}' does not exist.");

        var resolvedStreamOffset = streamOffset;
        if (queue.Type == RabbitQueueType.Stream && resolvedStreamOffset == null && _repo != null)
        {
            var savedOffset = _repo.GetRabbitStreamConsumerOffsetAsync(resolvedVhost, queueName, consumerTag).GetAwaiter().GetResult();
            if (savedOffset.HasValue)
                resolvedStreamOffset = new RabbitStreamOffsetSpec { Kind = RabbitStreamOffsetKind.Offset, Offset = savedOffset.Value + 1 };
        }

        Func<RabbitMessage, bool> callback = onMessage;
        if (queue.Type == RabbitQueueType.Stream && autoAck)
        {
            callback = msg =>
            {
                bool accepted = onMessage(msg);
                if (accepted)
                    PersistStreamConsumerOffset(resolvedVhost, queueName, consumerTag, msg.StreamOffset);
                return accepted;
            };
        }

        var registration = new RabbitQueue.ConsumerRegistration(
            callback,
            autoAck,
            autoAck ? DeletePersistedMessage : null,
            ownerId,
            cancelHandler,
            exclusiveConsumer);

        if (!queue.TryRegisterConsumer(consumerTag, registration, prefetchCount, out var error))
            throw new InvalidOperationException(error);

        if (queue.Type == RabbitQueueType.Stream)
            queue.SetStreamOffset(consumerTag, resolvedStreamOffset);

        if (drainImmediately)
            queue.DrainToConsumers();
    }

    public void CancelConsumer(string queueName, string consumerTag)
        => CancelConsumer(DefaultVhost, queueName, consumerTag);

    public bool TryResetStreamConsumerOffset(string vhost, string queueName, string consumerTag, RabbitStreamOffsetSpec? streamOffset, out string? error, out long nextOffset)
    {
        error = null;
        nextOffset = 0;

        if (string.IsNullOrWhiteSpace(consumerTag))
        {
            error = "Consumer tag is required.";
            return false;
        }

        var resolvedVhost = NormalizeVhost(vhost);
        if (!Queues.TryGetValue(QueueKey(resolvedVhost, queueName), out var queue))
        {
            error = $"Queue '{queueName}' does not exist.";
            return false;
        }

        if (queue.Type != RabbitQueueType.Stream)
        {
            error = $"Queue '{queueName}' is not a stream queue.";
            return false;
        }

        nextOffset = queue.ResolveRequestedStreamOffset(streamOffset);
        var normalizedOffset = new RabbitStreamOffsetSpec
        {
            Kind = RabbitStreamOffsetKind.Offset,
            Offset = nextOffset
        };

        bool hasActiveConsumer = queue.Consumers.ContainsKey(consumerTag);
        if (!hasActiveConsumer && _repo == null)
        {
            error = $"Consumer '{consumerTag}' is not active and no repository is configured to persist the reset offset.";
            return false;
        }

        if (hasActiveConsumer)
        {
            queue.SetStreamOffset(consumerTag, normalizedOffset);
            queue.DrainToConsumers();
        }

        if (_repo != null)
            PersistStreamConsumerOffset(resolvedVhost, queueName, consumerTag, Math.Max(0, nextOffset - 1));

        return true;
    }

    public bool TryQueryStreamConsumerOffset(string vhost, string queueName, string consumerTag, out string? error, out long nextOffset)
    {
        error = null;
        nextOffset = 0;

        var resolvedVhost = NormalizeVhost(vhost);
        if (!Queues.TryGetValue(QueueKey(resolvedVhost, queueName), out var queue))
        {
            error = $"Queue '{queueName}' does not exist.";
            return false;
        }

        if (queue.Type != RabbitQueueType.Stream)
        {
            error = $"Queue '{queueName}' is not a stream queue.";
            return false;
        }

        var snapshot = queue.GetStreamOffsetsSnapshot();
        if (snapshot.TryGetValue(consumerTag, out nextOffset))
            return true;

        if (_repo != null)
        {
            var lastOffset = _repo.GetRabbitStreamConsumerOffsetAsync(resolvedVhost, queueName, consumerTag).GetAwaiter().GetResult();
            if (lastOffset.HasValue)
            {
                nextOffset = lastOffset.Value + 1;
                return true;
            }
        }

        error = $"Offset for consumer '{consumerTag}' was not found.";
        return false;
    }

    public bool TryStoreStreamConsumerOffset(string vhost, string queueName, string consumerTag, long nextOffset, out string? error)
    {
        error = null;
        var resolvedVhost = NormalizeVhost(vhost);
        if (!Queues.TryGetValue(QueueKey(resolvedVhost, queueName), out var queue))
        {
            error = $"Queue '{queueName}' does not exist.";
            return false;
        }

        if (queue.Type != RabbitQueueType.Stream)
        {
            error = $"Queue '{queueName}' is not a stream queue.";
            return false;
        }

        var offsetSpec = new RabbitStreamOffsetSpec { Kind = RabbitStreamOffsetKind.Offset, Offset = nextOffset };
        queue.SetStreamOffset(consumerTag, offsetSpec);

        if (_repo != null)
            _repo.UpdateRabbitStreamConsumerOffsetAsync(resolvedVhost, queueName, consumerTag, Math.Max(0, nextOffset - 1)).GetAwaiter().GetResult();

        return true;
    }

    public bool TryResetSuperStreamConsumerOffset(string vhost, string exchangeName, string consumerTag, RabbitStreamOffsetSpec? streamOffset, out string? error, out Dictionary<string, long> partitionOffsets)
    {
        error = null;
        partitionOffsets = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);

        if (string.IsNullOrWhiteSpace(consumerTag))
        {
            error = "Consumer tag is required.";
            return false;
        }

        var resolvedVhost = NormalizeVhost(vhost);
        if (!Exchanges.TryGetValue(ExchangeKey(resolvedVhost, exchangeName), out var exchange))
        {
            error = $"Exchange '{exchangeName}' does not exist.";
            return false;
        }

        if (exchange.Type != ExchangeType.SuperStream)
        {
            error = $"Exchange '{exchangeName}' is not a super stream.";
            return false;
        }

        var partitions = exchange.GetSuperStreamPartitions()
            .Select(GetBindingDisplayName)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(static x => x, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        if (partitions.Length == 0)
        {
            error = $"Super stream '{exchangeName}' has no partitions.";
            return false;
        }

        foreach (var partition in partitions)
        {
            if (!TryResetStreamConsumerOffset(resolvedVhost, partition, consumerTag, streamOffset, out var partitionError, out var nextOffset))
            {
                error = $"Partition '{partition}' reset failed: {partitionError}";
                partitionOffsets.Clear();
                return false;
            }

            partitionOffsets[partition] = nextOffset;
        }

        return true;
    }

    public bool TryResolveSuperStreamPartition(string vhost, string exchangeName, string routingKey, string? partitionKey, out string? error, out string? partition)
    {
        error = null;
        partition = null;

        var resolvedVhost = NormalizeVhost(vhost);
        if (!Exchanges.TryGetValue(ExchangeKey(resolvedVhost, exchangeName), out var exchange))
        {
            error = $"Exchange '{exchangeName}' does not exist.";
            return false;
        }

        if (exchange.Type != ExchangeType.SuperStream)
        {
            error = $"Exchange '{exchangeName}' is not a super stream.";
            return false;
        }

        string effectiveKey = !string.IsNullOrWhiteSpace(partitionKey) ? partitionKey : routingKey;
        if (string.IsNullOrWhiteSpace(effectiveKey))
        {
            error = "Routing key or partition key is required.";
            return false;
        }

        var resolved = exchange.ResolveSuperStreamPartition(effectiveKey);
        if (string.IsNullOrWhiteSpace(resolved))
        {
            error = $"Super stream '{exchangeName}' has no partitions.";
            return false;
        }

        partition = GetBindingDisplayName(resolved);
        return true;
    }

    public void UpdateStreamPublisherSequence(string vhost, string streamName, string publisherRef, ulong publishingId)
    {
        if (string.IsNullOrWhiteSpace(publisherRef))
            return;

        var key = $"{NormalizeVhost(vhost)}|{streamName}|{publisherRef}";
        _streamPublisherSequences.AddOrUpdate(
            key,
            publishingId,
            (_, current) => publishingId > current ? publishingId : current);
    }

    public ulong GetStreamPublisherSequence(string vhost, string streamName, string publisherRef)
    {
        if (string.IsNullOrWhiteSpace(publisherRef))
            return 0;

        var key = $"{NormalizeVhost(vhost)}|{streamName}|{publisherRef}";
        return _streamPublisherSequences.TryGetValue(key, out var sequence) ? sequence : 0;
    }

    public bool TryRegisterSingleActiveStreamConsumer(string vhost, string streamName, string reference, string consumerId, out bool isActive)
    {
        var key = $"{NormalizeVhost(vhost)}|{streamName}|{reference}";
        while (true)
        {
            if (_streamActiveConsumers.TryGetValue(key, out var activeConsumerId))
            {
                isActive = string.Equals(activeConsumerId, consumerId, StringComparison.Ordinal);
                return true;
            }

            if (_streamActiveConsumers.TryAdd(key, consumerId))
            {
                isActive = true;
                return true;
            }
        }
    }

    public bool ReleaseSingleActiveStreamConsumer(string vhost, string streamName, string reference, string consumerId)
    {
        var key = $"{NormalizeVhost(vhost)}|{streamName}|{reference}";
        return _streamActiveConsumers.TryGetValue(key, out var activeConsumerId) &&
               string.Equals(activeConsumerId, consumerId, StringComparison.Ordinal) &&
               _streamActiveConsumers.TryRemove(key, out _);
    }

    public IReadOnlyList<string> GetSuperStreamPartitionNames(string vhost, string exchangeName)
    {
        var resolvedVhost = NormalizeVhost(vhost);
        if (!Exchanges.TryGetValue(ExchangeKey(resolvedVhost, exchangeName), out var exchange) ||
            exchange.Type != ExchangeType.SuperStream)
        {
            return Array.Empty<string>();
        }

        return exchange.GetSuperStreamPartitions()
            .Select(GetBindingDisplayName)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(static x => x, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    public void CancelConsumer(string vhost, string queueName, string consumerTag)
    {
        var resolvedVhost = NormalizeVhost(vhost);
        if (Queues.TryGetValue(QueueKey(resolvedVhost, queueName), out var queue))
        {
            var removed = queue.RemoveConsumer(consumerTag);
            removed?.CancelHandler?.Invoke(queueName, consumerTag);
            TryDeleteAutoDeleteQueue(resolvedVhost, queueName, queue);
        }
    }

    // --- Ack / Nack / Reject -------------------------------------------------

    public void Ack(string queueName, ulong deliveryTag, bool multiple = false)
        => Ack(DefaultVhost, queueName, deliveryTag, multiple);

    public void Ack(string vhost, string queueName, ulong deliveryTag, bool multiple = false)
    {
        if (!Queues.TryGetValue(QueueKey(NormalizeVhost(vhost), queueName), out var queue)) return;

        if (multiple)
        {
            var toAck = queue.Unacked.Keys.Where(k => k <= deliveryTag).ToList();
            foreach (var tag in toAck)
            {
                if (queue.TryRemoveUnacked(tag, out var entry))
                {
                    if (queue.Type == RabbitQueueType.Stream)
                        PersistStreamConsumerOffset(NormalizeVhost(vhost), queueName, entry.ConsumerTag, entry.Msg.StreamOffset);
                    if (queue.Type != RabbitQueueType.Stream)
                        DeletePersistedMessage(entry.Msg);
                }
            }
        }
        else if (queue.TryRemoveUnacked(deliveryTag, out var entry))
        {
            if (queue.Type == RabbitQueueType.Stream)
                PersistStreamConsumerOffset(NormalizeVhost(vhost), queueName, entry.ConsumerTag, entry.Msg.StreamOffset);
            if (queue.Type != RabbitQueueType.Stream)
                DeletePersistedMessage(entry.Msg);
        }

        queue.DrainToConsumers();
    }

    public void Nack(string queueName, ulong deliveryTag, bool multiple = false, bool requeue = true)
        => Nack(DefaultVhost, queueName, deliveryTag, multiple, requeue);

    public void Nack(string vhost, string queueName, ulong deliveryTag, bool multiple = false, bool requeue = true)
    {
        if (!Queues.TryGetValue(QueueKey(NormalizeVhost(vhost), queueName), out var queue)) return;

        var tags = multiple
            ? queue.Unacked.Keys.Where(k => k <= deliveryTag).ToList()
            : (queue.Unacked.ContainsKey(deliveryTag) ? new List<ulong> { deliveryTag } : new List<ulong>());

        foreach (var tag in tags)
        {
            if (!queue.TryRemoveUnacked(tag, out var entry)) continue;
            var msg = entry.Msg;
            msg.DeathCount++;

            if (requeue)
            {
                if (queue.Type == RabbitQueueType.Stream)
                {
                    msg.Redelivered = true;
                    queue.ResetStreamOffset(entry.ConsumerTag, msg.StreamOffset);
                }
                else
                {
                    queue.Requeue(msg);
                }
            }
            else
            {
                DeadLetter(queue, msg, "nack");
                if (queue.Type != RabbitQueueType.Stream)
                    DeletePersistedMessage(msg);
            }
        }

        queue.DrainToConsumers();
    }

    public void Reject(string queueName, ulong deliveryTag, bool requeue = false)
        => Reject(DefaultVhost, queueName, deliveryTag, requeue);

    public void Reject(string vhost, string queueName, ulong deliveryTag, bool requeue = false)
        => Nack(vhost, queueName, deliveryTag, multiple: false, requeue: requeue);

    // --- Dead Letter ---------------------------------------------------------

    private void DeadLetter(RabbitQueue sourceQueue, RabbitMessage msg, string reason)
    {
        if (string.IsNullOrEmpty(sourceQueue.DeadLetterExchange)) return;

        string dlxName = sourceQueue.DeadLetterExchange!;
        string dlrk = sourceQueue.DeadLetterRoutingKey ?? msg.RoutingKey;

        var headers = new Dictionary<string, string>(msg.Headers ?? new())
        {
            ["x-death-reason"] = reason,
            ["x-death-count"] = msg.DeathCount.ToString(),
            ["x-original-exchange"] = msg.OriginalExchange ?? "",
            ["x-original-routing-key"] = msg.OriginalRoutingKey ?? ""
        };

        Publish(sourceQueue.Vhost, dlxName, dlrk, msg.Payload, headers, msg.Priority);
    }

    // --- QoS -----------------------------------------------------------------

    public void SetQos(string queueName, string consumerTag, int prefetchCount)
        => SetQos(DefaultVhost, queueName, consumerTag, prefetchCount);

    public void SetQos(string vhost, string queueName, string consumerTag, int prefetchCount)
    {
        if (Queues.TryGetValue(QueueKey(NormalizeVhost(vhost), queueName), out var queue))
            queue.PrefetchCount[consumerTag] = prefetchCount;
    }

    public void DrainQueue(string queueName)
        => DrainQueue(DefaultVhost, queueName);

    public void DrainQueue(string vhost, string queueName)
    {
        if (Queues.TryGetValue(QueueKey(NormalizeVhost(vhost), queueName), out var queue))
            queue.DrainToConsumers();
    }

    // --- Publisher Confirms --------------------------------------------------

    public long GetNextPublishSeq(string queueName)
        => GetNextPublishSeq(DefaultVhost, queueName);

    public long GetNextPublishSeq(string vhost, string queueName)
        => Queues.TryGetValue(QueueKey(NormalizeVhost(vhost), queueName), out var queue) ? queue.NextPublishSeq() : 0;

    // --- TTL background loop -------------------------------------------------

    public void StartTtlLoop(TimeSpan interval, CancellationToken ct = default)
    {
        _ = Task.Run(async () =>
        {
            while (!ct.IsCancellationRequested)
            {
                PurgeExpiredMessages();
                PurgeExpiredQueues();
                await Task.Delay(interval, ct).ConfigureAwait(false);
            }
        }, ct);
    }

    private void PurgeExpiredMessages()
    {
        foreach (var queue in Queues.Values)
            queue.PurgeExpired(msg => DeadLetter(queue, msg, "expired"));
    }

    private void PurgeExpiredQueues()
    {
        var now = DateTime.UtcNow;
        foreach (var queue in Queues.Values.ToArray())
        {
            if (queue.QueueTtlMs.HasValue &&
                queue.Consumers.IsEmpty &&
                (now - queue.LastUsed).TotalMilliseconds > queue.QueueTtlMs.Value)
            {
                TryDeleteQueue(queue.Vhost, queue.Name, ifUnused: false, ifEmpty: false, out _);
            }
        }
    }

    // --- Monitoring ----------------------------------------------------------

    public IEnumerable<string> GetExchangeNames(string vhost)
    {
        var resolvedVhost = NormalizeVhost(vhost);
        EnsureBuiltInExchanges(resolvedVhost);
        return Exchanges.Values
            .Where(exchange => string.Equals(exchange.Vhost, resolvedVhost, StringComparison.Ordinal))
            .Select(exchange => exchange.Name);
    }

    public bool HasExchange(string name)
        => HasExchange(DefaultVhost, name);

    public bool HasExchange(string vhost, string name)
        => Exchanges.ContainsKey(ExchangeKey(NormalizeVhost(vhost), name));

    public Exchange? GetExchange(string name)
        => GetExchange(DefaultVhost, name);

    public Exchange? GetExchange(string vhost, string name)
        => Exchanges.TryGetValue(ExchangeKey(NormalizeVhost(vhost), name), out var exchange) ? exchange : null;

    public object GetStats()
        => GetStats(null);

    public object GetStats(string? vhost)
    {
        var resolvedVhost = string.IsNullOrWhiteSpace(vhost) ? null : NormalizeVhost(vhost);
        var exchanges = Exchanges.Values
            .Where(exchange => resolvedVhost == null || string.Equals(exchange.Vhost, resolvedVhost, StringComparison.Ordinal))
            .Select(exchange => new
            {
                vhost = exchange.Vhost,
                name = exchange.Name,
                type = exchange.Type.ToString(),
                durable = exchange.Durable,
                super_stream_partition_count = exchange.SuperStreamPartitions,
                super_stream_partitions = exchange.Type == ExchangeType.SuperStream
                    ? exchange.GetSuperStreamPartitions().Select(GetBindingDisplayName).ToArray()
                    : Array.Empty<string>()
            });
        var queues = Queues.Values
            .Where(queue => resolvedVhost == null || string.Equals(queue.Vhost, resolvedVhost, StringComparison.Ordinal))
            .Select(queue => new
            {
                vhost = queue.Vhost,
                name = queue.Name,
                messages = queue.Count,
                bytes = queue.Type == RabbitQueueType.Stream ? queue.StreamBytes : 0,
                consumers = queue.Consumers.Count,
                unacked = queue.Unacked.Count,
                queue_type = queue.Type.ToString().ToLowerInvariant(),
                stream_head_offset = queue.Type == RabbitQueueType.Stream ? queue.StreamHeadOffset : 0,
                stream_tail_offset = queue.Type == RabbitQueueType.Stream ? queue.StreamTailOffset : 0,
                stream_max_length_messages = queue.StreamMaxLengthMessages,
                stream_max_length_bytes = queue.StreamMaxLengthBytes,
                stream_max_age_ms = queue.StreamMaxAgeMs,
                stream_offsets = queue.Type == RabbitQueueType.Stream
                    ? queue.GetStreamOffsetsSnapshot()
                    : new Dictionary<string, long>(),
                stream_consumer_lag = queue.Type == RabbitQueueType.Stream
                    ? queue.GetStreamConsumerLagSnapshot()
                    : new Dictionary<string, long>(),
                durable = queue.Durable,
                dlx = queue.DeadLetterExchange,
                max_priority = queue.MaxPriority
            });

        return new { exchanges, queues };
    }

    public bool HasQueue(string name)
        => HasQueue(DefaultVhost, name);

    public bool HasQueue(string vhost, string name)
        => Queues.ContainsKey(QueueKey(NormalizeVhost(vhost), name));

    public RabbitQueue? GetQueue(string name)
        => GetQueue(DefaultVhost, name);

    public RabbitQueue? GetQueue(string vhost, string name)
        => Queues.TryGetValue(QueueKey(NormalizeVhost(vhost), name), out var queue) ? queue : null;

    public void DeleteExclusiveQueuesForOwner(string ownerId)
    {
        if (string.IsNullOrWhiteSpace(ownerId))
            return;

        var ownedQueues = Queues
            .Where(entry => entry.Value.Exclusive &&
                            string.Equals(entry.Value.ExclusiveOwner, ownerId, StringComparison.Ordinal))
            .Select(entry => (entry.Value.Vhost, entry.Value.Name))
            .ToList();

        foreach (var (vhost, name) in ownedQueues)
            TryDeleteQueue(vhost, name, ifUnused: false, ifEmpty: false, out _);
    }

    public async Task InitializeAsync(CancellationToken ct = default)
    {
        if (_repo == null) return;

        var exchanges = await _repo.GetRabbitExchangesAsync(ct);
        foreach (var exchange in exchanges)
        {
            EnsureBuiltInExchanges(exchange.Vhost);
            if (IsBuiltInExchange(exchange.Name)) continue;
            Exchanges[ExchangeKey(exchange.Vhost, exchange.Name)] = new Exchange(
                exchange.Vhost,
                exchange.Name,
                Enum.TryParse<ExchangeType>(exchange.Type, true, out var type) ? type : ExchangeType.Direct,
                exchange.Durable,
                exchange.AutoDelete,
                exchange.SuperStreamPartitions);
        }

        var queues = await _repo.GetRabbitQueuesAsync(ct);
        foreach (var queue in queues)
        {
            EnsureBuiltInExchanges(queue.Vhost);
            Queues[QueueKey(queue.Vhost, queue.Name)] = new RabbitQueue(queue.Vhost, queue.Name, new RabbitQueueArgs
            {
                Type = queue.Type,
                Durable = queue.Durable,
                Exclusive = queue.Exclusive,
                AutoDelete = queue.AutoDelete,
                MaxPriority = queue.MaxPriority,
                DeadLetterExchange = queue.DeadLetterExchange,
                DeadLetterRoutingKey = queue.DeadLetterRoutingKey,
                MessageTtlMs = queue.MessageTtlMs,
                QueueTtlMs = queue.QueueTtlMs,
                StreamMaxLengthMessages = queue.StreamMaxLengthMessages,
                StreamMaxLengthBytes = queue.StreamMaxLengthBytes,
                StreamMaxAgeMs = queue.StreamMaxAgeMs
            });
        }

        var bindings = await _repo.GetRabbitBindingsAsync(ct);
        foreach (var binding in bindings)
        {
            if (!Exchanges.TryGetValue(ExchangeKey(binding.Vhost, binding.ExchangeName), out var exchange))
                continue;

            if (binding.DestinationKind == BindingDestinationKind.Exchange)
            {
                var destinationExchangeKey = ExchangeKey(binding.Vhost, binding.DestinationName);
                if (Exchanges.ContainsKey(destinationExchangeKey))
                    exchange.Bind(destinationExchangeKey, binding.RoutingKey, binding.HeaderArgs);
                continue;
            }

            var destinationQueueKey = QueueKey(binding.Vhost, binding.DestinationName);
            if (Queues.ContainsKey(destinationQueueKey))
                exchange.Bind(destinationQueueKey, binding.RoutingKey, binding.HeaderArgs);
        }

        var messages = await _repo.GetRabbitMessagesAsync(ct);
        foreach (var message in messages)
        {
            if (!Queues.TryGetValue(QueueKey(message.Vhost, message.QueueName), out var queue))
                continue;

            queue.Restore(new RabbitMessage
            {
                PersistedId = message.Id,
                RoutingKey = message.RoutingKey,
                Payload = message.Payload,
                Headers = message.Headers,
                Properties = message.Properties,
                Priority = message.Priority,
                ExpiresAt = message.ExpiresAt,
                CreatedAt = message.CreatedAt,
                DeathCount = message.DeathCount,
                OriginalExchange = message.OriginalExchange,
                OriginalRoutingKey = message.OriginalRoutingKey
            });
        }
    }

    private void DeletePersistedMessage(RabbitMessage message)
    {
        if (_repo == null || message.PersistedId <= 0) return;
        _repo.DeleteRabbitMessageAsync(message.PersistedId).GetAwaiter().GetResult();
        message.PersistedId = 0;
    }

    private void PersistStreamConsumerOffset(string vhost, string queueName, string consumerTag, long offset)
    {
        if (_repo == null || string.IsNullOrWhiteSpace(consumerTag) || offset < 0)
            return;

        _repo.UpdateRabbitStreamConsumerOffsetAsync(vhost, queueName, consumerTag, offset).GetAwaiter().GetResult();
    }

    private void EnsureBuiltInExchanges(string vhost)
    {
        RegisterBuiltIn(vhost, "", ExchangeType.Direct);
        RegisterBuiltIn(vhost, "amq.direct", ExchangeType.Direct);
        RegisterBuiltIn(vhost, "amq.fanout", ExchangeType.Fanout);
        RegisterBuiltIn(vhost, "amq.topic", ExchangeType.Topic);
        RegisterBuiltIn(vhost, "amq.headers", ExchangeType.Headers);
        RegisterBuiltIn(vhost, "amq.match", ExchangeType.Headers);
    }

    private void RegisterBuiltIn(string vhost, string name, ExchangeType type)
        => Exchanges[ExchangeKey(vhost, name)] = new Exchange(vhost, name, type, durable: true);

    private static string NormalizeVhost(string? vhost)
        => string.IsNullOrWhiteSpace(vhost) ? DefaultVhost : vhost.Trim();

    private static bool IsBuiltInExchange(string name)
        => string.IsNullOrEmpty(name) || name.StartsWith("amq.", StringComparison.OrdinalIgnoreCase);

    public bool IsBuiltInExchangeName(string name)
        => IsBuiltInExchange(name);

    private static string ExchangeKey(string vhost, string name)
        => $"{NormalizeVhost(vhost)}\u001Fex\u001F{name}";

    private static string QueueKey(string vhost, string name)
        => $"{NormalizeVhost(vhost)}\u001Fq\u001F{name}";

    private static string GetBindingDisplayName(string bindingKey)
    {
        if (string.IsNullOrEmpty(bindingKey))
            return bindingKey;

        int markerIndex = bindingKey.LastIndexOf('\u001F');
        return markerIndex >= 0 && markerIndex < bindingKey.Length - 1
            ? bindingKey[(markerIndex + 1)..]
            : bindingKey;
    }

    private int RoutePublishToExchange(
        string vhost,
        string exchangeName,
        string routingKey,
        byte[] payload,
        Dictionary<string, string>? headers,
        byte priority,
        int? messageTtlMs,
        string originalExchange,
        RabbitMessageProperties? properties,
        bool immediatePersist,
        HashSet<string> visitedExchanges,
        HashSet<string> deliveredQueues)
    {
        if (exchangeName == string.Empty)
        {
            var defaultQueueKey = QueueKey(vhost, routingKey);
            if (Queues.TryGetValue(defaultQueueKey, out var defaultQueue) && deliveredQueues.Add(defaultQueueKey))
            {
                EnqueueToQueue(defaultQueue, routingKey, payload, headers, priority, messageTtlMs, originalExchange, properties, immediatePersist);
                return 1;
            }

            return 0;
        }

        var exchangeKey = ExchangeKey(vhost, exchangeName);
        if (!Exchanges.TryGetValue(exchangeKey, out var exchange) || !visitedExchanges.Add(exchangeKey))
            return 0;

        var delivered = 0;
        string effectiveRoutingKey = exchange.Type == ExchangeType.SuperStream &&
                                     headers != null &&
                                     headers.TryGetValue(SuperStreamPartitionKeyHeader, out var partitionKey) &&
                                     !string.IsNullOrWhiteSpace(partitionKey)
            ? partitionKey
            : routingKey;

        foreach (var destination in exchange.Route(effectiveRoutingKey, headers).Distinct(StringComparer.OrdinalIgnoreCase))
        {
            if (Queues.TryGetValue(destination, out var queue))
            {
                if (!deliveredQueues.Add(destination))
                    continue;

                EnqueueToQueue(queue, routingKey, payload, headers, priority, messageTtlMs, originalExchange, properties, immediatePersist);
                delivered++;
                continue;
            }

            if (!Exchanges.ContainsKey(destination))
                continue;

            var parts = destination.Split("\u001Fex\u001F", 2);
            if (parts.Length != 2)
                continue;

            delivered += RoutePublishToExchange(
                vhost,
                parts[1],
                routingKey,
                payload,
                headers,
                priority,
                messageTtlMs,
                originalExchange,
                properties,
                immediatePersist,
                visitedExchanges,
                deliveredQueues);
        }

        visitedExchanges.Remove(exchangeKey);
        return delivered;
    }

    private void TryDeleteAutoDeleteQueue(string vhost, string queueName, RabbitQueue queue)
    {
        if (!queue.AutoDelete || !queue.Consumers.IsEmpty) return;
        if (queue.Count > 0 || !queue.Unacked.IsEmpty) return;
        DeleteQueue(vhost, queueName);
    }

    private void TryDeleteAutoDeleteExchange(string vhost, string exchangeName, Exchange exchange)
    {
        if (!exchange.AutoDelete || exchange.HasBindings())
            return;

        DeleteExchange(vhost, exchangeName, ifUnused: false);
    }

    private void RemoveQueueBindings(string vhost, string queueName)
    {
        string resolvedVhost = NormalizeVhost(vhost);
        string queueKey = QueueKey(resolvedVhost, queueName);

        foreach (var exchange in Exchanges.Values.Where(x => string.Equals(x.Vhost, resolvedVhost, StringComparison.Ordinal)))
        {
            bool removedAny = false;
            foreach (var (routingKey, destinations) in exchange.GetBindings())
            {
                if (!destinations.Contains(queueKey, StringComparer.Ordinal))
                    continue;

                if (exchange.Unbind(queueKey, routingKey))
                    removedAny = true;
            }

            if (!removedAny)
                continue;

            if (_repo != null && !IsBuiltInExchange(exchange.Name))
                _repo.DeleteRabbitBindingsForDestinationAsync(resolvedVhost, exchange.Name, queueName, BindingDestinationKind.Queue).GetAwaiter().GetResult();

            TryDeleteAutoDeleteExchange(resolvedVhost, exchange.Name, exchange);
        }
    }
}
