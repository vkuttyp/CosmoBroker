using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO.Pipelines;

namespace CosmoBroker.JetStream.Models
{
    public enum AckPolicy
    {
        None,
        All,
        Explicit
    }

    public enum DeliverPolicy
    {
        All,
        Last,
        New,
        ByStartSequence,
        ByStartTime
    }

    public class ConsumerConfig
    {
        [System.Text.Json.Serialization.JsonPropertyName("durable_name")]
        public string DurableName { get; set; } = string.Empty;

        [System.Text.Json.Serialization.JsonPropertyName("deliver_subject")]
        public string? DeliverSubject { get; set; } // If null, it's a Pull Consumer

        [System.Text.Json.Serialization.JsonPropertyName("ack_policy")]
        public AckPolicy AckPolicy { get; set; } = AckPolicy.Explicit;

        [System.Text.Json.Serialization.JsonPropertyName("deliver_policy")]
        public DeliverPolicy DeliverPolicy { get; set; } = DeliverPolicy.All;

        [System.Text.Json.Serialization.JsonPropertyName("ack_wait")]
        public long AckWait { get; set; } = 30; // Seconds

        [System.Text.Json.Serialization.JsonPropertyName("max_deliver")]
        public int MaxDeliver { get; set; } = -1;

        [System.Text.Json.Serialization.JsonPropertyName("filter_subject")]
        public string FilterSubject { get; set; } = ">";

        [System.Text.Json.Serialization.JsonPropertyName("max_ack_pending")]
        public int MaxAckPending { get; set; } = 1024;
    }

    public class Consumer
    {
        public string Name { get; set; }
        public string StreamName { get; set; }
        public ConsumerConfig Config { get; }

        public long LastDeliveredSeq { get; set; } = 0;
        public long LastAckedSeq { get; set; } = 0;

        // SID or other identifier for the delivery route
        public string? DeliverSubject => Config.DeliverSubject;

        // Tracks unacknowledged messages: Sequence -> Message
        public ConcurrentDictionary<long, StreamMessage> InFlight { get; } = new();
        
        // Tracks delivery attempts: Sequence -> Count
        public ConcurrentDictionary<long, int> DeliveryAttempts { get; } = new();

        // For Pull Consumers: tracks requests for batches of messages
        // Value is (Count, ReplyTo)
        public ConcurrentQueue<(int Count, string ReplyTo)> PendingPullRequests { get; } = new();

        // Pre-parsed FilterSubject tokens for zero-alloc matching.
        // null means ">" (match everything).
        public readonly string[]? FilterTokens;

        // Pre-built ack reply prefix: "$JS.ACK.{streamName}.{name}."
        // Only the sequence number is appended per message.
        public string AckReplyPrefix = string.Empty;

        public Consumer(string name, string streamName, ConsumerConfig config)
        {
            Name = name;
            StreamName = streamName;
            Config = config;
            FilterTokens = config.FilterSubject == ">" ? null : config.FilterSubject.Split('.');
        }

        public bool IsPull => string.IsNullOrEmpty(Config.DeliverSubject);
    }
}
