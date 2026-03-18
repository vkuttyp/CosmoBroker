using System;
using System.Collections.Generic;
using System.Linq;

namespace CosmoBroker.JetStream.Models
{
    public enum RetentionPolicy
    {
        Limits,
        Interest,
        WorkQueue
    }

    public class StreamConfig
    {
        [System.Text.Json.Serialization.JsonPropertyName("name")]
        public string Name { get; set; } = string.Empty;

        [System.Text.Json.Serialization.JsonPropertyName("subjects")]
        public List<string> Subjects { get; set; } = new();

        [System.Text.Json.Serialization.JsonPropertyName("retention")]
        public RetentionPolicy Retention { get; set; } = RetentionPolicy.Limits;

        [System.Text.Json.Serialization.JsonPropertyName("max_msgs")]
        public int MaxMsgs { get; set; } = -1;

        [System.Text.Json.Serialization.JsonPropertyName("max_bytes")]
        public long MaxBytes { get; set; } = -1;

        [System.Text.Json.Serialization.JsonPropertyName("max_age")]
        public TimeSpan MaxAge { get; set; } = TimeSpan.Zero;

        [System.Text.Json.Serialization.JsonPropertyName("replicas")]
        public int Replicas { get; set; } = 1;
    }

    public class JetStreamEntity
    {
        public StreamConfig Config { get; }
        public long LastSequence { get; private set; } = 0;
        public long TotalBytes { get; private set; } = 0;

        public List<StreamMessage> Messages { get; } = new();
        public List<Consumer> Consumers { get; } = new();

        public string Name => Config.Name;

        public JetStreamEntity(StreamConfig config)
        {
            Config = config;
        }

        public void LoadMessages(IEnumerable<StreamMessage> messages)
        {
            Messages.Clear();
            Messages.AddRange(messages);
            if (Messages.Count > 0)
            {
                LastSequence = Messages.Max(m => m.Sequence);
                TotalBytes = Messages.Sum(m => (long)m.Payload.Length);
            }
        }

        public void SetLastSequence(long seq)
        {
            LastSequence = seq;
        }

        public StreamMessage AddMessage(string subject, byte[] payload, TimeSpan? ttl = null)
        {
            LastSequence++;

            var msg = new StreamMessage
            {
                Sequence = LastSequence,
                Subject = subject,
                Payload = payload,
                Timestamp = DateTime.UtcNow,
                ExpiresAt = ttl.HasValue ? DateTime.UtcNow.Add(ttl.Value) : null
            };

            Messages.Add(msg);
            TotalBytes += payload.Length;

            ApplyRetention();

            return msg;
        }

        private void ApplyRetention()
        {
            var now = DateTime.UtcNow;

            // 1. Per-Message TTL (Priority)
            int expiredCount = Messages.RemoveAll(m => m.ExpiresAt.HasValue && m.ExpiresAt.Value < now);
            if (expiredCount > 0)
            {
                TotalBytes = Messages.Sum(m => (long)m.Payload.Length);
            }

            if (Config.Retention == RetentionPolicy.Limits)
            {
                // 2. Stream Max Age (Global)
                if (Config.MaxAge > TimeSpan.Zero)
                {
                    var cutoff = now - Config.MaxAge;
                    Messages.RemoveAll(m => m.Timestamp < cutoff);
                }

                // 3. Max Messages
                if (Config.MaxMsgs > 0 && Messages.Count > Config.MaxMsgs)
                {
                    int toRemove = Messages.Count - Config.MaxMsgs;
                    Messages.RemoveRange(0, toRemove);
                }

                // 4. Max Bytes
                if (Config.MaxBytes > 0)
                {
                    while (Messages.Count > 0 && Messages.Sum(m => (long)m.Payload.Length) > Config.MaxBytes)
                    {
                        Messages.RemoveAt(0);
                    }
                }
                
                TotalBytes = Messages.Sum(m => (long)m.Payload.Length);
            }
        }

        public void RemoveMessage(long sequence)
        {
            var msg = Messages.FirstOrDefault(m => m.Sequence == sequence);
            if (msg != null)
            {
                TotalBytes -= msg.Payload.Length;
                Messages.Remove(msg);
            }
        }
    }

    public class StreamMessage
    {
        public long Sequence { get; set; }
        public string Subject { get; set; } = string.Empty;
        public byte[] Payload { get; set; } = Array.Empty<byte>();
        public DateTime Timestamp { get; set; }
        public DateTime? ExpiresAt { get; set; }
        public Dictionary<string, string> Headers { get; } = new();
    }
}
