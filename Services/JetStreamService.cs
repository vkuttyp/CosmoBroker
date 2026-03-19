using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CosmoBroker.JetStream.Models;
using CosmoBroker.Persistence;

namespace CosmoBroker.Services
{
    public class JetStreamService
    {
        private readonly Dictionary<string, JetStreamEntity> _streams = new();
        private readonly TopicTree _topicTree;
        private readonly MessageRepository? _repo;

        public JetStreamService(TopicTree topicTree, MessageRepository? repo = null)
        {
            _topicTree = topicTree;
            _repo = repo;
        }

        public async Task InitializeAsync()
        {
            if (_repo == null) return;

            var streams = await _repo.GetStreamsAsync();
            foreach (var s in streams)
            {
                var subjects = s.Subjects.Split(',', StringSplitOptions.RemoveEmptyEntries).ToList();
                var config = new StreamConfig { Name = s.Name, Subjects = subjects };
                var entity = new JetStreamEntity(config);
                
                var messages = await _repo.GetJetStreamMessagesAsync(s.Name, 0, 100);
                entity.LoadMessages(messages.Select(m => new StreamMessage {
                    Sequence = m.Id,
                    Subject = m.Subject,
                    Payload = m.Payload,
                    Timestamp = DateTime.UtcNow
                }));

                _streams[s.Name] = entity;
            }
        }

        public JetStreamEntity CreateStream(StreamConfig config)
        {
            var stream = new JetStreamEntity(config);
            _streams[config.Name] = stream;
            
            if (_repo != null)
            {
                _ = _repo.SaveStreamAsync(config.Name, string.Join(",", config.Subjects));
            }

            return stream;
        }

        public void AddConsumer(Consumer consumer)
        {
            if (_streams.TryGetValue(consumer.StreamName, out var stream))
            {
                stream.Consumers.Add(consumer);
                
                if (_repo != null)
                {
                    _ = Task.Run(async () => {
                        long offset = await _repo.GetConsumerOffsetAsync(consumer.Name);
                        consumer.LastDeliveredSeq = offset;
                        consumer.LastAckedSeq = offset;
                        if (!consumer.IsPull) ReplayMissedMessages(consumer, stream);
                    });
                }
                else
                {
                    if (!consumer.IsPull) ReplayMissedMessages(consumer, stream);
                }
            }
        }

        public async Task Publish(string streamName, string subject, byte[] payload, TimeSpan? ttl = null, string? msgId = null)
        {
            if (!_streams.TryGetValue(streamName, out var stream))
                throw new Exception($"Stream {streamName} does not exist");

            var now = DateTime.UtcNow;
            if (stream.IsDuplicate(msgId, now)) return;

            long sequence = 0;
            if (_repo != null)
            {
                sequence = await _repo.SaveJetStreamMessageAsync(streamName, subject, payload);
                stream.SetLastSequence(sequence);
            }

            var msg = stream.AddMessage(subject, payload, ttl);
            if (sequence > 0) msg.Sequence = sequence;

            await BroadcastToConsumers(stream, msg);

            // Mirror and Source propagation (best-effort, in-memory)
            foreach (var target in _streams.Values)
            {
                if (target.Config.Mirror?.Name == streamName ||
                    target.Config.Sources.Any(s => s.Name == streamName))
                {
                    _ = Publish(target.Name, subject, payload, ttl, msgId);
                }
            }
        }

        private async Task BroadcastToConsumers(JetStreamEntity stream, StreamMessage msg)
        {
            foreach (var consumer in stream.Consumers)
            {
                if (SubjectMatchesAny(consumer.Config.FilterSubject, msg.Subject))
                {
                    if (consumer.IsPull)
                    {
                        ProcessPullRequests(consumer, stream);
                        continue;
                    }

                    await DeliverMessage(consumer, stream, msg);
                }
            }
        }

        private async Task DeliverMessage(Consumer consumer, JetStreamEntity stream, StreamMessage msg)
        {
            string ackReply = $"$JS.ACK.{stream.Name}.{consumer.Name}.{msg.Sequence}";
            
            TimeSpan? remainingTtl = null;
            if (msg.ExpiresAt.HasValue)
            {
                remainingTtl = msg.ExpiresAt.Value - DateTime.UtcNow;
                if (remainingTtl.Value.TotalSeconds <= 0) return; // Expired already
            }

            _topicTree.PublishWithTTL(consumer.DeliverSubject ?? "", new System.Buffers.ReadOnlySequence<byte>(msg.Payload), ackReply, remainingTtl);

            consumer.LastDeliveredSeq = msg.Sequence;
            if (consumer.Config.AckPolicy != AckPolicy.None)
            {
                consumer.InFlight[msg.Sequence] = msg;
                consumer.DeliveryAttempts.AddOrUpdate(msg.Sequence, 1, (_, count) => count + 1);
            }
            else if (stream.Config.Retention == RetentionPolicy.WorkQueue)
            {
                stream.RemoveMessage(msg.Sequence);
            }
        }

        public void ProcessPullRequests(Consumer consumer, JetStreamEntity stream)
        {
            if (!consumer.IsPull) return;

            while (consumer.PendingPullRequests.TryPeek(out var req))
            {
                var missed = stream.Messages
                    .Where(m => m.Sequence > consumer.LastDeliveredSeq && SubjectMatchesAny(consumer.Config.FilterSubject, m.Subject))
                    .Take(req.Count)
                    .ToList();

                if (missed.Count == 0) break;

                consumer.PendingPullRequests.TryDequeue(out _);

                foreach (var msg in missed)
                {
                    string ackReply = $"$JS.ACK.{stream.Name}.{consumer.Name}.{msg.Sequence}";
                    
                    TimeSpan? remainingTtl = null;
                    if (msg.ExpiresAt.HasValue)
                    {
                        remainingTtl = msg.ExpiresAt.Value - DateTime.UtcNow;
                        if (remainingTtl.Value.TotalSeconds <= 0) continue;
                    }

                    _topicTree.PublishWithTTL(req.ReplyTo, new System.Buffers.ReadOnlySequence<byte>(msg.Payload), ackReply, remainingTtl);
                    
                    consumer.LastDeliveredSeq = msg.Sequence;
                    if (consumer.Config.AckPolicy != AckPolicy.None)
                    {
                        consumer.InFlight[msg.Sequence] = msg;
                        consumer.DeliveryAttempts.AddOrUpdate(msg.Sequence, 1, (_, count) => count + 1);
                    }
                    else if (stream.Config.Retention == RetentionPolicy.WorkQueue)
                    {
                        stream.RemoveMessage(msg.Sequence);
                    }
                }
            }
        }

        private void ReplayMissedMessages(Consumer consumer, JetStreamEntity stream)
        {
            var missed = stream.Messages
                .Where(m => m.Sequence > consumer.LastDeliveredSeq && SubjectMatchesAny(consumer.Config.FilterSubject, m.Subject));

            foreach (var msg in missed)
            {
                _ = DeliverMessage(consumer, stream, msg);
            }
        }

        public void RequestNext(string streamName, string consumerName, int batch, string replyTo)
        {
            if (!_streams.TryGetValue(streamName, out var stream)) return;
            var consumer = stream.Consumers.FirstOrDefault(c => c.Name == consumerName);
            if (consumer == null || !consumer.IsPull) return;

            consumer.PendingPullRequests.Enqueue((batch, replyTo));
            ProcessPullRequests(consumer, stream);
        }

        private bool SubjectMatchesAny(string pattern, string subject)
        {
            return SubjectMatches(pattern, subject);
        }

        private bool SubjectMatches(string pattern, string subject)
        {
            if (pattern == ">") return true;
            var pTokens = pattern.Split('.');
            var sTokens = subject.Split('.');

            for (int i = 0; i < pTokens.Length; i++)
            {
                if (i >= sTokens.Length) return false;
                if (pTokens[i] == ">") return true;
                if (pTokens[i] == "*") continue;
                if (pTokens[i] != sTokens[i]) return false;
            }
            return pTokens.Length == sTokens.Length;
        }

        public void Ack(string streamName, string consumerName, long sequence)
        {
            if (!_streams.TryGetValue(streamName, out var stream)) return;
            var consumer = stream.Consumers.FirstOrDefault(c => c.Name == consumerName);
            if (consumer == null) return;

            if (consumer.Config.AckPolicy == AckPolicy.All)
            {
                var toAck = consumer.InFlight.Keys.Where(k => k <= sequence).ToList();
                foreach (var seq in toAck) consumer.InFlight.TryRemove(seq, out _);
                consumer.LastAckedSeq = Math.Max(consumer.LastAckedSeq, sequence);
                if (_repo != null)
                {
                    _ = _repo.UpdateConsumerOffsetAsync(consumer.Name, streamName, consumer.LastAckedSeq);
                }
                if (stream.Config.Retention == RetentionPolicy.WorkQueue)
                {
                    foreach (var seq in toAck) stream.RemoveMessage(seq);
                }
            }
            else if (consumer.InFlight.TryRemove(sequence, out _))
            {
                consumer.LastAckedSeq = Math.Max(consumer.LastAckedSeq, sequence);
                
                if (_repo != null)
                {
                    _ = _repo.UpdateConsumerOffsetAsync(consumer.Name, streamName, sequence);
                }

                if (stream.Config.Retention == RetentionPolicy.WorkQueue)
                {
                    stream.RemoveMessage(sequence);
                }
            }
        }

        public void Nack(string streamName, string consumerName, long sequence)
        {
            if (!_streams.TryGetValue(streamName, out var stream)) return;
            var consumer = stream.Consumers.FirstOrDefault(c => c.Name == consumerName);
            if (consumer == null) return;

            if (consumer.InFlight.TryRemove(sequence, out _))
            {
                consumer.LastDeliveredSeq = Math.Max(consumer.LastDeliveredSeq - 1, 0);
            }
            if (consumer.InFlight.TryGetValue(sequence, out var msg))
            {
                int attempts = consumer.DeliveryAttempts.GetValueOrDefault(sequence, 0);
                if (consumer.Config.MaxDeliver > 0 && attempts >= consumer.Config.MaxDeliver)
                {
                    consumer.InFlight.TryRemove(sequence, out _);
                    if (stream.Config.Retention == RetentionPolicy.WorkQueue)
                    {
                        stream.RemoveMessage(sequence);
                    }
                    return;
                }

                _ = DeliverMessage(consumer, stream, msg);
            }
        }

        public void Term(string streamName, string consumerName, long sequence)
        {
            if (!_streams.TryGetValue(streamName, out var stream)) return;
            var consumer = stream.Consumers.FirstOrDefault(c => c.Name == consumerName);
            if (consumer == null) return;

            consumer.InFlight.TryRemove(sequence, out _);
            if (stream.Config.Retention == RetentionPolicy.WorkQueue)
            {
                stream.RemoveMessage(sequence);
            }
        }

        public async Task<byte[]?> HandleApiRequest(string subject, byte[] payload)
        {
            var parts = subject.Split('.');
            if (parts.Length < 3) return null;

            var jsonOptions = new System.Text.Json.JsonSerializerOptions { PropertyNameCaseInsensitive = true };

            if (parts[2] == "STREAM")
            {
                if (parts.Length >= 4 && parts[3] == "INFO")
                {
                    string streamName = parts[4];
                    if (_streams.TryGetValue(streamName, out var stream))
                    {
                        var info = new {
                            type = "io.nats.jetstream.api.v1.stream_info_response",
                            config = stream.Config,
                            state = new { messages = stream.Messages.Count, bytes = stream.TotalBytes, last_seq = stream.LastSequence }
                        };
                        return System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(info);
                    }
                }
                else if (parts.Length >= 4 && (parts[3] == "CREATE" || parts[3] == "UPDATE"))
                {
                    string streamName = parts[4];
                    StreamConfig? config = null;
                    try {
                        config = System.Text.Json.JsonSerializer.Deserialize<StreamConfig>(payload, jsonOptions);
                    } catch { }

                    if (config == null) {
                        config = new StreamConfig { Name = streamName, Subjects = new List<string> { streamName + ".>" } };
                    }
                    config.Name = streamName;

                    var stream = CreateStream(config);
                    var resp = new {
                        type = "io.nats.jetstream.api.v1.stream_create_response",
                        config = stream.Config
                    };
                    return System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(resp);
                }
            }
            
            if (parts[2] == "CONSUMER")
            {
                if (parts.Length >= 6 && parts[3] == "CREATE")
                {
                    string streamName = parts[4];
                    string consumerName = parts[5];
                    ConsumerConfig? config = null;
                    try {
                        config = System.Text.Json.JsonSerializer.Deserialize<ConsumerConfig>(payload, jsonOptions);
                    } catch { }

                    config ??= new ConsumerConfig { DurableName = consumerName };
                    
                    var consumer = new Consumer(consumerName, streamName, config);
                    AddConsumer(consumer);

                    var resp = new {
                        type = "io.nats.jetstream.api.v1.consumer_create_response",
                        config = consumer.Config
                    };
                    return System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(resp);
                }
            }

            return null;
        }

        public void StartRedeliveryLoop(TimeSpan checkInterval)
        {
            _ = Task.Run(async () =>
            {
                while (true)
                {
                    ApplyFlowControl();
                    foreach (var stream in _streams.Values)
                    {
                        foreach (var consumer in stream.Consumers)
                        {
                            if (consumer.Config.AckPolicy == AckPolicy.None) continue;
                            foreach (var kv in consumer.InFlight)
                            {
                                var msg = kv.Value;
                                if (DateTime.UtcNow - msg.Timestamp > TimeSpan.FromSeconds(consumer.Config.AckWait))
                                {
                                    int attempts = consumer.DeliveryAttempts.GetValueOrDefault(msg.Sequence, 0);
                                    if (consumer.Config.MaxDeliver > 0 && attempts >= consumer.Config.MaxDeliver)
                                    {
                                        consumer.InFlight.TryRemove(msg.Sequence, out _);
                                        continue;
                                    }
                                    _ = DeliverMessage(consumer, stream, msg);
                                    msg.Timestamp = DateTime.UtcNow; 
                                }
                            }
                        }
                    }
                    await Task.Delay(checkInterval);
                }
            });
        }

        private void ApplyFlowControl()
        {
            // Basic flow control: if too many in-flight messages, avoid further delivery.
            foreach (var stream in _streams.Values)
            {
                foreach (var consumer in stream.Consumers)
                {
                    if (consumer.Config.MaxDeliver == 0) continue;
                    if (consumer.InFlight.Count > 1000)
                    {
                        // Best-effort: trim oldest in-flight entries by removing arbitrary ones.
                        foreach (var key in consumer.InFlight.Keys.Take(100))
                        {
                            consumer.InFlight.TryRemove(key, out _);
                        }
                    }
                }
            }
        }

        public IEnumerable<string> GetMatchingStreams(string subject)
        {
            return _streams.Values
                .Where(s => s.Config.Subjects.Any(p => SubjectMatches(p, subject)))
                .Select(s => s.Name);
        }

        public object GetStats() => new {
            streams = _streams.Values.Select(s => new {
                name = s.Name,
                subjects = s.Config.Subjects,
                messages = s.Messages.Count,
                bytes = s.TotalBytes,
                last_seq = s.LastSequence,
                consumers = s.Consumers.Count
            })
        };

        public bool HasStreams => _streams.Count > 0;

        public string Snapshot()
        {
            var snap = new {
                streams = _streams.Values.Select(s => new {
                    config = s.Config,
                    messages = s.Messages.Select(m => new {
                        seq = m.Sequence,
                        subject = m.Subject,
                        payload = Convert.ToBase64String(m.Payload),
                        ts = m.Timestamp,
                        exp = m.ExpiresAt
                    }).ToList()
                }).ToList()
            };
            return System.Text.Json.JsonSerializer.Serialize(snap);
        }

        public void Restore(string snapshotJson)
        {
            var doc = System.Text.Json.JsonDocument.Parse(snapshotJson);
            if (!doc.RootElement.TryGetProperty("streams", out var streams)) return;

            _streams.Clear();
            foreach (var s in streams.EnumerateArray())
            {
                var configJson = s.GetProperty("config").GetRawText();
                var config = System.Text.Json.JsonSerializer.Deserialize<StreamConfig>(configJson);
                if (config == null) continue;
                var entity = new JetStreamEntity(config);

                if (s.TryGetProperty("messages", out var msgs))
                {
                    foreach (var m in msgs.EnumerateArray())
                    {
                        var payload = Convert.FromBase64String(m.GetProperty("payload").GetString() ?? "");
                        var msg = new StreamMessage {
                            Sequence = m.GetProperty("seq").GetInt64(),
                            Subject = m.GetProperty("subject").GetString() ?? "",
                            Payload = payload,
                            Timestamp = m.GetProperty("ts").GetDateTime(),
                            ExpiresAt = m.TryGetProperty("exp", out var exp) && exp.ValueKind != System.Text.Json.JsonValueKind.Null ? exp.GetDateTime() : null
                        };
                        entity.Messages.Add(msg);
                    }
                }

                if (entity.Messages.Count > 0)
                {
                    entity.SetLastSequence(entity.Messages.Max(m => m.Sequence));
                }
                _streams[config.Name] = entity;
            }
        }
    }
}
