using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CosmoBroker.JetStream.Models;

namespace CosmoBroker.Services
{
    public class JetStreamService
    {
        private readonly Dictionary<string, JetStreamEntity> _streams = new();

        public JetStreamEntity CreateStream(string name, string subjectPattern)
        {
            var stream = new JetStreamEntity(name, subjectPattern);
            _streams[name] = stream;
            return stream;
        }

        public void AddConsumer(Consumer consumer)
        {
            if (_streams.TryGetValue(consumer.StreamName, out var stream))
            {
                stream.Consumers.Add(consumer);
                ReplayMissedMessages(consumer, stream);
            }
        }

        public async Task Publish(string streamName, string subject, byte[] payload)
        {
            if (!_streams.TryGetValue(streamName, out var stream))
                throw new Exception($"Stream {streamName} does not exist");

            var msg = stream.AddMessage(subject, payload);
            await BroadcastToConsumers(stream, msg);
        }

        private async Task BroadcastToConsumers(JetStreamEntity stream, StreamMessage msg)
        {
            foreach (var consumer in stream.Consumers)
            {
                if (SubjectMatches(stream.SubjectPattern, msg.Subject))
                {
                    await consumer.PipeWriter.WriteAsync(FormatMessage(msg));
                    await consumer.PipeWriter.FlushAsync();

                    consumer.LastDeliveredSeq = msg.Sequence;

                    // Track as unacked
                    consumer.InFlight[msg.Sequence] = msg;
                }
            }
        }

        private void ReplayMissedMessages(Consumer consumer, JetStreamEntity stream)
        {
            var missed = stream.Messages
                .Where(m => m.Sequence > consumer.LastDeliveredSeq);

            foreach (var msg in missed)
            {
                _ = consumer.PipeWriter.WriteAsync(FormatMessage(msg));

                consumer.LastDeliveredSeq = msg.Sequence;
                consumer.InFlight[msg.Sequence] = msg;
            }
        }

        private bool SubjectMatches(string pattern, string subject)
        {
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

        private ReadOnlyMemory<byte> FormatMessage(StreamMessage msg)
        {
            var header = $"MSG {msg.Subject} {msg.Sequence} {msg.Payload.Length}\r\n";
            var headerBytes = Encoding.UTF8.GetBytes(header);

            var full = new byte[headerBytes.Length + msg.Payload.Length + 2];

            Buffer.BlockCopy(headerBytes, 0, full, 0, headerBytes.Length);
            Buffer.BlockCopy(msg.Payload, 0, full, headerBytes.Length, msg.Payload.Length);

            full[^2] = (byte)'\r';
            full[^1] = (byte)'\n';

            return full;
        }

        public void Ack(string streamName, string consumerName, long sequence)
        {
            var consumer = GetConsumer(streamName, consumerName);

            if (consumer.InFlight.TryRemove(sequence, out _))
            {
                // ACK success → message removed
            }
        }

        public async Task Nack(string streamName, string consumerName, long sequence)
        {
            var consumer = GetConsumer(streamName, consumerName);

            if (consumer.InFlight.TryGetValue(sequence, out var msg))
            {
                // Redeliver
                await consumer.PipeWriter.WriteAsync(FormatMessage(msg));
                await consumer.PipeWriter.FlushAsync();
            }
        }

        private Consumer GetConsumer(string streamName, string consumerName)
        {
            if (!_streams.TryGetValue(streamName, out var stream))
                throw new Exception("Stream not found");

            var consumer = stream.Consumers.FirstOrDefault(c => c.Name == consumerName);

            if (consumer == null)
                throw new Exception("Consumer not found");

            return consumer;
        }

        public void StartRedeliveryLoop(TimeSpan ackWait)
        {
            _ = Task.Run(async () =>
            {
                while (true)
                {
                    foreach (var stream in _streams.Values)
                    {
                        foreach (var consumer in stream.Consumers)
                        {
                            foreach (var kv in consumer.InFlight)
                            {
                                var msg = kv.Value;

                                if (DateTime.UtcNow - msg.Timestamp > ackWait)
                                {
                                    await consumer.PipeWriter.WriteAsync(FormatMessage(msg));
                                    await consumer.PipeWriter.FlushAsync();

                                    // Update timestamp to avoid tight loop
                                    msg.Timestamp = DateTime.UtcNow;
                                }
                            }
                        }
                    }

                    await Task.Delay(1000);
                }
            });
        }
    }
}