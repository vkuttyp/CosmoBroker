using System;
using System.Collections.Generic;

namespace CosmoBroker.JetStream.Models
{
    public class JetStreamEntity
    {
        public string Name { get; set; }
        public string SubjectPattern { get; set; }
        public long LastSequence { get; private set; } = 0;
        public int MaxMessages { get; set; } = 1000;

        public List<StreamMessage> Messages { get; } = new();
        public List<Consumer> Consumers { get; } = new();

        public JetStreamEntity(string name, string subjectPattern)
        {
            Name = name;
            SubjectPattern = subjectPattern;
        }

        public StreamMessage AddMessage(string subject, byte[] payload)
        {
            LastSequence++;

            var msg = new StreamMessage
            {
                Sequence = LastSequence,
                Subject = subject,
                Payload = payload,
                Timestamp = DateTime.UtcNow
            };

            Messages.Add(msg);

            // Retention
            if (Messages.Count > MaxMessages)
                Messages.RemoveRange(0, Messages.Count - MaxMessages);

            return msg;
        }
    }

    public class StreamMessage
    {
        public long Sequence { get; set; }
        public string Subject { get; set; }
        public byte[] Payload { get; set; }
        public DateTime Timestamp { get; set; }
    }
}