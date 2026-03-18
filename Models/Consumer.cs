using System.Collections.Concurrent;
using System.IO.Pipelines;

namespace CosmoBroker.JetStream.Models
{
    public class Consumer
    {
        public string Name { get; set; }
        public string StreamName { get; set; }

        public long LastDeliveredSeq { get; set; } = 0;

        public PipeWriter PipeWriter { get; set; }

        // NEW: Track unacknowledged messages
        public ConcurrentDictionary<long, StreamMessage> InFlight { get; } = new();

        public Consumer(string name, string streamName, PipeWriter pipeWriter)
        {
            Name = name;
            StreamName = streamName;
            PipeWriter = pipeWriter;
        }
    }
}