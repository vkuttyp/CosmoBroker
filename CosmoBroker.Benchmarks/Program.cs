using System;
using System.Diagnostics;
using System.Threading.Tasks;
using NATS.Client.Core;
using System.Collections.Generic;
using System.Linq;

namespace CosmoBroker.Benchmarks;

class Program
{
    static async Task Main(string[] args)
    {
        string url = args.Length > 0 ? args[0] : "nats://localhost:4222";
        string label = args.Length > 1 ? args[1] : "Target";
        int count = 100_000;

        Console.WriteLine($"Benchmarking {label} at {url} with {count:N0} messages...");

        await using var nats = new NatsConnection(new NatsOpts { Url = url });
        await nats.ConnectAsync();

        // 1. Throughput Test (Pub only)
        Console.WriteLine("--- Throughput (PUB) ---");
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < count; i++)
        {
            await nats.PublishAsync("bench.foo", i);
        }
        // Wait for all to be sent
        await nats.PingAsync(); 
        sw.Stop();
        
        double msgPerSec = count / sw.Elapsed.TotalSeconds;
        Console.WriteLine($"Sent {count:N0} messages in {sw.Elapsed.TotalSeconds:F2}s ({msgPerSec:N0} msg/sec)");

        // 2. Latency Test (Req-Rep style or Ping)
        Console.WriteLine("--- Latency (RTT) ---");
        var latencies = new List<double>();
        for (int i = 0; i < 1000; i++)
        {
            var lsw = Stopwatch.StartNew();
            await nats.PingAsync();
            lsw.Stop();
            latencies.Add(lsw.Elapsed.TotalMilliseconds);
        }
        Console.WriteLine($"Average RTT: {latencies.Average():F3} ms");
        Console.WriteLine($"Min RTT: {latencies.Min():F3} ms");
        Console.WriteLine($"Max RTT: {latencies.Max():F3} ms");

        Console.WriteLine("Done.");
    }
}
