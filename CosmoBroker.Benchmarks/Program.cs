using System;
using System.Diagnostics;
using System.Threading.Tasks;
using NATS.Client.Core;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace CosmoBroker.Benchmarks;

class Program
{
    static async Task Main(string[] args)
    {
        var options = BenchOptions.Parse(args);

        Console.WriteLine($"Benchmarking {options.Label} at {options.Url}");
        Console.WriteLine($"Messages: {options.Count:N0}, Payload: {options.PayloadBytes} bytes, Publishers: {options.Publishers}");

        var opts = new NatsOpts
        {
            Url = options.Url,
            SubPendingChannelCapacity = options.SubPendingCapacity,
            SubPendingChannelFullMode = System.Threading.Channels.BoundedChannelFullMode.Wait
        };
        await using var pubConn = new NatsConnection(opts);
        await using var subConn = new NatsConnection(opts);
        await pubConn.ConnectAsync();
        await subConn.ConnectAsync();

        // Warmup
        await pubConn.PublishAsync("bench.warmup", new byte[16]);
        await pubConn.PingAsync();

        // 1. Throughput + Tail Drop (PUB/SUB)
        Console.WriteLine("--- Throughput + Tail Drop (PUB/SUB) ---");
        var received = 0L;
        using var subCts = new CancellationTokenSource();
        var subTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var _ in subConn.SubscribeAsync<byte[]>("bench.foo", cancellationToken: subCts.Token))
                {
                    Interlocked.Increment(ref received);
                    if (received >= options.Count)
                        break;
                }
            }
            catch (OperationCanceledException)
            {
            }
        });

        var payload = new byte[options.PayloadBytes];
        Random.Shared.NextBytes(payload);

        var sw = Stopwatch.StartNew();
        var publishers = new List<Task>();
        var perPublisher = options.Count / options.Publishers;
        var remainder = options.Count % options.Publishers;
        for (int p = 0; p < options.Publishers; p++)
        {
            var toSend = perPublisher + (p == 0 ? remainder : 0);
            publishers.Add(Task.Run(() =>
            {
                for (int i = 0; i < toSend; i++)
                {
                    _ = pubConn.PublishAsync("bench.foo", payload);
                }
            }));
        }

        await Task.WhenAll(publishers);
        await pubConn.PingAsync();
        sw.Stop();

        // Wait for receiver to catch up
        var deadline = DateTime.UtcNow.AddMilliseconds(options.ReceiveTimeoutMs);
        while (Interlocked.Read(ref received) < options.Count && DateTime.UtcNow < deadline)
        {
            await Task.Delay(50);
        }
        subCts.Cancel();
        await subTask;

        var elapsed = sw.Elapsed.TotalSeconds;
        var msgPerSec = options.Count / elapsed;
        var dropped = options.Count - (int)Interlocked.Read(ref received);
        var dropRate = options.Count == 0 ? 0 : (double)dropped / options.Count * 100.0;

        Console.WriteLine($"Sent: {options.Count:N0} in {elapsed:F2}s ({msgPerSec:N0} msg/sec)");
        Console.WriteLine($"Received: {received:N0}, Dropped: {dropped:N0} ({dropRate:F2}%)");

        // 2. Latency (Ping RTT)
        Console.WriteLine("--- Latency (RTT via Ping) ---");
        var latencies = new List<double>(options.LatencySamples);
        for (int i = 0; i < options.LatencySamples; i++)
        {
            var lsw = Stopwatch.StartNew();
            await pubConn.PingAsync();
            lsw.Stop();
            latencies.Add(lsw.Elapsed.TotalMilliseconds);
        }

        latencies.Sort();
        Console.WriteLine($"Avg RTT: {latencies.Average():F3} ms");
        Console.WriteLine($"P50 RTT: {Percentile(latencies, 50):F3} ms");
        Console.WriteLine($"P95 RTT: {Percentile(latencies, 95):F3} ms");
        Console.WriteLine($"P99 RTT: {Percentile(latencies, 99):F3} ms");

        Console.WriteLine("Done.");
    }

    static double Percentile(IReadOnlyList<double> sorted, int percentile)
    {
        if (sorted.Count == 0) return 0;
        var rank = (percentile / 100.0) * (sorted.Count - 1);
        var low = (int)Math.Floor(rank);
        var high = (int)Math.Ceiling(rank);
        if (low == high) return sorted[low];
        var weight = rank - low;
        return sorted[low] * (1 - weight) + sorted[high] * weight;
    }
}

sealed record BenchOptions
{
    public string Url { get; init; } = "nats://localhost:4222";
    public string Label { get; init; } = "Target";
    public int Count { get; init; } = 500_000;
    public int PayloadBytes { get; init; } = 256;
    public int Publishers { get; init; } = 1;
    public int LatencySamples { get; init; } = 1_000;
    public int ReceiveTimeoutMs { get; init; } = 30_000;
    public int SubPendingCapacity { get; init; } = 1_000_000;

    public static BenchOptions Parse(string[] args)
    {
        var opts = new BenchOptions();
        for (int i = 0; i < args.Length; i++)
        {
            var key = args[i];
            if (i + 1 >= args.Length) break;
            var val = args[i + 1];
            switch (key)
            {
                case "--url":
                    opts = opts with { Url = val };
                    i++;
                    break;
                case "--label":
                    opts = opts with { Label = val };
                    i++;
                    break;
                case "--count":
                    if (int.TryParse(val, out var count))
                        opts = opts with { Count = count };
                    i++;
                    break;
                case "--payload":
                    if (int.TryParse(val, out var payload))
                        opts = opts with { PayloadBytes = payload };
                    i++;
                    break;
                case "--publishers":
                    if (int.TryParse(val, out var pubs) && pubs > 0)
                        opts = opts with { Publishers = pubs };
                    i++;
                    break;
                case "--latency":
                    if (int.TryParse(val, out var lat))
                        opts = opts with { LatencySamples = lat };
                    i++;
                    break;
                case "--timeout":
                    if (int.TryParse(val, out var timeout))
                        opts = opts with { ReceiveTimeoutMs = timeout };
                    i++;
                    break;
                case "--sub-pending":
                    if (int.TryParse(val, out var pending) && pending > 0)
                        opts = opts with { SubPendingCapacity = pending };
                    i++;
                    break;
            }
        }
        return opts;
    }
}
