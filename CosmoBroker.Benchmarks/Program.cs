using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using CosmoBroker.Client;
using NATS.Client.Core;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

namespace CosmoBroker.Benchmarks;

class Program
{
    static async Task Main(string[] args)
    {
        var options = BenchOptions.Parse(args).ApplyProfileDefaults();
        var mode = options.ResolveMode();

        switch (mode)
        {
            case BenchMode.CosmoClient:
                await RunCosmoClientBench(options);
                break;
            case BenchMode.Nats:
                await RunNatsClientBench(options);
                break;
            case BenchMode.CosmoRabbitMq:
                await RunCosmoRabbitMqBench(options);
                break;
            case BenchMode.RabbitMq:
                await RunRabbitMqBench(options);
                break;
            case BenchMode.CompareAmqp:
                await RunAmqpComparison(options);
                break;
            case BenchMode.Stream:
                await RunRabbitStreamBench(options);
                break;
            case BenchMode.CompareAmqpMatrix:
                await RunAmqpComparisonMatrix(options);
                break;
            default:
                throw new InvalidOperationException($"Unsupported benchmark mode '{mode}'.");
        }
    }

    static async Task RunCosmoClientBench(BenchOptions options)
    {
        WriteHeader(options, "NATS PUB/SUB");

        var cosmoOpts = new CosmoClientOptions { Url = options.Url };
        await using var pubConn = new CosmoClient(cosmoOpts);
        await using var subConn = new CosmoClient(cosmoOpts);
        await pubConn.ConnectAsync();
        await subConn.ConnectAsync();

        await pubConn.PublishAsync("bench.warmup", new byte[16]);
        await pubConn.PingAsync();
        await subConn.PingAsync();

        var received = 0L;
        using var subCts = new CancellationTokenSource();

        var subTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var _ in subConn.SubscribeAsync("bench.foo", ct: subCts.Token))
                {
                    Interlocked.Increment(ref received);
                    if (received >= options.Count)
                        break;
                }
            }
            catch (OperationCanceledException) { }
        });

        await Task.Delay(200);

        var payload = CreatePayload(options.PayloadBytes);
        var throughput = await RunThroughputAsync(
            options,
            async () => await pubConn.PublishAsync("bench.foo", payload),
            async () => await pubConn.PingAsync(),
            () => Interlocked.Read(ref received));

        subCts.Cancel();
        await subTask;

        PrintThroughput(throughput);

        Console.WriteLine("--- Latency (RTT via Ping) ---");
        var latencies = new List<double>(options.LatencySamples);
        for (int i = 0; i < options.LatencySamples; i++)
        {
            var lsw = Stopwatch.StartNew();
            await pubConn.PingAsync();
            lsw.Stop();
            latencies.Add(lsw.Elapsed.TotalMilliseconds);
        }

        PrintLatencies(latencies);
        Console.WriteLine("Done.");
    }

    static async Task RunNatsClientBench(BenchOptions options)
    {
        WriteHeader(options, "NATS PUB/SUB");

        var opts = new NatsOpts
        {
            Url = options.Url,
            SubPendingChannelCapacity = options.SubPendingCapacity,
            SubPendingChannelFullMode = BoundedChannelFullMode.Wait
        };

        await using var pubConn = new NatsConnection(opts);
        await using var subConn = new NatsConnection(opts);
        await pubConn.ConnectAsync();
        await subConn.ConnectAsync();

        await pubConn.PublishAsync("bench.warmup", new byte[16]);
        await pubConn.PingAsync();

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
            catch (OperationCanceledException) { }
        });

        await Task.Delay(100);

        var payload = CreatePayload(options.PayloadBytes);
        var throughput = await RunThroughputAsync(
            options,
            async () => await pubConn.PublishAsync("bench.foo", payload),
            async () => await pubConn.PingAsync(),
            () => Interlocked.Read(ref received));

        subCts.Cancel();
        await subTask;

        PrintThroughput(throughput);

        Console.WriteLine("--- Latency (RTT via Ping) ---");
        var latencies = new List<double>(options.LatencySamples);
        for (int i = 0; i < options.LatencySamples; i++)
        {
            var lsw = Stopwatch.StartNew();
            await pubConn.PingAsync();
            lsw.Stop();
            latencies.Add(lsw.Elapsed.TotalMilliseconds);
        }

        PrintLatencies(latencies);
        Console.WriteLine("Done.");
    }

    static async Task RunCosmoRabbitMqBench(BenchOptions options)
    {
        WriteHeader(options, "RabbitMQ-style queue benchmark over $RMQ.*");

        var clientOptions = new CosmoClientOptions { Url = options.Url };
        await using var controlConn = new CosmoClient(clientOptions);
        await using var pubConn = new CosmoClient(clientOptions);
        await using var subConn = new CosmoClient(clientOptions);
        await controlConn.ConnectAsync();
        await pubConn.ConnectAsync();
        await subConn.ConnectAsync();

        string suffix = Guid.NewGuid().ToString("N");
        string exchange = $"bench.exchange.{suffix}";
        string throughputQueue = $"bench.queue.throughput.{suffix}";
        string latencyQueue = $"bench.queue.latency.{suffix}";
        string throughputReply = $"_bench.rmq.throughput.{suffix}";
        string latencyReply = $"_bench.rmq.latency.{suffix}";

        await EnsureOkAsync(await RmqRequestAsync(controlConn, "$RMQ.EXCHANGE.DECLARE",
            new { name = exchange, type = "Direct", durable = false }));
        await EnsureOkAsync(await RmqRequestAsync(controlConn, "$RMQ.QUEUE.DECLARE",
            new { name = throughputQueue, durable = false, autoDelete = true }));
        await EnsureOkAsync(await RmqRequestAsync(controlConn, "$RMQ.QUEUE.BIND",
            new { exchange, queue = throughputQueue, routingKey = "throughput" }));

        var received = 0L;
        string throughputReady = $"ready:{Guid.NewGuid():N}";
        using var throughputSubCts = new CancellationTokenSource();
        var throughputEnumerator = subConn.SubscribeAsync(throughputReply, ct: throughputSubCts.Token).GetAsyncEnumerator();
        var throughputReadyMove = throughputEnumerator.MoveNextAsync().AsTask();

        await controlConn.PublishAsync(throughputReply, throughputReady);
        await controlConn.PingAsync();
        if (!await throughputReadyMove.WaitAsync(TimeSpan.FromSeconds(5)))
            throw new TimeoutException();
        if (throughputEnumerator.Current.GetStringData() != throughputReady)
            throw new InvalidOperationException("Unexpected first message on throughput reply subject.");

        var throughputSubTask = Task.Run(async () =>
        {
            try
            {
                while (await throughputEnumerator.MoveNextAsync())
                {
                    Interlocked.Increment(ref received);
                    if (received >= options.Count)
                        break;
                }
            }
            catch (OperationCanceledException) { }
        });
        await EnsureOkAsync(await RmqRequestAsync(controlConn, $"$RMQ.CONSUME.{throughputQueue}",
            new { replySubject = throughputReply, prefetchCount = 0 }));

        var payload = CreatePayload(options.PayloadBytes);
        string throughputSubject = $"$RMQ.PUBLISH.{exchange}.throughput";

        var throughput = await RunThroughputAsync(
            options,
            async () => await pubConn.PublishAsync(throughputSubject, payload),
            async () => await pubConn.PingAsync(),
            () => Interlocked.Read(ref received));

        throughputSubCts.Cancel();
        await throughputSubTask;

        PrintThroughput(throughput);

        await EnsureOkAsync(await RmqRequestAsync(controlConn, "$RMQ.QUEUE.DECLARE",
            new { name = latencyQueue, durable = false, autoDelete = true }));
        await EnsureOkAsync(await RmqRequestAsync(controlConn, "$RMQ.QUEUE.BIND",
            new { exchange, queue = latencyQueue, routingKey = "latency" }));

        var latencyChannel = Channel.CreateUnbounded<bool>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true
        });

        string latencyReady = $"ready:{Guid.NewGuid():N}";
        using var latencySubCts = new CancellationTokenSource();
        var latencyEnumerator = subConn.SubscribeAsync(latencyReply, ct: latencySubCts.Token).GetAsyncEnumerator();
        var latencyReadyMove = latencyEnumerator.MoveNextAsync().AsTask();

        await controlConn.PublishAsync(latencyReply, latencyReady);
        await controlConn.PingAsync();
        if (!await latencyReadyMove.WaitAsync(TimeSpan.FromSeconds(5)))
            throw new TimeoutException();
        if (latencyEnumerator.Current.GetStringData() != latencyReady)
            throw new InvalidOperationException("Unexpected first message on latency reply subject.");

        var latencySubTask = Task.Run(async () =>
        {
            try
            {
                while (await latencyEnumerator.MoveNextAsync())
                {
                    await latencyChannel.Writer.WriteAsync(true, latencySubCts.Token);
                }
            }
            catch (OperationCanceledException) { }
        });
        await EnsureOkAsync(await RmqRequestAsync(controlConn, $"$RMQ.CONSUME.{latencyQueue}",
            new { replySubject = latencyReply, prefetchCount = 0 }));

        Console.WriteLine("--- Latency (publish to consumer delivery) ---");
        string latencySubject = $"$RMQ.PUBLISH.{exchange}.latency";
        var latencies = new List<double>(options.LatencySamples);
        for (int i = 0; i < options.LatencySamples; i++)
        {
            var lsw = Stopwatch.StartNew();
            await pubConn.PublishAsync(latencySubject, payload);
            await pubConn.PingAsync();
            await latencyChannel.Reader.ReadAsync();
            lsw.Stop();
            latencies.Add(lsw.Elapsed.TotalMilliseconds);
        }

        latencySubCts.Cancel();
        latencyChannel.Writer.TryComplete();
        await latencySubTask;

        PrintLatencies(latencies);
        Console.WriteLine("Done.");
    }

    static async Task RunRabbitMqBench(BenchOptions options)
    {
        WriteHeader(options, "AMQP queue benchmark");

        var factory = new ConnectionFactory
        {
            Uri = new Uri(options.Url),
            DispatchConsumersAsync = true,
            AutomaticRecoveryEnabled = false
        };

        using var connection = factory.CreateConnection();
        using var pubModel = connection.CreateModel();
        using var subModel = connection.CreateModel();

        string suffix = Guid.NewGuid().ToString("N");
        string exchange = $"bench.exchange.{suffix}";
        string throughputQueue = $"bench.queue.throughput.{suffix}";
        string latencyQueue = $"bench.queue.latency.{suffix}";

        pubModel.ExchangeDeclare(exchange, ExchangeType.Direct, durable: false, autoDelete: true);
        pubModel.QueueDeclare(throughputQueue, durable: false, exclusive: false, autoDelete: true);
        pubModel.QueueBind(throughputQueue, exchange, "throughput");

        var received = 0L;
        var throughputConsumer = new AsyncEventingBasicConsumer(subModel);
        throughputConsumer.Received += (_, _) =>
        {
            Interlocked.Increment(ref received);
            return Task.CompletedTask;
        };
        subModel.BasicConsume(throughputQueue, autoAck: true, throughputConsumer);

        await Task.Delay(200);

        var payload = CreatePayload(options.PayloadBytes);
        var throughput = await RunThroughputAsync(
            options,
            () =>
            {
                pubModel.BasicPublish(exchange, "throughput", basicProperties: null, body: payload);
                return Task.CompletedTask;
            },
            () => Task.CompletedTask,
            () => Interlocked.Read(ref received));

        PrintThroughput(throughput);

        using var latencyModel = connection.CreateModel();
        latencyModel.QueueDeclare(latencyQueue, durable: false, exclusive: false, autoDelete: true);
        latencyModel.QueueBind(latencyQueue, exchange, "latency");

        var latencyChannel = Channel.CreateUnbounded<bool>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true
        });

        var latencyConsumer = new AsyncEventingBasicConsumer(latencyModel);
        latencyConsumer.Received += (_, _) =>
        {
            latencyChannel.Writer.TryWrite(true);
            return Task.CompletedTask;
        };
        latencyModel.BasicConsume(latencyQueue, autoAck: true, latencyConsumer);

        await Task.Delay(200);

        Console.WriteLine("--- Latency (publish to consumer delivery) ---");
        var latencies = new List<double>(options.LatencySamples);
        for (int i = 0; i < options.LatencySamples; i++)
        {
            var lsw = Stopwatch.StartNew();
            pubModel.BasicPublish(exchange, "latency", basicProperties: null, body: payload);
            await latencyChannel.Reader.ReadAsync();
            lsw.Stop();
            latencies.Add(lsw.Elapsed.TotalMilliseconds);
        }

        latencyChannel.Writer.TryComplete();
        PrintLatencies(latencies);
        Console.WriteLine("Done.");
    }

    static async Task RunAmqpComparison(BenchOptions options)
    {
        var cosmoUrl = options.CosmoUrl ?? "amqp://guest:guest@127.0.0.1:5675/";
        var rabbitUrl = options.RabbitUrl ?? "amqp://guest:guest@127.0.0.1:5672/";

        Console.WriteLine("AMQP comparison using RabbitMQ.Client");
        Console.WriteLine($"CosmoBroker: {cosmoUrl}");
        Console.WriteLine($"RabbitMQ:    {rabbitUrl}");
        Console.WriteLine($"Messages: {options.Count:N0}, Payload: {options.PayloadBytes} bytes, Publishers: {options.Publishers}");
        Console.WriteLine();

        var cosmo = await RunAmqpComparisonTarget("CosmoBroker", cosmoUrl, options);
        var rabbit = await RunAmqpComparisonTarget("RabbitMQ", rabbitUrl, options);

        PrintAmqpComparison(cosmo, rabbit);
    }

    static async Task RunAmqpComparisonMatrix(BenchOptions options)
    {
        var cosmoUrl = options.CosmoUrl ?? "amqp://guest:guest@127.0.0.1:5675/";
        var rabbitUrl = options.RabbitUrl ?? "amqp://guest:guest@127.0.0.1:5672/";
        var cases = new[]
        {
            new MatrixCase("baseline", options.Count, options.PayloadBytes, options.Publishers, options.LatencySamples, false, false),
            new MatrixCase("concurrency-x4", options.Count, options.PayloadBytes, Math.Max(options.Publishers, 4), options.LatencySamples, false, false),
            new MatrixCase("payload-4k", options.Count, 4096, options.Publishers, options.LatencySamples, false, false),
            new MatrixCase("payload-64k", Math.Max(2_000, Math.Min(options.Count, 10_000)), 65536, options.Publishers, Math.Min(options.LatencySamples, 100), false, false),
            new MatrixCase("durable", Math.Max(5_000, Math.Min(options.Count, 20_000)), options.PayloadBytes, options.Publishers, Math.Min(options.LatencySamples, 200), true, false),
            new MatrixCase("confirms", Math.Max(5_000, Math.Min(options.Count, 20_000)), options.PayloadBytes, options.Publishers, Math.Min(options.LatencySamples, 200), false, true),
            new MatrixCase("durable-confirms", Math.Max(2_000, Math.Min(options.Count, 10_000)), options.PayloadBytes, options.Publishers, Math.Min(options.LatencySamples, 100), true, true)
        };

        Console.WriteLine("AMQP performance matrix using RabbitMQ.Client");
        Console.WriteLine($"CosmoBroker: {cosmoUrl}");
        Console.WriteLine($"RabbitMQ:    {rabbitUrl}");
        Console.WriteLine();

        var results = new List<MatrixResult>(cases.Length);
        foreach (var matrixCase in cases)
        {
            Console.WriteLine($"=== Case: {matrixCase.Name} ===");
            Console.WriteLine($"Messages: {matrixCase.Count:N0}, Payload: {matrixCase.PayloadBytes} bytes, Publishers: {matrixCase.Publishers}, Durable: {matrixCase.Durable}, Confirms: {matrixCase.Confirms}");
            var cosmo = await RunAmqpPerfRepeated("CosmoBroker", cosmoUrl, options, matrixCase);
            var rabbit = await RunAmqpPerfRepeated("RabbitMQ", rabbitUrl, options, matrixCase);
            results.Add(new MatrixResult(matrixCase, cosmo, rabbit));
            Console.WriteLine();
        }

        Console.WriteLine("=== Matrix Summary ===");
        Console.WriteLine("Case | Cosmo msg/s | Rabbit msg/s | Cosmo avg ms | Rabbit avg ms | Verdict");
        foreach (var result in results)
        {
            string verdict = SummarizeMatrixVerdict(result);
            Console.WriteLine($"{result.Case.Name} | {result.Cosmo.Throughput.MessagesPerSecond:N0} | {result.Rabbit.Throughput.MessagesPerSecond:N0} | {result.Cosmo.Latency.AvgMs:F3} | {result.Rabbit.Latency.AvgMs:F3} | {verdict}");
        }
    }

    static async Task<AmqpTargetResult> RunAmqpComparisonTarget(string label, string url, BenchOptions options)
    {
        Console.WriteLine($"--- Running {label} ---");
        var scenarios = await RunAmqpScenarios(label, url);
        var perf = await RunAmqpPerf(label, url, options);
        Console.WriteLine();
        return new AmqpTargetResult(label, url, scenarios, perf);
    }

    static async Task<AmqpPerformanceResult> RunAmqpPerf(string label, string url, BenchOptions options)
    {
        var matrixCase = new MatrixCase("single", options.Count, options.PayloadBytes, options.Publishers, options.LatencySamples, false, false);
        return await RunAmqpPerfRepeated(label, url, options, matrixCase);
    }

    static async Task<AmqpPerformanceResult> RunAmqpPerfRepeated(string label, string url, BenchOptions options, MatrixCase matrixCase)
    {
        int warmupRuns = Math.Max(0, options.WarmupRuns);
        int repeats = Math.Max(1, options.Repeats);
        var runs = new List<AmqpPerformanceResult>(repeats);

        for (int i = 0; i < warmupRuns; i++)
        {
            Console.WriteLine($"{label} warmup {i + 1}/{warmupRuns}");
            await RunAmqpPerfCase(label, url, options, matrixCase);
        }

        for (int i = 0; i < repeats; i++)
        {
            if (repeats > 1)
                Console.WriteLine($"{label} run {i + 1}/{repeats}");

            runs.Add(await RunAmqpPerfCase(label, url, options, matrixCase));
        }

        if (runs.Count == 1)
            return runs[0];

        return SelectMedianRun(runs);
    }

    static async Task<AmqpPerformanceResult> RunAmqpPerfCase(string label, string url, BenchOptions options, MatrixCase matrixCase)
    {
        var factory = new ConnectionFactory
        {
            Uri = new Uri(url),
            DispatchConsumersAsync = true,
            AutomaticRecoveryEnabled = false,
            RequestedHeartbeat = TimeSpan.FromSeconds(5)
        };

        using var connection = factory.CreateConnection();
        using var pubModel = connection.CreateModel();
        using var subModel = connection.CreateModel();

        string suffix = Guid.NewGuid().ToString("N");
        string exchange = $"cmp.exchange.{suffix}";
        string throughputQueue = $"cmp.queue.throughput.{suffix}";
        string latencyQueue = $"cmp.queue.latency.{suffix}";
        bool durable = matrixCase.Durable;
        bool autoDelete = !durable;

        pubModel.ExchangeDeclare(exchange, ExchangeType.Direct, durable: durable, autoDelete: autoDelete);
        pubModel.QueueDeclare(throughputQueue, durable: durable, exclusive: false, autoDelete: autoDelete);
        pubModel.QueueBind(throughputQueue, exchange, "throughput");

        var received = 0L;
        var throughputConsumer = new AsyncEventingBasicConsumer(subModel);
        throughputConsumer.Received += (_, _) =>
        {
            Interlocked.Increment(ref received);
            return Task.CompletedTask;
        };
        subModel.BasicConsume(throughputQueue, autoAck: true, throughputConsumer);

        await Task.Delay(150);

        var payload = CreatePayload(matrixCase.PayloadBytes);
        IBasicProperties? throughputProps = null;
        if (durable)
        {
            throughputProps = pubModel.CreateBasicProperties();
            throughputProps.Persistent = true;
            throughputProps.DeliveryMode = 2;
        }
        if (matrixCase.Confirms)
            pubModel.ConfirmSelect();

        var throughputOptions = options with
        {
            Count = matrixCase.Count,
            Publishers = matrixCase.Publishers,
            PayloadBytes = matrixCase.PayloadBytes,
            LatencySamples = matrixCase.LatencySamples
        };

        var throughput = await RunThroughputAsync(
            throughputOptions,
            () =>
            {
                pubModel.BasicPublish(exchange, "throughput", basicProperties: throughputProps, body: payload);
                if (matrixCase.Confirms)
                    pubModel.WaitForConfirmsOrDie(TimeSpan.FromSeconds(5));
                return Task.CompletedTask;
            },
            () => Task.CompletedTask,
            () => Interlocked.Read(ref received));

        using var latencyModel = connection.CreateModel();
        latencyModel.QueueDeclare(latencyQueue, durable: durable, exclusive: false, autoDelete: autoDelete);
        latencyModel.QueueBind(latencyQueue, exchange, "latency");

        var latencyChannel = Channel.CreateUnbounded<bool>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true
        });

        var latencyConsumer = new AsyncEventingBasicConsumer(latencyModel);
        latencyConsumer.Received += (_, _) =>
        {
            latencyChannel.Writer.TryWrite(true);
            return Task.CompletedTask;
        };
        latencyModel.BasicConsume(latencyQueue, autoAck: true, latencyConsumer);

        await Task.Delay(150);

        IBasicProperties? latencyProps = null;
        if (durable)
        {
            latencyProps = pubModel.CreateBasicProperties();
            latencyProps.Persistent = true;
            latencyProps.DeliveryMode = 2;
        }

        var latencies = new List<double>(matrixCase.LatencySamples);
        for (int i = 0; i < matrixCase.LatencySamples; i++)
        {
            var sw = Stopwatch.StartNew();
            pubModel.BasicPublish(exchange, "latency", basicProperties: latencyProps, body: payload);
            if (matrixCase.Confirms)
                pubModel.WaitForConfirmsOrDie(TimeSpan.FromSeconds(5));
            await latencyChannel.Reader.ReadAsync();
            sw.Stop();
            latencies.Add(sw.Elapsed.TotalMilliseconds);
        }

        latencyChannel.Writer.TryComplete();

        Console.WriteLine($"{label} throughput: {throughput.MessagesPerSecond:N0} msg/sec, dropped {throughput.Dropped:N0}");
        Console.WriteLine($"{label} latency: avg {latencies.Average():F3} ms, p95 {Percentile(latencies, 95):F3} ms");

        return new AmqpPerformanceResult(
            throughput,
            new LatencySummary(
                latencies.Average(),
                Percentile(latencies, 50),
                Percentile(latencies, 95),
                Percentile(latencies, 99)));
    }

    static AmqpPerformanceResult SelectMedianRun(List<AmqpPerformanceResult> runs)
    {
        int medianIndex = runs.Count / 2;
        return runs
            .OrderBy(run => run.Throughput.MessagesPerSecond)
            .ThenBy(run => run.Latency.AvgMs)
            .ElementAt(medianIndex);
    }

    static async Task<List<ScenarioResult>> RunAmqpScenarios(string label, string url)
    {
        var results = new List<ScenarioResult>();
        var factory = new ConnectionFactory
        {
            Uri = new Uri(url),
            DispatchConsumersAsync = true,
            AutomaticRecoveryEnabled = false,
            RequestedHeartbeat = TimeSpan.FromSeconds(5)
        };

        results.Add(await RunScenario("basic_publish_get", async () =>
        {
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();
            string suffix = Guid.NewGuid().ToString("N");
            string exchange = $"cmp.scenario.basic.{suffix}";
            string queue = $"cmp.scenario.basic.q.{suffix}";
            channel.ExchangeDeclare(exchange, ExchangeType.Direct, durable: false, autoDelete: true);
            channel.QueueDeclare(queue, durable: false, exclusive: false, autoDelete: true);
            channel.QueueBind(queue, exchange, "rk");
            channel.BasicPublish(exchange, "rk", basicProperties: null, body: Encoding.UTF8.GetBytes("hello"));
            var result = channel.BasicGet(queue, autoAck: true);
            return result == null
                ? "missing"
                : $"ok body={Encoding.UTF8.GetString(result.Body.ToArray())} redelivered={result.Redelivered}";
        }));

        results.Add(await RunScenario("mandatory_return", async () =>
        {
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();
            string exchange = $"cmp.scenario.return.{Guid.NewGuid():N}";
            channel.ExchangeDeclare(exchange, ExchangeType.Direct, durable: false, autoDelete: true);
            var returned = new TaskCompletionSource<BasicReturnEventArgs>(TaskCreationOptions.RunContinuationsAsynchronously);
            channel.BasicReturn += (_, ea) => returned.TrySetResult(ea);
            channel.BasicPublish(exchange, "missing", mandatory: true, basicProperties: null, body: Encoding.UTF8.GetBytes("miss"));
            var result = await returned.Task.WaitAsync(TimeSpan.FromSeconds(5));
            return $"code={result.ReplyCode} text={result.ReplyText}";
        }));

        results.Add(await RunScenario("passive_missing_queue", async () =>
        {
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();
            try
            {
                channel.QueueDeclarePassive($"cmp.missing.{Guid.NewGuid():N}");
                return "unexpected-success";
            }
            catch (OperationInterruptedException ex)
            {
                return $"close code={ex.ShutdownReason.ReplyCode} text={ex.ShutdownReason.ReplyText}";
            }
        }));

        results.Add(await RunScenario("passive_missing_exchange", async () =>
        {
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();
            try
            {
                channel.ExchangeDeclarePassive($"cmp.missing.exchange.{Guid.NewGuid():N}");
                return "unexpected-success";
            }
            catch (OperationInterruptedException ex)
            {
                return $"close code={ex.ShutdownReason.ReplyCode} text={ex.ShutdownReason.ReplyText}";
            }
        }));

        results.Add(await RunScenario("default_exchange_bind_denied", async () =>
        {
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();
            string queue = $"cmp.default.bind.{Guid.NewGuid():N}";
            channel.QueueDeclare(queue, durable: false, exclusive: false, autoDelete: true);
            try
            {
                channel.QueueBind(queue, string.Empty, "rk");
                return "unexpected-success";
            }
            catch (OperationInterruptedException ex)
            {
                return $"close code={ex.ShutdownReason.ReplyCode} text={ex.ShutdownReason.ReplyText}";
            }
        }));

        results.Add(await RunScenario("unknown_ack_tag", async () =>
        {
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();
            channel.BasicAck(999999, false);
            await Task.Delay(300);
            return channel.CloseReason == null
                ? $"open={channel.IsOpen}"
                : $"close code={channel.CloseReason.ReplyCode} text={channel.CloseReason.ReplyText}";
        }));

        results.Add(await RunScenario("tx_commit_without_select", async () =>
        {
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();
            try
            {
                channel.TxCommit();
                return "unexpected-success";
            }
            catch (OperationInterruptedException ex)
            {
                return $"close code={ex.ShutdownReason.ReplyCode} text={ex.ShutdownReason.ReplyText}";
            }
        }));

        results.Add(await RunScenario("queue_delete_if_unused", async () =>
        {
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();
            string queue = $"cmp.delete.ifunused.{Guid.NewGuid():N}";
            channel.QueueDeclare(queue, durable: false, exclusive: false, autoDelete: true);
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.Received += async (_, _) => await Task.CompletedTask;
            channel.BasicConsume(queue, autoAck: true, consumer: consumer);
            try
            {
                channel.QueueDelete(queue, ifUnused: true, ifEmpty: false);
                return "unexpected-success";
            }
            catch (OperationInterruptedException ex)
            {
                return $"close code={ex.ShutdownReason.ReplyCode} text={ex.ShutdownReason.ReplyText}";
            }
        }));

        results.Add(await RunScenario("default_exchange_unbind_denied", async () =>
        {
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();
            string queue = $"cmp.default.unbind.{Guid.NewGuid():N}";
            channel.QueueDeclare(queue, durable: false, exclusive: false, autoDelete: true);
            try
            {
                channel.QueueUnbind(queue, string.Empty, "rk", null);
                return "unexpected-success";
            }
            catch (OperationInterruptedException ex)
            {
                return $"close code={ex.ShutdownReason.ReplyCode} text={ex.ShutdownReason.ReplyText}";
            }
        }));

        results.Add(await RunScenario("duplicate_consumer_tag", async () =>
        {
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();
            string exchange = $"cmp.duptag.x.{Guid.NewGuid():N}";
            string q1 = $"cmp.duptag.q1.{Guid.NewGuid():N}";
            string q2 = $"cmp.duptag.q2.{Guid.NewGuid():N}";
            channel.ExchangeDeclare(exchange, ExchangeType.Direct, durable: false, autoDelete: true);
            channel.QueueDeclare(q1, durable: false, exclusive: false, autoDelete: true);
            channel.QueueDeclare(q2, durable: false, exclusive: false, autoDelete: true);
            channel.QueueBind(q1, exchange, "q1");
            channel.QueueBind(q2, exchange, "q2");

            var first = new AsyncEventingBasicConsumer(channel);
            first.Received += async (_, _) => await Task.CompletedTask;
            channel.BasicConsume(q1, autoAck: true, consumerTag: "dup-tag", noLocal: false, exclusive: false, arguments: null, consumer: first);

            var second = new AsyncEventingBasicConsumer(channel);
            second.Received += async (_, _) => await Task.CompletedTask;
            try
            {
                channel.BasicConsume(q2, autoAck: true, consumerTag: "dup-tag", noLocal: false, exclusive: false, arguments: null, consumer: second);
                return "unexpected-success";
            }
            catch (OperationInterruptedException ex)
            {
                return $"close code={ex.ShutdownReason.ReplyCode} text={ex.ShutdownReason.ReplyText}";
            }
        }));

        results.Add(await RunScenario("publisher_confirms", async () =>
        {
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();
            string exchange = $"cmp.confirm.x.{Guid.NewGuid():N}";
            string queue = $"cmp.confirm.q.{Guid.NewGuid():N}";
            channel.ExchangeDeclare(exchange, ExchangeType.Direct, durable: false, autoDelete: true);
            channel.QueueDeclare(queue, durable: false, exclusive: false, autoDelete: true);
            channel.QueueBind(queue, exchange, "rk");
            channel.ConfirmSelect();
            channel.BasicPublish(exchange, "rk", basicProperties: null, body: Encoding.UTF8.GetBytes("confirm"));
            bool confirmed = channel.WaitForConfirms(TimeSpan.FromSeconds(5));
            var result = channel.BasicGet(queue, autoAck: true);
            return $"confirmed={confirmed} delivered={(result != null)}";
        }));

        results.Add(await RunScenario("tx_commit_roundtrip", async () =>
        {
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();
            string exchange = $"cmp.tx.commit.x.{Guid.NewGuid():N}";
            string queue = $"cmp.tx.commit.q.{Guid.NewGuid():N}";
            channel.ExchangeDeclare(exchange, ExchangeType.Direct, durable: false, autoDelete: true);
            channel.QueueDeclare(queue, durable: false, exclusive: false, autoDelete: true);
            channel.QueueBind(queue, exchange, "rk");
            channel.TxSelect();
            channel.BasicPublish(exchange, "rk", basicProperties: null, body: Encoding.UTF8.GetBytes("tx-commit"));
            channel.TxCommit();
            var result = channel.BasicGet(queue, autoAck: true);
            return result == null ? "missing" : $"ok body={Encoding.UTF8.GetString(result.Body.ToArray())}";
        }));

        results.Add(await RunScenario("tx_rollback_roundtrip", async () =>
        {
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();
            string exchange = $"cmp.tx.rollback.x.{Guid.NewGuid():N}";
            string queue = $"cmp.tx.rollback.q.{Guid.NewGuid():N}";
            channel.ExchangeDeclare(exchange, ExchangeType.Direct, durable: false, autoDelete: true);
            channel.QueueDeclare(queue, durable: false, exclusive: false, autoDelete: true);
            channel.QueueBind(queue, exchange, "rk");
            channel.TxSelect();
            channel.BasicPublish(exchange, "rk", basicProperties: null, body: Encoding.UTF8.GetBytes("tx-rollback"));
            channel.TxRollback();
            var result = channel.BasicGet(queue, autoAck: true);
            return result == null ? "empty" : $"unexpected body={Encoding.UTF8.GetString(result.Body.ToArray())}";
        }));

        results.Add(await RunScenario("exclusive_queue_other_connection", async () =>
        {
            using var ownerConnection = factory.CreateConnection();
            using var ownerChannel = ownerConnection.CreateModel();
            var declared = ownerChannel.QueueDeclare(string.Empty, durable: false, exclusive: true, autoDelete: true);

            using var otherConnection = factory.CreateConnection();
            using var otherChannel = otherConnection.CreateModel();
            try
            {
                otherChannel.QueueDeclarePassive(declared.QueueName);
                return "unexpected-success";
            }
            catch (OperationInterruptedException ex)
            {
                return $"close code={ex.ShutdownReason.ReplyCode} text={ex.ShutdownReason.ReplyText}";
            }
        }));

        results.Add(await RunScenario("basic_get_redelivery_after_channel_close", async () =>
        {
            using var connection = factory.CreateConnection();
            using var firstChannel = connection.CreateModel();
            using var secondChannel = connection.CreateModel();
            string exchange = $"cmp.getclose.x.{Guid.NewGuid():N}";
            string queue = $"cmp.getclose.q.{Guid.NewGuid():N}";
            firstChannel.ExchangeDeclare(exchange, ExchangeType.Direct, durable: false, autoDelete: true);
            firstChannel.QueueDeclare(queue, durable: false, exclusive: false, autoDelete: true);
            firstChannel.QueueBind(queue, exchange, "rk");
            firstChannel.BasicPublish(exchange, "rk", basicProperties: null, body: Encoding.UTF8.GetBytes("close-requeue"));

            var first = firstChannel.BasicGet(queue, autoAck: false);
            if (first == null)
                return "first-missing";
            firstChannel.Close();

            var second = secondChannel.BasicGet(queue, autoAck: true);
            if (second == null)
                return "second-missing";
            return $"redelivered={second.Redelivered} body={Encoding.UTF8.GetString(second.Body.ToArray())}";
        }));

        results.Add(await RunScenario("auth_wrong_password", async () =>
        {
            var deniedFactory = new ConnectionFactory
            {
                Uri = new Uri(url),
                UserName = "guest",
                Password = "definitely-wrong"
            };

            try
            {
                using var _ = deniedFactory.CreateConnection();
                return "unexpected-success";
            }
            catch (BrokerUnreachableException ex) when (ex.InnerException is OperationInterruptedException op)
            {
                return $"close code={op.ShutdownReason.ReplyCode} text={op.ShutdownReason.ReplyText}";
            }
            catch (OperationInterruptedException ex)
            {
                return $"close code={ex.ShutdownReason.ReplyCode} text={ex.ShutdownReason.ReplyText}";
            }
            catch (Exception ex)
            {
                return $"exception {ex.GetType().Name}: {ex.Message}";
            }
        }));

        Console.WriteLine($"{label} scenarios: {results.Count} complete");
        return results;
    }

    static async Task<ScenarioResult> RunScenario(string name, Func<Task<string>> action)
    {
        var sw = Stopwatch.StartNew();
        try
        {
            string value = await action();
            sw.Stop();
            return new ScenarioResult(name, !value.StartsWith("unexpected-success", StringComparison.Ordinal), value, sw.Elapsed.TotalMilliseconds);
        }
        catch (Exception ex)
        {
            sw.Stop();
            return new ScenarioResult(name, false, $"exception {ex.GetType().Name}: {ex.Message}", sw.Elapsed.TotalMilliseconds);
        }
    }

    static void PrintAmqpComparison(AmqpTargetResult cosmo, AmqpTargetResult rabbit)
    {
        Console.WriteLine("=== Scenario Diff ===");
        var rabbitByName = rabbit.Scenarios.ToDictionary(x => x.Name, StringComparer.Ordinal);
        int anomalies = 0;
        foreach (var left in cosmo.Scenarios)
        {
            if (!rabbitByName.TryGetValue(left.Name, out var right))
            {
                Console.WriteLine($"ANOMALY {left.Name}: missing on RabbitMQ side");
                anomalies++;
                continue;
            }

            string leftNormalized = NormalizeScenarioValue(left.Value);
            string rightNormalized = NormalizeScenarioValue(right.Value);
            bool different = !string.Equals(leftNormalized, rightNormalized, StringComparison.Ordinal);
            var prefix = different ? "ANOMALY" : "OK";
            Console.WriteLine($"{prefix} {left.Name}");
            Console.WriteLine($"  CosmoBroker: {left.Value}");
            Console.WriteLine($"  RabbitMQ:    {right.Value}");
            if (different)
                anomalies++;
        }

        Console.WriteLine();
        Console.WriteLine("=== Performance Diff ===");
        Console.WriteLine($"Throughput: CosmoBroker {cosmo.Performance.Throughput.MessagesPerSecond:N0} msg/sec vs RabbitMQ {rabbit.Performance.Throughput.MessagesPerSecond:N0} msg/sec");
        Console.WriteLine($"Drop rate:  CosmoBroker {cosmo.Performance.Throughput.DropRate:F2}% vs RabbitMQ {rabbit.Performance.Throughput.DropRate:F2}%");
        Console.WriteLine($"Latency avg: CosmoBroker {cosmo.Performance.Latency.AvgMs:F3} ms vs RabbitMQ {rabbit.Performance.Latency.AvgMs:F3} ms");
        Console.WriteLine($"Latency p95: CosmoBroker {cosmo.Performance.Latency.P95Ms:F3} ms vs RabbitMQ {rabbit.Performance.Latency.P95Ms:F3} ms");
        Console.WriteLine();
        Console.WriteLine($"Anomalies detected: {anomalies}");
    }

    static string NormalizeScenarioValue(string value)
    {
        var normalized = value;
        normalized = Regex.Replace(normalized, @"cmp\.missing\.exchange\.[0-9a-f]+", "cmp.missing.exchange.<id>", RegexOptions.IgnoreCase);
        normalized = Regex.Replace(normalized, @"cmp\.missing\.[0-9a-f]+", "cmp.missing.<id>", RegexOptions.IgnoreCase);
        normalized = Regex.Replace(normalized, @"amq\.gen-[A-Za-z0-9_-]+", "amq.gen-<id>", RegexOptions.IgnoreCase);
        normalized = Regex.Replace(normalized, @"queue 'cmp\.delete\.ifunused\.[0-9a-f]+'", "queue 'cmp.delete.ifunused.<id>'", RegexOptions.IgnoreCase);

        const string resourceLockedPrefix = "RESOURCE_LOCKED - cannot obtain exclusive access to locked queue 'amq.gen-<id>' in vhost '/'";
        if (normalized.StartsWith("close code=405 text=RESOURCE_LOCKED - cannot obtain exclusive access to locked queue 'amq.gen-<id>' in vhost '/'", StringComparison.Ordinal))
            return $"close code=405 text={resourceLockedPrefix}";

        return normalized;
    }

    static string SummarizeMatrixVerdict(MatrixResult result)
    {
        double throughputRatio = result.Rabbit.Throughput.MessagesPerSecond == 0
            ? 0
            : result.Cosmo.Throughput.MessagesPerSecond / result.Rabbit.Throughput.MessagesPerSecond;
        double latencyRatio = result.Rabbit.Latency.AvgMs == 0
            ? 0
            : result.Cosmo.Latency.AvgMs / result.Rabbit.Latency.AvgMs;

        if (throughputRatio >= 1.10 && latencyRatio <= 0.90)
            return "Cosmo ahead";
        if (throughputRatio <= 0.90 && latencyRatio >= 1.10)
            return "Rabbit ahead";
        if (throughputRatio <= 0.75)
            return "Optimize throughput";
        if (latencyRatio >= 1.25)
            return "Optimize latency";
        return "Comparable";
    }

    static async Task<ThroughputResult> RunThroughputAsync(
        BenchOptions options,
        Func<Task> publishOnce,
        Func<Task> flushAsync,
        Func<long> received)
    {
        Console.WriteLine("--- Throughput (1 producer group, 1 consumer) ---");

        var sw = Stopwatch.StartNew();
        var publishers = new List<Task>(options.Publishers);
        int perPublisher = options.Count / options.Publishers;
        int remainder = options.Count % options.Publishers;

        for (int publisherIndex = 0; publisherIndex < options.Publishers; publisherIndex++)
        {
            int toSend = perPublisher + (publisherIndex == 0 ? remainder : 0);
            publishers.Add(Task.Run(async () =>
            {
                for (int i = 0; i < toSend; i++)
                    await publishOnce();
            }));
        }

        await Task.WhenAll(publishers);
        await flushAsync();
        sw.Stop();

        var deadline = DateTime.UtcNow.AddMilliseconds(options.ReceiveTimeoutMs);
        while (received() < options.Count && DateTime.UtcNow < deadline)
            await Task.Delay(50);

        long receivedCount = received();
        int dropped = options.Count - (int)receivedCount;
        return new ThroughputResult(
            sw.Elapsed.TotalSeconds,
            options.Count / sw.Elapsed.TotalSeconds,
            receivedCount,
            dropped,
            options.Count == 0 ? 0 : (double)dropped / options.Count * 100.0);
    }

    static async Task<JsonDocument> RmqRequestAsync(CosmoClient client, string subject, object requestBody)
    {
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(requestBody);
        var reply = await client.RequestAsync(subject, payload, TimeSpan.FromSeconds(5));
        return JsonDocument.Parse(reply.GetStringData());
    }

    static Task EnsureOkAsync(JsonDocument response)
    {
        if (!response.RootElement.TryGetProperty("ok", out var ok) || !ok.GetBoolean())
        {
            string error = response.RootElement.TryGetProperty("error", out var err)
                ? err.GetString() ?? "unknown error"
                : "unknown error";
            throw new InvalidOperationException($"RabbitMQ API call failed: {error}");
        }

        return Task.CompletedTask;
    }

    static void WriteHeader(BenchOptions options, string workload)
    {
        Console.WriteLine($"Benchmarking {options.Label} at {options.Url}");
        Console.WriteLine($"Mode: {options.ResolveMode()}");
        Console.WriteLine($"Workload: {workload}");
        Console.WriteLine($"Messages: {options.Count:N0}, Payload: {options.PayloadBytes} bytes, Publishers: {options.Publishers}");
    }

    static void PrintThroughput(ThroughputResult result)
    {
        Console.WriteLine($"Sent in {result.ElapsedSeconds:F2}s ({result.MessagesPerSecond:N0} msg/sec)");
        Console.WriteLine($"Received: {result.Received:N0}, Dropped: {result.Dropped:N0} ({result.DropRate:F2}%)");
    }

    static void PrintLatencies(List<double> latencies)
    {
        latencies.Sort();
        Console.WriteLine($"Avg RTT: {latencies.Average():F3} ms");
        Console.WriteLine($"P50 RTT: {Percentile(latencies, 50):F3} ms");
        Console.WriteLine($"P95 RTT: {Percentile(latencies, 95):F3} ms");
        Console.WriteLine($"P99 RTT: {Percentile(latencies, 99):F3} ms");
    }

    static byte[] CreatePayload(int size)
    {
        var payload = new byte[size];
        Random.Shared.NextBytes(payload);
        return payload;
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

    static async Task RunRabbitStreamBench(BenchOptions options)
    {
        WriteHeader(options, "RabbitMQ Stream (StreamSystem)");

        var systemConfig = new StreamSystemConfig
        {
            UserName = "guest",
            Password = "guest",
            Endpoints = new List<EndPoint> { ParseEndpoint(options.StreamUrl) }
        };

        await using var system = await StreamSystem.Create(systemConfig);
        var streamName = $"bench.stream.{Guid.NewGuid():N}";
        await system.CreateStream(new StreamSpec(streamName));

        if (options.Publishers <= 0)
            throw new InvalidOperationException("Stream benchmark requires at least one publisher.");

        var received = 0L;
        var consumer = await Consumer.Create(new ConsumerConfig(system, streamName)
        {
            Reference = $"bench-stream-consumer-{Guid.NewGuid():N}",
            OffsetSpec = new OffsetTypeFirst(),
            MessageHandler = (_, _, _, _) =>
            {
                Interlocked.Increment(ref received);
                return Task.CompletedTask;
            }
        });

        var payload = CreatePayload(options.PayloadBytes);
        var sw = Stopwatch.StartNew();
        var producers = new List<Task>(options.Publishers);
        var perProducer = options.Count / options.Publishers;
        var remainder = options.Count % options.Publishers;

        for (int i = 0; i < options.Publishers; i++)
        {
            var publisherCount = perProducer + (i < remainder ? 1 : 0);
            producers.Add(Task.Run(async () =>
            {
                var producer = await Producer.Create(new ProducerConfig(system, streamName));
                try
                {
                    for (int j = 0; j < publisherCount; j++)
                        await producer.Send(new Message(payload));
                }
                finally
                {
                    await producer.Close();
                }
            }));
        }

        await Task.WhenAll(producers);
        while (Interlocked.Read(ref received) < options.Count)
            await Task.Delay(10);

        sw.Stop();
        var throughput = new ThroughputResult(
            sw.Elapsed.TotalSeconds,
            options.Count / sw.Elapsed.TotalSeconds,
            Interlocked.Read(ref received),
            0,
            0);

        PrintThroughput(throughput);

        await consumer.Close();
        await system.Close();
    }

    static IPEndPoint ParseEndpoint(string url)
    {
        var uri = new Uri(url);
        var host = uri.Host;
        var port = uri.Port > 0 ? uri.Port : 5552;
        var addresses = Dns.GetHostAddresses(host);
        var address = addresses.FirstOrDefault(a => a.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
            ?? addresses.First();
        return new IPEndPoint(address, port);
    }
}

enum BenchMode
{
    Nats,
    CosmoClient,
    CosmoRabbitMq,
    RabbitMq,
    Stream,
    CompareAmqp,
    CompareAmqpMatrix
}

sealed record ThroughputResult(
    double ElapsedSeconds,
    double MessagesPerSecond,
    long Received,
    int Dropped,
    double DropRate);

sealed record LatencySummary(
    double AvgMs,
    double P50Ms,
    double P95Ms,
    double P99Ms);

sealed record ScenarioResult(
    string Name,
    bool Success,
    string Value,
    double ElapsedMs);

sealed record AmqpPerformanceResult(
    ThroughputResult Throughput,
    LatencySummary Latency);

sealed record AmqpTargetResult(
    string Label,
    string Url,
    List<ScenarioResult> Scenarios,
    AmqpPerformanceResult Performance);

sealed record MatrixCase(
    string Name,
    int Count,
    int PayloadBytes,
    int Publishers,
    int LatencySamples,
    bool Durable,
    bool Confirms);

sealed record MatrixResult(
    MatrixCase Case,
    AmqpPerformanceResult Cosmo,
    AmqpPerformanceResult Rabbit);

sealed record BenchOptions
{
    public string Url { get; init; } = "nats://localhost:4222";
    public string Label { get; init; } = "Target";
    public string? Mode { get; init; }
    public string? CosmoUrl { get; init; }
    public string? RabbitUrl { get; init; }
    public string StreamUrl { get; init; } = "amqp://localhost:5552";
    public int Count { get; init; } = 500_000;
    public int PayloadBytes { get; init; } = 256;
    public int Publishers { get; init; } = 1;
    public int LatencySamples { get; init; } = 1_000;
    public int Repeats { get; init; } = 1;
    public int WarmupRuns { get; init; } = 1;
    public int ReceiveTimeoutMs { get; init; } = 30_000;
    public int SubPendingCapacity { get; init; } = 1_000_000;
    public string? Profile { get; init; }

    public BenchMode ResolveMode()
    {
        if (!string.IsNullOrWhiteSpace(Mode))
        {
            return Mode.Trim().ToLowerInvariant() switch
            {
                "nats" => BenchMode.Nats,
                "cosmo" or "cosmo-client" or "cosmobroker" or "cosmobroker-client" => BenchMode.CosmoClient,
                "cosmo-rmq" or "cosmo-rabbitmq" or "cosmobroker-rmq" => BenchMode.CosmoRabbitMq,
                "rabbitmq" or "amqp" => BenchMode.RabbitMq,
                "stream" or "rmq-stream" or "rabbitmq-stream" => BenchMode.Stream,
                "compare-amqp" or "amqp-compare" or "compare" => BenchMode.CompareAmqp,
                "compare-amqp-matrix" or "amqp-matrix" or "compare-matrix" => BenchMode.CompareAmqpMatrix,
                _ => throw new InvalidOperationException($"Unknown benchmark mode '{Mode}'.")
            };
        }

        if (Label.Contains("CosmoBroker.Client", StringComparison.OrdinalIgnoreCase))
            return BenchMode.CosmoClient;
        if (Label.Contains("RabbitMQ", StringComparison.OrdinalIgnoreCase))
            return BenchMode.RabbitMq;
        return BenchMode.Nats;
    }

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
                case "--mode":
                    opts = opts with { Mode = val };
                    i++;
                    break;
                case "--cosmo-url":
                    opts = opts with { CosmoUrl = val };
                    i++;
                    break;
                case "--stream-url":
                    opts = opts with { StreamUrl = val };
                    i++;
                    break;
                case "--rabbit-url":
                    opts = opts with { RabbitUrl = val };
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
                case "--repeats":
                    if (int.TryParse(val, out var repeats) && repeats > 0)
                        opts = opts with { Repeats = repeats };
                    i++;
                    break;
                case "--warmup-runs":
                    if (int.TryParse(val, out var warmups) && warmups >= 0)
                        opts = opts with { WarmupRuns = warmups };
                    i++;
                    break;
                case "--profile":
                    opts = opts with { Profile = val };
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

    public BenchOptions ApplyProfileDefaults()
    {
        if (string.IsNullOrWhiteSpace(Profile))
            return this;

        return Profile.Trim().ToLowerInvariant() switch
        {
            "quick" => this with
            {
                Count = 1_000,
                LatencySamples = 5,
                Repeats = 1,
                WarmupRuns = 0,
                ReceiveTimeoutMs = 15_000
            },
            "stable" => this with
            {
                Count = 10_000,
                LatencySamples = 25,
                Repeats = 3,
                WarmupRuns = 1,
                ReceiveTimeoutMs = 30_000
            },
            _ => this
        };
    }

}
