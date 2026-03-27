using System;
using System.IO;
using System.Runtime;
using System.Threading;
using System.Threading.Tasks;
using CosmoBroker;
using CosmoBroker.Persistence;
using CosmoBroker.Services;

namespace CosmoBroker.Server;

class Program
{
    static async Task Main(string[] args)
    {
        // Pre-warm the thread pool to avoid the ~500 ms/thread spin-up latency under
        // connection bursts. Default minimum is Environment.ProcessorCount, which is
        // too low for a broker that gets hundreds of simultaneous connects.
        int cpus = Environment.ProcessorCount;
        ThreadPool.SetMinThreads(workerThreads: cpus * 8, completionPortThreads: cpus * 4);

        // SustainedLowLatency asks the GC to keep Gen2 collections rare,
        // trading peak memory for lower tail latency on the hot path.
        GCSettings.LatencyMode = GCLatencyMode.SustainedLowLatency;

        Console.WriteLine("Starting CosmoBroker Standalone...");

        var port = 4222;
        var amqpPort = 0;
        var streamPort = 0;
        var monitorPort = 8222;
        var enableNats = true;
        var enableAmqp = false;
        var enableStream = false;
        MessageRepository? repo = null;

        static bool TryParseBool(string? value, out bool result)
        {
            if (bool.TryParse(value, out result))
                return true;

            if (string.Equals(value, "1", StringComparison.Ordinal))
            {
                result = true;
                return true;
            }

            if (string.Equals(value, "0", StringComparison.Ordinal))
            {
                result = false;
                return true;
            }

            result = false;
            return false;
        }

        var envPort = Environment.GetEnvironmentVariable("COSMOBROKER_PORT");
        if (!string.IsNullOrWhiteSpace(envPort) && int.TryParse(envPort, out var envParsed))
            port = envParsed;
        else if (args.Length > 0 && int.TryParse(args[0], out var argPort))
            port = argPort;

        var envMonitor = Environment.GetEnvironmentVariable("COSMOBROKER_MONITOR_PORT");
        if (!string.IsNullOrWhiteSpace(envMonitor) && int.TryParse(envMonitor, out var envMonParsed))
            monitorPort = envMonParsed;
        else if (args.Length > 1 && int.TryParse(args[1], out var argMonPort))
            monitorPort = argMonPort;

        var envAmqpPort = Environment.GetEnvironmentVariable("COSMOBROKER_AMQP_PORT");
        if (!string.IsNullOrWhiteSpace(envAmqpPort) && int.TryParse(envAmqpPort, out var envAmqpParsed))
            amqpPort = envAmqpParsed;
        else if (args.Length > 2 && int.TryParse(args[2], out var argAmqpPort))
            amqpPort = argAmqpPort;

        var envStreamPort = Environment.GetEnvironmentVariable("COSMOBROKER_STREAM_PORT");
        if (!string.IsNullOrWhiteSpace(envStreamPort) && int.TryParse(envStreamPort, out var envStreamParsed))
            streamPort = envStreamParsed;
        else if (args.Length > 3 && int.TryParse(args[3], out var argStreamPort))
            streamPort = argStreamPort;

        var streamAdvertisedHost = Environment.GetEnvironmentVariable("COSMOBROKER_STREAM_ADVERTISED_HOST");

        var envEnableNats = Environment.GetEnvironmentVariable("COSMOBROKER_ENABLE_NATS");
        if (!string.IsNullOrWhiteSpace(envEnableNats) && TryParseBool(envEnableNats, out var parsedEnableNats))
            enableNats = parsedEnableNats;

        var envEnableAmqp = Environment.GetEnvironmentVariable("COSMOBROKER_ENABLE_AMQP");
        if (!string.IsNullOrWhiteSpace(envEnableAmqp) && TryParseBool(envEnableAmqp, out var parsedEnableAmqp))
            enableAmqp = parsedEnableAmqp;

        var envEnableStream = Environment.GetEnvironmentVariable("COSMOBROKER_ENABLE_RMQ_STREAM");
        if (!string.IsNullOrWhiteSpace(envEnableStream) && TryParseBool(envEnableStream, out var parsedEnableStream))
            enableStream = parsedEnableStream;

        if (!enableNats)
            port = 0;

        if (enableAmqp && amqpPort <= 0)
            amqpPort = 5672;

        if (!enableAmqp)
            amqpPort = 0;

        if (enableStream && streamPort <= 0)
            streamPort = 5552;

        if (!enableStream)
            streamPort = 0;

        var configPath = Environment.GetEnvironmentVariable("COSMOBROKER_CONFIG");
        BrokerConfig? config = null;
        if (!string.IsNullOrWhiteSpace(configPath) && File.Exists(configPath))
            config = ConfigParser.LoadFile(configPath);

        var repoConnection = Environment.GetEnvironmentVariable("COSMOBROKER_REPO") ??
                             config?.RepoConnectionString;
        if (!string.IsNullOrWhiteSpace(repoConnection))
            repo = new MessageRepository(repoConnection);

        var server = new BrokerServer(
            port: port,
            amqpPort: amqpPort,
            streamPort: streamPort,
            repo: repo,
            monitorPort: monitorPort,
            streamAdvertisedHost: streamAdvertisedHost);
        var cts = new CancellationTokenSource();

        // Handle both Ctrl+C (SIGINT) and SIGTERM (docker stop / systemd)
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };
        AppDomain.CurrentDomain.ProcessExit += (_, _) => cts.Cancel();

        try
        {
            await server.StartAsync(cts.Token);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[CosmoBroker] Failed to start: {ex.Message}");
            Environment.Exit(1);
        }

        var natsStatus = port > 0 ? port.ToString() : "disabled";
        var amqpStatus = amqpPort > 0 ? amqpPort.ToString() : "disabled";
        var streamStatus = streamPort > 0 ? streamPort.ToString() : "disabled";
        Console.WriteLine($"[CosmoBroker] Server is running. NATS: {natsStatus}, AMQP: {amqpStatus}, STREAM: {streamStatus}. Press Ctrl+C to stop.");

        try
        {
            await Task.Delay(-1, cts.Token);
        }
        catch (OperationCanceledException) { }

        Console.WriteLine("[CosmoBroker] Shutting down gracefully...");

        // Lame-duck: notify clients to reconnect before closing the listen socket
        server.EnterLameDuckMode();
        await Task.Delay(500); // brief window for clients to reconnect

        using var shutdownTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            await server.DisposeAsync().AsTask().WaitAsync(shutdownTimeout.Token);
        }
        catch (OperationCanceledException)
        {
            Console.Error.WriteLine("[CosmoBroker] Shutdown timed out after 10s, forcing exit.");
        }

        Console.WriteLine("[CosmoBroker] Stopped.");
    }
}
