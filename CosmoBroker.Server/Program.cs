using System;
using System.Threading;
using System.Threading.Tasks;
using CosmoBroker;

namespace CosmoBroker.Server;

class Program
{
    static async Task Main(string[] args)
    {
        Console.WriteLine("Starting CosmoBroker Standalone...");

        var port = 4222;
        var monitorPort = 8222;

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

        var server = new BrokerServer(port: port, monitorPort: monitorPort);
        var cts = new CancellationTokenSource();

        Console.CancelKeyPress += (s, e) => {
            e.Cancel = true;
            cts.Cancel();
        };

        await server.StartAsync(cts.Token);
        
        Console.WriteLine("Server is running. Press Ctrl+C to stop.");
        
        try 
        {
            await Task.Delay(-1, cts.Token);
        }
        catch (OperationCanceledException) { }
        
        Console.WriteLine("Shutting down...");
        await server.DisposeAsync();
    }
}
