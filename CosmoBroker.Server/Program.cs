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
        
        var server = new BrokerServer(port: 4222);
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
