using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using System.Collections.Generic;
using System.Linq;
using System.IO;

namespace CosmoBroker.Tests;

public class TestBase : IAsyncDisposable
{
    protected readonly BrokerServer Server;
    protected readonly int Port;
    protected readonly int MonitorPort;
    protected readonly CancellationTokenSource Cts = new();
    protected readonly ITestOutputHelper Output;

    public TestBase(ITestOutputHelper output, Auth.IAuthenticator? auth = null)
    {
        Output = output;
        MonitorPort = GetFreePort();
        // Pass port=0 so the OS assigns an ephemeral port at construction time, eliminating
        // the TOCTOU race between GetFreePort() releasing the port and StartAsync() binding it.
        Server = new BrokerServer(0, authenticator: auth, monitorPort: MonitorPort);
        Port = Server.BoundPort;
    }

    protected static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    public virtual async ValueTask DisposeAsync()
    {
        Cts.Cancel();
        await Server.DisposeAsync();
    }

    protected async Task<TestClient> CreateClientAsync()
    {
        var client = new TcpClient();
        await client.ConnectAsync("127.0.0.1", Port);
        var testClient = new TestClient(client, Output);
        // Read initial INFO
        await testClient.ReadResponseAsync(); 
        return testClient;
    }

    protected async Task<TestClient> CreateClientAsync(int port, ITestOutputHelper output)
    {
        var client = new TcpClient();
        await client.ConnectAsync("127.0.0.1", port);
        var testClient = new TestClient(client, output);
        await testClient.ReadResponseAsync();
        return testClient;
    }

    protected async Task AuthenticateAsync(TestClient client, string user, string pass)
    {
        var connect = new {
            user = user,
            pass = pass,
            verbose = false,
            protocol = 1
        };
        string json = System.Text.Json.JsonSerializer.Serialize(connect);
        await client.SendAsync($"CONNECT {json}\r\n");
        string resp = await client.ReadResponseAsync();
        Assert.Contains("+OK", resp);
    }
}

public class TestClient : IDisposable
{
    private readonly TcpClient _client;
    private readonly Stream _stream;
    private readonly ITestOutputHelper _output;

    public TestClient(TcpClient client, ITestOutputHelper output)
    {
        _client = client;
        _stream = client.GetStream();
        _output = output;
    }

    public async Task SendAsync(string cmd)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(cmd);
        await _stream.WriteAsync(bytes);
        await _stream.FlushAsync();
    }

    public async Task<string> ReadResponseAsync(int timeoutMs = 2000)
    {
        byte[] buffer = new byte[16384];
        var delay = 20;
        int totalDelayed = 0;

        while (!_client.GetStream().DataAvailable && totalDelayed < timeoutMs)
        {
            await Task.Delay(delay);
            totalDelayed += delay;
        }

        if (!_client.GetStream().DataAvailable)
        {
            _output.WriteLine("[Client Read] TIMEOUT - No data available");
            return string.Empty;
        }

        using var ms = new MemoryStream();
        int idleDelay = 0;
        while (idleDelay < 100)
        {
            while (_client.GetStream().DataAvailable)
            {
                int read = await _stream.ReadAsync(buffer);
                if (read <= 0) break;
                ms.Write(buffer, 0, read);
                idleDelay = 0;
            }

            if (_client.GetStream().DataAvailable) continue;
            await Task.Delay(10);
            idleDelay += 10;
        }

        var result = Encoding.UTF8.GetString(ms.ToArray());
        _output.WriteLine($"[Client Read] {result.Replace("\r", "\\r").Replace("\n", "\\n")}");
        return result;
    }

    public void Dispose()
    {
        _stream.Dispose();
        _client.Dispose();
    }
}
