using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;

namespace CosmoBroker.Services;

public class LeafnodeManager
{
    private readonly BrokerServer _server;
    private readonly TopicTree _topicTree;
    private readonly List<Uri> _remoteHubs = new();
    private readonly ConcurrentDictionary<Uri, BrokerConnection> _connections = new();

    public LeafnodeManager(BrokerServer server, TopicTree topicTree)
    {
        _server = server;
        _topicTree = topicTree;
    }

    public void AddHub(string uri)
    {
        _remoteHubs.Add(new Uri(uri));
    }

    public async Task StartAsync(CancellationToken ct)
    {
        foreach (var hub in _remoteHubs)
        {
            _ = ConnectToHubAsync(hub, ct);
        }
    }

    private async Task ConnectToHubAsync(Uri hub, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                await socket.ConnectAsync(hub.Host, hub.Port, ct);
                socket.NoDelay = true;

                Stream stream = new NetworkStream(socket, ownsSocket: true);
                
                // If Hub requires TLS, we'd wrap in SslStream here (Stubbed)

                // Leafnode connection acts as a standard NATS client but with bridge permissions
                var connection = new BrokerConnection(stream, hub.ToString(), _topicTree, null, null, null, _server);
                _connections[hub] = connection;

                Console.WriteLine($"[Leafnode] Connected to hub {hub}");

                // Bridge local subscriptions to the hub
                await SyncLocalSubsToHub(connection);

                await connection.RunAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Leafnode] Hub connection error {hub}: {ex.Message}. Retrying...");
                await Task.Delay(5000, ct);
            }
        }
    }

    private async Task SyncLocalSubsToHub(BrokerConnection hubConn)
    {
        // Tell the hub we want ALL subjects our local clients are interested in
        var subs = _server.GetLocalSubscriptions();
        foreach (var sub in subs)
        {
            await hubConn.SendRawAsync($"SUB {sub.Subject} {sub.SID}\r\n");
        }
    }

    public void NotifyLocalSub(string subject, string sid)
    {
        foreach (var conn in _connections.Values)
        {
            _ = conn.SendRawAsync($"SUB {subject} {sid}\r\n");
        }
    }
}
