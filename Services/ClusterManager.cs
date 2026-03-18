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

public class ClusterManager
{
    private readonly BrokerServer _server;
    private readonly TopicTree _topicTree;
    private readonly List<IPEndPoint> _peers = new();
    private readonly ConcurrentDictionary<IPEndPoint, BrokerConnection> _connections = new();

    public ClusterManager(BrokerServer server, TopicTree topicTree)
    {
        _server = server;
        _topicTree = topicTree;
    }

    public void AddPeer(IPEndPoint endPoint)
    {
        _peers.Add(endPoint);
    }

    public async Task StartAsync(CancellationToken ct)
    {
        foreach (var peer in _peers)
        {
            _ = ConnectToPeerAsync(peer, ct);
        }
    }

    private async Task ConnectToPeerAsync(IPEndPoint peer, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                await socket.ConnectAsync(peer, ct);
                socket.NoDelay = true;

                Stream stream = new NetworkStream(socket, ownsSocket: true);

                // Create a connection marked as a Route
                var connection = new BrokerConnection(stream, peer.ToString(), _topicTree, null, null, null, _server);
                connection.IsRoute = true;
                _connections[peer] = connection;

                Console.WriteLine($"[ClusterManager] Connected to peer {peer}");

                // Sync all local subscriptions to the new peer
                await SyncSubscriptionsAsync(connection);

                // Run the connection loop
                await connection.RunAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ClusterManager] Error connecting to {peer}: {ex.Message}. Retrying in 5s...");
                try { await Task.Delay(5000, ct); } catch { break; }
            }
            finally
            {
                _connections.TryRemove(peer, out _);
            }
        }
    }

    private async Task SyncSubscriptionsAsync(BrokerConnection peerConnection)
    {
        var subs = _server.GetLocalSubscriptions();
        foreach (var sub in subs)
        {
            var cmd = string.IsNullOrEmpty(sub.QueueGroup) 
                ? $"SUB {sub.Subject} {sub.SID}\r\n" 
                : $"SUB {sub.Subject} {sub.QueueGroup} {sub.SID}\r\n";
            await peerConnection.SendRawAsync(cmd);
        }
    }

    // This would be called by BrokerServer when a NEW local subscription is added
    public void BroadcastSubscription(string subject, string sid, string? queueGroup = null)
    {
        foreach (var conn in _connections.Values)
        {
            // Send SUB to peer
            var cmd = string.IsNullOrEmpty(queueGroup) 
                ? $"SUB {subject} {sid}\r\n" 
                : $"SUB {subject} {queueGroup} {sid}\r\n";
            
            _ = conn.SendRawAsync(cmd);
        }
    }

    public void BroadcastUnsubscription(string sid)
    {
        foreach (var conn in _connections.Values)
        {
            // Send UNSUB to peer
            var cmd = $"UNSUB {sid}\r\n";
            _ = conn.SendRawAsync(cmd);
        }
    }
}
