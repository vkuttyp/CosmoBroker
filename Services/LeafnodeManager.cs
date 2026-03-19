using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
using CosmoBroker.Auth;

namespace CosmoBroker.Services;

public class LeafnodeManager
{
    private readonly BrokerServer _server;
    private readonly TopicTree _topicTree;
    private readonly List<LeafnodeHubConfig> _remoteHubs = new();
    private readonly ConcurrentDictionary<Uri, BrokerConnection> _connections = new();

    public LeafnodeManager(BrokerServer server, TopicTree topicTree)
    {
        _server = server;
        _topicTree = topicTree;
    }

    public void AddHub(string uri, LeafnodeHubOptions? options = null)
    {
        var hub = new Uri(uri);
        options ??= new LeafnodeHubOptions();
        // Auto-enable TLS based on scheme when not explicitly set.
        if (!options.UseTls.HasValue)
        {
            options.UseTls = hub.Scheme.Equals("tls", StringComparison.OrdinalIgnoreCase) ||
                             hub.Scheme.Equals("ssl", StringComparison.OrdinalIgnoreCase) ||
                             hub.Scheme.Equals("nats+tls", StringComparison.OrdinalIgnoreCase);
        }
        _remoteHubs.Add(new LeafnodeHubConfig(hub, options));
    }

    public async Task StartAsync(CancellationToken ct)
    {
        foreach (var hub in _remoteHubs)
        {
            _ = ConnectToHubAsync(hub, ct);
        }
    }

    private async Task ConnectToHubAsync(LeafnodeHubConfig hubConfig, CancellationToken ct)
    {
        var hub = hubConfig.Hub;
        var options = hubConfig.Options;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                await socket.ConnectAsync(hub.Host, hub.Port, ct);
                socket.NoDelay = true;

                Stream stream = new NetworkStream(socket, ownsSocket: true);
                
                if (options.UseTls == true)
                {
                    var sslStream = new SslStream(stream, false, (sender, cert, chain, errors) =>
                    {
                        if (options.InsecureSkipVerify) return true;
                        return errors == SslPolicyErrors.None;
                    });

                    var clientCerts = new X509CertificateCollection();
                    if (options.ClientCertificate != null) clientCerts.Add(options.ClientCertificate);

                    var targetHost = options.SslTargetHost ?? hub.Host;
                    await sslStream.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
                    {
                        TargetHost = targetHost,
                        ClientCertificates = clientCerts,
                        EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13
                    }, ct);

                    stream = sslStream;
                }

                // Leafnode connection acts as a NATS client (no server INFO push)
                var connection = new BrokerConnection(stream, hub.ToString(), _topicTree, null, null, null, _server, sendInfoOnConnect: false);
                _connections[hub] = connection;

                Console.WriteLine($"[Leafnode] Connected to hub {hub}");

                // Start connection loop before sending commands
                _ = Task.Run(async () => await connection.RunAsync(), ct);

                if (options.ConnectOptions != null)
                {
                    string json = System.Text.Json.JsonSerializer.Serialize(options.ConnectOptions);
                    await connection.SendRawAsync($"CONNECT {json}\r\n");
                }

                // Bridge local subscriptions to the hub
                await SyncLocalSubsToHub(connection);
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

public sealed record LeafnodeHubConfig(Uri Hub, LeafnodeHubOptions Options);

public sealed class LeafnodeHubOptions
{
    public bool? UseTls { get; set; }
    public bool InsecureSkipVerify { get; set; }
    public X509Certificate2? ClientCertificate { get; set; }
    public string? SslTargetHost { get; set; }
    public Auth.ConnectOptions? ConnectOptions { get; set; }
}
