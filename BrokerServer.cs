using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using System.IO.Pipelines;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Net.Security;
using System.IO;

namespace CosmoBroker;

public class BrokerServer : IAsyncDisposable
{
    private readonly int _port;
    private Socket? _listenSocket;
    private CancellationTokenSource? _cts;
    private Task? _acceptTask;
    private readonly TopicTree _topicTree = new();
    private readonly Persistence.MessageRepository? _repo;
    private readonly Auth.IAuthenticator? _authenticator;
    private readonly Services.JetStreamService _jetStream;
    private readonly Services.ClusterManager _cluster;
    private readonly Services.MonitoringService _monitor;
    private readonly Services.LeafnodeManager _leafnodes;
    private readonly ConcurrentDictionary<BrokerConnection, byte> _connections = new();
    private readonly X509Certificate2? _serverCertificate;
    private readonly DateTime _startTime = DateTime.UtcNow;
    private long _totalConnections = 0;
    private readonly List<IPEndPoint> _peerEndPoints = new();
    private bool _lameDuckMode = false;

    public BrokerServer(int port = 4222, Persistence.MessageRepository? repo = null, Auth.IAuthenticator? authenticator = null, X509Certificate2? serverCertificate = null, int monitorPort = 8222)
    {
        _port = port;
        _repo = repo;
        _authenticator = authenticator;
        _serverCertificate = serverCertificate;
        _jetStream = new Services.JetStreamService(_topicTree, _repo);
        _cluster = new Services.ClusterManager(this, _topicTree);
        _monitor = new Services.MonitoringService(this, monitorPort);
        _leafnodes = new Services.LeafnodeManager(this, _topicTree);
        
        _jetStream.StartRedeliveryLoop(TimeSpan.FromSeconds(30));
    }

    public void AddPeer(string host, int port)
    {
        var ips = Dns.GetHostAddresses(host);
        if (ips.Length > 0)
        {
            var ep = new IPEndPoint(ips[0], port);
            _peerEndPoints.Add(ep);
            _cluster.AddPeer(ep);
        }
    }

    public void AddLeafnodeHub(string uri, Services.LeafnodeHubOptions? options = null) => _leafnodes.AddHub(uri, options);

    public void EnterLameDuckMode()
    {
        if (_lameDuckMode) return;
        _lameDuckMode = true;
        Console.WriteLine("[CosmoBroker] Entering Lame Duck Mode. Notifying clients...");
        foreach (var conn in _connections.Keys) _ = conn.SendInfo();
        _listenSocket?.Close(); 
    }

    public async Task StartAsync(CancellationToken ct = default)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        if (_repo != null)
        {
            await _repo.InitializeAsync(_cts.Token);
            await _jetStream.InitializeAsync();
        }
        await _cluster.StartAsync(_cts.Token);
        await _leafnodes.StartAsync(_cts.Token);
        _monitor.Start(_cts.Token);

        _listenSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        _listenSocket.Bind(new IPEndPoint(IPAddress.Any, _port));
        _listenSocket.Listen(1000);

        Console.WriteLine($"[CosmoBroker] Listening on port {_port} {( _serverCertificate != null ? "(TLS enabled)" : "" )}...");
        _acceptTask = AcceptLoopAsync(_cts.Token);
    }

    public void NotifySubscription(string subject, string sid, string? queueGroup = null)
    {
        _cluster.BroadcastSubscription(subject, sid, queueGroup);
        _leafnodes.NotifyLocalSub(subject, sid);
    }

    public void NotifyUnsubscription(string sid)
    {
        _cluster.BroadcastUnsubscription(sid);
        _leafnodes.NotifyLocalUnsub(sid);
    }

    public bool HasSubscribers(string subject) => _topicTree.HasSubscribers(subject);

    public IEnumerable<(string Subject, string SID, string? QueueGroup)> GetLocalSubscriptions()
    {
        return _connections.Keys.Where(c => !c.IsRoute).SelectMany(c => c.GetLocalSubscriptions());
    }

    public object GetVarz() => new {
        server_id = "cosmo-broker",
        version = "1.0.0",
        uptime = (DateTime.UtcNow - _startTime).ToString(),
        connections = _connections.Count,
        total_connections = Interlocked.Read(ref _totalConnections),
        routes = _peerEndPoints.Count,
        port = _port,
        tls_enabled = _serverCertificate != null,
        lame_duck_mode = _lameDuckMode
    };

    public object GetConnz() => new { connections = _connections.Keys.Select(c => c.GetStats()) };
    public object GetJsz() => _jetStream.GetStats();

    private async Task AcceptLoopAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested && !_lameDuckMode)
            {
                var socket = await _listenSocket!.AcceptAsync(ct);
                socket.NoDelay = true;
                Interlocked.Increment(ref _totalConnections);
                
                _ = Task.Run(async () => {
                    try {
                        Stream stream = new NetworkStream(socket, ownsSocket: true);
                        Auth.AuthResult? certAuth = null;

                        if (_serverCertificate != null)
                        {
                            var sslStream = new SslStream(stream, false);
                            await sslStream.AuthenticateAsServerAsync(new SslServerAuthenticationOptions {
                                ServerCertificate = _serverCertificate,
                                ClientCertificateRequired = true // Request certs for potential cert-based auth
                            }, ct);
                            stream = sslStream;

                            if (sslStream.RemoteCertificate != null && _authenticator is Auth.X509Authenticator x509)
                            {
                                certAuth = x509.AuthenticateCertificate(new X509Certificate2(sslStream.RemoteCertificate));
                            }
                        }

                        var connection = new BrokerConnection(stream, socket.RemoteEndPoint?.ToString() ?? "unknown", _topicTree, _repo, _authenticator, _jetStream, this);
                        if (certAuth != null && certAuth.Success) connection.ApplyAuth(certAuth);

                        _connections[connection] = 0;
                        try { await connection.RunAsync(); } finally { _connections.TryRemove(connection, out _); }
                    }
                    catch { try { socket.Close(); } catch { } }
                }, ct);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex) { if (!_lameDuckMode) Console.WriteLine($"[CosmoBroker] Accept error: {ex.Message}"); }
    }

    public async ValueTask DisposeAsync()
    {
        _cts?.Cancel();
        _listenSocket?.Dispose();
        if (_acceptTask != null) { try { await _acceptTask; } catch { } }
    }
}
