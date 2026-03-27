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
    private readonly int _amqpPort;
    private readonly int _streamPort;
    private readonly string _streamAdvertisedHost;
    private Socket? _listenSocket;
    private Socket? _amqpListenSocket;
    private Socket? _streamListenSocket;
    private CancellationTokenSource? _cts;
    private Task? _acceptTask;
    private Task? _amqpAcceptTask;
    private Task? _streamAcceptTask;
    private readonly TopicTree _topicTree = new();
    private readonly Sublist _sublist = new();
    private readonly Persistence.MessageRepository? _repo;
    private readonly Auth.IAuthenticator? _authenticator;
    private readonly Services.JetStreamService _jetStream;
    private readonly Services.ClusterManager _cluster;
    private readonly Services.MonitoringService _monitor;
    private readonly Services.LeafnodeManager _leafnodes;
    private readonly RabbitMQ.ExchangeManager _rmqExchanges;
    public RabbitMQ.RabbitMQService RabbitMQ { get; }
    private readonly ConcurrentDictionary<BrokerConnection, byte> _connections = new();
    private readonly X509Certificate2? _serverCertificate;
    private readonly DateTime _startTime = DateTime.UtcNow;
    private long _totalConnections = 0;
    private readonly List<IPEndPoint> _peerEndPoints = new();
    private int _lameDuckModeFlag = 0; // 0 = normal, 1 = lame-duck (Interlocked)
    private bool _lameDuckMode => Volatile.Read(ref _lameDuckModeFlag) == 1;
    public bool LameDuckMode => _lameDuckMode;
    public bool UseSublist { get; set; } = true;

    // Cached INFO response bytes. Two variants (auth/no-auth) for the common case.
    // Lame-duck mode is rare; its bytes are built on demand and not cached.
    private byte[]? _infoNoAuth;
    private byte[]? _infoAuth;

    public byte[] GetInfoBytes(bool authRequired)
    {
        if (_lameDuckMode)
            return BuildInfoBytes(authRequired, lameDuck: true);
        if (authRequired)
            return _infoAuth ??= BuildInfoBytes(true, lameDuck: false);
        return _infoNoAuth ??= BuildInfoBytes(false, lameDuck: false);
    }

    internal static byte[] BuildInfoBytes(bool authRequired, bool lameDuck, string nonce = "")
    {
        string auth = authRequired ? "true" : "false";
        string ldm  = lameDuck    ? "true" : "false";
        string msg  = $"INFO {{\"server_id\":\"cosmo-broker\",\"version\":\"1.0.0\",\"auth_required\":{auth},\"nonce\":\"{nonce}\",\"lame_duck_mode\":{ldm},\"headers\":true,\"max_payload\":1048576}}\r\n";
        return System.Text.Encoding.UTF8.GetBytes(msg);
    }

    public BrokerServer(int port = 4222, int amqpPort = 0, int streamPort = 0, Persistence.MessageRepository? repo = null, Auth.IAuthenticator? authenticator = null, X509Certificate2? serverCertificate = null, int monitorPort = 8222, string? streamAdvertisedHost = null)
    {
        _port = port;
        _amqpPort = amqpPort;
        _streamPort = streamPort;
        _streamAdvertisedHost = string.IsNullOrWhiteSpace(streamAdvertisedHost) ? "localhost" : streamAdvertisedHost;
        _repo = repo;
        _authenticator = authenticator;
        _serverCertificate = serverCertificate;
        _jetStream = new Services.JetStreamService(_topicTree, _repo);
        _cluster = new Services.ClusterManager(this, _topicTree);
        _monitor = new Services.MonitoringService(this, monitorPort);
        _leafnodes = new Services.LeafnodeManager(this, _topicTree);
        _rmqExchanges = new RabbitMQ.ExchangeManager(_repo);
        RabbitMQ = new RabbitMQ.RabbitMQService(_rmqExchanges);

        _jetStream.StartRedeliveryLoop(TimeSpan.FromSeconds(30));
        _rmqExchanges.StartTtlLoop(TimeSpan.FromSeconds(5));
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

    public void AddGateway(string host, int port)
    {
        var ips = Dns.GetHostAddresses(host);
        if (ips.Length > 0)
        {
            var ep = new IPEndPoint(ips[0], port);
            _cluster.AddGateway(ep);
        }
    }

    public void AddLeafnodeHub(string uri, Services.LeafnodeHubOptions? options = null) => _leafnodes.AddHub(uri, options);

    public void EnterLameDuckMode()
    {
        if (Interlocked.CompareExchange(ref _lameDuckModeFlag, 1, 0) != 0) return;
        Console.WriteLine("[CosmoBroker] Entering Lame Duck Mode. Notifying clients...");
        foreach (var conn in _connections.Keys) conn.SendInfo();
        _listenSocket?.Close();
    }

    public async Task StartAsync(CancellationToken ct = default)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        if (_repo != null)
        {
            await _repo.InitializeAsync(_cts.Token);
            await _jetStream.InitializeAsync();
            await _rmqExchanges.InitializeAsync(_cts.Token);
        }
        await _cluster.StartAsync(_cts.Token);
        await _leafnodes.StartAsync(_cts.Token);
        _monitor.Start(_cts.Token);

        if (_port > 0)
        {
            _listenSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _listenSocket.ReceiveBufferSize = 8 * 1024 * 1024; // 8MB buffer
            _listenSocket.SendBufferSize = 8 * 1024 * 1024;    // 8MB buffer
            _listenSocket.Bind(new IPEndPoint(IPAddress.Any, _port));
            _listenSocket.Listen(10000); // Increased from 1000

            Console.WriteLine($"[CosmoBroker] NATS listener on port {_port} {( _serverCertificate != null ? "(TLS enabled)" : "" )}...");
            _acceptTask = AcceptLoopAsync(_cts.Token);
        }
        else
        {
            Console.WriteLine("[CosmoBroker] NATS listener disabled.");
        }

        if (_amqpPort > 0)
        {
            _amqpListenSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _amqpListenSocket.ReceiveBufferSize = 4 * 1024 * 1024;
            _amqpListenSocket.SendBufferSize = 4 * 1024 * 1024;
            _amqpListenSocket.Bind(new IPEndPoint(IPAddress.Any, _amqpPort));
            _amqpListenSocket.Listen(1024);
            Console.WriteLine($"[CosmoBroker] AMQP 0-9-1 listener on port {_amqpPort}...");
            _amqpAcceptTask = AcceptAmqpLoopAsync(_cts.Token);
        }
        else
        {
            Console.WriteLine("[CosmoBroker] AMQP listener disabled.");
        }

        if (_streamPort > 0)
        {
            _streamListenSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _streamListenSocket.ReceiveBufferSize = 4 * 1024 * 1024;
            _streamListenSocket.SendBufferSize = 4 * 1024 * 1024;
            _streamListenSocket.Bind(new IPEndPoint(IPAddress.Any, _streamPort));
            _streamListenSocket.Listen(1024);
            Console.WriteLine($"[CosmoBroker] RabbitMQ Stream listener on port {_streamPort}...");
            _streamAcceptTask = AcceptStreamLoopAsync(_cts.Token);
        }
        else
        {
            Console.WriteLine("[CosmoBroker] RabbitMQ Stream listener disabled.");
        }
    }

    public void NotifySubscription(string subject, string sid, string? queueGroup = null)
    {
        _cluster.BroadcastSubscription(subject, sid, queueGroup);
        _leafnodes.NotifyLocalSub(subject, sid);
    }

    public void AddSublist(string subject, BrokerConnection conn, string sid, string? queueGroup, object? state = null)
    {
        _sublist.Add(subject, conn, sid, queueGroup, state);
    }
    public void RemoveSublist(string subject, BrokerConnection conn, string sid, string? queueGroup) => _sublist.Remove(subject, conn, sid, queueGroup);
    public Sublist.SublistResult MatchSublist(string subject)
    {
        Span<byte> buf = stackalloc byte[subject.Length * 3];
        int len = System.Text.Encoding.UTF8.GetBytes(subject, buf);
        return _sublist.Match(buf.Slice(0, len));
    }
    public Sublist.SublistResult MatchSublist(ReadOnlySpan<byte> subject) => _sublist.Match(subject);

    public bool TryMatchSublistLiteral(string subject, out List<Sublist.SubEntry> psubs, out Dictionary<string, Sublist.QueueGroup> qsubs)
    {
        Span<byte> buf = stackalloc byte[subject.Length * 3];
        int len = System.Text.Encoding.UTF8.GetBytes(subject, buf);
        return _sublist.TryMatchLiteral(buf.Slice(0, len), out psubs, out qsubs);
    }
    public bool TryMatchSublistLiteral(ReadOnlySpan<byte> subject, out List<Sublist.SubEntry> psubs, out Dictionary<string, Sublist.QueueGroup> qsubs) => _sublist.TryMatchLiteral(subject, out psubs, out qsubs);
    public bool SublistHasWildcards => _sublist.HasWildcards;
    public long SublistVersion => _sublist.Version;

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
    public object GetRmqz() => _rmqExchanges.GetStats();
    public bool TryResetRabbitStreamOffset(string vhost, string queueName, string consumerTag, RabbitMQ.RabbitStreamOffsetSpec? offset, out string? error, out long nextOffset)
        => _rmqExchanges.TryResetStreamConsumerOffset(vhost, queueName, consumerTag, offset, out error, out nextOffset);
    public bool TryResetRabbitSuperStreamOffset(string vhost, string exchangeName, string consumerTag, RabbitMQ.RabbitStreamOffsetSpec? offset, out string? error, out Dictionary<string, long> partitionOffsets)
        => _rmqExchanges.TryResetSuperStreamConsumerOffset(vhost, exchangeName, consumerTag, offset, out error, out partitionOffsets);
    public bool TryResolveRabbitSuperStreamPartition(string vhost, string exchangeName, string routingKey, string? partitionKey, out string? error, out string? partition)
        => _rmqExchanges.TryResolveSuperStreamPartition(vhost, exchangeName, routingKey, partitionKey, out error, out partition);
    public object GetRoutez() => new { routes = _cluster.RouteCount };
    public object GetGatewayz() => new { gateways = _cluster.GatewayCount };
    public object GetLeafz() => new { leafnodes = _leafnodes.ConnectionCount };

    private async Task AcceptLoopAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested && !_lameDuckMode)
            {
                var socket = await _listenSocket!.AcceptAsync(ct);
                socket.NoDelay = true;
                socket.ReceiveBufferSize = 8 * 1024 * 1024;
                socket.SendBufferSize = 8 * 1024 * 1024;
                Interlocked.Increment(ref _totalConnections);
                
                _ = Task.Run(async () => {
                    Stream? stream = null;
                    try {
                        stream = new NetworkStream(socket, ownsSocket: true);
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

                        var connection = new BrokerConnection(stream, socket.RemoteEndPoint?.ToString() ?? "unknown", _topicTree, _repo, _authenticator, _jetStream, this, rmqService: RabbitMQ);
                        if (certAuth != null && certAuth.Success) connection.ApplyAuth(certAuth);

                        _connections[connection] = 0;
                        try { await connection.RunAsync(); } finally { _connections.TryRemove(connection, out _); }
                    }
                    catch { try { stream?.Dispose(); } catch { } try { socket.Dispose(); } catch { } }
                }, ct);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex) { if (!_lameDuckMode) Console.WriteLine($"[CosmoBroker] Accept error: {ex.Message}"); }
    }

    private async Task AcceptAmqpLoopAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested && !_lameDuckMode)
            {
                var socket = await _amqpListenSocket!.AcceptAsync(ct);
                socket.NoDelay = true;
                socket.ReceiveBufferSize = 4 * 1024 * 1024;
                socket.SendBufferSize = 4 * 1024 * 1024;
                Interlocked.Increment(ref _totalConnections);

                _ = Task.Run(async () =>
                {
                    Stream? stream = null;
                    try
                    {
                        stream = new NetworkStream(socket, ownsSocket: true);
                        await using var connection = new AMQP.AmqpConnection(stream, _rmqExchanges, _authenticator);
                        await connection.RunAsync(ct);
                    }
                    catch
                    {
                        try { stream?.Dispose(); } catch { }
                        try { socket.Dispose(); } catch { }
                    }
                }, ct);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex) { if (!_lameDuckMode) Console.WriteLine($"[CosmoBroker] AMQP accept error: {ex.Message}"); }
    }

    private async Task AcceptStreamLoopAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested && !_lameDuckMode)
            {
                var socket = await _streamListenSocket!.AcceptAsync(ct);
                socket.NoDelay = true;
                socket.ReceiveBufferSize = 4 * 1024 * 1024;
                socket.SendBufferSize = 4 * 1024 * 1024;
                Interlocked.Increment(ref _totalConnections);

                _ = Task.Run(async () =>
                {
                    Stream? stream = null;
                    try
                    {
                        stream = new NetworkStream(socket, ownsSocket: true);
                        await using var connection = new RabbitMQ.Streams.StreamConnection(stream, _rmqExchanges, _authenticator, _streamPort, _streamAdvertisedHost);
                        await connection.RunAsync(ct);
                    }
                    catch
                    {
                        try { stream?.Dispose(); } catch { }
                        try { socket.Dispose(); } catch { }
                    }
                }, ct);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex) { if (!_lameDuckMode) Console.WriteLine($"[CosmoBroker] Stream accept error: {ex.Message}"); }
    }

    public async ValueTask DisposeAsync()
    {
        _cts?.Cancel();
        _listenSocket?.Dispose();
        _amqpListenSocket?.Dispose();
        _streamListenSocket?.Dispose();
        if (_acceptTask != null) { try { await _acceptTask; } catch { } }
        if (_amqpAcceptTask != null) { try { await _amqpAcceptTask; } catch { } }
        if (_streamAcceptTask != null) { try { await _streamAcceptTask; } catch { } }
    }
}
