using System;
using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Linq;
using System.Collections.Generic;
using System.IO;

namespace CosmoBroker;

public class BrokerConnection
{
    private readonly Stream _stream;
    private readonly bool _sendInfoOnConnect;
    private readonly string _remoteEndPoint;
    private readonly TopicTree _topicTree;
    private readonly Persistence.MessageRepository? _repo;
    private readonly Auth.IAuthenticator? _authenticator;
    private readonly Services.JetStreamService _jetStream;
    private readonly BrokerServer? _server;
    private readonly Pipe _readerPipe;
    private readonly Pipe _writerPipe;
    
    public enum ProtocolType
    {
        Unknown,
        NATS,
        MQTT,
        WebSocket
    }

    private ProtocolType _protocol = ProtocolType.Unknown;
    private bool _isAuthenticated = false;
    private bool _certAuthenticated = false;
    public bool IsRoute { get; set; } = false;
    public Auth.Account? Account { get; private set; }
    public Auth.User? User { get; private set; }
    public bool SupportsHeaders { get; set; } = true;

    private bool _wsHandshakeComplete = false;

    public long BytesIn { get; private set; }
    public long BytesOut { get; private set; }
    public long MsgIn { get; private set; }
    public long MsgOut { get; private set; }

    // Optimization: Cached byte arrays for common protocol tokens
    private static readonly byte[] MsgVerb = "MSG "u8.ToArray();
    private static readonly byte[] HMsgVerb = "HMSG "u8.ToArray();
    private static readonly byte[] Space = " "u8.ToArray();
    private static readonly byte[] Crlf = "\r\n"u8.ToArray();
    private static readonly byte[] Pong = "PONG\r\n"u8.ToArray();
    private static readonly byte[] HeaderMagic = "NATS/1.0\r\nNats-Msg-TTL: "u8.ToArray();
    private static readonly byte[] HeaderEnd = "\r\n\r\n"u8.ToArray();

    // Optimization: Per-connection byte cache for strings to avoid re-encoding
    private readonly ConcurrentDictionary<string, byte[]> _stringByteCache = new();

    public object GetStats() => new {
        protocol = _protocol.ToString(),
        remote_addr = _remoteEndPoint,
        bytes_in = BytesIn,
        bytes_out = BytesOut,
        msg_in = MsgIn,
        msg_out = MsgOut,
        subscriptions = _subscriptions.Count,
        account = Account?.Name
    };

    private class Subscription
    {
        public string Subject { get; init; } = string.Empty;
        public string? QueueGroup { get; init; }
        public int? MaxMsgs { get; set; }
        public int ReceivedMsgs { get; set; }
        public bool IsRemote { get; set; } = false;
    }

    private readonly ConcurrentDictionary<string, Subscription> _subscriptions = new();

    public BrokerConnection(Stream stream, string remoteEndPoint, TopicTree topicTree, Persistence.MessageRepository? repo = null, Auth.IAuthenticator? authenticator = null, Services.JetStreamService? jetStream = null, BrokerServer? server = null, bool sendInfoOnConnect = true)
    {
        _stream = stream;
        _remoteEndPoint = remoteEndPoint;
        _topicTree = topicTree;
        _repo = repo;
        _authenticator = authenticator;
        _jetStream = jetStream ?? new Services.JetStreamService(_topicTree, _repo);
        _server = server;
        _readerPipe = new Pipe();
        _writerPipe = new Pipe();
        _sendInfoOnConnect = sendInfoOnConnect;

        if (_authenticator == null)
        {
            _isAuthenticated = true;
            Account = new Auth.Account { Name = "global" };
            User = new Auth.User { Name = "anonymous", AccountName = "global" };
        }
    }

    public async Task RunAsync()
    {
        var readTask = FillPipeAsync(_stream, _readerPipe.Writer);
        var processTask = ProcessPipeAsync(_readerPipe.Reader);
        var writeTask = FlushPipeAsync(_stream, _writerPipe.Reader);

        if (_sendInfoOnConnect)
        {
            _ = Task.Run(async () => {
                await Task.Delay(50);
                if (_protocol == ProtocolType.Unknown || _protocol == ProtocolType.NATS)
                {
                    _protocol = ProtocolType.NATS;
                    await SendInfo();
                }
            });
        }

        await Task.WhenAny(readTask, processTask, writeTask);
        Cleanup();
    }

    public void ApplyAuth(Auth.AuthResult result)
    {
        if (result.Success)
        {
            _isAuthenticated = true;
            _certAuthenticated = true;
            Account = result.Account;
            User = result.User;
        }
    }

    public IEnumerable<(string Subject, string SID, string? QueueGroup)> GetLocalSubscriptions()
    {
        return _subscriptions
            .Where(s => !s.Value.IsRemote)
            .Select(s => (s.Value.Subject, s.Key, s.Value.QueueGroup));
    }

    public async Task SendRawAsync(string rawCommand)
    {
        var bytes = Encoding.UTF8.GetBytes(rawCommand);
        BytesOut += bytes.Length;
        _writerPipe.Writer.Write(bytes);
        await _writerPipe.Writer.FlushAsync();
    }

    public async Task SendInfo()
    {
        bool authRequired = _authenticator != null;
        string nonce = "secure_nonce_12345";
        bool ldm = _server?.GetVarz() is { } v && (bool)((dynamic)v).lame_duck_mode;
        
        string infoJson = $"{{\"server_id\":\"cosmo-broker\",\"version\":\"1.0.0\",\"auth_required\":{authRequired.ToString().ToLower()},\"nonce\":\"{nonce}\",\"lame_duck_mode\":{ldm.ToString().ToLower()},\"headers\":true,\"max_payload\":1048576}}";
        string infoStr = $"INFO {infoJson}\r\n";
        
        byte[] bytes = Encoding.UTF8.GetBytes(infoStr);
        BytesOut += bytes.Length;
        _writerPipe.Writer.Write(bytes);
        await _writerPipe.Writer.FlushAsync();
    }

    private async Task FillPipeAsync(Stream stream, PipeWriter writer)
    {
        const int minimumBufferSize = 512;
        try
        {
            while (true)
            {
                var memory = writer.GetMemory(minimumBufferSize);
                int bytesRead = await stream.ReadAsync(memory);
                if (bytesRead == 0) break;
                BytesIn += bytesRead;
                writer.Advance(bytesRead);
                var result = await writer.FlushAsync();
                if (result.IsCompleted || result.IsCanceled) break;
            }
        }
        catch { }
        finally { await writer.CompleteAsync(); }
    }

    private async Task ProcessPipeAsync(PipeReader reader)
    {
        try
        {
            while (true)
            {
                var result = await reader.ReadAsync();
                var buffer = result.Buffer;

                while (true)
                {
                    var linePosition = buffer.PositionOf((byte)'\n');
                    if (linePosition == null) break;

                    var line = buffer.Slice(0, linePosition.Value);
                    var lineSpan = line.IsSingleSegment ? line.FirstSpan : line.ToArray().AsSpan();
                    
                    if (_protocol == ProtocolType.Unknown)
                    {
                        if (lineSpan.Length >= 3 && lineSpan[0] == 'G' && lineSpan[1] == 'E' && lineSpan[2] == 'T')
                            _protocol = ProtocolType.WebSocket;
                        else if (lineSpan[0] == 0x10)
                            _protocol = ProtocolType.MQTT;
                        else
                            _protocol = ProtocolType.NATS;
                    }

                    if (_protocol == ProtocolType.WebSocket) { if (!ProcessWebSocket(ref buffer)) break; continue; }
                    if (_protocol == ProtocolType.MQTT) { if (!ProcessMqtt(ref buffer)) break; continue; }

                    if (lineSpan.Length >= 4)
                    {
                        if ((lineSpan[0] | 0x20) == 'p' && (lineSpan[1] | 0x20) == 'i' && (lineSpan[2] | 0x20) == 'n' && (lineSpan[3] | 0x20) == 'g')
                        {
                            HandlePing();
                            buffer = buffer.Slice(buffer.GetPosition(1, linePosition.Value));
                            continue;
                        }

                        if ((lineSpan[0] | 0x20) == 'p' || ((lineSpan[0] | 0x20) == 'h' && lineSpan.Length > 5))
                        {
                            string lineStr = Encoding.UTF8.GetString(lineSpan).TrimEnd('\r');
                            var parts = lineStr.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                            if (parts.Length > 0 && (parts[0].Equals("PUB", StringComparison.OrdinalIgnoreCase) || parts[0].Equals("HPUB", StringComparison.OrdinalIgnoreCase)))
                            {
                                bool isHPub = parts[0].Equals("HPUB", StringComparison.OrdinalIgnoreCase);
                                int lastIdx = parts.Length - 1;
                                if (int.TryParse(parts[lastIdx], out int totalLength))
                                {
                                    int headerLen = isHPub ? int.Parse(parts[lastIdx - 1]) : 0;
                                    var payloadStart = buffer.GetPosition(1, linePosition.Value);
                                    var remaining = buffer.Slice(payloadStart);
                                    
                                    if (remaining.Length >= totalLength + 2)
                                    {
                                        var fullPayload = remaining.Slice(0, totalLength);
                                        if (isHPub) HandleHPub(parts[1], parts.Length == 5 ? parts[2] : null, headerLen, fullPayload);
                                        else HandlePub(parts[1], parts.Length == 4 ? parts[2] : null, fullPayload);
                                        
                                        buffer = remaining.Slice(totalLength + 2);
                                        continue; 
                                    }
                                    else break;
                                }
                            }
                        }
                    }
                    
                    NatsParser.ParseCommand(this, line, ref buffer, out bool _);
                    buffer = buffer.Slice(buffer.GetPosition(1, linePosition.Value));
                }

                reader.AdvanceTo(buffer.Start, buffer.End);
                if (result.IsCompleted) break;
            }
        }
        catch { }
        finally { await reader.CompleteAsync(); }
    }

    private bool ProcessWebSocket(ref ReadOnlySequence<byte> buffer)
    {
        if (!_wsHandshakeComplete)
        {
            if (WebSockets.WebSocketFramer.TryParseUpgradeRequest(buffer, out string request, out int bytesConsumed))
            {
                var response = WebSockets.WebSocketFramer.CreateHandshakeResponse(request);
                BytesOut += response.Length;
                _writerPipe.Writer.Write(response);
                _ = _writerPipe.Writer.FlushAsync();
                buffer = buffer.Slice(bytesConsumed);
                _wsHandshakeComplete = true;
                _isAuthenticated = true;
                return true;
            }
            return false;
        }
        while (WebSockets.WebSocketFramer.TryUnframeMessage(ref buffer, out byte[] payload))
        {
            var seq = new ReadOnlySequence<byte>(payload);
            NatsParser.ParseCommand(this, seq, ref seq, out _);
        }
        return true;
    }

    private bool ProcessMqtt(ref ReadOnlySequence<byte> buffer)
    {
        while (MQTT.MqttParser.TryParsePacket(ref buffer, out byte packetType, out byte[] payload))
        {
            switch (packetType)
            {
                case 1:
                    var ack = MQTT.MqttParser.CreateConnAck();
                    BytesOut += ack.Length;
                    _writerPipe.Writer.Write(ack);
                    _ = _writerPipe.Writer.FlushAsync();
                    _isAuthenticated = true;
                    break;
                case 3:
                    if (payload.Length > 2)
                    {
                        int topicLen = (payload[0] << 8) | payload[1];
                        if (payload.Length >= 2 + topicLen)
                        {
                            string topic = Encoding.UTF8.GetString(payload, 2, topicLen);
                            var msgPayload = new ReadOnlySequence<byte>(payload, 2 + topicLen, payload.Length - (2 + topicLen));
                            HandlePub(topic, null, msgPayload);
                        }
                    }
                    break;
                case 8:
                    if (payload.Length > 2)
                    {
                        int packetId = (payload[0] << 8) | payload[1];
                        int topicLen = (payload[2] << 8) | payload[3];
                        if (payload.Length >= 4 + topicLen)
                        {
                            string topic = Encoding.UTF8.GetString(payload, 4, topicLen);
                            HandleSub(topic, Guid.NewGuid().ToString());
                        }
                        var sack = MQTT.MqttParser.CreateSubAck(packetId);
                        BytesOut += sack.Length;
                        _writerPipe.Writer.Write(sack);
                        _ = _writerPipe.Writer.FlushAsync();
                    }
                    break;
                case 12:
                    var resp = MQTT.MqttParser.CreatePingResp();
                    BytesOut += resp.Length;
                    _writerPipe.Writer.Write(resp);
                    _ = _writerPipe.Writer.FlushAsync();
                    break;
            }
        }
        return true;
    }

    private async Task FlushPipeAsync(Stream stream, PipeReader reader)
    {
        try
        {
            while (true)
            {
                var result = await reader.ReadAsync();
                var buffer = result.Buffer;
                foreach (var segment in buffer) await stream.WriteAsync(segment);
                reader.AdvanceTo(buffer.End);
                if (result.IsCompleted || result.IsCanceled) break;
            }
        }
        catch { }
        finally { await reader.CompleteAsync(); }
    }

    private bool IsAllowedPublish(string subject)
    {
        if (IsRoute) return true;
        if (Account == null) return false;
        if (Account.DenyPublish.Any(p => subject.StartsWith(p))) return false;
        if (Account.AllowPublish.Count > 0 && !Account.AllowPublish.Any(p => subject.StartsWith(p))) return false;
        return true;
    }

    private bool IsAllowedSubscribe(string subject)
    {
        if (IsRoute) return true;
        if (Account == null) return false;
        if (Account.DenySubscribe.Any(p => subject.StartsWith(p))) return false;
        if (Account.AllowSubscribe.Count > 0 && !Account.AllowSubscribe.Any(p => subject.StartsWith(p))) return false;
        return true;
    }

    public void HandleSub(string subject, string sid, string? queueGroup = null, string? durableName = null, bool isRemote = false)
    {
        if (!_isAuthenticated) { SendError("Authorization Violation"); return; }
        if (!IsAllowedSubscribe(subject)) { SendError($"Permissions Violation for Subscription to {subject}"); return; }
        
        // Fast scoping
        string scopedSubject = subject;
        if (Account != null && !string.IsNullOrEmpty(Account.SubjectPrefix))
            scopedSubject = $"{Account.SubjectPrefix}.{subject}";

        var sub = new Subscription { Subject = scopedSubject, QueueGroup = queueGroup, IsRemote = isRemote };
        _subscriptions[sid] = sub;
        _topicTree.Subscribe(scopedSubject, this, sid, queueGroup);
        if (!isRemote)
        {
            _server?.NotifySubscription(scopedSubject, sid, queueGroup);
            if (!string.IsNullOrEmpty(durableName) && _repo != null)
            {
                _ = Task.Run(async () =>
                {
                    long lastId = await _repo.GetConsumerOffsetAsync(durableName);
                    var messages = await _repo.GetMessagesAsync(scopedSubject, lastId);
                    foreach (var m in messages)
                    {
                        SendMessageWithTTL(m.Subject, sid, new ReadOnlySequence<byte>(m.Payload), null, null);
                        lastId = m.Id;
                    }
                    if (lastId > 0) await _repo.UpdateConsumerOffsetAsync(durableName, scopedSubject, lastId);
                });
            }
        }
    }

    public void HandleUnsub(string sid, int? maxMsgs = null)
    {
        if (!_isAuthenticated) return;
        if (_subscriptions.TryGetValue(sid, out var sub))
        {
            if (maxMsgs.HasValue && sub.ReceivedMsgs < maxMsgs.Value) sub.MaxMsgs = maxMsgs;
            else if (_subscriptions.TryRemove(sid, out _))
            {
                _topicTree.Unsubscribe(sub.Subject, this, sid, sub.QueueGroup);
                if (!sub.IsRemote) _server?.NotifyUnsubscription(sid);
            }
        }
    }

    public void HandleHPub(string subject, string? replyTo, int headerLen, ReadOnlySequence<byte> fullPayload)
    {
        var headersBytes = fullPayload.Slice(0, headerLen).ToArray();
        var payload = fullPayload.Slice(headerLen);
        var headersStr = Encoding.UTF8.GetString(headersBytes);
        
        TimeSpan? ttl = null;
        var lines = headersStr.Split("\r\n");
        foreach (var line in lines)
        {
            if (line.StartsWith("Nats-Msg-TTL:", StringComparison.OrdinalIgnoreCase))
            {
                if (int.TryParse(line.Substring(13).Trim(), out int seconds))
                    ttl = TimeSpan.FromSeconds(seconds);
            }
        }
        HandlePub(subject, replyTo, payload, ttl);
    }

    public void HandlePub(string subject, string? replyTo, ReadOnlySequence<byte> payload, TimeSpan? ttl = null)
    {
        if (!_isAuthenticated) { SendError("Authorization Violation"); return; }
        MsgIn++;
        if (!IsAllowedPublish(subject)) { SendError($"Permissions Violation for Publish to {subject}"); return; }
        
        // Fast mapping/scoping
        string mappedSubject = subject;
        if (Account != null && Account.Name != "global") mappedSubject = Account.Mappings.Map(subject);

        string scopedSubject = mappedSubject;
        if (Account != null && !string.IsNullOrEmpty(Account.SubjectPrefix))
            scopedSubject = $"{Account.SubjectPrefix}.{mappedSubject}";

        string? scopedReplyTo = replyTo;
        if (replyTo != null && Account != null && !string.IsNullOrEmpty(Account.SubjectPrefix))
            scopedReplyTo = $"{Account.SubjectPrefix}.{replyTo}";
        
        if (!IsRoute)
        {
            if (scopedSubject.StartsWith("$JS.", StringComparison.OrdinalIgnoreCase))
            {
                if (scopedSubject.StartsWith("$JS.API.", StringComparison.OrdinalIgnoreCase))
                {
                    var payloadBytes = payload.ToArray();
                    _ = Task.Run(async () => {
                        var response = await _jetStream.HandleApiRequest(scopedSubject, payloadBytes);
                        if (response != null && !string.IsNullOrEmpty(replyTo))
                            _topicTree.Publish(replyTo, new ReadOnlySequence<byte>(response), source: this);
                    });
                    return;
                }
                if (scopedSubject.StartsWith("$JS.ACK.", StringComparison.OrdinalIgnoreCase))
                {
                    var parts = scopedSubject.Split('.');
                    if (parts.Length >= 6 && long.TryParse(parts[5], out long seq))
                    {
                        if (payload.Length > 0) {
                            var pStr = Encoding.UTF8.GetString(payload.ToArray()).Trim();
                            if (pStr.StartsWith("-NAK")) _jetStream.Nack(parts[2], parts[3], seq);
                            else if (pStr.StartsWith("+TERM")) _jetStream.Term(parts[2], parts[3], seq);
                            else _jetStream.Ack(parts[2], parts[3], seq);
                        } else _jetStream.Ack(parts[2], parts[3], seq);
                    }
                    return;
                }
            }

            if (_repo != null && scopedSubject.StartsWith("persist.", StringComparison.OrdinalIgnoreCase))
                _ = Task.Run(async () => await _repo.SaveMessageAsync(scopedSubject, payload.ToArray()));

            if (_jetStream.HasStreams)
            {
                foreach (var streamName in _jetStream.GetMatchingStreams(scopedSubject))
                    _ = _jetStream.Publish(streamName, scopedSubject, payload.ToArray(), ttl);
            }
        }
        _topicTree.PublishWithTTL(scopedSubject, payload, scopedReplyTo, ttl, this);
    }

    public void HandlePing()
    {
        BytesOut += Pong.Length;
        _writerPipe.Writer.Write(Pong);
        _ = _writerPipe.Writer.FlushAsync();
    }

    public async Task HandleConnect(Auth.ConnectOptions options)
    {
        NoEcho = options.NoEcho;
        if (_authenticator != null)
        {
            if (_certAuthenticated && _authenticator is Auth.X509Authenticator)
            {
                var ok = "+OK\r\n"u8;
                BytesOut += ok.Length;
                _writerPipe.Writer.Write(ok);
                await _writerPipe.Writer.FlushAsync();
                return;
            }

            var result = await _authenticator.AuthenticateAsync(options);
            _isAuthenticated = result.Success;
            if (!_isAuthenticated)
            {
                SendError($"Authentication Failed: {result.ErrorMessage}");
                await Task.Delay(50);
                _stream.Close();
            }
            else
            {
                Account = result.Account;
                User = result.User;
                var ok = "+OK\r\n"u8;
                BytesOut += ok.Length;
                _writerPipe.Writer.Write(ok);
                await _writerPipe.Writer.FlushAsync();
            }
        }
    }

    public bool NoEcho { get; private set; }

    public void SendMessage(string subject, string sid, ReadOnlySequence<byte> payload, string? replyTo = null) => SendMessageWithTTL(subject, sid, payload, replyTo, null);

    private byte[] GetCachedBytes(string s) => _stringByteCache.GetOrAdd(s, static str => Encoding.UTF8.GetBytes(str));

    public void SendMessageWithTTL(string subject, string sid, ReadOnlySequence<byte> payload, string? replyTo, TimeSpan? ttl)
    {
        if (!_subscriptions.TryGetValue(sid, out var sub)) return;
        sub.ReceivedMsgs++;
        MsgOut++;

        string clientSubject = subject;
        if (Account != null && !string.IsNullOrEmpty(Account.SubjectPrefix) && subject.StartsWith(Account.SubjectPrefix + "."))
            clientSubject = subject.Substring(Account.SubjectPrefix.Length + 1);

        string? clientReplyTo = replyTo;
        if (clientReplyTo != null && Account != null && !string.IsNullOrEmpty(Account.SubjectPrefix) && clientReplyTo.StartsWith(Account.SubjectPrefix + "."))
            clientReplyTo = clientReplyTo.Substring(Account.SubjectPrefix.Length + 1);

        var writer = _writerPipe.Writer;

        if (_protocol == ProtocolType.MQTT)
        {
            var frame = MQTT.MqttParser.FramePublish(clientSubject, payload.IsSingleSegment ? payload.FirstSpan : payload.ToArray());
            BytesOut += frame.Length;
            writer.Write(frame);
            _ = writer.FlushAsync();
            return;
        }

        bool useHeaders = ttl.HasValue && SupportsHeaders;
        int headerLen = 0;
        byte[]? ttlBytes = null;
        if (useHeaders)
        {
            ttlBytes = GetCachedBytes(((int)ttl!.Value.TotalSeconds).ToString());
            headerLen = HeaderMagic.Length + ttlBytes.Length + HeaderEnd.Length;
        }

        int totalPayloadLen = (int)payload.Length + headerLen;
        byte[] totalPayloadLenBytes = GetCachedBytes(totalPayloadLen.ToString());

        if (_protocol == ProtocolType.WebSocket)
        {
            int estimatedSize = 256 + totalPayloadLen;
            byte[] rented = ArrayPool<byte>.Shared.Rent(estimatedSize);
            try {
                using var ms = new MemoryStream(rented);
                WriteNatsMessageToStream(ms, useHeaders, clientSubject, sid, clientReplyTo, headerLen, totalPayloadLenBytes, ttlBytes, payload);
                var wsFrame = WebSockets.WebSocketFramer.FrameMessage(rented.AsSpan(0, (int)ms.Position));
                BytesOut += wsFrame.Length;
                writer.Write(wsFrame);
            } finally {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
        else
        {
            writer.Write(useHeaders ? HMsgVerb : MsgVerb);
            writer.Write(GetCachedBytes(clientSubject));
            writer.Write(Space);
            writer.Write(GetCachedBytes(sid));
            writer.Write(Space);
            if (!string.IsNullOrEmpty(clientReplyTo)) {
                writer.Write(GetCachedBytes(clientReplyTo));
                writer.Write(Space);
            }
            if (useHeaders) {
                writer.Write(GetCachedBytes(headerLen.ToString()));
                writer.Write(Space);
            }
            writer.Write(totalPayloadLenBytes);
            writer.Write(Crlf);
            if (useHeaders) {
                writer.Write(HeaderMagic);
                writer.Write(ttlBytes!);
                writer.Write(HeaderEnd);
            }
            foreach (var seg in payload) writer.Write(seg.Span);
            writer.Write(Crlf);
            BytesOut += 50 + totalPayloadLen; 
        }

        _ = writer.FlushAsync();

        if (sub.MaxMsgs.HasValue && sub.ReceivedMsgs >= sub.MaxMsgs.Value)
        {
            if (_subscriptions.TryRemove(sid, out _))
            {
                _topicTree.Unsubscribe(sub.Subject, this, sid, sub.QueueGroup);
                if (!sub.IsRemote) _server?.NotifyUnsubscription(sid);
            }
        }
    }

    private void WriteNatsMessageToStream(Stream s, bool useHeaders, string subject, string sid, string? replyTo, int hLen, byte[] totalLenBytes, byte[]? ttlBytes, ReadOnlySequence<byte> payload)
    {
        s.Write(useHeaders ? HMsgVerb : MsgVerb);
        s.Write(GetCachedBytes(subject)); s.Write(Space);
        s.Write(GetCachedBytes(sid)); s.Write(Space);
        if (!string.IsNullOrEmpty(replyTo)) { s.Write(GetCachedBytes(replyTo)); s.Write(Space); }
        if (useHeaders) { s.Write(GetCachedBytes(hLen.ToString())); s.Write(Space); }
        s.Write(totalLenBytes); s.Write(Crlf);
        if (useHeaders) { s.Write(HeaderMagic); s.Write(ttlBytes!); s.Write(HeaderEnd); }
        foreach (var seg in payload) s.Write(seg.Span);
        s.Write(Crlf);
    }

    private void SendError(string message)
    {
        var err = Encoding.UTF8.GetBytes($"-ERR '{message}'\r\n");
        BytesOut += err.Length;
        _writerPipe.Writer.Write(err);
        _ = _writerPipe.Writer.FlushAsync();
    }

    private void Cleanup()
    {
        foreach (var sub in _subscriptions) _topicTree.Unsubscribe(sub.Value.Subject, this, sub.Key, sub.Value.QueueGroup);
        _subscriptions.Clear();
        try { _stream.Close(); } catch { }
    }
}
