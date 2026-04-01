using System;
using System.Buffers;
using System.Buffers.Text;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Linq;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Diagnostics;
using System.Threading.Channels;
using NATS.NKeys;

namespace CosmoBroker;

public class BrokerConnection
{
    private static readonly bool PerfEnabled =
        Environment.GetEnvironmentVariable("COSMOBROKER_PERF") == "1";
    private static readonly long PerfIntervalTicks = Stopwatch.Frequency; // 1 second
    private static long _perfPubNs;
    private static long _perfMatchNs;
    private static long _perfSendNs;
    private static long _perfPubCount;
    private static long _perfMatchCount;
    private static long _perfSendCount;
    private static long _lastPerfLog;

    private const int MaxPayloadBytes = 1048576;
    private readonly string _nonce = GenerateNonce();

    private static string GenerateNonce()
    {
        Span<byte> bytes = stackalloc byte[18]; // 18 bytes → 24-char base64
        System.Security.Cryptography.RandomNumberGenerator.Fill(bytes);
        return Convert.ToBase64String(bytes);
    }
    private readonly Stream _stream;
    private readonly bool _sendInfoOnConnect;
    private readonly string _remoteEndPoint;
    private readonly TopicTree _topicTree;
    private readonly Persistence.MessageRepository? _repo;
    private readonly Auth.IAuthenticator? _authenticator;
    private readonly Services.JetStreamService _jetStream;
    private readonly BrokerServer? _server;
    private readonly RabbitMQ.RabbitMQService? _rmqService;
    private readonly Pipe _readerPipe;
    private readonly Pipe _sendPipe;
    
    public enum ProtocolType
    {
        Unknown,
        NATS,
        MQTT,
        WebSocket
    }

    private volatile ProtocolType _protocol = ProtocolType.Unknown;
    private bool _isAuthenticated = false;
    private bool _certAuthenticated = false;
    public bool IsRoute { get; set; } = false;
    public bool IsLeaf { get; set; } = false;
    public Auth.Account? Account { get; private set; }
    public Auth.User? User { get; private set; }
    public bool SupportsHeaders { get; set; } = true;

    private bool _wsHandshakeComplete = false;

    public long BytesIn { get; private set; }
    public long BytesOut { get; private set; }
    private long _droppedMsgs = 0;
    private long _pendingBytes = 0;
    public long MsgIn { get; private set; }
    public long MsgOut { get; private set; }

    // Optimization: Cached byte arrays for common protocol tokens
    private static ReadOnlySpan<byte> MsgVerb => "MSG "u8;
    private static ReadOnlySpan<byte> HMsgVerb => "HMSG "u8;
    private static ReadOnlySpan<byte> Space => " "u8;
    private static ReadOnlySpan<byte> Crlf => "\r\n"u8;
    private static readonly byte[] Pong = "PONG\r\n"u8.ToArray();
    private static ReadOnlySpan<byte> HeaderMagic => "NATS/1.0\r\nNats-Msg-TTL: "u8;
    private static ReadOnlySpan<byte> HeaderEnd => "\r\n\r\n"u8;
    private static readonly long MaxBufferedBytes =
        long.TryParse(Environment.GetEnvironmentVariable("COSMOBROKER_MAX_BUFFER_BYTES"), out var maxBuf)
            ? maxBuf
            : 1024L * 1024 * 1024; // 1GB default headroom
    private static readonly long SoftLimitBytes = (MaxBufferedBytes * 7) / 8; // 87.5% limit
    private const int SendBatchBytes = 128 * 1024; // 128KB batching


    // Optimization: Per-connection byte cache for strings to avoid re-encoding
    private readonly ConcurrentDictionary<string, byte[]> _stringByteCache = new();
    private static readonly ThreadLocal<L1MatchCache> MatchCache = new(() => new L1MatchCache());

    // Batch-flush: connections written during a ProcessPipeAsync drain, flushed once at end.
    [ThreadStatic] private static bool t_batchFlushActive;
    [ThreadStatic] private static List<BrokerConnection>? t_batchFlushList;

    // Scatter-gather: reusable segment list for multi-segment socket sends.
    private readonly List<ArraySegment<byte>> _sendSegments = new(8);

    public object GetStats() => new {
        protocol = _protocol.ToString(),
        remote_addr = _remoteEndPoint,
        bytes_in = BytesIn,
        bytes_out = BytesOut,
        msg_in = MsgIn,
        msg_out = MsgOut,
        msg_drop = Interlocked.Read(ref _droppedMsgs),
        subscriptions = _subscriptions.Count,
        account = Account?.Name
    };

    private class Subscription
    {
        public string Subject { get; init; } = string.Empty;
        public string? QueueGroup { get; init; }
        public int? MaxMsgs { get; set; }
        public int ReceivedMsgs; // accessed via Interlocked
        public bool IsRemote { get; set; } = false;
        public byte[]? CachedSidPart { get; set; } // " <sid> " encoded
        // Ensures MaxMsgs auto-unsubscribe fires exactly once across concurrent deliveries.
        public int UnsubscribedFlag; // 0 = live, 1 = unsubscribed (Interlocked)
    }

    private readonly ConcurrentDictionary<string, Subscription> _subscriptions = new();

    private readonly struct OutboundBuffer
    {
        public OutboundBuffer(byte[] buffer, int length, bool pooled)
        {
            Buffer = buffer;
            Length = length;
            Pooled = pooled;
        }
        public byte[] Buffer { get; }
        public int Length { get; }
        public bool Pooled { get; }
    }

    public BrokerConnection(Stream stream, string remoteEndPoint, TopicTree topicTree, Persistence.MessageRepository? repo = null, Auth.IAuthenticator? authenticator = null, Services.JetStreamService? jetStream = null, BrokerServer? server = null, bool sendInfoOnConnect = true, RabbitMQ.RabbitMQService? rmqService = null)
    {
        _stream = stream;
        _remoteEndPoint = remoteEndPoint;
        _topicTree = topicTree;
        _repo = repo;
        _authenticator = authenticator;
        _jetStream = jetStream ?? new Services.JetStreamService(_topicTree, _repo);
        _server = server;
        _readerPipe = new Pipe(new PipeOptions(
            pauseWriterThreshold: 64 * 1024 * 1024,
            resumeWriterThreshold: 32 * 1024 * 1024,
            minimumSegmentSize: 64 * 1024,
            useSynchronizationContext: false
        ));
        _sendPipe = new Pipe(new PipeOptions(
            pauseWriterThreshold: MaxBufferedBytes,
            resumeWriterThreshold: SoftLimitBytes,
            minimumSegmentSize: 16 * 1024,
            useSynchronizationContext: false
        ));
        _sendInfoOnConnect = sendInfoOnConnect;
        _rmqService = rmqService;

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
        var sendTask = SendLoopAsync(_stream, _sendPipe.Reader);

        if (_sendInfoOnConnect)
        {
            _ = Task.Run(async () => {
                await Task.Delay(50);
                if (_protocol == ProtocolType.Unknown || _protocol == ProtocolType.NATS)
                {
                    _protocol = ProtocolType.NATS;
                    SendInfo();
                }
            });
        }

        await Task.WhenAny(readTask, processTask, sendTask);
        // Cancel the other two tasks so they exit cleanly.
        _readerPipe.Writer.CancelPendingFlush();
        await _readerPipe.Writer.CompleteAsync();
        _sendPipe.Writer.CancelPendingFlush();
        await _sendPipe.Writer.CompleteAsync();
        _stream.Close();
        await Task.WhenAll(readTask, processTask, sendTask).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        Cleanup();
    }

    public void Close()
    {
        try { _readerPipe.Reader.CancelPendingRead(); } catch { }
        try { _readerPipe.Writer.CancelPendingFlush(); } catch { }
        try { _sendPipe.Reader.CancelPendingRead(); } catch { }
        try { _sendPipe.Writer.CancelPendingFlush(); } catch { }
        try { _stream.Close(); } catch { }
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

    public Task SendRawAsync(string rawCommand)
    {
        var bytes = Encoding.UTF8.GetBytes(rawCommand);
        BytesOut += bytes.Length;
        EnqueueBuffer(bytes, bytes.Length, pooled: false);
        return Task.CompletedTask;
    }

    public void SendInfo()
    {
        bool authRequired = _authenticator != null;
        // Always build per-connection bytes so the unique nonce is included.
        // For unauthenticated connections the nonce field is unused but still unique.
        byte[] bytes = BrokerServer.BuildInfoBytes(authRequired, _server?.LameDuckMode ?? false, _nonce);
        BytesOut += bytes.Length;
        EnqueueBuffer(bytes, bytes.Length, pooled: false);
    }

    private async Task FillPipeAsync(Stream stream, PipeWriter writer)
    {
        const int minimumBufferSize = 64 * 1024;
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
        catch (OperationCanceledException) { }
        catch (Exception ex) when (ex is not OutOfMemoryException) { Console.Error.WriteLine($"[CosmoBroker] FillPipeAsync: {ex.Message}"); }
        finally { await writer.CompleteAsync(); }
    }

    private bool ProcessPubPayload(ref ReadOnlySequence<byte> buffer, SequencePosition linePosition, ReadOnlySpan<byte> subject, ReadOnlySpan<byte> replyTo, int totalLength, int headerLen, bool isHPub, out bool accepted)
    {
        var pubStart = PerfEnabled ? Stopwatch.GetTimestamp() : 0L;
        var payloadStart = buffer.GetPosition(1, linePosition);
        var remaining = buffer.Slice(payloadStart);
        accepted = true;

        if (totalLength > MaxPayloadBytes)
        {
            SendError("Maximum Payload Exceeded");
            if (remaining.Length >= totalLength + 2)
            {
                buffer = remaining.Slice(totalLength + 2);
                return true;
            }
            return false;
        }

        if (remaining.Length >= totalLength + 2)
        {
            if (!_isAuthenticated) { SendError("Authorization Violation"); buffer = remaining.Slice(totalLength + 2); return true; }

            var fullPayload = remaining.Slice(0, totalLength);
            string? replyToStr = replyTo.IsEmpty ? null : GetCachedSubjectString(replyTo);

            if (!isHPub && _jetStream.HasStreams == false && (Account == null || string.IsNullOrEmpty(Account.SubjectPrefix))
                && !(subject.Length >= 4 && subject[0] == '$' && (subject[1] | 0x20) == 'j' && (subject[2] | 0x20) == 's' && subject[3] == '.')
                && !(subject.Length >= 5 && subject[0] == '$' && (subject[1] | 0x20) == 'r' && (subject[2] | 0x20) == 'm' && (subject[3] | 0x20) == 'q' && subject[4] == '.'))
            {
                var l1 = MatchCache.Value!;
                long subVersion = _server!.SublistVersion;
                var (res, dhSubjectStr) = l1.Get(subject, subVersion);
                
                if (res == null)
                {
                    if (_server!.SublistHasWildcards == false &&
                        _server.TryMatchSublistLiteral(subject, out var dhLpsubs, out var dhLqsubs))
                    {
                        res = new Sublist.SublistResult { Version = subVersion };
                        res.Psubs.AddRange(dhLpsubs);
                        foreach(var q in dhLqsubs.Values) res.Qsubs.Add(q);
                    }
                    else
                    {
                        res = _server.MatchSublist(subject);
                    }
                    dhSubjectStr = GetCachedSubjectString(subject);
                    l1.Set(subject, res, dhSubjectStr, subVersion);
                }

                if (res != null)
                {
                    var m0 = PerfEnabled ? Stopwatch.GetTimestamp() : 0L;

                    // 1. Deliver to individual subscribers (Psubs)
                    if (res.Psubs.Count > 0)
                    {
                        foreach (var sub in res.Psubs)
                        {
                            if (sub.Conn == this && NoEcho) continue;
                            if ((IsRoute || IsLeaf) && (sub.Conn.IsRoute || sub.Conn.IsLeaf)) continue;
                            if (!sub.Conn.SendMessageDirect(sub, subject, fullPayload, replyToStr))
                                accepted = false;
                        }
                    }

                    // 2. Deliver to queue groups (Qsubs)
                    if (res.Qsubs.Count > 0)
                    {
                        foreach (var group in res.Qsubs)
                        {
                            int count = group.Members.Count;
                            if (count == 0) continue;

                            int startIdx = (int)((uint)group.NextIndex() % (uint)count);
                            for (int i = 0; i < count; i++)
                            {
                                var sub = group.Members[(startIdx + i) % count];
                                if (sub.Conn == this && NoEcho) continue;
                                if ((IsRoute || IsLeaf) && (sub.Conn.IsRoute || sub.Conn.IsLeaf)) continue;

                                if (sub.Conn.SendMessageWithTTL(subject, sub.Sid, fullPayload, replyToStr, null))
                                {
                                    break;
                                }
                                else accepted = false;
                            }
                        }
                    }

                    if (res.Psubs.Count == 0 && res.Qsubs.Count == 0)
                    {
                        if (!_topicTree.PublishWithTTL(subject, fullPayload, replyToStr, null, this))
                            accepted = false;
                    }

                    if (PerfEnabled)
                    {
                        Interlocked.Add(ref _perfMatchNs, ElapsedNs(m0));
                        Interlocked.Increment(ref _perfMatchCount);
                    }
                }
            }
            else
            {
                string subjectStr = GetCachedSubjectString(subject);
                if (isHPub) HandleHPub(subjectStr, replyToStr, headerLen, fullPayload);
                else HandlePub(subjectStr, replyToStr, fullPayload);
            }

            buffer = remaining.Slice(totalLength + 2);
            if (PerfEnabled)
            {
                Interlocked.Add(ref _perfPubNs, ElapsedNs(pubStart));
                Interlocked.Increment(ref _perfPubCount);
                MaybeLogPerf();
            }
            return true;
        }
        return false;
    }

    private async Task ProcessPipeAsync(PipeReader reader)
    {
        try
        {
            while (true)
            {
                var result = await reader.ReadAsync();
                var buffer = result.Buffer;

                // Activate batch-flush: all SendMessageWithTTL calls during this drain
                // cycle will queue their connections here; we flush them all at once below.
                t_batchFlushActive = true;
                (t_batchFlushList ??= new List<BrokerConnection>()).Clear();

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

                        if ((lineSpan[0] | 0x20) == 'm' && (IsRoute || IsLeaf))
                        {
                            // Span-based parse: MSG <subject> <sid> [reply-to] <#bytes>
                            // Avoids full-line UTF8 decode + string.Split allocation.
                            var msgLine = lineSpan[lineSpan.Length - 1] == '\r'
                                ? lineSpan.Slice(0, lineSpan.Length - 1)
                                : lineSpan;

                            if (msgLine.Length > 4) // at minimum "MSG x"
                            {
                                var tok = msgLine.Slice(4); // skip "MSG "
                                int sp1 = tok.IndexOf((byte)' ');
                                if (sp1 != -1)
                                {
                                    var subjectBytes = tok.Slice(0, sp1);
                                    tok = tok.Slice(sp1 + 1);

                                    int sp2 = tok.IndexOf((byte)' '); // skip sid
                                    if (sp2 != -1)
                                    {
                                        tok = tok.Slice(sp2 + 1);

                                        // Remaining: [reply-to] <#bytes>
                                        int lastSp = tok.LastIndexOf((byte)' ');
                                        var lenBytes = lastSp == -1 ? tok : tok.Slice(lastSp + 1);

                                        if (Utf8Parser.TryParse(lenBytes, out int totalLength, out _))
                                        {
                                            var payloadStart = buffer.GetPosition(1, linePosition.Value);
                                            var remaining = buffer.Slice(payloadStart);

                                            if (remaining.Length >= totalLength + 2)
                                            {
                                                string subject = Encoding.UTF8.GetString(subjectBytes);
                                                string? replyTo = lastSp != -1
                                                    ? Encoding.UTF8.GetString(tok.Slice(0, lastSp))
                                                    : null;

                                                HandlePub(subject, replyTo, remaining.Slice(0, totalLength));
                                                buffer = remaining.Slice(totalLength + 2);
                                                continue;
                                            }
                                            else break;
                                        }
                                    }
                                }
                            }

                            // Malformed line — skip past newline
                            buffer = buffer.Slice(buffer.GetPosition(1, linePosition.Value));
                            continue;
                        }

                        if ((lineSpan[0] | 0x20) == 'p' && (lineSpan[1] | 0x20) == 'u' && (lineSpan[2] | 0x20) == 'b' && (lineSpan[3] == ' ' || lineSpan[3] == '\r'))
                        {
                            // PUB <subject> [reply-to] <payload-bytes>
                            var parts = lineSpan;
                            int space1 = parts.IndexOf((byte)' ');
                            if (space1 != -1)
                            {
                                var remainingLine = parts.Slice(space1 + 1);
                                int space2 = remainingLine.IndexOf((byte)' ');
                                if (space2 != -1)
                                {
                                    var subject = remainingLine.Slice(0, space2);
                                    var rest = remainingLine.Slice(space2 + 1);
                                    int space3 = rest.IndexOf((byte)' ');
                                    int totalLength;
                                    
                                    if (space3 != -1)
                                    {
                                        var replyTo = rest.Slice(0, space3);
                                        if (Utf8Parser.TryParse(rest.Slice(space3 + 1), out totalLength, out _))
                                        {
                                            if (ProcessPubPayload(ref buffer, linePosition.Value, subject, replyTo, totalLength, 0, false, out bool acc)) 
                                            {
                                                if (!acc) await Task.Yield();
                                                continue; 
                                            }
                                            else break;
                                        }
                                    }
                                    else if (Utf8Parser.TryParse(rest, out totalLength, out _))
                                    {
                                        if (ProcessPubPayload(ref buffer, linePosition.Value, subject, default, totalLength, 0, false, out bool acc))
                                        {
                                            if (!acc) await Task.Yield();
                                            continue;
                                        }
                                        else break;
                                    }
                                }
                            }
                        }

                        if ((lineSpan[0] | 0x20) == 'h' && lineSpan.Length > 5 && (lineSpan[1] | 0x20) == 'p' && (lineSpan[2] | 0x20) == 'u' && (lineSpan[3] | 0x20) == 'b')
                        {
                            // HPUB <subject> [reply-to] <header-bytes> <total-bytes>
                            var parts = lineSpan;
                            int space1 = parts.IndexOf((byte)' ');
                            if (space1 != -1)
                            {
                                var remainingLine = parts.Slice(space1 + 1);
                                int space2 = remainingLine.IndexOf((byte)' ');
                                if (space2 != -1)
                                {
                                    var subject = remainingLine.Slice(0, space2);
                                    var rest = remainingLine.Slice(space2 + 1);
                                    int space3 = rest.IndexOf((byte)' ');
                                    if (space3 != -1)
                                    {
                                        var nextPart = rest.Slice(space3 + 1);
                                        int space4 = nextPart.IndexOf((byte)' ');
                                        if (space4 != -1)
                                        {
                                            // ReplyTo present: HPUB <subj> <reply> <hdr> <tot>
                                            var replyTo = rest.Slice(0, space3);
                                            if (Utf8Parser.TryParse(nextPart.Slice(0, space4), out int headerLen, out _) && 
                                                Utf8Parser.TryParse(nextPart.Slice(space4 + 1), out int totalLen, out _))
                                            {
                                                if (ProcessPubPayload(ref buffer, linePosition.Value, subject, replyTo, totalLen, headerLen, true, out bool acc))
                                                {
                                                    if (!acc) await Task.Yield();
                                                    continue;
                                                }
                                                else break;
                                            }
                                        }
                                        else
                                        {
                                            // No ReplyTo: HPUB <subj> <hdr> <tot>
                                            if (Utf8Parser.TryParse(rest.Slice(0, space3), out int headerLen, out _) && 
                                                Utf8Parser.TryParse(rest.Slice(space3 + 1), out int totalLen, out _))
                                            {
                                                if (ProcessPubPayload(ref buffer, linePosition.Value, subject, default, totalLen, headerLen, true, out bool acc))
                                                {
                                                    if (!acc) await Task.Yield();
                                                    continue;
                                                }
                                                else break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    
                    NatsParser.ParseCommand(this, line, ref buffer, out bool _);
                    buffer = buffer.Slice(buffer.GetPosition(1, linePosition.Value));
                }

                // Deactivate batch-flush and flush all subscriber pipes once.
                t_batchFlushActive = false;
                var bfl = t_batchFlushList;
                if (bfl != null && bfl.Count > 0)
                {
                    foreach (var c in bfl)
                        _ = c._sendPipe.Writer.FlushAsync();
                    bfl.Clear();
                }

                _ = _sendPipe.Writer.FlushAsync();

                reader.AdvanceTo(buffer.Start, buffer.End);
                if (result.IsCompleted) break;
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex) when (ex is not OutOfMemoryException) { Console.Error.WriteLine($"[CosmoBroker] ProcessPipeAsync: {ex.Message}"); }
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
                EnqueueBuffer(response, response.Length, pooled: false);
                buffer = buffer.Slice(bytesConsumed);
                _wsHandshakeComplete = true;
                // Do NOT set _isAuthenticated here — WebSocket clients must go through
                // the normal NATS CONNECT+auth flow after the WebSocket framing is established.
                // Unauthenticated connections are authenticated only when no authenticator is configured.
                if (_authenticator == null) _isAuthenticated = true;
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
                    // Only auto-authenticate MQTT CONNECT when no authenticator is configured.
                    // With an authenticator, the MQTT credentials in the payload should be validated.
                    if (_authenticator == null) _isAuthenticated = true;
                    var ack = MQTT.MqttParser.CreateConnAck();
                    BytesOut += ack.Length;
                    EnqueueBuffer(ack, ack.Length, pooled: false);
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
                        EnqueueBuffer(sack, sack.Length, pooled: false);
                    }
                    break;
                case 12:
                    var resp = MQTT.MqttParser.CreatePingResp();
                    BytesOut += resp.Length;
                    EnqueueBuffer(resp, resp.Length, pooled: false);
                    break;
            }
        }
        return true;
    }

    private async Task SendLoopAsync(Stream stream, PipeReader reader)
    {
        var socket = (stream as NetworkStream)?.Socket;

        try
        {
            while (true)
            {
                var result = await reader.ReadAsync();
                var buffer = result.Buffer;
                if (buffer.IsEmpty && result.IsCompleted) break;
                var remaining = buffer;

                try
                {
                    while (!remaining.IsEmpty)
                    {
                        int bytesSent;

                        if (socket != null)
                        {
                            if (remaining.IsSingleSegment)
                            {
                                bytesSent = await socket.SendAsync(remaining.First, SocketFlags.None);
                            }
                            else
                            {
                                // Scatter-gather: one syscall for all currently buffered segments.
                                _sendSegments.Clear();
                                foreach (var mem in remaining)
                                {
                                    if (System.Runtime.InteropServices.MemoryMarshal.TryGetArray(mem, out var seg))
                                        _sendSegments.Add(seg);
                                }

                                bytesSent = await socket.SendAsync(_sendSegments, SocketFlags.None);
                            }
                        }
                        else
                        {
                            foreach (var memory in remaining)
                                await stream.WriteAsync(memory);
                            await stream.FlushAsync();
                            bytesSent = (int)remaining.Length;
                        }

                        if (bytesSent <= 0)
                            throw new IOException("Socket closed during send.");

                        remaining = remaining.Slice(bytesSent);
                        Interlocked.Add(ref _pendingBytes, -bytesSent);
                    }
                }
                finally
                {
                    reader.AdvanceTo(remaining.Start, buffer.End);
                }
            }
        }
        catch (Exception ex)
        {
            if (PerfEnabled) Console.WriteLine($"[CosmoBroker] Flush error ({_remoteEndPoint}): {ex.Message}");
        }
        finally { await reader.CompleteAsync(); }
    }

    private bool IsAllowedPublish(string subject)
    {
        if (IsRoute) return true;
        if (Account == null) return false;
        if (Account.DenyPublish.Any(p => subject.StartsWith(p, StringComparison.Ordinal))) return false;
        if (Account.AllowPublish.Count > 0 && !Account.AllowPublish.Any(p => subject.StartsWith(p, StringComparison.Ordinal))) return false;
        return true;
    }

    private bool IsAllowedSubscribe(string subject)
    {
        if (IsRoute) return true;
        if (Account == null) return false;
        if (Account.DenySubscribe.Any(p => subject.StartsWith(p, StringComparison.Ordinal))) return false;
        if (Account.AllowSubscribe.Count > 0 && !Account.AllowSubscribe.Any(p => subject.StartsWith(p, StringComparison.Ordinal))) return false;
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

        // Optimization: Pre-calculate the " <sid> " part of the MSG frame
        byte[] sidPart = Encoding.UTF8.GetBytes(" " + sid + " ");

        var sub = new Subscription { Subject = scopedSubject, QueueGroup = queueGroup, IsRemote = isRemote, CachedSidPart = sidPart };
        _subscriptions[sid] = sub;
        _topicTree.Subscribe(scopedSubject, this, sid, queueGroup);
        if (_server?.UseSublist == true) _server.AddSublist(scopedSubject, this, sid, queueGroup, sub);
        
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
                if (_server?.UseSublist == true) _server.RemoveSublist(sub.Subject, this, sid, sub.QueueGroup);
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
        string? msgId = null;
        var lines = headersStr.Split("\r\n");
        foreach (var line in lines)
        {
            if (line.StartsWith("Nats-Msg-TTL:", StringComparison.OrdinalIgnoreCase))
            {
                if (int.TryParse(line.Substring(13).Trim(), out int seconds))
                    ttl = TimeSpan.FromSeconds(seconds);
            }
            if (line.StartsWith("Nats-Msg-Id:", StringComparison.OrdinalIgnoreCase))
            {
                msgId = line.Substring(12).Trim();
            }
        }
        HandlePub(subject, replyTo, payload, ttl, msgId);
    }

    public void HandlePub(string subject, string? replyTo, ReadOnlySequence<byte> payload, TimeSpan? ttl = null, string? msgId = null)
    {
        if (!_isAuthenticated) { SendError("Authorization Violation"); return; }
        MsgIn++;
        if (!IsAllowedPublish(subject)) { SendError($"Permissions Violation for Publish to {subject}"); return; }
        bool isRabbitApi = subject.StartsWith("$RMQ.", StringComparison.OrdinalIgnoreCase);
        bool isJetStreamApi = subject.StartsWith("$JS.", StringComparison.OrdinalIgnoreCase);
        
        // Fast mapping/scoping
        string scopedSubject = subject;
        if (Account != null)
        {
            var mappedSubject = (Account.Name != "global" && !isRabbitApi && !isJetStreamApi)
                ? Account.Mappings.Map(subject)
                : subject;
            if (!string.IsNullOrEmpty(Account.SubjectPrefix) && !isRabbitApi && !isJetStreamApi)
            {
                scopedSubject = Account.SubjectPrefix + "." + mappedSubject;
            }
            else
            {
                scopedSubject = mappedSubject;
            }
        }

        string? scopedReplyTo = replyTo;
        if (replyTo != null && Account != null && !string.IsNullOrEmpty(Account.SubjectPrefix))
            scopedReplyTo = Account.SubjectPrefix + "." + replyTo;
        
        if (!IsRoute && !IsLeaf)
        {
            // ── $RMQ.* — RabbitMQ API (exchange/queue/pub/consume/ack/nack/qos) ──
            if (isRabbitApi && _rmqService != null)
            {
                var payloadBytes = payload.ToArray();
                try
                {
                    var response = _rmqService.HandleRequest(
                        subject, payloadBytes, scopedReplyTo,
                        (subj, bytes) => _topicTree.Publish(subj, new ReadOnlySequence<byte>(bytes), source: this),
                        Account,
                        User,
                        _nonce);
                    if (response != null && !string.IsNullOrEmpty(scopedReplyTo))
                        _topicTree.Publish(scopedReplyTo, new ReadOnlySequence<byte>(response), source: this);
                }
                catch (Exception ex) { Console.Error.WriteLine($"[RabbitMQ] API error: {ex.Message}"); }
                return;
            }

            // ── $JS.API.* — JetStream API ──
            if (isJetStreamApi)
            {
                if (subject.StartsWith("$JS.API.", StringComparison.OrdinalIgnoreCase))
                {
                    var payloadBytes = payload.ToArray();
                    var capturedReplyTo = scopedReplyTo;
                    _ = Task.Run(async () => {
                        try
                        {
                            var response = await _jetStream.HandleApiRequest(subject, payloadBytes);
                            if (response != null && !string.IsNullOrEmpty(capturedReplyTo))
                                _topicTree.Publish(capturedReplyTo, new ReadOnlySequence<byte>(response), source: this);
                        }
                        catch (Exception ex) { Console.Error.WriteLine($"[JetStream] API request error: {ex.Message}"); }
                    });
                    return;
                }
                if (subject.StartsWith("$JS.ACK.", StringComparison.OrdinalIgnoreCase))
                {
                    // Format: $JS.ACK.<streamName>.<consumerName>.<sequence>
                    // Span-based parse — no Split allocation.
                    var ackSpan = subject.AsSpan(8); // skip "$JS.ACK."
                    int d1 = ackSpan.IndexOf('.');
                    if (d1 != -1)
                    {
                        string streamName = ackSpan.Slice(0, d1).ToString();
                        var rest = ackSpan.Slice(d1 + 1);
                        int d2 = rest.IndexOf('.');
                        if (d2 != -1)
                        {
                            string consumerName = rest.Slice(0, d2).ToString();
                            if (long.TryParse(rest.Slice(d2 + 1), out long seq))
                            {
                                if (payload.Length > 0) {
                                    var pStr = Encoding.UTF8.GetString(payload.ToArray()).Trim();
                                    if (pStr.StartsWith("-NAK")) _jetStream.Nack(streamName, consumerName, seq);
                                    else if (pStr.StartsWith("+TERM")) _jetStream.Term(streamName, consumerName, seq);
                                    else _jetStream.Ack(streamName, consumerName, seq);
                                } else _jetStream.Ack(streamName, consumerName, seq);
                            }
                        }
                    }
                    return;
                }
            }

            if (_repo != null && scopedSubject.StartsWith("persist.", StringComparison.OrdinalIgnoreCase))
            {
                var persistPayload = payload.ToArray();
                var persistSubject = scopedSubject;
                _ = Task.Run(async () => {
                    try { await _repo.SaveMessageAsync(persistSubject, persistPayload); }
                    catch (Exception ex) { Console.Error.WriteLine($"[Persist] SaveMessageAsync error: {ex.Message}"); }
                });
            }

            if (_jetStream.HasStreams)
            {
                var streamMatches = _jetStream.GetMatchingStreams(scopedSubject).ToArray();
                if (streamMatches.Length > 0)
                {
                    var publishPayload = payload.ToArray();

                    if (!string.IsNullOrEmpty(scopedReplyTo))
                    {
                        var capturedReplyTo = scopedReplyTo;
                        _ = Task.Run(async () =>
                        {
                            try
                            {
                                Services.JetStreamPublishResult? ack = null;
                                foreach (var streamName in streamMatches)
                                {
                                    var result = await _jetStream.Publish(streamName, scopedSubject, publishPayload, ttl, msgId);
                                    ack ??= result;
                                }

                                if (ack != null)
                                {
                                    var response = Encoding.UTF8.GetBytes(System.Text.Json.JsonSerializer.Serialize(new
                                    {
                                        stream = ack.StreamName,
                                        seq = ack.Sequence,
                                        duplicate = ack.Duplicate
                                    }));
                                    _topicTree.Publish(capturedReplyTo, new ReadOnlySequence<byte>(response), source: this);
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.Error.WriteLine($"[JetStream] Publish error: {ex.Message}");
                            }
                        });
                    }
                    else
                    {
                        foreach (var streamName in streamMatches)
                            _ = _jetStream.Publish(streamName, scopedSubject, publishPayload, ttl, msgId);
                    }
                }
            }
        }
        _topicTree.PublishWithTTL(scopedSubject, payload, scopedReplyTo, ttl, this);
    }

    public void HandlePing()
    {
        BytesOut += Pong.Length;
        EnqueueBuffer(Pong, Pong.Length, pooled: false);
    }

    public async Task HandleConnect(Auth.ConnectOptions options)
    {
        NoEcho = options.NoEcho;
        if (options.Route)
        {
            IsRoute = true;
            _isAuthenticated = true;
            Account = new Auth.Account { Name = "global" };
            User = new Auth.User { Name = "route", AccountName = "global" };
            var ok = "+OK\r\n"u8;
            BytesOut += ok.Length;
            EnqueueBuffer(ok.ToArray(), ok.Length, pooled: false);
            return;
        }
        if (options.Leaf)
        {
            IsLeaf = true;
        }
        if (_authenticator != null)
        {
            if (!string.IsNullOrEmpty(options.Nkey))
            {
                if (string.IsNullOrEmpty(options.Sig))
                {
                    SendError("Missing NKEY signature");
                    await Task.Delay(50);
                    _stream.Close();
                    return;
                }

                if (!VerifyEd25519Signature(options.Nkey, options.Sig, _nonce))
                {
                    SendError("Invalid NKEY signature");
                    await Task.Delay(50);
                    _stream.Close();
                    return;
                }
            }

            if (_certAuthenticated && _authenticator is Auth.X509Authenticator)
            {
                var ok = "+OK\r\n"u8;
                BytesOut += ok.Length;
                EnqueueBuffer(ok.ToArray(), ok.Length, pooled: false);
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
                EnqueueBuffer(ok.ToArray(), ok.Length, pooled: false);
            }
        }
    }

    public bool NoEcho { get; private set; }

    public void SendMessage(string subject, string sid, ReadOnlySequence<byte> payload, string? replyTo = null) => SendMessageWithTTL(subject, sid, payload, replyTo, null);

    private const int StringByteCacheMax = 512;

    private byte[] GetCachedBytes(string s)
    {
        if (_stringByteCache.TryGetValue(s, out var cached)) return cached;
        // Evict half the entries when the cache is full to bound per-connection memory use
        // on connections that publish to many distinct dynamic subjects.
        if (_stringByteCache.Count >= StringByteCacheMax)
        {
            int toRemove = StringByteCacheMax / 2;
            foreach (var key in _stringByteCache.Keys)
            {
                if (--toRemove < 0) break;
                _stringByteCache.TryRemove(key, out _);
            }
        }
        return _stringByteCache.GetOrAdd(s, static str => Encoding.UTF8.GetBytes(str));
    }

    private void EnqueueBuffer(byte[] buffer, int length, bool pooled)
    {
        if (length <= 0) return;
        Interlocked.Add(ref _pendingBytes, length);
        _sendPipe.Writer.Write(buffer.AsSpan(0, length));
        _ = _sendPipe.Writer.FlushAsync();
        if (pooled) ArrayPool<byte>.Shared.Return(buffer);
    }

    private OutboundBuffer BuildNatsMessage(ReadOnlySpan<byte> subject, string sid, string? replyTo, bool useHeaders, int headerLen, ReadOnlySpan<byte> totalPayloadLenBytes, ReadOnlySpan<byte> ttlBytes, ReadOnlySequence<byte> payload)
    {
        var sidBytes = GetCachedBytes(sid);
        var replyBytes = !string.IsNullOrEmpty(replyTo) ? GetCachedBytes(replyTo) : null;
        
        Span<byte> hlenBuf = stackalloc byte[16];
        int hlenLen = 0;
        if (useHeaders) Utf8Formatter.TryFormat(headerLen, hlenBuf, out hlenLen);

        int size = 0;
        size += (useHeaders ? HMsgVerb.Length : MsgVerb.Length);
        size += subject.Length + Space.Length;
        size += sidBytes.Length + Space.Length;
        if (replyBytes != null) size += replyBytes.Length + Space.Length;
        if (useHeaders) size += hlenLen + Space.Length;
        size += totalPayloadLenBytes.Length + Crlf.Length;
        if (useHeaders) size += HeaderMagic.Length + ttlBytes.Length + HeaderEnd.Length;
        size += (int)payload.Length + Crlf.Length;

        var buffer = ArrayPool<byte>.Shared.Rent(size);
        var span = buffer.AsSpan();
        var offset = 0;

        (useHeaders ? HMsgVerb : MsgVerb).CopyTo(span.Slice(offset));
        offset += (useHeaders ? HMsgVerb.Length : MsgVerb.Length);

        subject.CopyTo(span.Slice(offset));
        offset += subject.Length;
        Space.CopyTo(span.Slice(offset));
        offset += Space.Length;

        sidBytes.CopyTo(span.Slice(offset));
        offset += sidBytes.Length;
        Space.CopyTo(span.Slice(offset));
        offset += Space.Length;

        if (replyBytes != null)
        {
            replyBytes.CopyTo(span.Slice(offset));
            offset += replyBytes.Length;
            Space.CopyTo(span.Slice(offset));
            offset += Space.Length;
        }
        if (useHeaders)
        {
            hlenBuf.Slice(0, hlenLen).CopyTo(span.Slice(offset));
            offset += hlenLen;
            Space.CopyTo(span.Slice(offset));
            offset += Space.Length;
        }
        totalPayloadLenBytes.CopyTo(span.Slice(offset));
        offset += totalPayloadLenBytes.Length;
        Crlf.CopyTo(span.Slice(offset));
        offset += Crlf.Length;

        if (useHeaders)
        {
            HeaderMagic.CopyTo(span.Slice(offset));
            offset += HeaderMagic.Length;
            ttlBytes.CopyTo(span.Slice(offset));
            offset += ttlBytes.Length;
            HeaderEnd.CopyTo(span.Slice(offset));
            offset += HeaderEnd.Length;
        }
        foreach (var seg in payload)
        {
            seg.Span.CopyTo(span.Slice(offset));
            offset += seg.Span.Length;
        }
        Crlf.CopyTo(span.Slice(offset));
        offset += Crlf.Length;

        return new OutboundBuffer(buffer, offset, pooled: true);
    }

    /// <summary>
    /// Fast-path delivery: subscription object carried directly from SubEntry.State,
    /// skipping the ConcurrentDictionary lookup on the hot path.
    /// </summary>
    internal bool SendMessageDirect(Sublist.SubEntry entry, ReadOnlySpan<byte> subject, ReadOnlySequence<byte> payload, string? replyTo)
    {
        var sub = entry.State as Subscription;
        if (sub == null)
            return SendMessageWithTTL(subject, entry.Sid, payload, replyTo, null);
        return SendMessageWithSubDirect(sub, entry.Sid, subject, payload, replyTo);
    }

    private bool SendMessageWithSubDirect(Subscription sub, string sid, ReadOnlySpan<byte> subject, ReadOnlySequence<byte> payload, string? replyTo)
    {
        int received = Interlocked.Increment(ref sub.ReceivedMsgs);
        MsgOut++;

        ReadOnlySpan<byte> clientSubject = subject;
        if (Account != null && !string.IsNullOrEmpty(Account.SubjectPrefix))
        {
            var prefix = GetCachedBytes(Account.SubjectPrefix);
            if (subject.Length > prefix.Length && subject.StartsWith(prefix) && subject[prefix.Length] == '.')
                clientSubject = subject.Slice(prefix.Length + 1);
        }

        if (_protocol == ProtocolType.MQTT || _protocol == ProtocolType.WebSocket)
            return SendMessageWithTTL(subject, sid, payload, replyTo, null);

        var writer = _sendPipe.Writer;
        int totalPayloadLen = (int)payload.Length;
        Span<byte> totPayloadLenBuf = stackalloc byte[16];
        Utf8Formatter.TryFormat(totalPayloadLen, totPayloadLenBuf, out int totPayloadLenLen);

        int headerEstimate = 32 + clientSubject.Length + (sub.CachedSidPart?.Length ?? 16) + (replyTo?.Length ?? 0) + totPayloadLenLen;
        var span = writer.GetSpan(headerEstimate);
        int offset = 0;

        MsgVerb.CopyTo(span.Slice(offset));
        offset += MsgVerb.Length;
        clientSubject.CopyTo(span.Slice(offset));
        offset += clientSubject.Length;

        if (sub.CachedSidPart != null)
        {
            sub.CachedSidPart.CopyTo(span.Slice(offset));
            offset += sub.CachedSidPart.Length;
        }
        else
        {
            Space.CopyTo(span.Slice(offset)); offset += Space.Length;
            offset += Encoding.UTF8.GetBytes(sid, span.Slice(offset));
            Space.CopyTo(span.Slice(offset)); offset += Space.Length;
        }

        if (replyTo != null)
        {
            offset += Encoding.UTF8.GetBytes(replyTo, span.Slice(offset));
            Space.CopyTo(span.Slice(offset));
            offset += Space.Length;
        }

        totPayloadLenBuf.Slice(0, totPayloadLenLen).CopyTo(span.Slice(offset));
        offset += totPayloadLenLen;
        Crlf.CopyTo(span.Slice(offset));
        offset += Crlf.Length;
        writer.Advance(offset);

        foreach (var seg in payload)
            writer.Write(seg.Span);
        writer.Write(Crlf);

        int totalWritten = offset + totalPayloadLen + Crlf.Length;
        BytesOut += totalWritten;
        Interlocked.Add(ref _pendingBytes, totalWritten);
        if (t_batchFlushActive)
            (t_batchFlushList ??= new List<BrokerConnection>()).Add(this);
        else
            _ = writer.FlushAsync();

        if (sub.MaxMsgs.HasValue && received >= sub.MaxMsgs.Value)
        {
            if (Interlocked.CompareExchange(ref sub.UnsubscribedFlag, 1, 0) == 0)
            {
                if (_subscriptions.TryRemove(sid, out _))
                {
                    _topicTree.Unsubscribe(sub.Subject, this, sid, sub.QueueGroup);
                    if (_server?.UseSublist == true) _server.RemoveSublist(sub.Subject, this, sid, sub.QueueGroup);
                    if (!sub.IsRemote) _server?.NotifyUnsubscription(sid);
                }
            }
        }

        return Volatile.Read(ref _pendingBytes) < SoftLimitBytes;
    }

    public bool SendMessageWithTTL(string subject, string sid, ReadOnlySequence<byte> payload, string? replyTo, TimeSpan? ttl)
    {
        if (!_subscriptions.TryGetValue(sid, out var sub))
        {
            return true;
        }

        int received = Interlocked.Increment(ref sub.ReceivedMsgs);
        MsgOut++;

        // Optimization: Zero-allocation prefix stripping using string span
        ReadOnlySpan<char> clientSubject = subject;
        if (Account != null && !string.IsNullOrEmpty(Account.SubjectPrefix))
        {
            var prefix = Account.SubjectPrefix;
            if (subject.Length > prefix.Length && subject.StartsWith(prefix) && subject[prefix.Length] == '.')
            {
                clientSubject = subject.AsSpan(prefix.Length + 1);
            }
        }

        if (_protocol == ProtocolType.MQTT)
        {
            var frame = MQTT.MqttParser.FramePublish(new string(clientSubject), payload.IsSingleSegment ? payload.FirstSpan : payload.ToArray());
            BytesOut += frame.Length;
            Interlocked.Add(ref _pendingBytes, frame.Length);
            _sendPipe.Writer.Write(frame);
            _ = _sendPipe.Writer.FlushAsync();
            return Volatile.Read(ref _pendingBytes) < SoftLimitBytes;
        }

        bool useHeaders = ttl.HasValue && SupportsHeaders;
        int headerLen = 0;
        Span<byte> ttlBytes = stackalloc byte[16];
        int ttlLen = 0;
        if (useHeaders)
        {
            Utf8Formatter.TryFormat((int)ttl!.Value.TotalSeconds, ttlBytes, out ttlLen);
            headerLen = HeaderMagic.Length + ttlLen + HeaderEnd.Length;
        }

        int totalPayloadLen = (int)payload.Length + headerLen;
        Span<byte> totPayloadLenBuf = stackalloc byte[16];
        Utf8Formatter.TryFormat(totalPayloadLen, totPayloadLenBuf, out int totPayloadLenLen);

        if (_protocol == ProtocolType.WebSocket)
        {
            // For WebSocket we still need bytes for BuildNatsMessage, but we can encode directly
            Span<byte> subjectBuf = stackalloc byte[clientSubject.Length * 3];
            int subjectByteCount = Encoding.UTF8.GetBytes(clientSubject, subjectBuf);
            var outbound = BuildNatsMessage(subjectBuf.Slice(0, subjectByteCount), sid, replyTo, useHeaders, headerLen, totPayloadLenBuf.Slice(0, totPayloadLenLen), ttlBytes.Slice(0, ttlLen), payload);
            var frame = WebSockets.WebSocketFramer.FrameMessage(outbound.Buffer.AsSpan(0, outbound.Length));
            BytesOut += frame.Length;
            EnqueueBuffer(frame, frame.Length, pooled: false);
            if (outbound.Pooled) ArrayPool<byte>.Shared.Return(outbound.Buffer);
        }
        else
        {
            // Direct write to PipeWriter to avoid extra copies and rentals
            var writer = _sendPipe.Writer;
            
            // Calculate header size
            int headerEstimate = 32 + (clientSubject.Length * 3) + (sub.CachedSidPart?.Length ?? 16) + (replyTo?.Length ?? 0) + totPayloadLenLen;
            var span = writer.GetSpan(headerEstimate);
            int offset = 0;

            (useHeaders ? HMsgVerb : MsgVerb).CopyTo(span.Slice(offset));
            offset += (useHeaders ? HMsgVerb.Length : MsgVerb.Length);

            int written = Encoding.UTF8.GetBytes(clientSubject, span.Slice(offset));
            offset += written;

            if (sub.CachedSidPart != null)
            {
                sub.CachedSidPart.CopyTo(span.Slice(offset));
                offset += sub.CachedSidPart.Length;
            }
            else
            {
                // Fallback if not cached
                Space.CopyTo(span.Slice(offset));
                offset += Space.Length;
                offset += Encoding.UTF8.GetBytes(sid, span.Slice(offset));
                Space.CopyTo(span.Slice(offset));
                offset += Space.Length;
            }

            if (replyTo != null)
            {
                offset += Encoding.UTF8.GetBytes(replyTo, span.Slice(offset));
                Space.CopyTo(span.Slice(offset));
                offset += Space.Length;
            }
            if (useHeaders)
            {
                Utf8Formatter.TryFormat(headerLen, span.Slice(offset), out int hlenLenWritten);
                offset += hlenLenWritten;
                Space.CopyTo(span.Slice(offset));
                offset += Space.Length;
            }
            totPayloadLenBuf.Slice(0, totPayloadLenLen).CopyTo(span.Slice(offset));
            offset += totPayloadLenLen;
            Crlf.CopyTo(span.Slice(offset));
            offset += Crlf.Length;
            
            writer.Advance(offset);

            if (useHeaders)
            {
                writer.Write(HeaderMagic);
                writer.Write(ttlBytes.Slice(0, ttlLen));
                writer.Write(HeaderEnd);
            }

            foreach (var seg in payload)
            {
                writer.Write(seg.Span);
            }
            writer.Write(Crlf);

            int totalWritten = offset + totalPayloadLen + Crlf.Length;
            BytesOut += totalWritten;
            Interlocked.Add(ref _pendingBytes, totalWritten);
            if (t_batchFlushActive)
                (t_batchFlushList ??= new List<BrokerConnection>()).Add(this);
            else
                _ = writer.FlushAsync();
        }

        if (sub.MaxMsgs.HasValue && received >= sub.MaxMsgs.Value)
        {
            if (Interlocked.CompareExchange(ref sub.UnsubscribedFlag, 1, 0) == 0)
            {
                if (_subscriptions.TryRemove(sid, out _))
                {
                    _topicTree.Unsubscribe(sub.Subject, this, sid, sub.QueueGroup);
                    if (_server?.UseSublist == true) _server.RemoveSublist(sub.Subject, this, sid, sub.QueueGroup);
                    if (!sub.IsRemote) _server?.NotifyUnsubscription(sid);
                }
            }
        }

        return Volatile.Read(ref _pendingBytes) < SoftLimitBytes;
    }

    public bool SendMessageWithTTL(ReadOnlySpan<byte> subject, string sid, ReadOnlySequence<byte> payload, string? replyTo, TimeSpan? ttl)
    {
        if (!_subscriptions.TryGetValue(sid, out var sub))
        {
            return true;
        }

        int received = Interlocked.Increment(ref sub.ReceivedMsgs);
        MsgOut++;

        // Optimization: Zero-allocation prefix stripping
        ReadOnlySpan<byte> clientSubject = subject;
        if (Account != null && !string.IsNullOrEmpty(Account.SubjectPrefix))
        {
            var prefix = GetCachedBytes(Account.SubjectPrefix);
            if (subject.Length > prefix.Length && subject.StartsWith(prefix) && subject[prefix.Length] == '.')
            {
                clientSubject = subject.Slice(prefix.Length + 1);
            }
        }

        if (_protocol == ProtocolType.MQTT)
        {
            var frame = MQTT.MqttParser.FramePublish(Encoding.UTF8.GetString(clientSubject), payload.IsSingleSegment ? payload.FirstSpan : payload.ToArray());
            BytesOut += frame.Length;
            Interlocked.Add(ref _pendingBytes, frame.Length);
            _sendPipe.Writer.Write(frame);
            _ = _sendPipe.Writer.FlushAsync();
            return Volatile.Read(ref _pendingBytes) < SoftLimitBytes;
        }

        bool useHeaders = ttl.HasValue && SupportsHeaders;
        int headerLen = 0;
        Span<byte> ttlBytes = stackalloc byte[16];
        int ttlLen = 0;
        if (useHeaders)
        {
            Utf8Formatter.TryFormat((int)ttl!.Value.TotalSeconds, ttlBytes, out ttlLen);
            headerLen = HeaderMagic.Length + ttlLen + HeaderEnd.Length;
        }

        int totalPayloadLen = (int)payload.Length + headerLen;
        Span<byte> totPayloadLenBuf = stackalloc byte[16];
        Utf8Formatter.TryFormat(totalPayloadLen, totPayloadLenBuf, out int totPayloadLenLen);

        if (_protocol == ProtocolType.WebSocket)
        {
            var outbound = BuildNatsMessage(clientSubject, sid, replyTo, useHeaders, headerLen, totPayloadLenBuf.Slice(0, totPayloadLenLen), ttlBytes.Slice(0, ttlLen), payload);
            var frame = WebSockets.WebSocketFramer.FrameMessage(outbound.Buffer.AsSpan(0, outbound.Length));
            BytesOut += frame.Length;
            EnqueueBuffer(frame, frame.Length, pooled: false);
            if (outbound.Pooled) ArrayPool<byte>.Shared.Return(outbound.Buffer);
        }
        else
        {
            // Direct write to PipeWriter to avoid extra copies and rentals
            var writer = _sendPipe.Writer;
            
            // Calculate header size
            int headerEstimate = 32 + clientSubject.Length + (sub.CachedSidPart?.Length ?? 16) + (replyTo?.Length ?? 0) + totPayloadLenLen;
            var span = writer.GetSpan(headerEstimate);
            int offset = 0;

            (useHeaders ? HMsgVerb : MsgVerb).CopyTo(span.Slice(offset));
            offset += (useHeaders ? HMsgVerb.Length : MsgVerb.Length);

            clientSubject.CopyTo(span.Slice(offset));
            offset += clientSubject.Length;

            if (sub.CachedSidPart != null)
            {
                sub.CachedSidPart.CopyTo(span.Slice(offset));
                offset += sub.CachedSidPart.Length;
            }
            else
            {
                // Fallback if not cached
                Space.CopyTo(span.Slice(offset));
                offset += Space.Length;
                offset += Encoding.UTF8.GetBytes(sid, span.Slice(offset));
                Space.CopyTo(span.Slice(offset));
                offset += Space.Length;
            }

            if (replyTo != null)
            {
                offset += Encoding.UTF8.GetBytes(replyTo, span.Slice(offset));
                Space.CopyTo(span.Slice(offset));
                offset += Space.Length;
            }
            if (useHeaders)
            {
                Utf8Formatter.TryFormat(headerLen, span.Slice(offset), out int hlenLenWritten);
                offset += hlenLenWritten;
                Space.CopyTo(span.Slice(offset));
                offset += Space.Length;
            }
            totPayloadLenBuf.Slice(0, totPayloadLenLen).CopyTo(span.Slice(offset));
            offset += totPayloadLenLen;
            Crlf.CopyTo(span.Slice(offset));
            offset += Crlf.Length;
            
            writer.Advance(offset);

            if (useHeaders)
            {
                writer.Write(HeaderMagic);
                writer.Write(ttlBytes.Slice(0, ttlLen));
                writer.Write(HeaderEnd);
            }

            foreach (var seg in payload)
            {
                writer.Write(seg.Span);
            }
            writer.Write(Crlf);

            int totalWritten = offset + totalPayloadLen + Crlf.Length;
            BytesOut += totalWritten;
            Interlocked.Add(ref _pendingBytes, totalWritten);
            if (t_batchFlushActive)
                (t_batchFlushList ??= new List<BrokerConnection>()).Add(this);
            else
                _ = writer.FlushAsync();
        }

        if (sub.MaxMsgs.HasValue && received >= sub.MaxMsgs.Value)
        {
            if (Interlocked.CompareExchange(ref sub.UnsubscribedFlag, 1, 0) == 0)
            {
                if (_subscriptions.TryRemove(sid, out _))
                {
                    _topicTree.Unsubscribe(sub.Subject, this, sid, sub.QueueGroup);
                    if (_server?.UseSublist == true) _server.RemoveSublist(sub.Subject, this, sid, sub.QueueGroup);
                    if (!sub.IsRemote) _server?.NotifyUnsubscription(sid);
                }
            }
        }

        return Volatile.Read(ref _pendingBytes) < SoftLimitBytes;
    }

    internal void SendError(string message)
    {
        var err = Encoding.UTF8.GetBytes($"-ERR '{message}'\r\n");
        BytesOut += err.Length;
        EnqueueBuffer(err, err.Length, pooled: false);
    }

    private static bool VerifyEd25519Signature(string publicKey, string signature, string data)
    {
        if (string.IsNullOrWhiteSpace(publicKey) || string.IsNullOrWhiteSpace(signature)) return false;
        try
        {
            var kp = KeyPair.FromPublicKey(publicKey);
            var sig = DecodeBase64Any(signature);
            var bytes = Encoding.UTF8.GetBytes(data);
            return kp.Verify(bytes, sig);
        }
        catch
        {
            return false;
        }
    }

    private static byte[] DecodeBase64Any(string input)
    {
        if (input.Contains('-') || input.Contains('_'))
            return Base64UrlDecode(input);
        return Convert.FromBase64String(input);
    }

    private static byte[] Base64UrlDecode(string input)
    {
        string padded = input.PadRight(input.Length + (4 - input.Length % 4) % 4, '=');
        string base64 = padded.Replace('-', '+').Replace('_', '/');
        return Convert.FromBase64String(base64);
    }

    private void Cleanup()
    {
        foreach (var sub in _subscriptions)
        {
            _topicTree.Unsubscribe(sub.Value.Subject, this, sub.Key, sub.Value.QueueGroup);
            // Also clean up Sublist entries to avoid memory leaks and delivery to dead connections.
            if (_server?.UseSublist == true)
                _server.RemoveSublist(sub.Value.Subject, this, sub.Key, sub.Value.QueueGroup);
        }
        _subscriptions.Clear();
        _sendPipe.Writer.Complete();
        try { _stream.Close(); } catch { }
    }

    private static readonly ThreadLocal<SubjectStringCache> SubjectCache = new(() => new SubjectStringCache());

    private static string GetCachedSubjectString(ReadOnlySpan<byte> bytes)
    {
        if (bytes.IsEmpty) return string.Empty;
        return SubjectCache.Value!.Get(bytes);
    }

    private sealed class SubjectStringCache
    {
        private const int Capacity = 4096;
        private const int Probe = 4;
        private readonly Entry[] _entries = new Entry[Capacity];
        private int _next;

        private struct Entry
        {
            public int Hash;
            public byte[]? Bytes;
            public string? Value;
        }

        public string Get(ReadOnlySpan<byte> bytes)
        {
            int hash = Hash(bytes);
            int idx = hash & (Capacity - 1);
            for (int i = 0; i < Probe; i++)
            {
                var e = _entries[(idx + i) & (Capacity - 1)];
                if (e.Bytes == null || e.Hash != hash) continue;
                if (bytes.SequenceEqual(e.Bytes)) return e.Value!;
            }

            var s = Encoding.UTF8.GetString(bytes);
            var copy = new byte[bytes.Length];
            bytes.CopyTo(copy);
            _entries[_next & (Capacity - 1)] = new Entry { Hash = hash, Bytes = copy, Value = s };
            _next++;
            return s;
        }

        private static int Hash(ReadOnlySpan<byte> bytes)
        {
            unchecked
            {
                const int fnvOffset = unchecked((int)2166136261);
                const int fnvPrime = 16777619;
                int hash = fnvOffset;
                for (int i = 0; i < bytes.Length; i++)
                {
                    hash ^= bytes[i];
                    hash *= fnvPrime;
                }
                return hash;
            }
        }
    }

    private long ElapsedNs(long start) => (long)((Stopwatch.GetTimestamp() - start) * (1_000_000_000.0 / Stopwatch.Frequency));

    private void MaybeLogPerf()
    {
        var now = Stopwatch.GetTimestamp();
        if (now - _lastPerfLog < PerfIntervalTicks) return;
        if (Interlocked.Exchange(ref _lastPerfLog, now) > now - PerfIntervalTicks) return;

        var pubs = Interlocked.Exchange(ref _perfPubCount, 0);
        var pubNs = Interlocked.Exchange(ref _perfPubNs, 0);
        var mcnt = Interlocked.Exchange(ref _perfMatchCount, 0);
        var mns = Interlocked.Exchange(ref _perfMatchNs, 0);
        var scnt = Interlocked.Exchange(ref _perfSendCount, 0);
        var sns = Interlocked.Exchange(ref _perfSendNs, 0);

        double pubUs = pubs == 0 ? 0 : pubNs / 1000.0 / pubs;
        double matchUs = mcnt == 0 ? 0 : mns / 1000.0 / mcnt;
        double sendUs = scnt == 0 ? 0 : sns / 1000.0 / scnt;

        Console.WriteLine($"[Perf] pub_us={pubUs:F2} match_us={matchUs:F2} send_us={sendUs:F2} count={pubs}");
    }

    private sealed class L1MatchCache
    {
        private const int Capacity = 1024;
        private readonly Entry[] _entries = new Entry[Capacity];

        private struct Entry
        {
            public int Hash;
            public byte[]? Bytes;
            public Sublist.SublistResult? Result;
            public string? SubjectStr;
            public long Version;
        }

        public (Sublist.SublistResult? Result, string? SubjectStr) Get(ReadOnlySpan<byte> bytes, long version)
        {
            int hash = Hash(bytes);
            int idx = hash & (Capacity - 1);
            var e = _entries[idx];
            if (e.Bytes != null && e.Hash == hash && e.Version == version && bytes.SequenceEqual(e.Bytes))
            {
                return (e.Result, e.SubjectStr);
            }
            return (null, null);
        }

        public void Set(ReadOnlySpan<byte> bytes, Sublist.SublistResult result, string subjectStr, long version)
        {
            int hash = Hash(bytes);
            int idx = hash & (Capacity - 1);
            var copy = new byte[bytes.Length];
            bytes.CopyTo(copy);
            _entries[idx] = new Entry { Hash = hash, Bytes = copy, Result = result, SubjectStr = subjectStr, Version = version };
        }

        private static int Hash(ReadOnlySpan<byte> bytes)
        {
            unchecked
            {
                const int fnvOffset = unchecked((int)2166136261);
                const int fnvPrime = 16777619;
                int hash = fnvOffset;
                for (int i = 0; i < bytes.Length; i++)
                {
                    hash ^= bytes[i];
                    hash *= fnvPrime;
                }
                return hash;
            }
        }
    }
}
