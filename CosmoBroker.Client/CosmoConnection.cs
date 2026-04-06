using System;
using System.Buffers;
using System.Buffers.Text;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using CosmoBroker.Client.Models;
using CosmoBroker.Client.Protocol;

namespace CosmoBroker.Client;

internal class CosmoConnection : IAsyncDisposable
{
    private readonly CosmoClientOptions _options;
    private Socket? _socket;
    private Stream? _netStream;          // NetworkStream or SslStream (socket I/O)
    private PipeReader? _reader;          // read path on _netStream
    private readonly Pipe _sendPipe;      // in-memory write buffer
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private Task? _flushTask;            // tracks pending pipe flush
    private Task? _readLoopTask;
    private Task? _writeLoopTask;
    private readonly CancellationTokenSource _cts = new();

    private readonly ConcurrentQueue<TaskCompletionSource> _pings = new();
    private int _connectedFlag;
    private int _disconnectHandled;
    private int _disposeStarted;

    private static readonly StreamPipeReaderOptions ReaderOptions =
        new(bufferSize: 65_536, minimumReadSize: 8_192);

    // In-memory pipe: large segments reduce fragmentation; bounded to apply backpressure.
    private static readonly PipeOptions SendPipeOptions = new PipeOptions(
        pauseWriterThreshold: 1024 * 1024,
        resumeWriterThreshold: 512 * 1024,
        minimumSegmentSize: 16_384,
        useSynchronizationContext: false);

    public ServerInfo? ServerInfo { get; private set; }
    public bool IsConnected => Volatile.Read(ref _connectedFlag) == 1 && !_cts.IsCancellationRequested;

    public Action<CosmoMessage>? OnMessage;
    public Action<Exception?>? OnDisconnected;

    public CosmoConnection(CosmoClientOptions options)
    {
        _options = options;
        _sendPipe = new Pipe(SendPipeOptions);
    }

    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        using var timeoutCts = new CancellationTokenSource(_options.Timeout);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);
        var ct = linkedCts.Token;

        var uri = new Uri(_options.Url);
        var host = uri.Host;
        var port = uri.Port == -1 ? 4222 : uri.Port;
        var useTls = uri.Scheme == "tls";

        _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
        {
            NoDelay = true,
            SendBufferSize = 1024 * 1024,
            ReceiveBufferSize = 1024 * 1024
        };
        await _socket.ConnectAsync(host, port, ct);

        var networkStream = new NetworkStream(_socket, ownsSocket: false);
        _netStream = networkStream;
        _reader = PipeReader.Create(_netStream, ReaderOptions);

        var infoResult = await _reader.ReadAsync(ct);
        var infoBuffer = infoResult.Buffer;

        if (TryParseInfo(ref infoBuffer, out var serverInfo))
        {
            ServerInfo = serverInfo;
            // Advance consumed position to after the INFO line; any remaining data is preserved.
            _reader.AdvanceTo(infoBuffer.Start);
        }
        else
        {
            throw new Exception("Failed to receive INFO from server.");
        }

        if (useTls || (ServerInfo?.TlsRequired ?? false))
        {
            await _reader.CompleteAsync();

            var sslStream = new SslStream(networkStream, false, (_, _, _, _) => true);
            var sslOptions = new SslClientAuthenticationOptions
            {
                TargetHost = host,
                ClientCertificates = _options.ClientCertificate != null ? new() { _options.ClientCertificate } : null,
                EnabledSslProtocols = System.Security.Authentication.SslProtocols.Tls12 | System.Security.Authentication.SslProtocols.Tls13
            };

            await sslStream.AuthenticateAsClientAsync(sslOptions, ct);
            _netStream = sslStream;
            _reader = PipeReader.Create(_netStream, ReaderOptions);
        }

        var connectReq = new ConnectRequest
        {
            Verbose = false,
            Pedantic = false,
            TlsRequired = useTls,
            User = _options.Username,
            Pass = _options.Password,
            AuthToken = _options.Token,
            Jwt = _options.Jwt
        };

        // Send CONNECT + PING directly on the stream (before write loop starts).
        var connectJson = JsonSerializer.Serialize(connectReq);
        await SendImmediateAsync(Encoding.UTF8.GetBytes($"CONNECT {connectJson}\r\n"), ct);
        await SendImmediateAsync("PING\r\n"u8.ToArray(), ct);

        _writeLoopTask = Task.Run(() => WriteLoopAsync(_cts.Token));
        _readLoopTask = Task.Run(() => ReadLoopAsync(_cts.Token));
        Volatile.Write(ref _connectedFlag, 1);
    }

    private async Task SendImmediateAsync(ReadOnlyMemory<byte> data, CancellationToken ct)
    {
        await _netStream!.WriteAsync(data, ct);
        await _netStream.FlushAsync(ct);
    }

    private static bool TryParseInfo(ref ReadOnlySequence<byte> buffer, out ServerInfo? info)
    {
        info = null;
        var reader = new SequenceReader<byte>(buffer);
        if (!reader.TryReadTo(out ReadOnlySequence<byte> line, "\r\n"u8))
            return false;

        var lineSpan = line.IsSingleSegment ? line.FirstSpan : line.ToArray().AsSpan();
        if (!lineSpan.StartsWith("INFO "u8))
            return false;

        var json = Encoding.UTF8.GetString(lineSpan.Slice(5));
        info = JsonSerializer.Deserialize<ServerInfo>(json);
        buffer = buffer.Slice(reader.Position);
        return true;
    }

    /// <summary>
    /// Reads from the in-memory send pipe and writes to the socket.
    /// Batches naturally: one ReadAsync drains all messages accumulated while the previous send was in progress.
    /// </summary>
    private async Task WriteLoopAsync(CancellationToken ct)
    {
        var reader = _sendPipe.Reader;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var result = await reader.ReadAsync(ct);
                var buffer = result.Buffer;

                if (!buffer.IsEmpty)
                {
                    foreach (var segment in buffer)
                        await _netStream!.WriteAsync(segment, ct);
                    reader.AdvanceTo(buffer.End);
                }
                else
                {
                    reader.AdvanceTo(buffer.Start);
                }

                if (result.IsCompleted) break;
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            RememberDisconnect(ex);
            Console.Error.WriteLine($"[CONN] WriteLoop: {ex.Message}");
        }
        finally
        {
            HandleDisconnect();
            await reader.CompleteAsync();
        }
    }

    private async Task ReadLoopAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var result = await _reader!.ReadAsync(ct);
                var buffer = result.Buffer;

                while (ProtocolParser.TryParseCommand(ref buffer, out var cmd))
                {
                    HandleCommand(in cmd);
                }

                _reader.AdvanceTo(buffer.Start, buffer.End);
                if (result.IsCompleted) break;
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            RememberDisconnect(ex);
            Console.Error.WriteLine($"[CONN] ReadLoop: {ex.Message}");
        }
        finally
        {
            HandleDisconnect();
        }
    }

    public async Task PingAsync(CancellationToken ct = default)
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _pings.Enqueue(tcs);
        await SendCommandAsync("PING", ct);
        await tcs.Task.WaitAsync(ct);
    }

    private void HandleCommand(in ParsedCommand cmd)
    {
        switch (cmd.Type)
        {
            case CommandType.Pong:
                if (_pings.TryDequeue(out var tcs)) tcs.TrySetResult();
                break;
            case CommandType.Ping:
                _ = SendCommandAsync("PONG");
                break;
            case CommandType.Msg:
                var payloadCopy = cmd.Payload.ToArray();
                OnMessage?.Invoke(new CosmoMessage
                {
                    Subject = cmd.Subject,
                    ReplyTo = cmd.ReplyTo,
                    Data = new ReadOnlySequence<byte>(payloadCopy)
                });
                break;
            case CommandType.HMsg:
                var hmsgPayload = cmd.Payload.ToArray();
                Dictionary<string, string>? headers = null;
                ReadOnlySequence<byte> body;
                if (cmd.HdrSize > 0 && cmd.HdrSize <= hmsgPayload.Length)
                {
                    headers = ParseNatsHeaders(hmsgPayload.AsSpan(0, cmd.HdrSize));
                    body = new ReadOnlySequence<byte>(hmsgPayload, cmd.HdrSize, hmsgPayload.Length - cmd.HdrSize);
                }
                else
                {
                    body = new ReadOnlySequence<byte>(hmsgPayload);
                }
                OnMessage?.Invoke(new CosmoMessage
                {
                    Subject = cmd.Subject,
                    ReplyTo = cmd.ReplyTo,
                    Data = body,
                    Headers = headers
                });
                break;
        }
    }

    /// <summary>
    /// Writes a command string into the send pipe.
    /// Fast-path: semaphore acquired synchronously → no async allocation in uncontested case.
    /// </summary>
    public ValueTask SendCommandAsync(string command, CancellationToken ct = default)
    {
        ThrowIfConnectionClosed(ct);

        try
        {
            if (!_writeLock.Wait(0))
                return SendCommandAsyncSlow(command, ct);
        }
        catch (ObjectDisposedException ex)
        {
            throw CreateConnectionClosedException(ex, ct);
        }

        try
        {
            ThrowIfConnectionClosed(ct);
            WriteCommandFrame(_sendPipe.Writer, command);
            var flush = EnqueueFlush(ct);
            ReleaseWriteLock();
            return flush;
        }
        catch
        {
            ReleaseWriteLock();
            throw;
        }
    }

    private async ValueTask SendCommandAsyncSlow(string command, CancellationToken ct)
    {
        ThrowIfConnectionClosed(ct);

        try
        {
            await _writeLock.WaitAsync(ct);
        }
        catch (ObjectDisposedException ex)
        {
            throw CreateConnectionClosedException(ex, ct);
        }

        try
        {
            ThrowIfConnectionClosed(ct);
            if (_flushTask is { IsCompleted: false }) await _flushTask;
            WriteCommandFrame(_sendPipe.Writer, command);
            await EnqueueFlush(ct);
        }
        finally { ReleaseWriteLock(); }
    }

    /// <summary>
    /// Serialises and enqueues a PUB frame into the send pipe.
    /// Fast-path: semaphore acquired synchronously → no async allocation in uncontested case.
    /// </summary>
    public ValueTask SendMessageAsync(string subject, string? replyTo, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        ThrowIfConnectionClosed(ct);

        try
        {
            if (!_writeLock.Wait(0))
                return SendMessageAsyncSlow(subject, replyTo, payload, ct);
        }
        catch (ObjectDisposedException ex)
        {
            throw CreateConnectionClosedException(ex, ct);
        }

        try
        {
            ThrowIfConnectionClosed(ct);
            WritePublishFrame(_sendPipe.Writer, subject, replyTo, payload);
            var flush = EnqueueFlush(ct);
            ReleaseWriteLock();
            return flush;
        }
        catch
        {
            ReleaseWriteLock();
            throw;
        }
    }

    private async ValueTask SendMessageAsyncSlow(string subject, string? replyTo, ReadOnlyMemory<byte> payload, CancellationToken ct)
    {
        ThrowIfConnectionClosed(ct);

        try
        {
            await _writeLock.WaitAsync(ct);
        }
        catch (ObjectDisposedException ex)
        {
            throw CreateConnectionClosedException(ex, ct);
        }

        try
        {
            ThrowIfConnectionClosed(ct);
            if (_flushTask is { IsCompleted: false }) await _flushTask;
            WritePublishFrame(_sendPipe.Writer, subject, replyTo, payload);
            await EnqueueFlush(ct);
        }
        finally { ReleaseWriteLock(); }
    }

    /// <summary>
    /// Triggers a pipe flush. Caller is responsible for releasing _writeLock.
    /// For an in-memory pipe, FlushAsync is synchronous unless backpressure kicks in.
    /// </summary>
    private ValueTask EnqueueFlush(CancellationToken ct)
    {
        var flush = _sendPipe.Writer.FlushAsync(ct);
        if (flush.IsCompletedSuccessfully)
        {
            _flushTask = null;
            return default;
        }
        _flushTask = flush.AsTask();
        return new ValueTask(_flushTask);
    }

    private static void WriteCommandFrame(PipeWriter writer, string command)
    {
        var maxLen = Encoding.UTF8.GetMaxByteCount(command.Length) + 2;
        var span = writer.GetSpan(maxLen);
        var len = Encoding.UTF8.GetBytes(command, span);
        span[len] = (byte)'\r';
        span[len + 1] = (byte)'\n';
        writer.Advance(len + 2);
    }

    private static void WritePublishFrame(PipeWriter writer, string subject, string? replyTo, ReadOnlyMemory<byte> payload)
    {
        // Header line: "PUB <subject> [<replyTo>] <bytes>\r\n"
        var subjByteLen = Encoding.UTF8.GetMaxByteCount(subject.Length);
        var replyByteLen = replyTo != null ? Encoding.UTF8.GetMaxByteCount(replyTo.Length) : 0;
        var headerMax = 4 + subjByteLen + (replyByteLen > 0 ? replyByteLen + 1 : 0) + 12;

        var span = writer.GetSpan(headerMax);
        "PUB "u8.CopyTo(span);
        int pos = 4;
        pos += Encoding.UTF8.GetBytes(subject, span.Slice(pos));
        if (replyByteLen > 0)
        {
            span[pos++] = (byte)' ';
            pos += Encoding.UTF8.GetBytes(replyTo!, span.Slice(pos));
        }
        span[pos++] = (byte)' ';
        Utf8Formatter.TryFormat(payload.Length, span.Slice(pos), out int numLen);
        pos += numLen;
        span[pos++] = (byte)'\r';
        span[pos++] = (byte)'\n';
        writer.Advance(pos);

        // Payload + trailing CRLF written directly into pipe segments (zero-copy for single-segment payloads).
        writer.Write(payload.Span);
        writer.Write("\r\n"u8);
    }

    private static Dictionary<string, string>? ParseNatsHeaders(ReadOnlySpan<byte> headerBlock)
    {
        var text = Encoding.UTF8.GetString(headerBlock);
        var lines = text.Split("\r\n", StringSplitOptions.RemoveEmptyEntries);
        if (lines.Length <= 1) return null;

        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (int i = 1; i < lines.Length; i++)
        {
            var colonIdx = lines[i].IndexOf(':');
            if (colonIdx < 0) continue;
            result[lines[i].Substring(0, colonIdx).Trim()] = lines[i].Substring(colonIdx + 1).Trim();
        }
        return result.Count > 0 ? result : null;
    }

    public async ValueTask DisposeAsync()
    {
        Interlocked.Exchange(ref _disposeStarted, 1);
        HandleDisconnect();
        _sendPipe.Writer.Complete();
        if (_readLoopTask != null) await _readLoopTask;
        if (_writeLoopTask != null) await _writeLoopTask;
        if (_reader != null) await _reader.CompleteAsync();
        _netStream?.Dispose();
        _socket?.Dispose();
        _cts.Dispose();
    }

    private void ThrowIfConnectionClosed(CancellationToken ct)
    {
        if (ct.IsCancellationRequested)
            ct.ThrowIfCancellationRequested();

        if (Volatile.Read(ref _disposeStarted) == 1 || _cts.IsCancellationRequested || !IsConnected)
            throw CreateConnectionClosedException(ct: ct);
    }

    private IOException CreateConnectionClosedException(Exception? inner = null, CancellationToken ct = default)
    {
        if (ct.IsCancellationRequested)
            throw new OperationCanceledException(ct);

        return new IOException("Connection closed.", inner);
    }

    private void ReleaseWriteLock()
    {
        try
        {
            _writeLock.Release();
        }
        catch (ObjectDisposedException)
        {
            // Concurrent teardown won the race; the caller will observe the closed connection elsewhere.
        }
    }

    private void HandleDisconnect()
    {
        if (Interlocked.Exchange(ref _disconnectHandled, 1) != 0)
            return;

        Volatile.Write(ref _connectedFlag, 0);

        try { _cts.Cancel(); } catch { }
        try { _reader?.CancelPendingRead(); } catch { }
        try { _sendPipe.Reader.CancelPendingRead(); } catch { }
        try { _sendPipe.Writer.CancelPendingFlush(); } catch { }
        try { _netStream?.Close(); } catch { }
        try { _socket?.Dispose(); } catch { }

        while (_pings.TryDequeue(out var pendingPing))
            pendingPing.TrySetException(new IOException("Connection closed."));

        OnDisconnected?.Invoke(_lastDisconnectException);
    }

    private void RememberDisconnect(Exception ex)
    {
        Interlocked.CompareExchange(ref _lastDisconnectException, ex, null);
    }

    private Exception? _lastDisconnectException;
}
