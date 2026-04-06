using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using CosmoBroker.Client.Models;
using System.IO;

namespace CosmoBroker.Client;

public class CosmoClient : IAsyncDisposable
{
    private readonly CosmoConnection _connection;
    private readonly string _inboxPrefix = $"_INBOX.{Guid.NewGuid():N}.";
    private long _nextSid = 0;

    private readonly ConcurrentDictionary<string, Channel<CosmoMessage>> _exactSubscriptions =
        new(StringComparer.OrdinalIgnoreCase);
    private readonly List<(string Pattern, Channel<CosmoMessage> Channel)> _wildcardSubscriptions = new();
    private readonly ReaderWriterLockSlim _wildcardLock = new();

    private readonly ConcurrentDictionary<string, TaskCompletionSource<CosmoMessage>> _requests =
        new(StringComparer.OrdinalIgnoreCase);
    private int _disconnectNotified;

    public bool IsConnected => _connection.IsConnected;

    public CosmoClient(CosmoClientOptions? options = null)
    {
        _connection = new CosmoConnection(options ?? new CosmoClientOptions());
        _connection.OnMessage = HandleIncomingMessage;
        _connection.OnDisconnected = HandleDisconnected;
    }

    public async Task ConnectAsync(CancellationToken ct = default)
    {
        await _connection.ConnectAsync(ct);
        var sid = Interlocked.Increment(ref _nextSid);
        await _connection.SendCommandAsync($"SUB {_inboxPrefix}* {sid}", ct);
    }

    public Task PingAsync(CancellationToken ct = default) => _connection.PingAsync(ct);

    private void HandleIncomingMessage(CosmoMessage msg)
    {
        if (_requests.TryRemove(msg.Subject, out var tcs))
        {
            tcs.TrySetResult(msg);
            return;
        }

        if (_exactSubscriptions.TryGetValue(msg.Subject, out var exactChannel))
            exactChannel.Writer.TryWrite(msg);

        if (_wildcardSubscriptions.Count > 0)
        {
            _wildcardLock.EnterReadLock();
            try
            {
                foreach (var (pattern, channel) in _wildcardSubscriptions)
                    if (Matches(pattern, msg.Subject))
                        channel.Writer.TryWrite(msg);
            }
            finally
            {
                _wildcardLock.ExitReadLock();
            }
        }
    }

    private void HandleDisconnected(Exception? exception = null)
    {
        if (Interlocked.Exchange(ref _disconnectNotified, 1) != 0)
            return;

        if (exception != null)
            Console.Error.WriteLine($"[CosmoClient] Disconnected due to {exception.GetType().Name}: {exception.Message}");

        var ex = new IOException("Connection closed.");

        foreach (var pending in _requests.Values)
            pending.TrySetException(ex);

        foreach (var sub in _exactSubscriptions.Values)
            sub.Writer.TryComplete();

        _wildcardLock.EnterReadLock();
        try
        {
            foreach (var sub in _wildcardSubscriptions)
                sub.Channel.Writer.TryComplete();
        }
        finally
        {
            _wildcardLock.ExitReadLock();
        }
    }

    private static bool Matches(string pattern, string subject)
    {
        if (pattern == ">") return true;
        if (pattern.EndsWith(".>"))
            return subject.StartsWith(pattern.AsSpan(0, pattern.Length - 1), StringComparison.OrdinalIgnoreCase);

        // Allocation-free span-based token matching.
        var pSpan = pattern.AsSpan();
        var sSpan = subject.AsSpan();

        while (true)
        {
            int pDot = pSpan.IndexOf('.');
            int sDot = sSpan.IndexOf('.');

            var pTok = pDot >= 0 ? pSpan.Slice(0, pDot) : pSpan;
            var sTok = sDot >= 0 ? sSpan.Slice(0, sDot) : sSpan;

            if (pTok.Equals(">", StringComparison.Ordinal)) return true;
            if (sSpan.IsEmpty && !pSpan.IsEmpty) return false;
            if (!pTok.Equals("*", StringComparison.Ordinal) &&
                !pTok.Equals(sTok, StringComparison.OrdinalIgnoreCase)) return false;

            bool pDone = pDot < 0;
            bool sDone = sDot < 0;
            if (pDone && sDone) return true;
            if (pDone || sDone) return false;

            pSpan = pSpan.Slice(pDot + 1);
            sSpan = sSpan.Slice(sDot + 1);
        }
    }

    public ValueTask PublishAsync(string subject, ReadOnlySequence<byte> payload, string? replyTo = null, CancellationToken ct = default)
    {
        if (payload.IsSingleSegment) return _connection.SendMessageAsync(subject, replyTo, payload.First, ct);
        return _connection.SendMessageAsync(subject, replyTo, payload.ToArray(), ct);
    }

    public ValueTask PublishAsync(string subject, ReadOnlyMemory<byte> payload, string? replyTo = null, CancellationToken ct = default)
        => _connection.SendMessageAsync(subject, replyTo, payload, ct);

    public ValueTask PublishAsync(string subject, string payload, string? replyTo = null, CancellationToken ct = default)
        => PublishAsync(subject, Encoding.UTF8.GetBytes(payload), replyTo, ct);

    public async Task<CosmoMessage> RequestAsync(string subject, ReadOnlyMemory<byte> payload, TimeSpan? timeout = null, CancellationToken ct = default)
    {
        var inbox = $"{_inboxPrefix}{Guid.NewGuid():N}";
        var tcs = new TaskCompletionSource<CosmoMessage>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(timeout ?? TimeSpan.FromSeconds(5));

        _requests[inbox] = tcs;
        await PublishAsync(subject, payload, inbox, ct);

        try
        {
            using var reg = timeoutCts.Token.Register(() => tcs.TrySetCanceled());
            return await tcs.Task;
        }
        finally
        {
            _requests.TryRemove(inbox, out _);
        }
    }

    public async IAsyncEnumerable<CosmoMessage> SubscribeAsync(string subject, string? queueGroup = null, [EnumeratorCancellation] CancellationToken ct = default)
    {
        var sid = Interlocked.Increment(ref _nextSid).ToString();
        var channel = Channel.CreateUnbounded<CosmoMessage>(new UnboundedChannelOptions { SingleReader = true });

        bool isWildcard = subject.Contains('*') || subject.Contains('>');
        if (isWildcard)
        {
            _wildcardLock.EnterWriteLock();
            try { _wildcardSubscriptions.Add((subject, channel)); }
            finally { _wildcardLock.ExitWriteLock(); }
        }
        else
        {
            _exactSubscriptions[subject] = channel;
        }

        var qGroupStr = queueGroup != null ? $" {queueGroup}" : "";
        await _connection.SendCommandAsync($"SUB {subject}{qGroupStr} {sid}", ct);

        try
        {
            await foreach (var msg in channel.Reader.ReadAllAsync(ct))
                yield return msg;
        }
        finally
        {
            if (isWildcard)
            {
                _wildcardLock.EnterWriteLock();
                try { _wildcardSubscriptions.RemoveAll(x => x.Pattern == subject && x.Channel == channel); }
                finally { _wildcardLock.ExitWriteLock(); }
            }
            else
            {
                _exactSubscriptions.TryRemove(subject, out _);
            }
            if (_connection.IsConnected)
            {
                using var unsubCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                try
                {
                    await _connection.SendCommandAsync($"UNSUB {sid}", unsubCts.Token).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is IOException or ObjectDisposedException or InvalidOperationException)
                {
                }
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        HandleDisconnected();

        await _connection.DisposeAsync();
    }
}
