using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using CosmoBroker.RabbitMQ;

namespace CosmoBroker.Services;

public class MonitoringService
{
    private readonly BrokerServer _server;
    private readonly int _port;
    private Socket? _listenSocket;
    private Task? _acceptTask;

    public MonitoringService(BrokerServer server, int port)
    {
        _server = server;
        _port = port;
    }

    public void Start(CancellationToken ct)
    {
        _listenSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        _listenSocket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        _listenSocket.Bind(new IPEndPoint(IPAddress.Any, _port));
        _listenSocket.Listen(10);

        Console.WriteLine($"[Monitoring] HTTP Stats listening on port {_port}...");
        _acceptTask = AcceptLoopAsync(ct);
    }

    public async Task StopAsync()
    {
        try { _listenSocket?.Dispose(); } catch { }
        if (_acceptTask != null)
        {
            try { await _acceptTask; } catch { }
        }
    }

    private async Task AcceptLoopAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var socket = await _listenSocket!.AcceptAsync(ct);
                _ = HandleRequestAsync(socket);
            }
        }
        catch { }
    }

    private async Task HandleRequestAsync(Socket socket)
    {
        try
        {
            using var stream = new NetworkStream(socket, ownsSocket: true);
            byte[] buffer = new byte[4096];
            int read = await stream.ReadAsync(buffer);
            if (read == 0) return;

            string request = Encoding.UTF8.GetString(buffer, 0, read);
            string line = request.Split("\r\n")[0];
            string[] parts = line.Split(' ');
            if (parts.Length < 2) return;

            string method = parts[0];
            string rawTarget = parts[1];
            string path = rawTarget;
            var query = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            int queryIndex = rawTarget.IndexOf('?');
            if (queryIndex >= 0)
            {
                path = rawTarget[..queryIndex];
                query = ParseQueryString(rawTarget[(queryIndex + 1)..]);
            }

            object? responseData = null;
            int statusCode = 200;
            string reasonPhrase = "OK";

            if (path == "/varz")
            {
                responseData = _server.GetVarz();
            }
            else if (path == "/connz")
            {
                responseData = _server.GetConnz();
            }
            else if (path == "/routez")
            {
                responseData = _server.GetRoutez();
            }
            else if (path == "/leafz")
            {
                responseData = _server.GetLeafz();
            }
            else if (path == "/gatewayz")
            {
                responseData = _server.GetGatewayz();
            }
            else if (path == "/jsz")
            {
                responseData = _server.GetJsz();
            }
            else if (path == "/rmqz")
            {
                responseData = _server.GetRmqz();
            }
            else if (string.Equals(method, "POST", StringComparison.OrdinalIgnoreCase) && path == "/rmq/stream/reset")
            {
                string vhost = query.TryGetValue("vhost", out var vhostValue) && !string.IsNullOrWhiteSpace(vhostValue)
                    ? vhostValue
                    : "/";
                string queue = query.TryGetValue("queue", out var queueValue) ? queueValue : string.Empty;
                string consumer = query.TryGetValue("consumer", out var consumerValue) ? consumerValue : string.Empty;
                string? offsetText = query.TryGetValue("offset", out var offsetValue) ? offsetValue : null;

                if (string.IsNullOrWhiteSpace(queue) || string.IsNullOrWhiteSpace(consumer))
                {
                    statusCode = 400;
                    reasonPhrase = "Bad Request";
                    responseData = new { ok = false, error = "Queue and consumer are required." };
                }
                else
                {
                    var streamOffset = ParseStreamOffset(offsetText);
                    if (_server.TryResetRabbitStreamOffset(vhost, queue, consumer, streamOffset, out var error, out var nextOffset))
                    {
                        responseData = new
                        {
                            ok = true,
                            vhost,
                            queue,
                            consumer,
                            next_offset = nextOffset
                        };
                    }
                    else
                    {
                        statusCode = 400;
                        reasonPhrase = "Bad Request";
                        responseData = new { ok = false, error = error ?? "Unable to reset stream offset." };
                    }
                }
            }
            else if (string.Equals(method, "POST", StringComparison.OrdinalIgnoreCase) && path == "/rmq/super-stream/reset")
            {
                string vhost = query.TryGetValue("vhost", out var vhostValue) && !string.IsNullOrWhiteSpace(vhostValue)
                    ? vhostValue
                    : "/";
                string exchange = query.TryGetValue("exchange", out var exchangeValue) ? exchangeValue : string.Empty;
                string consumer = query.TryGetValue("consumer", out var consumerValue) ? consumerValue : string.Empty;
                string? offsetText = query.TryGetValue("offset", out var offsetValue) ? offsetValue : null;

                if (string.IsNullOrWhiteSpace(exchange) || string.IsNullOrWhiteSpace(consumer))
                {
                    statusCode = 400;
                    reasonPhrase = "Bad Request";
                    responseData = new { ok = false, error = "Exchange and consumer are required." };
                }
                else
                {
                    var streamOffset = ParseStreamOffset(offsetText);
                    if (_server.TryResetRabbitSuperStreamOffset(vhost, exchange, consumer, streamOffset, out var error, out var partitions))
                    {
                        responseData = new
                        {
                            ok = true,
                            vhost,
                            exchange,
                            consumer,
                            partitions
                        };
                    }
                    else
                    {
                        statusCode = 400;
                        reasonPhrase = "Bad Request";
                        responseData = new { ok = false, error = error ?? "Unable to reset super stream offsets." };
                    }
                }
            }
            else if (string.Equals(method, "GET", StringComparison.OrdinalIgnoreCase) && path == "/rmq/super-stream/route")
            {
                string vhost = query.TryGetValue("vhost", out var vhostValue) && !string.IsNullOrWhiteSpace(vhostValue)
                    ? vhostValue
                    : "/";
                string exchange = query.TryGetValue("exchange", out var exchangeValue) ? exchangeValue : string.Empty;
                string routingKey = query.TryGetValue("routing_key", out var routingKeyValue) ? routingKeyValue : string.Empty;
                string? partitionKey = query.TryGetValue("partition_key", out var partitionKeyValue) ? partitionKeyValue : null;

                if (string.IsNullOrWhiteSpace(exchange))
                {
                    statusCode = 400;
                    reasonPhrase = "Bad Request";
                    responseData = new { ok = false, error = "Exchange is required." };
                }
                else if (_server.TryResolveRabbitSuperStreamPartition(vhost, exchange, routingKey, partitionKey, out var error, out var partition))
                {
                    responseData = new
                    {
                        ok = true,
                        vhost,
                        exchange,
                        routing_key = routingKey,
                        partition_key = partitionKey,
                        partition
                    };
                }
                else
                {
                    statusCode = 400;
                    reasonPhrase = "Bad Request";
                    responseData = new { ok = false, error = error ?? "Unable to resolve super stream partition." };
                }
            }
            else if (string.Equals(method, "POST", StringComparison.OrdinalIgnoreCase) && path == "/rmq/stream/retention")
            {
                string vhost = query.TryGetValue("vhost", out var vhostValue) && !string.IsNullOrWhiteSpace(vhostValue)
                    ? vhostValue
                    : "/";
                string queue = query.TryGetValue("queue", out var queueValue) ? queueValue : string.Empty;
                long? maxLengthMessages = ParseLong(query.TryGetValue("max_length_messages", out var maxLengthValue) ? maxLengthValue : null);
                long? maxLengthBytes = ParseLong(query.TryGetValue("max_length_bytes", out var maxBytesValue) ? maxBytesValue : null);
                long? maxAgeMs = ParseLong(query.TryGetValue("max_age_ms", out var maxAgeValue) ? maxAgeValue : null);

                if (string.IsNullOrWhiteSpace(queue))
                {
                    statusCode = 400;
                    reasonPhrase = "Bad Request";
                    responseData = new { ok = false, error = "Queue is required." };
                }
                else if (_server.TryUpdateRabbitStreamRetention(vhost, queue, maxLengthMessages, maxLengthBytes, maxAgeMs, out var error))
                {
                    responseData = new
                    {
                        ok = true,
                        vhost,
                        queue,
                        max_length_messages = maxLengthMessages,
                        max_length_bytes = maxLengthBytes,
                        max_age_ms = maxAgeMs
                    };
                }
                else
                {
                    statusCode = 400;
                    reasonPhrase = "Bad Request";
                    responseData = new { ok = false, error = error ?? "Unable to update stream retention." };
                }
            }
            else if (string.Equals(method, "POST", StringComparison.OrdinalIgnoreCase) && path == "/rmq/super-stream/retention")
            {
                string vhost = query.TryGetValue("vhost", out var vhostValue) && !string.IsNullOrWhiteSpace(vhostValue)
                    ? vhostValue
                    : "/";
                string exchange = query.TryGetValue("exchange", out var exchangeValue) ? exchangeValue : string.Empty;
                long? maxLengthMessages = ParseLong(query.TryGetValue("max_length_messages", out var maxLengthValue) ? maxLengthValue : null);
                long? maxLengthBytes = ParseLong(query.TryGetValue("max_length_bytes", out var maxBytesValue) ? maxBytesValue : null);
                long? maxAgeMs = ParseLong(query.TryGetValue("max_age_ms", out var maxAgeValue) ? maxAgeValue : null);

                if (string.IsNullOrWhiteSpace(exchange))
                {
                    statusCode = 400;
                    reasonPhrase = "Bad Request";
                    responseData = new { ok = false, error = "Exchange is required." };
                }
                else if (_server.TryUpdateRabbitSuperStreamRetention(vhost, exchange, maxLengthMessages, maxLengthBytes, maxAgeMs, out var error))
                {
                    responseData = new
                    {
                        ok = true,
                        vhost,
                        exchange,
                        max_length_messages = maxLengthMessages,
                        max_length_bytes = maxLengthBytes,
                        max_age_ms = maxAgeMs
                    };
                }
                else
                {
                    statusCode = 400;
                    reasonPhrase = "Bad Request";
                    responseData = new { ok = false, error = error ?? "Unable to update super stream retention." };
                }
            }
            else
            {
                responseData = new { error = "Not Found", paths = new[] { "/varz", "/connz", "/routez", "/leafz", "/gatewayz", "/jsz", "/rmqz" } };
                statusCode = 404;
                reasonPhrase = "Not Found";
            }

            string json = JsonSerializer.Serialize(responseData, new JsonSerializerOptions { WriteIndented = true });
            byte[] jsonBytes = Encoding.UTF8.GetBytes(json);

            string headers = $"HTTP/1.1 {statusCode} {reasonPhrase}\r\n" +
                             "Content-Type: application/json\r\n" +
                             $"Content-Length: {jsonBytes.Length}\r\n" +
                             "Connection: close\r\n\r\n";
            
            await stream.WriteAsync(Encoding.UTF8.GetBytes(headers));
            await stream.WriteAsync(jsonBytes);
            await stream.FlushAsync();
        }
        catch { }
    }

    private static Dictionary<string, string> ParseQueryString(string queryString)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (string.IsNullOrWhiteSpace(queryString))
            return result;

        foreach (var part in queryString.Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var pieces = part.Split('=', 2);
            var key = WebUtility.UrlDecode(pieces[0]);
            var value = pieces.Length > 1 ? WebUtility.UrlDecode(pieces[1]) : string.Empty;
            if (!string.IsNullOrWhiteSpace(key))
                result[key] = value;
        }

        return result;
    }

    private static RabbitStreamOffsetSpec? ParseStreamOffset(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        if (long.TryParse(value, out var numericOffset))
            return new RabbitStreamOffsetSpec { Kind = RabbitStreamOffsetKind.Offset, Offset = numericOffset };

        return value.Trim().ToLowerInvariant() switch
        {
            "first" => new RabbitStreamOffsetSpec { Kind = RabbitStreamOffsetKind.First },
            "last" => new RabbitStreamOffsetSpec { Kind = RabbitStreamOffsetKind.Last },
            "next" => new RabbitStreamOffsetSpec { Kind = RabbitStreamOffsetKind.Next },
            _ => null
        };
    }

    private static long? ParseLong(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;
        return long.TryParse(value, out var result) ? result : null;
    }
}
