using System;
using System.Buffers;
using System.Text;

namespace CosmoBroker;

public static class NatsParser
{
    public static void ParseCommand(BrokerConnection connection, ReadOnlySequence<byte> line, ref ReadOnlySequence<byte> fullBuffer, out bool msgParsed)
    {
        msgParsed = false;
        if (line.IsEmpty) return;

        var lineSpan = line.IsSingleSegment ? line.FirstSpan : line.ToArray().AsSpan();
        if (lineSpan.Length < 3) return;

        // Command detection via Span (case insensitive) to avoid allocations for common commands
        if (StartsWith(lineSpan, "PING")) { connection.HandlePing(); return; }
        if (StartsWith(lineSpan, "PONG")) { return; }
        if (StartsWith(lineSpan, "INFO")) { _ = connection.SendInfo(); return; }

        // ARGUMENT PARSING OPTIMIZATION: Use Span-based splitting
        // NATS commands are space-separated
        string commandStr = Encoding.UTF8.GetString(lineSpan).TrimEnd('\r');
        var parts = commandStr.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length == 0) return;

        string verb = parts[0].ToUpperInvariant();

        switch (verb)
        {
            case "SUB":
                if (parts.Length >= 3)
                {
                    // Standard NATS: SUB <subject> [queue group] <sid>
                    string subject = parts[1];
                    string sid;
                    string? queueGroup = null;
                    string? durable = null;

                    if (parts.Length == 3)
                    {
                        sid = parts[2];
                    }
                    else if (parts.Length == 4)
                    {
                        if (int.TryParse(parts[2], out _)) {
                            sid = parts[2];
                            durable = parts[3];
                        } else {
                            queueGroup = parts[2];
                            sid = parts[3];
                        }
                    }
                    else
                    {
                        queueGroup = parts[2];
                        sid = parts[3];
                        durable = parts[4];
                    }

                    connection.HandleSub(subject, sid, queueGroup, durable, isRemote: connection.IsRoute);
                }
                break;

            case "UNSUB":
                if (parts.Length >= 2)
                {
                    string sid = parts[1];
                    int? maxMsgs = null;
                    if (parts.Length == 3 && int.TryParse(parts[2], out int m))
                    {
                        maxMsgs = m;
                    }
                    connection.HandleUnsub(sid, maxMsgs);
                }
                break;

            case "PUB":
            case "HPUB":
                // Handled in fast-path in BrokerConnection.cs
                break;
                
            case "CONNECT":
                {
                    int firstBrace = commandStr.IndexOf('{');
                    if (firstBrace != -1)
                    {
                        string json = commandStr.Substring(firstBrace);
                        try
                        {
                            var options = System.Text.Json.JsonSerializer.Deserialize<Auth.ConnectOptions>(json);
                            if (options != null)
                            {
                                _ = connection.HandleConnect(options);
                            }
                        }
                        catch { }
                    }
                }
                break;
        }
    }

    private static bool StartsWith(ReadOnlySpan<byte> span, string verb)
    {
        if (span.Length < verb.Length) return false;
        for (int i = 0; i < verb.Length; i++)
        {
            byte b = span[i];
            char c = verb[i];
            // Case-insensitive ASCII check
            if (b != c && b != (c + 32) && b != (c - 32)) return false; 
        }
        return span.Length == verb.Length || span[verb.Length] == ' ' || span[verb.Length] == '\r';
    }
}
