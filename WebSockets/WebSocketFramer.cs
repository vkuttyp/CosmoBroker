using System;
using System.Buffers;
using System.Security.Cryptography;
using System.Text;

namespace CosmoBroker.WebSockets;

public static class WebSocketFramer
{
    public static bool TryParseUpgradeRequest(ReadOnlySequence<byte> buffer, out string requestStr, out int bytesConsumed)
    {
        requestStr = string.Empty;
        bytesConsumed = 0;
        
        var seqSpan = buffer.IsSingleSegment ? buffer.FirstSpan : buffer.ToArray().AsSpan();
        string text = Encoding.UTF8.GetString(seqSpan);
        
        int endOfHeaders = text.IndexOf("\r\n\r\n");
        if (endOfHeaders != -1)
        {
            requestStr = text.Substring(0, endOfHeaders + 4);
            bytesConsumed = Encoding.UTF8.GetByteCount(requestStr);
            return true;
        }
        return false;
    }

    public static byte[] CreateHandshakeResponse(string request)
    {
        string key = string.Empty;
        var lines = request.Split("\r\n", StringSplitOptions.RemoveEmptyEntries);
        foreach (var line in lines)
        {
            if (line.StartsWith("Sec-WebSocket-Key:", StringComparison.OrdinalIgnoreCase))
            {
                key = line.Substring("Sec-WebSocket-Key:".Length).Trim();
                break;
            }
        }

        string magicStr = key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
        byte[] magicBytes = SHA1.HashData(Encoding.UTF8.GetBytes(magicStr));
        string accept = Convert.ToBase64String(magicBytes);

        string response = "HTTP/1.1 101 Switching Protocols\r\n" +
                          "Upgrade: websocket\r\n" +
                          "Connection: Upgrade\r\n" +
                          $"Sec-WebSocket-Accept: {accept}\r\n\r\n";
                          
        return Encoding.UTF8.GetBytes(response);
    }

    public static bool TryUnframeMessage(ref ReadOnlySequence<byte> buffer, out byte[] payload)
    {
        payload = Array.Empty<byte>();
        if (buffer.Length < 2) return false;

        var span = buffer.IsSingleSegment ? buffer.FirstSpan : buffer.ToArray().AsSpan();
        
        bool fin = (span[0] & 0b10000000) != 0;
        int opcode = span[0] & 0b00001111;
        bool mask = (span[1] & 0b10000000) != 0;
        int payloadLen = span[1] & 0b01111111;

        int offset = 2;

        if (payloadLen == 126)
        {
            if (buffer.Length < 4) return false;
            payloadLen = (span[2] << 8) | span[3];
            offset = 4;
        }
        else if (payloadLen == 127)
        {
            if (buffer.Length < 10) return false;
            // Just take the lower 4 bytes for simplicity in this prototype
            payloadLen = (span[6] << 24) | (span[7] << 16) | (span[8] << 8) | span[9];
            offset = 10;
        }

        byte[] maskingKey = new byte[4];
        if (mask)
        {
            if (buffer.Length < offset + 4) return false;
            maskingKey[0] = span[offset++];
            maskingKey[1] = span[offset++];
            maskingKey[2] = span[offset++];
            maskingKey[3] = span[offset++];
        }

        if (buffer.Length < offset + payloadLen) return false;

        payload = new byte[payloadLen];
        for (int i = 0; i < payloadLen; i++)
        {
            payload[i] = (byte)(span[offset + i] ^ (mask ? maskingKey[i % 4] : (byte)0));
        }

        buffer = buffer.Slice(offset + payloadLen);
        return true;
    }

    public static byte[] FrameMessage(ReadOnlySpan<byte> payload)
    {
        int headerLen = payload.Length <= 125 ? 2 : (payload.Length <= 65535 ? 4 : 10);
        byte[] frame = new byte[headerLen + payload.Length];
        
        frame[0] = 0b10000001; // FIN + Text opcode
        
        if (payload.Length <= 125)
        {
            frame[1] = (byte)payload.Length;
        }
        else if (payload.Length <= 65535)
        {
            frame[1] = 126;
            frame[2] = (byte)((payload.Length >> 8) & 255);
            frame[3] = (byte)(payload.Length & 255);
        }
        else
        {
            frame[1] = 127;
            // Simplified 64-bit length
            frame[6] = (byte)((payload.Length >> 24) & 255);
            frame[7] = (byte)((payload.Length >> 16) & 255);
            frame[8] = (byte)((payload.Length >> 8) & 255);
            frame[9] = (byte)(payload.Length & 255);
        }

        payload.CopyTo(frame.AsSpan(headerLen));
        return frame;
    }
}
