using System;
using System.Buffers;
using System.Text;

namespace CosmoBroker.MQTT;

public static class MqttParser
{
    public static bool TryParsePacket(ref ReadOnlySequence<byte> buffer, out byte packetType, out byte[] payload)
    {
        packetType = 0;
        payload = Array.Empty<byte>();

        if (buffer.Length < 2) return false;

        var span = buffer.IsSingleSegment ? buffer.FirstSpan : buffer.ToArray().AsSpan();
        packetType = (byte)(span[0] >> 4);
        
        int multiplier = 1;
        int value = 0;
        int offset = 1;
        byte encodedByte;

        do
        {
            if (offset >= buffer.Length) return false;
            encodedByte = span[offset++];
            value += (encodedByte & 127) * multiplier;
            multiplier *= 128;
            if (multiplier > 128 * 128 * 128) return false; // Malformed Remaining Length
        } while ((encodedByte & 128) != 0);

        int totalLen = offset + value;
        if (buffer.Length < totalLen) return false;

        payload = span.Slice(offset, value).ToArray();
        buffer = buffer.Slice(totalLen);
        return true;
    }

    public static byte[] CreateConnAck()
    {
        return new byte[] { 0x20, 0x02, 0x00, 0x00 }; // CONNACK, Length 2, Session Present 0, Return Code 0 (Accepted)
    }

    public static byte[] CreatePingResp()
    {
        return new byte[] { 0xD0, 0x00 }; // PINGRESP, Length 0
    }
    
    public static byte[] CreateSubAck(int packetId)
    {
        return new byte[] { 0x90, 0x03, (byte)(packetId >> 8), (byte)(packetId & 255), 0x00 }; // SUBACK, Length 3, PacketId, Success QOS 0
    }

    public static byte[] FramePublish(string topic, ReadOnlySpan<byte> payload)
    {
        byte[] topicBytes = Encoding.UTF8.GetBytes(topic);
        int remLen = 2 + topicBytes.Length + payload.Length;
        
        // Simplified variable length encoding (assuming small messages < 128 bytes for demo)
        // In reality, this needs the full loop
        byte[] frame = new byte[2 + remLen];
        frame[0] = 0x30; // PUBLISH (QoS 0)
        frame[1] = (byte)remLen; // Only valid if remLen < 128
        
        frame[2] = (byte)(topicBytes.Length >> 8);
        frame[3] = (byte)(topicBytes.Length & 255);
        
        Array.Copy(topicBytes, 0, frame, 4, topicBytes.Length);
        payload.CopyTo(frame.AsSpan(4 + topicBytes.Length));
        
        return frame;
    }
}
