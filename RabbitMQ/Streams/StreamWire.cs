using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace CosmoBroker.RabbitMQ.Streams;

internal static class StreamWire
{
    public const ushort ResponseMask = 0x8000;
    public const ushort Version1 = 1;

    public static async Task<byte[]?> ReadFrameAsync(Stream stream, CancellationToken ct)
    {
        var lengthBuffer = new byte[4];
        if (!await ReadExactlyAsync(stream, lengthBuffer, ct))
            return null;

        var length = BinaryPrimitives.ReadUInt32BigEndian(lengthBuffer);
        var frame = new byte[length];
        if (!await ReadExactlyAsync(stream, frame, ct))
            return null;

        return frame;
    }

    public static async Task WriteFrameAsync(Stream stream, byte[] frame, CancellationToken ct)
    {
        var lengthBuffer = new byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(lengthBuffer, (uint)frame.Length);
        await stream.WriteAsync(lengthBuffer, ct);
        await stream.WriteAsync(frame, ct);
        await stream.FlushAsync(ct);
    }

    private static async Task<bool> ReadExactlyAsync(Stream stream, byte[] buffer, CancellationToken ct)
    {
        var offset = 0;
        while (offset < buffer.Length)
        {
            var read = await stream.ReadAsync(buffer.AsMemory(offset, buffer.Length - offset), ct);
            if (read == 0)
                return false;
            offset += read;
        }

        return true;
    }

    public static ushort ReadUInt16(ReadOnlySpan<byte> buffer, ref int offset)
    {
        var value = BinaryPrimitives.ReadUInt16BigEndian(buffer[offset..]);
        offset += 2;
        return value;
    }

    public static uint ReadUInt32(ReadOnlySpan<byte> buffer, ref int offset)
    {
        var value = BinaryPrimitives.ReadUInt32BigEndian(buffer[offset..]);
        offset += 4;
        return value;
    }

    public static ulong ReadUInt64(ReadOnlySpan<byte> buffer, ref int offset)
    {
        var value = BinaryPrimitives.ReadUInt64BigEndian(buffer[offset..]);
        offset += 8;
        return value;
    }

    public static long ReadInt64(ReadOnlySpan<byte> buffer, ref int offset)
    {
        var value = BinaryPrimitives.ReadInt64BigEndian(buffer[offset..]);
        offset += 8;
        return value;
    }

    public static int ReadInt32(ReadOnlySpan<byte> buffer, ref int offset)
    {
        var value = BinaryPrimitives.ReadInt32BigEndian(buffer[offset..]);
        offset += 4;
        return value;
    }

    public static byte ReadByte(ReadOnlySpan<byte> buffer, ref int offset)
    {
        var value = buffer[offset];
        offset += 1;
        return value;
    }

    public static string ReadString(ReadOnlySpan<byte> buffer, ref int offset)
    {
        var length = ReadUInt16(buffer, ref offset);
        if (length == 0)
            return string.Empty;

        var value = Encoding.UTF8.GetString(buffer[offset..(offset + length)]);
        offset += length;
        return value;
    }

    public static byte[] ReadBytes(ReadOnlySpan<byte> buffer, ref int offset)
    {
        var length = ReadUInt32(buffer, ref offset);
        var value = buffer[offset..(offset + (int)length)].ToArray();
        offset += (int)length;
        return value;
    }

    public static Dictionary<string, string> ReadStringMap(ReadOnlySpan<byte> buffer, ref int offset)
    {
        var count = ReadInt32(buffer, ref offset);
        var map = new Dictionary<string, string>(count, StringComparer.Ordinal);
        for (var i = 0; i < count; i++)
            map[ReadString(buffer, ref offset)] = ReadString(buffer, ref offset);
        return map;
    }

    public static void WriteUInt16(Span<byte> buffer, ref int offset, ushort value)
    {
        BinaryPrimitives.WriteUInt16BigEndian(buffer[offset..], value);
        offset += 2;
    }

    public static void WriteInt16(Span<byte> buffer, ref int offset, short value)
    {
        BinaryPrimitives.WriteInt16BigEndian(buffer[offset..], value);
        offset += 2;
    }

    public static void WriteUInt32(Span<byte> buffer, ref int offset, uint value)
    {
        BinaryPrimitives.WriteUInt32BigEndian(buffer[offset..], value);
        offset += 4;
    }

    public static void WriteInt32(Span<byte> buffer, ref int offset, int value)
    {
        BinaryPrimitives.WriteInt32BigEndian(buffer[offset..], value);
        offset += 4;
    }

    public static void WriteUInt64(Span<byte> buffer, ref int offset, ulong value)
    {
        BinaryPrimitives.WriteUInt64BigEndian(buffer[offset..], value);
        offset += 8;
    }

    public static void WriteInt64(Span<byte> buffer, ref int offset, long value)
    {
        BinaryPrimitives.WriteInt64BigEndian(buffer[offset..], value);
        offset += 8;
    }

    public static void WriteByte(Span<byte> buffer, ref int offset, byte value)
    {
        buffer[offset] = value;
        offset += 1;
    }

    public static void WriteString(Span<byte> buffer, ref int offset, string value)
    {
        if (string.IsNullOrEmpty(value))
        {
            WriteUInt16(buffer, ref offset, 0);
            return;
        }

        var byteCount = Encoding.UTF8.GetByteCount(value);
        WriteUInt16(buffer, ref offset, (ushort)byteCount);
        Encoding.UTF8.GetBytes(value, buffer[offset..]);
        offset += byteCount;
    }

    public static void WriteBytes(Span<byte> buffer, ref int offset, ReadOnlySpan<byte> value)
    {
        WriteUInt32(buffer, ref offset, (uint)value.Length);
        value.CopyTo(buffer[offset..]);
        offset += value.Length;
    }

    public static void WriteStringMap(Span<byte> buffer, ref int offset, IDictionary<string, string> map)
    {
        WriteInt32(buffer, ref offset, map.Count);
        foreach (var (key, value) in map)
        {
            WriteString(buffer, ref offset, key);
            WriteString(buffer, ref offset, value);
        }
    }
}
