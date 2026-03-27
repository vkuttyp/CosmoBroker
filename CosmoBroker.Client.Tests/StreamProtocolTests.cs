using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using Xunit;

namespace CosmoBroker.Client.Tests;

public class StreamProtocolTests : IAsyncDisposable
{
    private const int StreamPort = 5559;
    private const int MonitorPort = 8259;
    private const int AmqpPort = 5689;
    private readonly BrokerServer _server;
    private readonly CancellationTokenSource _cts = new();

    public StreamProtocolTests()
    {
        _server = new BrokerServer(port: 0, amqpPort: AmqpPort, streamPort: StreamPort, monitorPort: MonitorPort);
        _ = _server.StartAsync(_cts.Token);
        WaitForPort("127.0.0.1", StreamPort, TimeSpan.FromSeconds(5));
        WaitForPort("127.0.0.1", AmqpPort, TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task StreamProtocol_ShouldHandshakeCreatePublishSubscribeAndTrackOffsets()
    {
        using var client = new TcpClient();
        await client.ConnectAsync("127.0.0.1", StreamPort);
        await using var stream = client.GetStream();
        await OpenStreamSessionAsync(stream);

        await WriteCommandAsync(stream, 13, writer =>
        {
            WriteUInt32(writer, 6);
            WriteString(writer, "stream.raw");
            WriteStringMap(writer, new Dictionary<string, string>());
        });

        var create = await ReadFrameAsync(stream);
        Assert.Equal((ushort)(13 | 0x8000), create.Key);
        Assert.Equal((uint)6, ReadUInt32(create.Payload, ref create.Offset));
        Assert.Equal((ushort)1, ReadUInt16(create.Payload, ref create.Offset));

        await WriteCommandAsync(stream, 1, writer =>
        {
            WriteUInt32(writer, 7);
            writer.WriteByte(1);
            WriteString(writer, string.Empty);
            WriteString(writer, "stream.raw");
        });

        var declarePublisher = await ReadFrameAsync(stream);
        Assert.Equal((ushort)(1 | 0x8000), declarePublisher.Key);
        Assert.Equal((uint)7, ReadUInt32(declarePublisher.Payload, ref declarePublisher.Offset));
        Assert.Equal((ushort)1, ReadUInt16(declarePublisher.Payload, ref declarePublisher.Offset));

        var publishedPayload = Encoding.UTF8.GetBytes("stream-hello");
        await WriteCommandAsync(stream, 2, writer =>
        {
            writer.WriteByte(1);
            WriteInt32(writer, 1);
            WriteUInt64(writer, 42);
            WriteUInt32(writer, (uint)publishedPayload.Length);
            writer.Write(publishedPayload);
        });

        var publishConfirm = await ReadFrameAsync(stream);
        Assert.Equal((ushort)3, publishConfirm.Key);
        Assert.Equal((byte)1, publishConfirm.Payload[publishConfirm.Offset++]);
        Assert.Equal(1, ReadInt32(publishConfirm.Payload, ref publishConfirm.Offset));
        Assert.Equal((ulong)42, ReadUInt64(publishConfirm.Payload, ref publishConfirm.Offset));

        await WriteCommandAsync(stream, 7, writer =>
        {
            WriteUInt32(writer, 8);
            writer.WriteByte(1);
            WriteString(writer, "stream.raw");
            WriteUInt16(writer, 1);
            WriteUInt16(writer, 1);
            WriteInt32(writer, 0);
        });

        var subscribe = await ReadFrameAsync(stream);
        Assert.Equal((ushort)(7 | 0x8000), subscribe.Key);
        Assert.Equal((uint)8, ReadUInt32(subscribe.Payload, ref subscribe.Offset));
        Assert.Equal((ushort)1, ReadUInt16(subscribe.Payload, ref subscribe.Offset));

        var deliver = await ReadFrameAsync(stream);
        Assert.Equal((ushort)8, deliver.Key);
        Assert.Equal((byte)1, deliver.Payload[deliver.Offset++]);
        Assert.True(deliver.Payload.Length > deliver.Offset);

        await WriteCommandAsync(stream, 11, writer =>
        {
            WriteUInt32(writer, 9);
            WriteString(writer, "stream-sub-1");
            WriteString(writer, "stream.raw");
        });

        var queryOffset = await ReadFrameAsync(stream);
        Assert.Equal((ushort)(11 | 0x8000), queryOffset.Key);
        Assert.Equal((uint)9, ReadUInt32(queryOffset.Payload, ref queryOffset.Offset));
        Assert.Equal((ushort)1, ReadUInt16(queryOffset.Payload, ref queryOffset.Offset));
        Assert.Equal((ulong)2, ReadUInt64(queryOffset.Payload, ref queryOffset.Offset));

        await WriteCommandAsync(stream, 10, writer =>
        {
            WriteString(writer, "manual-consumer");
            WriteString(writer, "stream.raw");
            WriteUInt64(writer, 12);
        });

        await WriteCommandAsync(stream, 11, writer =>
        {
            WriteUInt32(writer, 10);
            WriteString(writer, "manual-consumer");
            WriteString(writer, "stream.raw");
        });

        var manualOffset = await ReadFrameAsync(stream);
        Assert.Equal((ushort)(11 | 0x8000), manualOffset.Key);
        Assert.Equal((uint)10, ReadUInt32(manualOffset.Payload, ref manualOffset.Offset));
        Assert.Equal((ushort)1, ReadUInt16(manualOffset.Payload, ref manualOffset.Offset));
        Assert.Equal((ulong)12, ReadUInt64(manualOffset.Payload, ref manualOffset.Offset));
    }

    [Fact]
    public async Task StreamProtocol_ShouldQuerySuperStreamPartitionsAndRoute()
    {
        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

        using (var connection = factory.CreateConnection())
        using (var channel = connection.CreateModel())
        {
            channel.ExchangeDeclare(
                "stream.super.raw",
                "x-super-stream",
                durable: true,
                autoDelete: false,
                arguments: new Dictionary<string, object?> { ["x-partitions"] = 2 });
        }

        using var client = new TcpClient();
        await client.ConnectAsync("127.0.0.1", StreamPort);
        await using var stream = client.GetStream();
        await OpenStreamSessionAsync(stream);

        await WriteCommandAsync(stream, 0x0019, writer =>
        {
            WriteUInt32(writer, 20);
            WriteString(writer, "stream.super.raw");
        });

        var partitions = await ReadFrameAsync(stream);
        Assert.Equal((ushort)(0x0019 | 0x8000), partitions.Key);
        Assert.Equal((uint)20, ReadUInt32(partitions.Payload, ref partitions.Offset));
        Assert.Equal((ushort)1, ReadUInt16(partitions.Payload, ref partitions.Offset));
        Assert.Equal(2, ReadInt32(partitions.Payload, ref partitions.Offset));
        var names = new[]
        {
            ReadString(partitions.Payload, ref partitions.Offset),
            ReadString(partitions.Payload, ref partitions.Offset)
        };
        Assert.All(names, name => Assert.Contains("stream.super.raw-", name, StringComparison.Ordinal));

        await WriteCommandAsync(stream, 0x0018, writer =>
        {
            WriteUInt32(writer, 21);
            WriteString(writer, "customer-42");
            WriteString(writer, "stream.super.raw");
        });

        var route = await ReadFrameAsync(stream);
        Assert.Equal((ushort)(0x0018 | 0x8000), route.Key);
        Assert.Equal((uint)21, ReadUInt32(route.Payload, ref route.Offset));
        Assert.Equal((ushort)1, ReadUInt16(route.Payload, ref route.Offset));
        Assert.Equal((uint)1, ReadUInt32(route.Payload, ref route.Offset));
        var resolved = ReadString(route.Payload, ref route.Offset);
        Assert.Contains("stream.super.raw-", resolved, StringComparison.Ordinal);
        Assert.Contains(names, name => name.Contains(resolved, StringComparison.Ordinal));
    }

    [Fact]
    public async Task StreamProtocol_ShouldAdvertiseConfiguredMetadataHost()
    {
        const int metadataStreamPort = 5561;
        const int metadataMonitorPort = 8261;
        using var metadataCts = new CancellationTokenSource();
        await using var metadataServer = new BrokerServer(
            port: 0,
            amqpPort: 0,
            streamPort: metadataStreamPort,
            monitorPort: metadataMonitorPort,
            streamAdvertisedHost: "stream.example.test");
        _ = metadataServer.StartAsync(metadataCts.Token);
        WaitForPort("127.0.0.1", metadataStreamPort, TimeSpan.FromSeconds(5));

        using var client = new TcpClient();
        await client.ConnectAsync("127.0.0.1", metadataStreamPort);
        await using var stream = client.GetStream();
        await OpenStreamSessionAsync(stream);

        await WriteCommandAsync(stream, 15, writer =>
        {
            WriteUInt32(writer, 30);
            WriteInt32(writer, 1);
            WriteString(writer, "missing-stream");
        });

        var metadata = await ReadFrameAsync(stream);
        Assert.Equal((ushort)(15 | 0x8000), metadata.Key);
        Assert.Equal((uint)30, ReadUInt32(metadata.Payload, ref metadata.Offset));
        Assert.Equal((uint)1, ReadUInt32(metadata.Payload, ref metadata.Offset));
        Assert.Equal((short)0, ReadInt16(metadata.Payload, ref metadata.Offset));
        Assert.Equal("stream.example.test", ReadString(metadata.Payload, ref metadata.Offset));
        Assert.Equal((uint)metadataStreamPort, ReadUInt32(metadata.Payload, ref metadata.Offset));

        metadataCts.Cancel();
    }

    [Fact]
    public async Task StreamProtocol_ShouldIssueConsumerUpdateForSingleActiveConsumers()
    {
        using var firstClient = new TcpClient();
        await firstClient.ConnectAsync("127.0.0.1", StreamPort);
        await using var firstStream = firstClient.GetStream();
        await OpenStreamSessionAsync(firstStream);

        await WriteCommandAsync(firstStream, 13, writer =>
        {
            WriteUInt32(writer, 40);
            WriteString(writer, "stream.sac.raw");
            WriteStringMap(writer, new Dictionary<string, string>());
        });
        _ = await ReadFrameAsync(firstStream);

        await WriteCommandAsync(firstStream, 7, writer =>
        {
            WriteUInt32(writer, 41);
            writer.WriteByte(1);
            WriteString(writer, "stream.sac.raw");
            WriteUInt16(writer, 3);
            WriteUInt16(writer, 10);
            WriteInt32(writer, 2);
            WriteString(writer, "name");
            WriteString(writer, "sac-ref");
            WriteString(writer, "single-active-consumer");
            WriteString(writer, "true");
        });

        var firstSubscribe = await ReadFrameAsync(firstStream);
        Assert.Equal((ushort)(7 | 0x8000), firstSubscribe.Key);
        Assert.Equal((uint)41, ReadUInt32(firstSubscribe.Payload, ref firstSubscribe.Offset));
        Assert.Equal((ushort)1, ReadUInt16(firstSubscribe.Payload, ref firstSubscribe.Offset));

        var firstUpdate = await ReadFrameAsync(firstStream);
        Assert.Equal((ushort)26, firstUpdate.Key);
        var firstCorrelation = ReadUInt32(firstUpdate.Payload, ref firstUpdate.Offset);
        Assert.Equal((byte)1, firstUpdate.Payload[firstUpdate.Offset++]);
        Assert.Equal((byte)1, firstUpdate.Payload[firstUpdate.Offset++]);

        await WriteCommandAsync(firstStream, (ushort)(26 | 0x8000), writer =>
        {
            WriteUInt32(writer, firstCorrelation);
            WriteUInt16(writer, 3);
        });

        using var secondClient = new TcpClient();
        await secondClient.ConnectAsync("127.0.0.1", StreamPort);
        await using var secondStream = secondClient.GetStream();
        await OpenStreamSessionAsync(secondStream);

        await WriteCommandAsync(secondStream, 7, writer =>
        {
            WriteUInt32(writer, 42);
            writer.WriteByte(1);
            WriteString(writer, "stream.sac.raw");
            WriteUInt16(writer, 3);
            WriteUInt16(writer, 10);
            WriteInt32(writer, 2);
            WriteString(writer, "name");
            WriteString(writer, "sac-ref");
            WriteString(writer, "single-active-consumer");
            WriteString(writer, "true");
        });

        var secondSubscribe = await ReadFrameAsync(secondStream);
        Assert.Equal((ushort)(7 | 0x8000), secondSubscribe.Key);
        Assert.Equal((uint)42, ReadUInt32(secondSubscribe.Payload, ref secondSubscribe.Offset));
        Assert.Equal((ushort)1, ReadUInt16(secondSubscribe.Payload, ref secondSubscribe.Offset));

        var secondUpdate = await ReadFrameAsync(secondStream);
        Assert.Equal((ushort)26, secondUpdate.Key);
        var secondCorrelation = ReadUInt32(secondUpdate.Payload, ref secondUpdate.Offset);
        Assert.Equal((byte)1, secondUpdate.Payload[secondUpdate.Offset++]);
        Assert.Equal((byte)0, secondUpdate.Payload[secondUpdate.Offset++]);

        await WriteCommandAsync(secondStream, (ushort)(26 | 0x8000), writer =>
        {
            WriteUInt32(writer, secondCorrelation);
            WriteUInt16(writer, 3);
        });
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        await _server.DisposeAsync();
        _cts.Dispose();
    }

    private static async Task WriteCommandAsync(Stream stream, ushort key, Action<MemoryStream> write)
    {
        using var payload = new MemoryStream();
        WriteUInt16(payload, key);
        WriteUInt16(payload, 1);
        write(payload);

        var frame = payload.ToArray();
        var header = new byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(header, (uint)frame.Length);
        await stream.WriteAsync(header);
        await stream.WriteAsync(frame);
        await stream.FlushAsync();
    }

    private static async Task OpenStreamSessionAsync(Stream stream)
    {
        await WriteCommandAsync(stream, 17, writer =>
        {
            WriteUInt32(writer, 1);
            WriteStringMap(writer, new Dictionary<string, string>());
        });

        var peerProps = await ReadFrameAsync(stream);
        Assert.Equal((ushort)(17 | 0x8000), peerProps.Key);
        Assert.Equal((uint)1, ReadUInt32(peerProps.Payload, ref peerProps.Offset));
        Assert.Equal((ushort)1, ReadUInt16(peerProps.Payload, ref peerProps.Offset));

        await WriteCommandAsync(stream, 18, writer => WriteUInt32(writer, 2));
        var saslHandshake = await ReadFrameAsync(stream);
        Assert.Equal((ushort)(18 | 0x8000), saslHandshake.Key);
        Assert.Equal((uint)2, ReadUInt32(saslHandshake.Payload, ref saslHandshake.Offset));
        Assert.Equal((ushort)1, ReadUInt16(saslHandshake.Payload, ref saslHandshake.Offset));
        Assert.Equal((uint)1, ReadUInt32(saslHandshake.Payload, ref saslHandshake.Offset));
        Assert.Equal("PLAIN", ReadString(saslHandshake.Payload, ref saslHandshake.Offset));

        await WriteCommandAsync(stream, 19, writer =>
        {
            WriteUInt32(writer, 3);
            WriteString(writer, "PLAIN");
            WriteBytes(writer, Encoding.UTF8.GetBytes("\0guest\0guest"));
        });

        var saslAuth = await ReadFrameAsync(stream);
        Assert.Equal((ushort)(19 | 0x8000), saslAuth.Key);
        Assert.Equal((uint)3, ReadUInt32(saslAuth.Payload, ref saslAuth.Offset));
        Assert.Equal((ushort)1, ReadUInt16(saslAuth.Payload, ref saslAuth.Offset));

        var tune = await ReadFrameAsync(stream);
        Assert.Equal((ushort)20, tune.Key);

        await WriteCommandAsync(stream, 20, static _ => { });
        await WriteCommandAsync(stream, 21, writer =>
        {
            WriteUInt32(writer, 4);
            WriteString(writer, "/");
        });

        var open = await ReadFrameAsync(stream);
        Assert.Equal((ushort)(21 | 0x8000), open.Key);
        Assert.Equal((uint)4, ReadUInt32(open.Payload, ref open.Offset));
        Assert.Equal((ushort)1, ReadUInt16(open.Payload, ref open.Offset));

        await WriteCommandAsync(stream, 0x001b, writer => WriteUInt32(writer, 5));
        var versions = await ReadFrameAsync(stream);
        Assert.Equal((ushort)(0x001b | 0x8000), versions.Key);
        Assert.Equal((uint)5, ReadUInt32(versions.Payload, ref versions.Offset));
        Assert.Equal((ushort)1, ReadUInt16(versions.Payload, ref versions.Offset));
    }

    private static async Task<StreamFrame> ReadFrameAsync(Stream stream)
    {
        var lengthBytes = await ReadExactAsync(stream, 4);
        var length = BinaryPrimitives.ReadUInt32BigEndian(lengthBytes);
        var payload = await ReadExactAsync(stream, (int)length);
        var offset = 0;
        var key = ReadUInt16(payload, ref offset);
        _ = ReadUInt16(payload, ref offset);
        return new StreamFrame(key, payload, offset);
    }

    private static async Task<byte[]> ReadExactAsync(Stream stream, int count, CancellationToken cancellationToken = default)
    {
        var buffer = new byte[count];
        var offset = 0;
        while (offset < count)
        {
            var read = await stream.ReadAsync(buffer.AsMemory(offset, count - offset), cancellationToken);
            if (read == 0)
                throw new EndOfStreamException();
            offset += read;
        }

        return buffer;
    }

    private static void WaitForPort(string host, int port, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                using var client = new TcpClient();
                var connect = client.ConnectAsync(host, port);
                if (connect.Wait(TimeSpan.FromMilliseconds(100)) && client.Connected)
                    return;
            }
            catch
            {
            }

            Thread.Sleep(50);
        }

        throw new TimeoutException($"Timed out waiting for {host}:{port}");
    }

    private static ushort ReadUInt16(byte[] buffer, ref int offset)
    {
        var value = BinaryPrimitives.ReadUInt16BigEndian(buffer.AsSpan(offset, 2));
        offset += 2;
        return value;
    }

    private static uint ReadUInt32(byte[] buffer, ref int offset)
    {
        var value = BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(offset, 4));
        offset += 4;
        return value;
    }

    private static short ReadInt16(byte[] buffer, ref int offset)
    {
        var value = BinaryPrimitives.ReadInt16BigEndian(buffer.AsSpan(offset, 2));
        offset += 2;
        return value;
    }

    private static int ReadInt32(byte[] buffer, ref int offset)
    {
        var value = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(offset, 4));
        offset += 4;
        return value;
    }

    private static ulong ReadUInt64(byte[] buffer, ref int offset)
    {
        var value = BinaryPrimitives.ReadUInt64BigEndian(buffer.AsSpan(offset, 8));
        offset += 8;
        return value;
    }

    private static string ReadString(byte[] buffer, ref int offset)
    {
        var length = ReadUInt16(buffer, ref offset);
        var value = Encoding.UTF8.GetString(buffer, offset, length);
        offset += length;
        return value;
    }

    private static void WriteUInt16(Stream stream, ushort value)
    {
        Span<byte> buffer = stackalloc byte[2];
        BinaryPrimitives.WriteUInt16BigEndian(buffer, value);
        stream.Write(buffer);
    }

    private static void WriteUInt32(Stream stream, uint value)
    {
        Span<byte> buffer = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(buffer, value);
        stream.Write(buffer);
    }

    private static void WriteInt32(Stream stream, int value)
    {
        Span<byte> buffer = stackalloc byte[4];
        BinaryPrimitives.WriteInt32BigEndian(buffer, value);
        stream.Write(buffer);
    }

    private static void WriteUInt64(Stream stream, ulong value)
    {
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        stream.Write(buffer);
    }

    private static void WriteString(Stream stream, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        WriteUInt16(stream, (ushort)bytes.Length);
        stream.Write(bytes);
    }

    private static void WriteBytes(Stream stream, byte[] value)
    {
        WriteUInt32(stream, (uint)value.Length);
        stream.Write(value);
    }

    private static void WriteStringMap(Stream stream, IReadOnlyDictionary<string, string> map)
    {
        WriteInt32(stream, map.Count);
        foreach (var entry in map)
        {
            WriteString(stream, entry.Key);
            WriteString(stream, entry.Value);
        }
    }

    private sealed class StreamFrame
    {
        public StreamFrame(ushort key, byte[] payload, int offset)
        {
            Key = key;
            Payload = payload;
            Offset = offset;
        }

        public ushort Key { get; }
        public byte[] Payload { get; }
        public int Offset;
    }
}
