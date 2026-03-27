using System;
using System.Buffers;
using System.Collections.Generic;
using System.Collections;
using System.Net;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using Xunit;
using StreamClient = global::RabbitMQ.Stream.Client.Client;
using StreamClientParameters = global::RabbitMQ.Stream.Client.ClientParameters;
using StreamDeliver = global::RabbitMQ.Stream.Client.Deliver;
using StreamPublish = global::RabbitMQ.Stream.Client.Publish;
using StreamMessage = global::RabbitMQ.Stream.Client.Message;
using StreamOffsetFirst = global::RabbitMQ.Stream.Client.OffsetTypeFirst;
using StreamRawSuperStreamConsumerConfig = global::RabbitMQ.Stream.Client.RawSuperStreamConsumerConfig;
using StreamRawSuperStreamProducerConfig = global::RabbitMQ.Stream.Client.RawSuperStreamProducerConfig;
using StreamSpec = global::RabbitMQ.Stream.Client.StreamSpec;
using StreamSystem = global::RabbitMQ.Stream.Client.StreamSystem;
using StreamSystemConfig = global::RabbitMQ.Stream.Client.StreamSystemConfig;
using StreamConsumer = global::RabbitMQ.Stream.Client.Reliable.Consumer;
using StreamConsumerConfig = global::RabbitMQ.Stream.Client.Reliable.ConsumerConfig;
using StreamProducer = global::RabbitMQ.Stream.Client.Reliable.Producer;
using StreamProducerConfig = global::RabbitMQ.Stream.Client.Reliable.ProducerConfig;
using StreamSuperStreamConfig = global::RabbitMQ.Stream.Client.Reliable.SuperStreamConfig;

namespace CosmoBroker.Client.Tests;

public sealed class OfficialStreamClientInteropTests : IAsyncDisposable
{
    private const int StreamPort = 5560;
    private const int AmqpPort = 5690;
    private const int MonitorPort = 8260;
    private readonly BrokerServer _server;
    private readonly CancellationTokenSource _cts = new();

    public OfficialStreamClientInteropTests()
    {
        _server = new BrokerServer(port: 0, amqpPort: AmqpPort, streamPort: StreamPort, monitorPort: MonitorPort);
        _ = _server.StartAsync(_cts.Token);
        WaitForPort("127.0.0.1", StreamPort, TimeSpan.FromSeconds(5));
        WaitForPort("127.0.0.1", AmqpPort, TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task OfficialStreamClient_ShouldConnectAndPerformBasicStreamOperations()
    {
        var streamName = $"official.client.stream.{Guid.NewGuid():N}";
        var superStreamName = $"official.client.super.{Guid.NewGuid():N}";

        var client = await StreamClient.Create(new StreamClientParameters
        {
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            Endpoint = new IPEndPoint(IPAddress.Loopback, StreamPort)
        });

        try
        {
            var create = await client.CreateStream(streamName, new Dictionary<string, string>());
            Assert.Equal("Ok", ReadProperty<object>(create, "ResponseCode").ToString());

            var createSuper = await client.CreateSuperStream(
                superStreamName,
                new List<string> { $"{superStreamName}-0", $"{superStreamName}-1" },
                new List<string> { "0", "1" },
                new Dictionary<string, string>());
            Assert.Equal("Ok", ReadProperty<object>(createSuper, "ResponseCode").ToString());

            var metadata = await client.QueryMetadata(new[] { streamName, "missing-stream" });
            var streamInfos = (IDictionary)ReadProperty<object>(metadata, "StreamInfos");
            Assert.True(streamInfos.Contains(streamName));
            Assert.True(streamInfos.Contains("missing-stream"));
            var streamInfo = streamInfos[streamName]!;
            var missingInfo = streamInfos["missing-stream"]!;
            Assert.Equal("Ok", ReadProperty<object>(streamInfo, "ResponseCode").ToString());
            Assert.Equal("StreamDoesNotExist", ReadProperty<object>(missingInfo, "ResponseCode").ToString());

            var stored = await client.StoreOffset("official-consumer", streamName, 7);
            Assert.True(stored);

            var queryOffset = await client.QueryOffset("official-consumer", streamName);
            Assert.Equal("Ok", ReadProperty<object>(queryOffset, "ResponseCode").ToString());
            Assert.Equal<ulong>(7, ReadProperty<ulong>(queryOffset, "Offset"));

            var partitions = await client.QueryPartition(superStreamName);
            Assert.Equal("Ok", ReadProperty<object>(partitions, "ResponseCode").ToString());
            var streams = ReadProperty<IReadOnlyList<string>>(partitions, "Streams");
            Assert.Equal(2, streams.Count);

            var route = await client.QueryRoute(superStreamName, "customer-77");
            Assert.Equal("Ok", ReadProperty<object>(route, "ResponseCode").ToString());
            var routeStreams = ReadProperty<IReadOnlyList<string>>(route, "Streams");
            Assert.Single(routeStreams);

            var stats = await client.StreamStats(streamName);
            Assert.Equal("Ok", ReadProperty<object>(stats, "ResponseCode").ToString());
            var statistic = (IDictionary)ReadProperty<object>(stats, "Statistic");
            Assert.True(statistic.Contains("first_chunk_id"));
            Assert.True(statistic.Contains("committed_chunk_id"));

            var deleteSuper = await client.DeleteSuperStream(superStreamName);
            Assert.Equal("Ok", ReadProperty<object>(deleteSuper, "ResponseCode").ToString());
        }
        finally
        {
            await client.Close("test-complete");
        }
    }

    [Fact]
    public async Task OfficialStreamClient_ShouldPublishAndReceiveDeliverFrame()
    {
        var streamName = $"official.client.deliver.{Guid.NewGuid():N}";
        var delivered = new TaskCompletionSource<(byte SubscriptionId, string Body)>(TaskCreationOptions.RunContinuationsAsynchronously);

        var client = await StreamClient.Create(new StreamClientParameters
        {
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            Endpoint = new IPEndPoint(IPAddress.Loopback, StreamPort)
        });

        try
        {
            var create = await client.CreateStream(streamName, new Dictionary<string, string>());
            Assert.Equal("Ok", ReadProperty<object>(create, "ResponseCode").ToString());

            var (subscriptionId, subscribe) = await client.Subscribe(
                streamName,
                new StreamOffsetFirst(),
                initialCredit: 10,
                properties: new Dictionary<string, string>(),
                deliverHandler: deliver =>
                {
                    try
                    {
                        delivered.TrySetResult((deliver.SubscriptionId, DecodeFirstChunkBody(deliver)));
                    }
                    catch (Exception ex)
                    {
                        delivered.TrySetException(ex);
                    }
                    return Task.CompletedTask;
                });

            Assert.Equal("Ok", ReadProperty<object>(subscribe, "ResponseCode").ToString());

            const string publisherRef = "pub-ref";
            var (publisherId, declare) = await client.DeclarePublisher(
                publisherRef: publisherRef,
                stream: streamName,
                confirmCallback: _ => { },
                errorCallback: _ => { });

            Assert.Equal("Ok", ReadProperty<object>(declare, "ResponseCode").ToString());

            var sent = await client.Publish(new StreamPublish(
                publisherId,
                new List<(ulong, StreamMessage)> { (1UL, new StreamMessage(Encoding.UTF8.GetBytes("hello-deliver"))) }));
            Assert.True(sent);

            var frame = await delivered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(subscriptionId, frame.SubscriptionId);
            Assert.Equal("hello-deliver", frame.Body);

            var sequence = await client.QueryPublisherSequence(publisherRef, streamName);
            Assert.Equal("Ok", ReadProperty<object>(sequence, "ResponseCode").ToString());
            Assert.Equal<ulong>(1, ReadProperty<ulong>(sequence, "Sequence"));

            await client.Unsubscribe(subscriptionId);
            await client.DeletePublisher(publisherId);
        }
        finally
        {
            await client.Close("test-complete");
        }
    }

    [Fact]
    public async Task OfficialStreamSystem_ShouldProduceAndConsumeSingleMessage()
    {
        var streamName = $"official.system.stream.{Guid.NewGuid():N}";
        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var confirmed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var system = await StreamSystem.Create(new StreamSystemConfig
        {
            UserName = "guest",
            Password = "guest",
            Endpoints = new List<EndPoint> { new IPEndPoint(IPAddress.Loopback, StreamPort) }
        });

        await system.CreateStream(new StreamSpec(streamName));

        var consumer = await StreamConsumer.Create(new StreamConsumerConfig(system, streamName)
        {
            Reference = "official-system-consumer",
            OffsetSpec = new StreamOffsetFirst(),
            MessageHandler = async (_, _, _, message) =>
            {
                received.TrySetResult("received");
                await Task.CompletedTask;
            }
        });

        var producer = await StreamProducer.Create(new StreamProducerConfig(system, streamName)
        {
            ConfirmationHandler = async confirmation =>
            {
                if (string.Equals(confirmation.Status.ToString(), "Confirmed", StringComparison.Ordinal))
                    confirmed.TrySetResult(true);
                await Task.CompletedTask;
            }
        });

        try
        {
            await producer.Send(new StreamMessage(Encoding.UTF8.GetBytes("hello-stream-system")));

            Assert.True(await confirmed.Task.WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.Equal("received", await received.Task.WaitAsync(TimeSpan.FromSeconds(10)));
        }
        finally
        {
            await producer.Close();
            await consumer.Close();
        }
    }

    [Fact]
    public async Task OfficialStreamSystem_ShouldPublishViaRawSuperStreamProducer()
    {
        var superStreamName = $"official.system.super.{Guid.NewGuid():N}";
        var received = new TaskCompletionSource<(string Stream, string Body)>(TaskCreationOptions.RunContinuationsAsynchronously);

        var client = await StreamClient.Create(new StreamClientParameters
        {
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            Endpoint = new IPEndPoint(IPAddress.Loopback, StreamPort)
        });

        await using var system = await StreamSystem.Create(new StreamSystemConfig
        {
            UserName = "guest",
            Password = "guest",
            Endpoints = new List<EndPoint> { new IPEndPoint(IPAddress.Loopback, StreamPort) }
        });

        try
        {
            var createSuper = await client.CreateSuperStream(
                superStreamName,
                new List<string> { $"{superStreamName}-0", $"{superStreamName}-1" },
                new List<string> { "0", "1" },
                new Dictionary<string, string>());
            Assert.Equal("Ok", ReadProperty<object>(createSuper, "ResponseCode").ToString());

            var partitions = await client.QueryPartition(superStreamName);
            Assert.Equal("Ok", ReadProperty<object>(partitions, "ResponseCode").ToString());
            var streams = ReadProperty<IReadOnlyList<string>>(partitions, "Streams");
            Assert.Equal(2, streams.Count);

            var subscriptions = new List<byte>();
            foreach (var stream in streams)
            {
                var (subscriptionId, subscribe) = await client.Subscribe(
                    stream,
                    new StreamOffsetFirst(),
                    initialCredit: 10,
                    properties: new Dictionary<string, string>(),
                    deliverHandler: deliver =>
                    {
                        try
                        {
                            received.TrySetResult((stream, DecodeFirstChunkBody(deliver)));
                        }
                        catch (Exception ex)
                        {
                            received.TrySetException(ex);
                        }

                        return Task.CompletedTask;
                    });

                Assert.Equal("Ok", ReadProperty<object>(subscribe, "ResponseCode").ToString());
                subscriptions.Add(subscriptionId);
            }

            var producer = await system.CreateRawSuperStreamProducer(new StreamRawSuperStreamProducerConfig(superStreamName)
            {
                Routing = _ => "customer-42"
            });

            try
            {
                await producer.Send(1UL, new StreamMessage(Encoding.UTF8.GetBytes("hello-super-stream")));
                var message = await received.Task.WaitAsync(TimeSpan.FromSeconds(10));
                Assert.Contains(message.Stream, streams);
                Assert.Equal("hello-super-stream", message.Body);
            }
            finally
            {
                await producer.Close();
            }

            foreach (var subscriptionId in subscriptions)
                await client.Unsubscribe(subscriptionId);

            var deleteSuper = await client.DeleteSuperStream(superStreamName);
            Assert.Equal("Ok", ReadProperty<object>(deleteSuper, "ResponseCode").ToString());
        }
        finally
        {
            await client.Close("test-complete");
        }
    }

    [Fact]
    public async Task OfficialStreamSystem_ShouldProduceAndConsumeViaSuperStream()
    {
        var superStreamName = $"official.system.reliable.super.{Guid.NewGuid():N}";
        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var confirmed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var client = await StreamClient.Create(new StreamClientParameters
        {
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            Endpoint = new IPEndPoint(IPAddress.Loopback, StreamPort)
        });

        await using var system = await StreamSystem.Create(new StreamSystemConfig
        {
            UserName = "guest",
            Password = "guest",
            Endpoints = new List<EndPoint> { new IPEndPoint(IPAddress.Loopback, StreamPort) }
        });

        try
        {
            var createSuper = await client.CreateSuperStream(
                superStreamName,
                new List<string> { $"{superStreamName}-0", $"{superStreamName}-1" },
                new List<string> { "0", "1" },
                new Dictionary<string, string>());
            Assert.Equal("Ok", ReadProperty<object>(createSuper, "ResponseCode").ToString());

            var consumer = await StreamConsumer.Create(new StreamConsumerConfig(system, superStreamName)
            {
                Reference = "official-system-super-consumer",
                IsSuperStream = true,
                OffsetSpec = new StreamOffsetFirst(),
                MessageHandler = async (_, _, _, message) =>
                {
                    received.TrySetResult(Encoding.UTF8.GetString(message.Data.Contents.ToArray()));
                    await Task.CompletedTask;
                }
            });

            var producer = await StreamProducer.Create(new StreamProducerConfig(system, superStreamName)
            {
                SuperStreamConfig = new StreamSuperStreamConfig
                {
                    Enabled = true,
                    Routing = _ => "customer-42"
                },
                ConfirmationHandler = async confirmation =>
                {
                    if (string.Equals(confirmation.Status.ToString(), "Confirmed", StringComparison.Ordinal))
                        confirmed.TrySetResult(true);
                    await Task.CompletedTask;
                }
            });

            try
            {
                await producer.Send(new StreamMessage(Encoding.UTF8.GetBytes("hello-reliable-super-stream")));

                Assert.True(await confirmed.Task.WaitAsync(TimeSpan.FromSeconds(10)));
                Assert.Equal("hello-reliable-super-stream", await received.Task.WaitAsync(TimeSpan.FromSeconds(10)));
            }
            finally
            {
                await producer.Close();
                await consumer.Close();
            }

            var deleteSuper = await client.DeleteSuperStream(superStreamName);
            Assert.Equal("Ok", ReadProperty<object>(deleteSuper, "ResponseCode").ToString());
        }
        finally
        {
            await client.Close("test-complete");
        }
    }

    [Fact]
    public async Task OfficialStreamSystem_ShouldPromoteSingleActiveConsumerViaSuperStream()
    {
        var superStreamName = $"official.system.sac.super.{Guid.NewGuid():N}";
        var firstReceived = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondInactive = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondPromoted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondReceivedSecondMessage = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        var client = await StreamClient.Create(new StreamClientParameters
        {
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            Endpoint = new IPEndPoint(IPAddress.Loopback, StreamPort)
        });

        await using var system = await StreamSystem.Create(new StreamSystemConfig
        {
            UserName = "guest",
            Password = "guest",
            Endpoints = new List<EndPoint> { new IPEndPoint(IPAddress.Loopback, StreamPort) }
        });

        try
        {
            var createSuper = await client.CreateSuperStream(
                superStreamName,
                new List<string> { $"{superStreamName}-0", $"{superStreamName}-1" },
                new List<string> { "0", "1" },
                new Dictionary<string, string>());
            Assert.Equal("Ok", ReadProperty<object>(createSuper, "ResponseCode").ToString());

            var firstConsumer = await StreamConsumer.Create(new StreamConsumerConfig(system, superStreamName)
            {
                Reference = "official-sac-super-consumer",
                IsSuperStream = true,
                IsSingleActiveConsumer = true,
                OffsetSpec = new StreamOffsetFirst(),
                ConsumerUpdateListener = (_, _, isActive) =>
                {
                    Assert.True(isActive);
                    return Task.FromResult<global::RabbitMQ.Stream.Client.IOffsetType>(new StreamOffsetFirst());
                },
                MessageHandler = async (_, _, _, message) =>
                {
                    var body = Encoding.UTF8.GetString(message.Data.Contents.ToArray());
                    if (string.Equals(body, "first-sac-super-message", StringComparison.Ordinal))
                        firstReceived.TrySetResult(body);
                    await Task.CompletedTask;
                }
            });

            var secondConsumer = await StreamConsumer.Create(new StreamConsumerConfig(system, superStreamName)
            {
                Reference = "official-sac-super-consumer",
                IsSuperStream = true,
                IsSingleActiveConsumer = true,
                OffsetSpec = new StreamOffsetFirst(),
                ConsumerUpdateListener = (_, _, isActive) =>
                {
                    if (isActive)
                    {
                        secondPromoted.TrySetResult(true);
                        return Task.FromResult<global::RabbitMQ.Stream.Client.IOffsetType>(new global::RabbitMQ.Stream.Client.OffsetTypeOffset(2));
                    }

                    secondInactive.TrySetResult(true);
                    return Task.FromResult<global::RabbitMQ.Stream.Client.IOffsetType>(new global::RabbitMQ.Stream.Client.OffsetTypeNext());
                },
                MessageHandler = async (_, _, _, message) =>
                {
                    var body = Encoding.UTF8.GetString(message.Data.Contents.ToArray());
                    if (string.Equals(body, "second-sac-super-message", StringComparison.Ordinal))
                        secondReceivedSecondMessage.TrySetResult(body);
                    await Task.CompletedTask;
                }
            });

            var producer = await StreamProducer.Create(new StreamProducerConfig(system, superStreamName)
            {
                SuperStreamConfig = new StreamSuperStreamConfig
                {
                    Enabled = true,
                    Routing = _ => "customer-sac"
                }
            });

            try
            {
                Assert.True(await secondInactive.Task.WaitAsync(TimeSpan.FromSeconds(10)));

                await producer.Send(new StreamMessage(Encoding.UTF8.GetBytes("first-sac-super-message")));
                Assert.Equal("first-sac-super-message", await firstReceived.Task.WaitAsync(TimeSpan.FromSeconds(10)));

                await firstConsumer.Close();
                Assert.True(await secondPromoted.Task.WaitAsync(TimeSpan.FromSeconds(10)));

                await producer.Send(new StreamMessage(Encoding.UTF8.GetBytes("second-sac-super-message")));
                Assert.Equal("second-sac-super-message", await secondReceivedSecondMessage.Task.WaitAsync(TimeSpan.FromSeconds(10)));
            }
            finally
            {
                await producer.Close();
                await secondConsumer.Close();
            }

            var deleteSuper = await client.DeleteSuperStream(superStreamName);
            Assert.Equal("Ok", ReadProperty<object>(deleteSuper, "ResponseCode").ToString());
        }
        finally
        {
            await client.Close("test-complete");
        }
    }

    [Fact]
    public async Task OfficialStreamSystem_ShouldConsumeViaRawSuperStreamConsumer()
    {
        var superStreamName = $"official.system.raw.super.consumer.{Guid.NewGuid():N}";
        var received = new TaskCompletionSource<(string Stream, string Body)>(TaskCreationOptions.RunContinuationsAsynchronously);

        var client = await StreamClient.Create(new StreamClientParameters
        {
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            Endpoint = new IPEndPoint(IPAddress.Loopback, StreamPort)
        });

        await using var system = await StreamSystem.Create(new StreamSystemConfig
        {
            UserName = "guest",
            Password = "guest",
            Endpoints = new List<EndPoint> { new IPEndPoint(IPAddress.Loopback, StreamPort) }
        });

        try
        {
            var createSuper = await client.CreateSuperStream(
                superStreamName,
                new List<string> { $"{superStreamName}-0", $"{superStreamName}-1" },
                new List<string> { "0", "1" },
                new Dictionary<string, string>());
            Assert.Equal("Ok", ReadProperty<object>(createSuper, "ResponseCode").ToString());

            var consumer = await system.CreateSuperStreamConsumer(new StreamRawSuperStreamConsumerConfig(superStreamName)
            {
                Reference = "official-raw-super-consumer",
                MessageHandler = async (stream, _, _, message) =>
                {
                    received.TrySetResult((stream, Encoding.UTF8.GetString(message.Data.Contents.ToArray())));
                    await Task.CompletedTask;
                }
            });

            var producer = await system.CreateRawSuperStreamProducer(new StreamRawSuperStreamProducerConfig(superStreamName)
            {
                Routing = _ => "customer-42"
            });

            try
            {
                await producer.Send(1UL, new StreamMessage(Encoding.UTF8.GetBytes("hello-raw-super-consumer")));
                var message = await received.Task.WaitAsync(TimeSpan.FromSeconds(10));

                Assert.StartsWith($"{superStreamName}-", message.Stream, StringComparison.Ordinal);
                Assert.Equal("hello-raw-super-consumer", message.Body);
            }
            finally
            {
                await producer.Close();
                await consumer.Close();
            }

            var deleteSuper = await client.DeleteSuperStream(superStreamName);
            Assert.Equal("Ok", ReadProperty<object>(deleteSuper, "ResponseCode").ToString());
        }
        finally
        {
            await client.Close("test-complete");
        }
    }

    [Fact]
    public async Task OfficialRawSuperStreamProducer_ShouldRecoverLastPublishingId()
    {
        var superStreamName = $"official.system.super.recovery.{Guid.NewGuid():N}";

        var client = await StreamClient.Create(new StreamClientParameters
        {
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            Endpoint = new IPEndPoint(IPAddress.Loopback, StreamPort)
        });

        await using var system = await StreamSystem.Create(new StreamSystemConfig
        {
            UserName = "guest",
            Password = "guest",
            Endpoints = new List<EndPoint> { new IPEndPoint(IPAddress.Loopback, StreamPort) }
        });

        try
        {
            var createSuper = await client.CreateSuperStream(
                superStreamName,
                new List<string> { $"{superStreamName}-0", $"{superStreamName}-1" },
                new List<string> { "0", "1" },
                new Dictionary<string, string>());
            Assert.Equal("Ok", ReadProperty<object>(createSuper, "ResponseCode").ToString());

            var keyA = "customer-a";
            var keyB = "customer-b";
            var routeA = await client.QueryRoute(superStreamName, keyA);
            var routeB = await client.QueryRoute(superStreamName, keyB);
            var partitionA = ReadProperty<IReadOnlyList<string>>(routeA, "Streams")[0];
            var partitionB = ReadProperty<IReadOnlyList<string>>(routeB, "Streams")[0];
            if (string.Equals(partitionA, partitionB, StringComparison.Ordinal))
            {
                keyB = "customer-c";
                var routeC = await client.QueryRoute(superStreamName, keyB);
                partitionB = ReadProperty<IReadOnlyList<string>>(routeC, "Streams")[0];
            }

            Assert.NotEqual(partitionA, partitionB);

            var producer = await system.CreateRawSuperStreamProducer(new StreamRawSuperStreamProducerConfig(superStreamName)
            {
                Reference = "official-super-producer",
                Routing = message => Encoding.UTF8.GetString(message.Data.Contents.ToArray())
            });
            try
            {
                await producer.Send(7UL, new StreamMessage(Encoding.UTF8.GetBytes(keyA)));
                await producer.Send(8UL, new StreamMessage(Encoding.UTF8.GetBytes(keyB)));
                Assert.Equal(7UL, await producer.GetLastPublishingId());
            }
            finally
            {
                await producer.Close();
            }

            producer = await system.CreateRawSuperStreamProducer(new StreamRawSuperStreamProducerConfig(superStreamName)
            {
                Reference = "official-super-producer",
                Routing = message => Encoding.UTF8.GetString(message.Data.Contents.ToArray())
            });
            try
            {
                Assert.Equal(7UL, await producer.GetLastPublishingId());
            }
            finally
            {
                await producer.Close();
            }

            var deleteSuper = await client.DeleteSuperStream(superStreamName);
            Assert.Equal("Ok", ReadProperty<object>(deleteSuper, "ResponseCode").ToString());
        }
        finally
        {
            await client.Close("test-complete");
        }
    }

    [Fact]
    public async Task OfficialStreamSystem_ShouldPromoteSingleActiveConsumer()
    {
        var streamName = $"official.system.sac.{Guid.NewGuid():N}";
        var firstReceived = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondInactive = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondPromoted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondReceivedSecondMessage = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var system = await StreamSystem.Create(new StreamSystemConfig
        {
            UserName = "guest",
            Password = "guest",
            Endpoints = new List<EndPoint> { new IPEndPoint(IPAddress.Loopback, StreamPort) }
        });

        await system.CreateStream(new StreamSpec(streamName));

        var firstConsumer = await StreamConsumer.Create(new StreamConsumerConfig(system, streamName)
        {
            Reference = "official-sac-consumer",
            IsSingleActiveConsumer = true,
            OffsetSpec = new StreamOffsetFirst(),
            ConsumerUpdateListener = (_, _, isActive) =>
            {
                Assert.True(isActive);
                return Task.FromResult<global::RabbitMQ.Stream.Client.IOffsetType>(new StreamOffsetFirst());
            },
            MessageHandler = async (_, _, _, message) =>
            {
                firstReceived.TrySetResult(Encoding.UTF8.GetString(message.Data.Contents.ToArray()));
                await Task.CompletedTask;
            }
        });

        var secondConsumer = await StreamConsumer.Create(new StreamConsumerConfig(system, streamName)
        {
            Reference = "official-sac-consumer",
            IsSingleActiveConsumer = true,
            OffsetSpec = new StreamOffsetFirst(),
            ConsumerUpdateListener = (_, _, isActive) =>
            {
                if (isActive)
                {
                    secondPromoted.TrySetResult(true);
                    return Task.FromResult<global::RabbitMQ.Stream.Client.IOffsetType>(new global::RabbitMQ.Stream.Client.OffsetTypeOffset(2));
                }

                secondInactive.TrySetResult(true);
                return Task.FromResult<global::RabbitMQ.Stream.Client.IOffsetType>(new global::RabbitMQ.Stream.Client.OffsetTypeNext());
            },
            MessageHandler = async (_, _, _, message) =>
            {
                var body = Encoding.UTF8.GetString(message.Data.Contents.ToArray());
                if (string.Equals(body, "second-sac-message", StringComparison.Ordinal))
                    secondReceivedSecondMessage.TrySetResult(body);
                await Task.CompletedTask;
            }
        });

        var producer = await StreamProducer.Create(new StreamProducerConfig(system, streamName));

        try
        {
            Assert.True(await secondInactive.Task.WaitAsync(TimeSpan.FromSeconds(10)));

            await producer.Send(new StreamMessage(Encoding.UTF8.GetBytes("first-sac-message")));
            Assert.Equal("first-sac-message", await firstReceived.Task.WaitAsync(TimeSpan.FromSeconds(10)));

                await firstConsumer.Close();
                Assert.True(await secondPromoted.Task.WaitAsync(TimeSpan.FromSeconds(10)));

                await producer.Send(new StreamMessage(Encoding.UTF8.GetBytes("second-sac-message")));
                Assert.Equal("second-sac-message", await secondReceivedSecondMessage.Task.WaitAsync(TimeSpan.FromSeconds(10)));
            }
            finally
            {
            await producer.Close();
            await secondConsumer.Close();
        }
    }

    [Fact]
    public async Task OfficialStreamClient_ShouldPromoteSingleActiveConsumer()
    {
        var streamName = $"official.client.sac.{Guid.NewGuid():N}";
        var firstActive = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstReceived = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondInactive = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondPromoted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondReceivedSecondMessage = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        var client = await StreamClient.Create(new StreamClientParameters
        {
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            Endpoint = new IPEndPoint(IPAddress.Loopback, StreamPort)
        });

        await using var system = await StreamSystem.Create(new StreamSystemConfig
        {
            UserName = "guest",
            Password = "guest",
            Endpoints = new List<EndPoint> { new IPEndPoint(IPAddress.Loopback, StreamPort) }
        });

        try
        {
            var create = await client.CreateStream(streamName, new Dictionary<string, string>());
            Assert.Equal("Ok", ReadProperty<object>(create, "ResponseCode").ToString());

            var (firstSubscriptionId, firstSubscribe) = await client.Subscribe(
                streamName,
                new StreamOffsetFirst(),
                initialCredit: 10,
                properties: new Dictionary<string, string>
                {
                    ["name"] = "official-client-sac",
                    ["single-active-consumer"] = "true"
                },
                deliverHandler: deliver =>
                {
                    firstReceived.TrySetResult(DecodeFirstChunkBody(deliver));
                    return Task.CompletedTask;
                },
                consumerUpdateHandler: isActive =>
                {
                    firstActive.TrySetResult(isActive);
                    return Task.FromResult<global::RabbitMQ.Stream.Client.IOffsetType>(new StreamOffsetFirst());
                });
            Assert.Equal("Ok", ReadProperty<object>(firstSubscribe, "ResponseCode").ToString());

            var (secondSubscriptionId, secondSubscribe) = await client.Subscribe(
                streamName,
                new StreamOffsetFirst(),
                initialCredit: 10,
                properties: new Dictionary<string, string>
                {
                    ["name"] = "official-client-sac",
                    ["single-active-consumer"] = "true"
                },
                deliverHandler: deliver =>
                {
                    var body = DecodeFirstChunkBody(deliver);
                    if (string.Equals(body, "second-client-sac-message", StringComparison.Ordinal))
                        secondReceivedSecondMessage.TrySetResult(body);
                    return Task.CompletedTask;
                },
                consumerUpdateHandler: isActive =>
                {
                    if (isActive)
                    {
                        secondPromoted.TrySetResult(true);
                        return Task.FromResult<global::RabbitMQ.Stream.Client.IOffsetType>(new global::RabbitMQ.Stream.Client.OffsetTypeOffset(2));
                    }

                    secondInactive.TrySetResult(true);
                    return Task.FromResult<global::RabbitMQ.Stream.Client.IOffsetType>(new global::RabbitMQ.Stream.Client.OffsetTypeNext());
                });
            Assert.Equal("Ok", ReadProperty<object>(secondSubscribe, "ResponseCode").ToString());

            var producer = await StreamProducer.Create(new StreamProducerConfig(system, streamName));
            try
            {
                Assert.True(await firstActive.Task.WaitAsync(TimeSpan.FromSeconds(10)));
                Assert.True(await secondInactive.Task.WaitAsync(TimeSpan.FromSeconds(10)));

                await producer.Send(new StreamMessage(Encoding.UTF8.GetBytes("first-client-sac-message")));
                Assert.Equal("first-client-sac-message", await firstReceived.Task.WaitAsync(TimeSpan.FromSeconds(10)));

                await client.Unsubscribe(firstSubscriptionId);
                Assert.True(await secondPromoted.Task.WaitAsync(TimeSpan.FromSeconds(10)));

                await producer.Send(new StreamMessage(Encoding.UTF8.GetBytes("second-client-sac-message")));
                Assert.Equal("second-client-sac-message", await secondReceivedSecondMessage.Task.WaitAsync(TimeSpan.FromSeconds(10)));
            }
            finally
            {
                await producer.Close();
                await client.Unsubscribe(secondSubscriptionId, ignoreIfAlreadyRemoved: true);
            }
        }
        finally
        {
            await client.Close("test-complete");
        }
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        await _server.DisposeAsync();
        _cts.Dispose();
    }

    private static T ReadProperty<T>(object value, string name)
    {
        var property = value.GetType().GetProperty(name, BindingFlags.Instance | BindingFlags.Public);
        Assert.True(
            property != null,
            $"Property '{name}' not found on {value.GetType().FullName}. Available: {string.Join(", ", Array.ConvertAll(value.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public), p => p.Name))}");
        var raw = property!.GetValue(value);
        Assert.NotNull(raw);
        return (T)raw!;
    }

    private static string DecodeFirstChunkBody(StreamDeliver deliver)
    {
        var chunkData = deliver.Chunk.Data.ToArray();
        var messageLength = System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(chunkData.AsSpan(0, 4));
        var payloadBytes = chunkData.AsMemory(4, (int)messageLength).ToArray();
        var payload = new ReadOnlySequence<byte>(payloadBytes);
        try
        {
            var message = StreamMessage.From(ref payload, messageLength);
            return Encoding.UTF8.GetString(message.Data.Contents.ToArray());
        }
        catch (Exception ex)
        {
            var preview = payloadBytes.AsSpan(0, Math.Min(payloadBytes.Length, 16)).ToArray();
            throw new Xunit.Sdk.XunitException(
                $"Failed to decode stream payload. length={messageLength}, preview={Convert.ToHexString(preview)}, error={ex.Message}");
        }
    }

    private static void WaitForPort(string host, int port, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                using var client = new System.Net.Sockets.TcpClient();
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
}
