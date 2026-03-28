# RabbitMQ And AMQP Guide

This guide covers how to use CosmoBroker as a RabbitMQ-compatible broker across both AMQP 0-9-1 and the RabbitMQ Stream protocol, what is currently supported, how authentication and persistence work, and how to compare it against RabbitMQ with the built-in tools.

## Overview

CosmoBroker exposes a native AMQP port that standard RabbitMQ clients can use directly. The intent is to support common RabbitMQ queue workflows without forcing users onto a custom protocol adapter.

CosmoBroker also exposes a native RabbitMQ Stream listener for standard `RabbitMQ.Stream.Client` workloads.

## Feature Highlights

- **Dual protocol surface** – NATS is the default, but you can enable AMQP 0-9-1 and the RabbitMQ Stream protocol independently via runtime flags.
- **Native RabbitMQ tooling** – `RabbitMQ.Client` and `RabbitMQ.Stream.Client` talk to CosmoBroker without adapters, including standard exchanges, queues, confirms, transactions, stream offsets, and stream metadata.
- **Management UI / API** – A CosmoApiServer-based dashboard (`CosmoBroker.Management`) proxies the broker monitor stats, exposes per-protocol views, and lets you reset stream offsets or adjust retention limits without redeploying.
- **Streaming parity foundation** – You get `x-queue-type=stream`, `x-stream-offset`, `x-super-stream` partitions, and logical super-stream metadata, with a plan to layer in more RabbitMQ stream/partition controls when the protocol surface expands.
- **Isolation switches** – Toggle each listener independently so a deployment can surface just the protocols the users need.

## Protocol Switches

| Variable | Default | Purpose |
|---|---|---|
| `COSMOBROKER_ENABLE_NATS` | `true` | Turn the NATS listener on/off. Disabling it keeps the monitoring endpoint and any enabled RabbitMQ listeners without the NATS port. |
| `COSMOBROKER_ENABLE_AMQP` | `false` | Enable the native AMQP 0-9-1 listener. When false, CosmoBroker still runs but the classic RabbitMQ port remains closed. |
| `COSMOBROKER_AMQP_PORT` | `5672` (when AMQP enabled) | Controls the port used by `RabbitMQ.Client`. |
| `COSMOBROKER_ENABLE_RMQ_STREAM` | `false` | Enable the RabbitMQ Stream protocol listener (`RabbitMQ.Stream.Client`). Runs on `COSMOBROKER_STREAM_PORT` when enabled. |
| `COSMOBROKER_STREAM_PORT` | `5552` (when stream enabled) | Port for the stream protocol listener. |

Disable both `COSMOBROKER_ENABLE_AMQP` and `COSMOBROKER_ENABLE_RMQ_STREAM` when you only want the NATS surface (or vice versa). Passing command line ports still works, and the `CosmoBroker.Server` host logs the enabled listeners on startup.

Client separation:

- use `CosmoBroker.Client` for CosmoBroker's NATS-compatible API
- use `RabbitMQ.Client` for CosmoBroker's AMQP / RabbitMQ-compatible API

This guide is about the AMQP side, so all client examples here use the standard RabbitMQ client library.
For the native stream listener, this guide also includes `RabbitMQ.Stream.Client` examples.

What works today:

- standard AMQP connection and channel lifecycle
- direct use from `RabbitMQ.Client`
- exchanges, queues, bindings, publishes, consumers, and `basic.get`
- manual ack, nack, reject, redelivery, and recover
- prefetch / qos and `channel.flow`
- publisher confirms
- transactions
- durable queue and durable message restore when a repository is configured
- vhost-aware permissions through the broker auth model
- RabbitMQ-style stream queues via `x-queue-type=stream`
- stream offsets using `x-stream-offset = first | last | next | <numeric>`
- persisted stream consumer resume when a repository is configured
- stream retention controls with `x-max-length`, `x-max-length-bytes`, and `x-max-age`
- a first partitioned super-stream foundation via `x-super-stream` with `x-partitions`
- super-stream partition retention propagation through `x-max-length`, `x-max-length-bytes`, and `x-max-age`
- logical super-stream management summaries with partition and consumer-state visibility
- native RabbitMQ Stream protocol support for:
  - create/delete stream
  - create/delete super stream
  - publish and publish confirm/error
  - subscribe, credit, and unsubscribe
  - offset store/query
  - metadata, route, partitions, and stream stats
  - single-active-consumer negotiation and promotion
- official `RabbitMQ.Stream.Client` interop for single-stream, super-stream, and single-active-consumer flows

What is still outside current scope:

- RabbitMQ quorum queues
- full RabbitMQ streams parity
- policies / operator policies
- clustering / replication parity
- RabbitMQ plugin ecosystem

For the current CosmoBroker operational UI and API layer, see [management.md](management.md).

## Start The Broker With AMQP Enabled

The server reads these ports:

- argument `1` or `COSMOBROKER_PORT`: NATS listener
- argument `2` or `COSMOBROKER_MONITOR_PORT`: monitor listener
- argument `3` or `COSMOBROKER_AMQP_PORT`: AMQP listener
- `COSMOBROKER_STREAM_PORT`: RabbitMQ Stream listener

Example:

```bash
COSMOBROKER_ENABLE_AMQP=true \
COSMOBROKER_AMQP_PORT=5672 \
dotnet run --project CosmoBroker.Server/CosmoBroker.Server.csproj
```

To enable the native RabbitMQ Stream listener too:

```bash
COSMOBROKER_ENABLE_AMQP=true \
COSMOBROKER_AMQP_PORT=5672 \
COSMOBROKER_ENABLE_RMQ_STREAM=true \
COSMOBROKER_STREAM_PORT=5552 \
dotnet run --project CosmoBroker.Server/CosmoBroker.Server.csproj
```

If external stream clients should reconnect through a public hostname, advertise it explicitly:

```bash
COSMOBROKER_ENABLE_RMQ_STREAM=true \
COSMOBROKER_STREAM_PORT=5552 \
COSMOBROKER_STREAM_ADVERTISED_HOST=broker.example.com \
dotnet run --project CosmoBroker.Server/CosmoBroker.Server.csproj
```

With explicit ports:

```bash
COSMOBROKER_ENABLE_NATS=false \
COSMOBROKER_ENABLE_AMQP=true \
dotnet run --project CosmoBroker.Server/CosmoBroker.Server.csproj -- 4222 8222 5672
```

With SQLite-backed persistence:

```bash
COSMOBROKER_REPO="Data Source=broker.db" \
COSMOBROKER_ENABLE_AMQP=true \
COSMOBROKER_AMQP_PORT=5672 \
dotnet run --project CosmoBroker.Server/CosmoBroker.Server.csproj
```

If you want CosmoBroker to act purely as an AMQP broker, disable the NATS listener:

```bash
COSMOBROKER_ENABLE_NATS=false \
COSMOBROKER_ENABLE_AMQP=true \
COSMOBROKER_AMQP_PORT=5672 \
dotnet run --project CosmoBroker.Server/CosmoBroker.Server.csproj
```

If you want only the native RabbitMQ Stream listener:

```bash
COSMOBROKER_ENABLE_NATS=false \
COSMOBROKER_ENABLE_AMQP=false \
COSMOBROKER_ENABLE_RMQ_STREAM=true \
COSMOBROKER_STREAM_PORT=5552 \
dotnet run --project CosmoBroker.Server/CosmoBroker.Server.csproj
```

## Management UI & HTTP API

CosmoBroker ships an independent management host built with `CosmoApiServer` that sits on top of the monitor endpoint. It exposes:

- server-rendered dashboards (`/`, `/connections`, `/jetstream`, `/rabbitmq`) with stream and super-stream telemetry
- HTTP JSON APIs such as `/api/overview`, `/api/rabbitmq`, `/api/rabbitmq/super-streams`, and `/api/rabbitmq/streams/reset-offset`
- retention tuning and offset reset forms that call the JSON API behind the scenes so you can operate streams without touching the broker binary
- opt-in Basic auth via `COSMOBROKER_MANAGEMENT_USERNAME` / `COSMOBROKER_MANAGEMENT_PASSWORD`, with `COSMOBROKER_MANAGEMENT_ALLOW_ANONYMOUS_HEALTH` controlling whether health is open

See [docs/management.md](management.md) for setup, UI guidance, and sample API calls.

## Default Authentication

If no custom authenticator is configured, the AMQP listener accepts:

- username: `guest`
- password: `guest`
- vhost: `/`

If you configure SQL-backed or another authenticator, the AMQP path uses the same auth model.

## Vhosts And Permissions

CosmoBroker’s RabbitMQ-style permission model is account-driven.

Supported permission concepts:

- allowed vhosts
- `configure` permissions for declare/bind/delete style operations
- `write` permissions for publish
- `read` permissions for consume/get

These checks are enforced on the native AMQP path as well as the internal RabbitMQ service layer.

At a high level:

- `exchange.declare`, `queue.declare`, bind, unbind, delete, and purge require configure access
- publish requires write access
- consume and get require read access
- opening a vhost requires vhost access

## Core AMQP Features

The native AMQP path currently supports these common RabbitMQ client operations:

### Connection And Channel

- `connection.start` / `start-ok`
- `connection.tune` / `tune-ok`
- `connection.open`
- `connection.close`
- `channel.open`
- `channel.close`
- `channel.flow`

### Exchange Operations

- `exchange.declare`
- `exchange.delete`
- `exchange.bind`
- `exchange.unbind`
- passive declare checks
- protected default exchange behavior

### Queue Operations

- `queue.declare`
- server-named queues
- stream queue declaration with `x-queue-type=stream`
- `queue.bind`
- `queue.unbind`
- `queue.delete`
- `queue.purge`
- passive declare checks
- exclusive queue ownership
- auto-delete and expiration behavior

### Message Flow

- `basic.publish`
- `basic.consume`
- `basic.cancel`
- server-driven consumer cancel notifications
- `basic.get`
- mandatory publish returns
- properties and headers on AMQP messages

### Delivery Control

- `basic.ack`
- `basic.nack`
- `basic.reject`
- `basic.recover`
- redelivery flags
- consumer prefetch / qos
- flow control on channel delivery
- stream consume offsets with `x-stream-offset`

### Reliability

- `confirm.select`
- `tx.select`
- `tx.commit`
- `tx.rollback`
- durable publish and restore with a configured repository

## RabbitMQ.Client Example

This example declares a durable direct exchange, a durable queue, binds them, consumes with manual ack, and publishes through the same AMQP API that RabbitMQ uses.

```csharp
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var factory = new ConnectionFactory
{
    HostName = "127.0.0.1",
    Port = 5672,
    UserName = "guest",
    Password = "guest"
};

await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

await channel.ExchangeDeclareAsync("orders", ExchangeType.Direct, durable: true);
await channel.QueueDeclareAsync("orders.created.q", durable: true, exclusive: false, autoDelete: false);
await channel.QueueBindAsync("orders.created.q", "orders", "created");

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += async (_, ea) =>
{
    var body = Encoding.UTF8.GetString(ea.Body.ToArray());
    Console.WriteLine(body);
    await channel.BasicAckAsync(ea.DeliveryTag, false);
};

await channel.BasicConsumeAsync("orders.created.q", autoAck: false, consumer);

var payload = Encoding.UTF8.GetBytes("{\"id\":123}");
await channel.BasicPublishAsync("orders", "created", mandatory: true, body: payload);
```

## Durable Publish With Confirms

If you want the broker to confirm that a publish completed through the durable path, configure persistence and use AMQP confirms.

```csharp
using System.Text;
using RabbitMQ.Client;

var factory = new ConnectionFactory
{
    HostName = "127.0.0.1",
    Port = 5672,
    UserName = "guest",
    Password = "guest"
};

await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

await channel.QueueDeclareAsync("jobs.durable", durable: true, exclusive: false, autoDelete: false);
await channel.ConfirmSelectAsync();

var props = new BasicProperties
{
    Persistent = true,
    ContentType = "application/json"
};

await channel.BasicPublishAsync("", "jobs.durable", false, props, Encoding.UTF8.GetBytes("{\"jobId\":42}"));
await channel.WaitForConfirmsOrDieAsync(TimeSpan.FromSeconds(5));
```

## Restart Persistence

When `COSMOBROKER_REPO` is set:

- durable exchanges are restored
- durable queues are restored
- durable bindings are restored
- durable messages are restored
- AMQP properties for durable messages are restored

This makes repo-backed CosmoBroker suitable for RabbitMQ-style durability testing and AMQP comparison runs.

## Native RabbitMQ Stream Client Example

CosmoBroker also supports the native RabbitMQ Stream protocol on a separate listener. Use the standard `RabbitMQ.Stream.Client` package for this path.

```csharp
using System.Net;
using System.Text;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

await using var system = await StreamSystem.Create(new StreamSystemConfig
{
    UserName = "guest",
    Password = "guest",
    Endpoints = new List<EndPoint> { new IPEndPoint(IPAddress.Loopback, 5552) }
});

await system.CreateStream(new StreamSpec("audit.native.stream"));

var delivered = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

var consumer = await Consumer.Create(new ConsumerConfig(system, "audit.native.stream")
{
    Reference = "audit-native-consumer",
    OffsetSpec = new OffsetTypeFirst(),
    MessageHandler = async (_, _, _, message) =>
    {
        delivered.TrySetResult(Encoding.UTF8.GetString(message.Data.Contents.ToArray()));
        await Task.CompletedTask;
    }
});

var producer = await Producer.Create(new ProducerConfig(system, "audit.native.stream"));
await producer.Send(new Message(Encoding.UTF8.GetBytes("event-1")));

Console.WriteLine(await delivered.Task);

await producer.Close();
await consumer.Close();
```

The current native stream listener supports:

- stream create/delete
- stream publish and subscribe
- offset store/query
- metadata and stream stats
- route and partitions queries
- native super-stream create/delete
- broker-scoped publisher sequence recovery
- repo-backed publisher-sequence recovery across broker restart
- repo-backed stream offset recovery across broker restart
- single-active-consumer negotiation and promotion

Advanced RabbitMQ Streams features like replication, segment-level operational controls, and full cluster behavior are still outside current scope.

## Stream Queue Example

CosmoBroker now supports a first RabbitMQ-style stream slice over AMQP queue declaration arguments.

```csharp
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var factory = new ConnectionFactory
{
    HostName = "127.0.0.1",
    Port = 5672,
    UserName = "guest",
    Password = "guest"
};

using var connection = factory.CreateConnection();
using var channel = connection.CreateModel();

channel.ExchangeDeclare("audit.stream.x", ExchangeType.Direct, durable: true, autoDelete: false);
channel.QueueDeclare(
    "audit.stream.q",
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: new Dictionary<string, object?>
    {
        ["x-queue-type"] = "stream",
        ["x-max-length"] = 5000L,
        ["x-max-length-bytes"] = 1024 * 1024L,
        ["x-max-age"] = "1h"
    });
channel.QueueBind("audit.stream.q", "audit.stream.x", "audit");

channel.BasicPublish("audit.stream.x", "audit", basicProperties: null, body: Encoding.UTF8.GetBytes("event-1"));
channel.BasicPublish("audit.stream.x", "audit", basicProperties: null, body: Encoding.UTF8.GetBytes("event-2"));

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.Received += async (_, ea) =>
{
    Console.WriteLine(Encoding.UTF8.GetString(ea.Body.ToArray()));
    channel.BasicAck(ea.DeliveryTag, false);
    await Task.CompletedTask;
};

channel.BasicConsume(
    "audit.stream.q",
    autoAck: false,
    consumerTag: "audit-reader",
    noLocal: false,
    exclusive: false,
    arguments: new Dictionary<string, object?> { ["x-stream-offset"] = "first" },
    consumer: consumer);
```

Current stream notes:

- stream queues are append-only and non-destructive
- `basic.get` is intentionally not supported for stream queues
- when a repository is configured, acknowledged stream consumer offsets are persisted by `consumerTag`
- stream retention currently supports count, bytes, and age trimming through queue arguments
- the management UI and `/api/rabbitmq` surface queue type, retention settings, bytes, head/tail offsets, tracked consumer offsets, and lag
- the management API can reset a persisted stream consumer offset through `/api/rabbitmq/streams/reset-offset`

## Super Stream Foundation Example

CosmoBroker also supports a first RabbitMQ-style super-stream foundation over AMQP exchange declaration arguments. This is not the dedicated RabbitMQ stream protocol. It is a partitioned stream model built on the AMQP path.

```csharp
using System.Text;
using RabbitMQ.Client;

var factory = new ConnectionFactory
{
    HostName = "127.0.0.1",
    Port = 5672,
    UserName = "guest",
    Password = "guest"
};

using var connection = factory.CreateConnection();
using var channel = connection.CreateModel();

channel.ExchangeDeclare(
    "orders.super",
    "x-super-stream",
    durable: true,
    autoDelete: false,
    arguments: new Dictionary<string, object?>
    {
        ["x-partitions"] = 4L
    });

channel.BasicPublish("orders.super", "customer-42", basicProperties: null, body: Encoding.UTF8.GetBytes("event-1"));
channel.BasicPublish("orders.super", "customer-42", basicProperties: null, body: Encoding.UTF8.GetBytes("event-2"));
```

Current super-stream notes:

- declaring `x-super-stream` auto-creates stream partitions as durable stream queues
- publishes are routed to one partition by a stable hash of the routing key
- publishers can override the partition hash key with the AMQP header `x-super-stream-partition-key`
- the same routing key stays on the same partition
- `x-partitions` is persisted and enforced on redeclare
- the generated partition queues and logical super-stream exchange metadata are visible in `/rmqz` and the management UI
- the management API can reset one consumer tag across every partition through `/api/rabbitmq/super-streams/reset-offset`
- the management API exposes logical super-stream summaries through `/api/rabbitmq/super-streams`
- this is a foundation layer, not full RabbitMQ super-stream product parity

## Compatibility Notes

The current AMQP work has been exercised heavily against `RabbitMQ.Client`.

Covered scenario areas include:

- basic publish/get
- mandatory returns
- passive declare failures
- queue preconditions
- default exchange restrictions
- publisher confirms
- transactions
- exclusive queues and exclusive consumers
- redelivery after channel close
- wrong-password rejection

The built-in scripted comparison currently reports zero anomalies in the exercised scenario matrix against a local RabbitMQ instance.

That does not mean full RabbitMQ product parity. It means the currently tested mainstream AMQP workflows match closely enough to avoid detected anomalies in the comparison harness.

## Benchmarking

### Functional Comparison

Run the scripted comparison:

```bash
bash benchmarks/compare_amqp.sh
```

This:

- starts CosmoBroker with native AMQP enabled
- connects to local RabbitMQ on `127.0.0.1:5672`
- runs the standard `RabbitMQ.Client` comparison scenarios
- writes a report to `benchmarks/amqp-compare-report.txt`

Common overrides:

```bash
COUNT=5000 PAYLOAD=256 LATENCY=200 TIMEOUT_MS=5000 bash benchmarks/compare_amqp.sh
```

### Performance Matrix

Use the benchmark project directly for broader coverage:

```bash
dotnet run --project CosmoBroker.Benchmarks/CosmoBroker.Benchmarks.csproj -- \
  --mode compare-amqp-matrix \
  --profile quick \
  --cosmo-url amqp://guest:guest@127.0.0.1:5679/ \
  --rabbit-url amqp://guest:guest@127.0.0.1:5672/
```

Profiles:

| Profile | Defaults |
|---|---|
| `quick` | `count=1000`, `latency=5`, `repeats=1`, `warmup-runs=0`, `timeout=15000` |
| `stable` | `count=10000`, `latency=25`, `repeats=3`, `warmup-runs=1`, `timeout=30000` |

The matrix mode is useful for:

- concurrency scaling
- larger payload sizes
- durable queue workloads
- publisher confirm workloads
- durable + confirm workloads

## Operational Notes

- AMQP is disabled unless you set `COSMOBROKER_AMQP_PORT` or pass a third CLI port argument.
- Durable RabbitMQ behavior depends on a configured repository.
- The comparison harness assumes RabbitMQ is reachable locally unless you override the URLs.
- The benchmark scripts are intended for local engineering feedback, not as a formal audited benchmark suite.

## Suggested Workflow

For local development:

1. Start CosmoBroker with AMQP enabled.
2. Run the AMQP interop tests.
3. Run `bash benchmarks/compare_amqp.sh`.
4. If you are tuning performance, run `compare-amqp-matrix` with `--profile stable`.

Useful commands:

```bash
dotnet test CosmoBroker.Client.Tests/CosmoBroker.Client.Tests.csproj --filter FullyQualifiedName~AmqpInteropTests
```

```bash
bash benchmarks/compare_amqp.sh
```

```bash
dotnet run --project CosmoBroker.Benchmarks/CosmoBroker.Benchmarks.csproj -- \
  --mode compare-amqp-matrix \
  --profile stable \
  --cosmo-url amqp://guest:guest@127.0.0.1:5679/ \
  --rabbit-url amqp://guest:guest@127.0.0.1:5672/
```
