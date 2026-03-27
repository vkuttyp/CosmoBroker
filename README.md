# CosmoBroker

CosmoBroker is a high-performance .NET broker with first-class NATS support and native AMQP 0-9-1 support for RabbitMQ-style workloads. It is designed to be fast on the hot path, durable when backed by SQLite/SQL storage, and practical to operate from code or CLI.

## What It Does

- NATS-compatible core messaging on port `4222` by default
- Native AMQP 0-9-1 listener for standard RabbitMQ clients
- Native RabbitMQ Stream listener for standard RabbitMQ stream clients
- RabbitMQ-style queues, exchanges, bindings, confirms, transactions, redelivery, and durable message flow
- JetStream-style persistence and stream features backed by SQLite/SQL
- MQTT and WebSocket protocol support
- SQL-backed authentication and account scoping
- Built-in AMQP comparison and benchmark tooling

## Client Libraries

Use the client that matches the protocol you want to speak:

- `CosmoBroker.Client` is the native .NET client for CosmoBroker's NATS-compatible surface
- `RabbitMQ.Client` should be used for AMQP / RabbitMQ-compatible workflows against CosmoBroker's AMQP port

CosmoBroker does not currently ship a separate custom AMQP client SDK. For AMQP use cases, the intended client is the standard RabbitMQ ecosystem client.

## Protocol Support

| Area | Status | Notes |
|---|---|---|
| NATS protocol | Supported | Standard pub/sub, request/reply, queue groups |
| AMQP 0-9-1 | Supported | Native listener for `RabbitMQ.Client` and compatible clients |
| RabbitMQ Stream protocol | Supported | Native listener for `RabbitMQ.Stream.Client` and compatible clients |
| RabbitMQ-style queues | Supported | Exchanges, queues, bindings, acks, nacks, reject, qos, confirms, tx, get, consume |
| JetStream-style persistence | Supported | Streams, consumers, mirrors, sources |
| MQTT 3.1.1 | Supported | Protocol sniffing path |
| WebSockets | Supported | Browser and proxy-friendly connectivity |

## RabbitMQ Support

CosmoBroker now exposes a native AMQP port in addition to its NATS listener. That means you can point standard RabbitMQ client libraries at CosmoBroker and run common queue workflows without using a custom adapter.

For a deeper guide focused on AMQP setup, compatibility, authentication, and benchmarking, see [docs/rabbitmq.md](docs/rabbitmq.md).

Currently covered in the native AMQP path:

- connection and channel open/close
- vhost-aware authentication and access checks
- `exchange.declare`, `exchange.delete`, `exchange.bind`, `exchange.unbind`
- `queue.declare`, `queue.bind`, `queue.unbind`, `queue.delete`, `queue.purge`
- `basic.publish`, `basic.consume`, `basic.get`
- `basic.ack`, `basic.nack`, `basic.reject`, `basic.recover`
- `basic.qos`, `channel.flow`
- `confirm.select`
- `tx.select`, `tx.commit`, `tx.rollback`
- server-named queues
- exclusive queues and exclusive consumers
- mandatory publish returns
- durable queues and durable messages when a repository is configured
- stream queues via `x-queue-type=stream`
- stream replay offsets via `x-stream-offset`
- stream retention via `x-max-length`, `x-max-length-bytes`, and `x-max-age`
- persisted stream consumer resume and management-visible stream lag
- a first partitioned stream foundation via `x-super-stream` with `x-partitions`
- native RabbitMQ Stream protocol for create/delete stream, publish, subscribe, offset store/query, metadata, route, partitions, stream stats, and single-active-consumer
- official `RabbitMQ.Stream.Client` interop for stream, super-stream, and single-active-consumer flows

Current RabbitMQ gap areas are mostly advanced product features rather than core AMQP correctness:

- quorum queues
- full RabbitMQ streams parity beyond the current native stream listener and super-stream foundation
- policies / operator policies
- clustering and replication parity with RabbitMQ
- plugin ecosystem parity

## Management UI And API

CosmoBroker now includes a separate management application built on `CosmoApiServer`. It provides a server-rendered dashboard plus a small HTTP API over the broker's monitor endpoint.

- dashboard project: `CosmoBroker.Management`
- UI pages: `/`, `/connections`, `/jetstream`, `/rabbitmq`
- API routes: `/api/health`, `/api/overview`, `/api/varz`, `/api/connections`, `/api/routes`, `/api/gateways`, `/api/leafs`, `/api/jetstream`, `/api/rabbitmq`
- optional local demo seeder: `tools/ManagementSeeder`
- optional HTTP Basic auth via `COSMOBROKER_MANAGEMENT_USERNAME` and `COSMOBROKER_MANAGEMENT_PASSWORD`

For setup and usage, see [docs/management.md](docs/management.md).

## Getting Started

### Run Standalone

This starts the broker with NATS enabled on `4222`, monitoring on `8222`, and AMQP disabled.

```bash
dotnet run --project CosmoBroker.Server/CosmoBroker.Server.csproj
```

### Run With Native AMQP Enabled

This enables the native RabbitMQ-compatible listener on `5672`.

```bash
COSMOBROKER_ENABLE_AMQP=true \
COSMOBROKER_AMQP_PORT=5672 \
dotnet run --project CosmoBroker.Server/CosmoBroker.Server.csproj
```

### Run With Native RabbitMQ Stream Enabled

This enables the native RabbitMQ Stream listener on `5552`.

```bash
COSMOBROKER_ENABLE_RMQ_STREAM=true \
COSMOBROKER_STREAM_PORT=5552 \
dotnet run --project CosmoBroker.Server/CosmoBroker.Server.csproj
```

If external clients should connect through a hostname other than `localhost`, set the advertised host too:

```bash
COSMOBROKER_ENABLE_RMQ_STREAM=true \
COSMOBROKER_STREAM_PORT=5552 \
COSMOBROKER_STREAM_ADVERTISED_HOST=broker.example.com \
dotnet run --project CosmoBroker.Server/CosmoBroker.Server.csproj
```

### Run With Persistence

This enables SQLite-backed persistence for JetStream-style data and durable RabbitMQ-style messages.

```bash
COSMOBROKER_REPO="Data Source=broker.db" \
COSMOBROKER_ENABLE_AMQP=true \
COSMOBROKER_AMQP_PORT=5672 \
dotnet run --project CosmoBroker.Server/CosmoBroker.Server.csproj
```

### Protocol Switches

You can turn NATS and AMQP on or off independently:

```bash
# NATS only
COSMOBROKER_ENABLE_NATS=true \
COSMOBROKER_ENABLE_AMQP=false \
dotnet run --project CosmoBroker.Server/CosmoBroker.Server.csproj
```

```bash
# AMQP only
COSMOBROKER_ENABLE_NATS=false \
COSMOBROKER_ENABLE_AMQP=true \
COSMOBROKER_AMQP_PORT=5672 \
dotnet run --project CosmoBroker.Server/CosmoBroker.Server.csproj
```

```bash
# both enabled
COSMOBROKER_ENABLE_NATS=true \
COSMOBROKER_ENABLE_AMQP=true \
COSMOBROKER_AMQP_PORT=5672 \
dotnet run --project CosmoBroker.Server/CosmoBroker.Server.csproj
```

If you disable both, the process can still run its background services and monitoring endpoint, but it will not accept NATS or AMQP client traffic.

### Docker Behavior

The Docker image remains safe for existing NATS-only deployments:

- NATS is enabled by default
- AMQP is disabled by default
- the container healthcheck still targets the NATS listener on `4222`

If you want AMQP in Docker, opt in explicitly:

```bash
docker run --rm -p 4222:4222 -p 8222:8222 -p 5672:5672 \
  -e COSMOBROKER_ENABLE_NATS=true \
  -e COSMOBROKER_ENABLE_AMQP=true \
  -e COSMOBROKER_AMQP_PORT=5672 \
  your-dockerhub-user/cosmobroker-server:latest
```

If you want the RabbitMQ Stream protocol in Docker, opt in explicitly too:

```bash
docker run --rm -p 4222:4222 -p 8222:8222 -p 5552:5552 \
  -e COSMOBROKER_ENABLE_NATS=true \
  -e COSMOBROKER_ENABLE_RMQ_STREAM=true \
  -e COSMOBROKER_STREAM_PORT=5552 \
  your-dockerhub-user/cosmobroker-server:latest
```

If you want a single RabbitMQ-style image that includes both the broker and the management UI, use:

```bash
docker run --rm -p 4222:4222 -p 8222:8222 -p 5672:5672 -p 9091:9091 \
  -e COSMOBROKER_ENABLE_NATS=true \
  -e COSMOBROKER_ENABLE_AMQP=true \
  -e COSMOBROKER_AMQP_PORT=5672 \
  your-dockerhub-user/cosmobroker-server-management:latest
```

That combined image starts:

- the broker on `4222`
- the monitor endpoint on `8222`
- the optional AMQP listener on `5672`
- the management UI on `9091`

### Runtime Environment Variables

| Variable | Purpose |
|---|---|
| `COSMOBROKER_PORT` | NATS listener port |
| `COSMOBROKER_MONITOR_PORT` | Monitoring endpoint port |
| `COSMOBROKER_ENABLE_NATS` | Enable or disable the NATS listener |
| `COSMOBROKER_ENABLE_AMQP` | Enable or disable the AMQP listener |
| `COSMOBROKER_AMQP_PORT` | Native AMQP listener port |
| `COSMOBROKER_ENABLE_RMQ_STREAM` | Enable or disable the RabbitMQ Stream listener |
| `COSMOBROKER_STREAM_PORT` | Native RabbitMQ Stream listener port |
| `COSMOBROKER_STREAM_ADVERTISED_HOST` | Hostname advertised to RabbitMQ stream clients in metadata responses |
| `COSMOBROKER_REPO` | Repository connection string |
| `COSMOBROKER_CONFIG` | Optional config file path |

## Example Usage

### NATS Publish / Subscribe

Use either `CosmoBroker.Client` or a standard NATS client. The example below uses `NATS.Client.Core`.

```csharp
using NATS.Client.Core;

await using var nats = new NatsConnection();
await nats.ConnectAsync();

var subTask = Task.Run(async () =>
{
    await foreach (var msg in nats.SubscribeAsync<string>("orders.created"))
    {
        Console.WriteLine($"received: {msg.Data}");
    }
});

await nats.PublishAsync("orders.created", "order-123");
```

### RabbitMQ.Client Against CosmoBroker

For AMQP, use the standard `RabbitMQ.Client` package against CosmoBroker's AMQP port:

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
await channel.QueueDeclareAsync("orders.q", durable: true, exclusive: false, autoDelete: false);
await channel.QueueBindAsync("orders.q", "orders", "created");

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += async (_, ea) =>
{
    var body = Encoding.UTF8.GetString(ea.Body.ToArray());
    Console.WriteLine($"received: {body}");
    await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
};

await channel.BasicConsumeAsync("orders.q", autoAck: false, consumer);

var payload = Encoding.UTF8.GetBytes("order-123");
await channel.BasicPublishAsync("orders", "created", mandatory: true, body: payload);
```

### RabbitMQ.Stream.Client Against CosmoBroker

For the native stream protocol, use the standard `RabbitMQ.Stream.Client` package against CosmoBroker's stream port:

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

await system.CreateStream(new StreamSpec("orders.stream"));

var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

var consumer = await Consumer.Create(new ConsumerConfig(system, "orders.stream")
{
    Reference = "orders-stream-consumer",
    OffsetSpec = new OffsetTypeFirst(),
    MessageHandler = async (_, _, _, message) =>
    {
        received.TrySetResult(Encoding.UTF8.GetString(message.Data.Contents.ToArray()));
        await Task.CompletedTask;
    }
});

var producer = await Producer.Create(new ProducerConfig(system, "orders.stream"));
await producer.Send(new Message(Encoding.UTF8.GetBytes("order-123")));

Console.WriteLine(await received.Task);

await producer.Close();
await consumer.Close();
```

### Durable Publish With Publisher Confirms

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

await channel.QueueDeclareAsync("durable.jobs", durable: true, exclusive: false, autoDelete: false);
await channel.ConfirmSelectAsync();

var props = new BasicProperties
{
    Persistent = true,
    ContentType = "application/json"
};

await channel.BasicPublishAsync("", "durable.jobs", false, props, Encoding.UTF8.GetBytes("{\"id\":1}"));
await channel.WaitForConfirmsOrDieAsync(TimeSpan.FromSeconds(5));
```

## Persistence

When `COSMOBROKER_REPO` is configured:

- durable RabbitMQ queues survive restart
- durable RabbitMQ messages survive restart
- exchange, queue, and binding metadata are restored on startup
- JetStream-style stream data is persisted

SQLite works well for local development and benchmarking. The repository abstraction also supports SQL-backed deployments used elsewhere in the broker.

## Benchmarks And Comparison

CosmoBroker includes benchmark modes for NATS, RabbitMQ-style `$RMQ.*` flows, native AMQP, and side-by-side RabbitMQ comparisons.

For a benchmark-focused write-up with current numbers and reproduction steps, see [docs/performance.md](docs/performance.md).

### Compare CosmoBroker vs RabbitMQ

If RabbitMQ is running locally on `127.0.0.1:5672`, use:

```bash
bash benchmarks/compare_amqp.sh
```

This script:

- starts a local CosmoBroker instance with native AMQP enabled
- runs the standard `RabbitMQ.Client` scenario comparison
- writes the output to [benchmarks/amqp-compare-report.txt](benchmarks/amqp-compare-report.txt)

### Run The Comparison Matrix

For broader performance coverage:

```bash
dotnet run --project CosmoBroker.Benchmarks/CosmoBroker.Benchmarks.csproj -- \
  --mode compare-amqp-matrix \
  --profile quick \
  --cosmo-url amqp://guest:guest@127.0.0.1:5679/ \
  --rabbit-url amqp://guest:guest@127.0.0.1:5672/
```

Available benchmark profiles:

| Profile | Purpose | Defaults |
|---|---|---|
| `quick` | fast local smoke test | `count=1000`, `latency=5`, `repeats=1`, `warmup-runs=0` |
| `stable` | slower but more repeatable comparison | `count=10000`, `latency=25`, `repeats=3`, `warmup-runs=1` |

You can still override any individual flag after selecting a profile.

### Current AMQP Comparison Status

The current `RabbitMQ.Client` comparison harness covers:

- basic publish/get
- mandatory returns
- passive declare failures
- queue delete preconditions
- default exchange restrictions
- publisher confirms
- transactions
- exclusive queues across connections
- redelivery after channel close
- wrong-password auth rejection

The latest scripted comparison run completed with `16` scenarios and `0` detected anomalies in the exercised matrix.

Compact benchmark summary:

| Case | CosmoBroker | RabbitMQ | Takeaway |
|---|---:|---:|---|
| Baseline AMQP | `742,611 msg/sec`, `0.174 ms` avg | `663,702 msg/sec`, `0.394 ms` avg | CosmoBroker faster |
| Functional parity | `0` anomalies | `0` anomalies | Match on exercised scenarios |
| Concurrency x4 | `~1.58M msg/sec` | `~1.66M msg/sec` | Roughly comparable |
| 64 KB payload | `~36.7k msg/sec` | `~22.9k msg/sec` | CosmoBroker ahead |
| Durable, repo-backed | `~3.36M msg/sec`, `0.400 ms` avg | `~2.33M msg/sec`, `0.663 ms` avg | CosmoBroker ahead in that run |
| Durable + confirms, repo-backed | `~5.1k msg/sec`, `0.659 ms` avg | `~2.85k msg/sec`, `0.820 ms` avg | CosmoBroker ahead |

For the fuller breakdown, see [docs/performance.md](docs/performance.md).

## Architecture Notes

Core server pieces:

| Component | Responsibility |
|---|---|
| `BrokerServer` | Process orchestration, listeners, monitoring, lifecycle |
| `BrokerConnection` | High-throughput connection handling and protocol sniffing |
| `ExchangeManager` | RabbitMQ-style exchanges, queues, bindings, routing |
| `RabbitQueue` | Queue state, delivery tracking, redelivery, consumer flow |
| `MessageRepository` | SQLite / SQL persistence for streams and durable RMQ state |

Hot-path design goals:

- low-allocation socket and pipe handling
- serialized queue drain to reduce publisher contention
- durable persistence support without forcing it on non-durable paths
- benchmark-driven AMQP parity work against a real RabbitMQ instance

## Development

Build the main projects:

```bash
dotnet build CosmoBroker.Server/CosmoBroker.Server.csproj
dotnet build CosmoBroker.Benchmarks/CosmoBroker.Benchmarks.csproj
```

Run the AMQP interop suite:

```bash
dotnet test CosmoBroker.Client.Tests/CosmoBroker.Client.Tests.csproj --filter FullyQualifiedName~AmqpInteropTests
```
