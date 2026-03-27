# RabbitMQ And AMQP Guide

This guide covers how to use CosmoBroker as a RabbitMQ-compatible AMQP 0-9-1 broker, what is currently supported, how authentication and persistence work, and how to compare it against RabbitMQ with the built-in tools.

## Overview

CosmoBroker exposes a native AMQP port that standard RabbitMQ clients can use directly. The intent is to support common RabbitMQ queue workflows without forcing users onto a custom protocol adapter.

Client separation:

- use `CosmoBroker.Client` for CosmoBroker's NATS-compatible API
- use `RabbitMQ.Client` for CosmoBroker's AMQP / RabbitMQ-compatible API

This guide is about the AMQP side, so all client examples here use the standard RabbitMQ client library.

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

What is still outside current scope:

- RabbitMQ quorum queues
- RabbitMQ streams parity
- policies / operator policies
- clustering / replication parity
- RabbitMQ plugin ecosystem

For the current CosmoBroker operational UI and API layer, see [management.md](management.md).

## Start The Broker With AMQP Enabled

The server reads three ports:

- argument `1` or `COSMOBROKER_PORT`: NATS listener
- argument `2` or `COSMOBROKER_MONITOR_PORT`: monitor listener
- argument `3` or `COSMOBROKER_AMQP_PORT`: AMQP listener

Example:

```bash
COSMOBROKER_ENABLE_AMQP=true \
COSMOBROKER_AMQP_PORT=5672 \
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
