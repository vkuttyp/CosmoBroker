# CosmoBroker

**CosmoBroker** is a high-performance, NATS-compatible distributed messaging engine built for .NET 10. It leverages `System.IO.Pipelines` and `Span<T>` to provide a zero-copy, ultra-low-latency messaging backbone that matches the official NATS feature set while adding native SQL-backed persistence.

---

## Performance

CosmoBroker is optimized for high-throughput and low-latency workloads. In standard NATS benchmarks (Release mode), it outperforms official NATS in native environments.

### Benchmark: CosmoBroker vs. Official NATS
*Test Environment: Local TCP, 100,000 messages, 128-byte payloads.*

| Metric | Official NATS (Docker) | **CosmoBroker (Native)** |
| :--- | :---: | :---: |
| **Throughput (PUB)** | ~1,016,000 msg/sec | **~1,222,000 msg/sec** |
| **Average Latency (RTT)** | 0.183 ms | **0.070 ms** |
| **Minimum Latency** | 0.122 ms | **0.020 ms** |

---

### v1.1.0 Ultra-Performance Architecture
The latest release (v1.1.0) introduces a redesigned core engine:
- **Zero-Allocation Hot Path**: Subjects and subscription metadata are handled via `ReadOnlySpan<T>`, eliminating heap allocations during message delivery.
- **Gathering I/O**: High-volume egress utilizes `Socket.SendAsync` with `IList<ArraySegment<byte>>` to minimize syscall overhead.
- **Lock-Free Wildcard Matching**: Lock contention is removed via volatile wildcard counters and versioned matching caches.
- **Global Batch Flush**: Optimized fan-out engine that batches outbound flushes for maximum throughput.

### SQLite JetStream Tuning (Safe Profile)
For SQLite persistence, JetStream writes are batched for durable throughput. You can tune batching via:
- `COSMOBROKER_JS_BATCH_SIZE` (default `128`)
- `COSMOBROKER_JS_BATCH_DELAY_MS` (default `2`)

You can also set these in a config file:
```
jetstream {
  batch_size: 256
  batch_delay_ms: 1
}
```
Then apply them when creating the repository:
```csharp
var config = Services.ConfigParser.LoadFile("broker.conf");
var repo = new MessageRepository(
    "Data Source=broker.db;",
    jetStreamBatchSize: config.JetStreamBatchSize,
    jetStreamBatchDelayMs: config.JetStreamBatchDelayMs
);
```

## Key Features

### 🚀 NATS Protocol & Advanced Streaming
- **Core Protocol**: Full support for `PUB`, `SUB`, `UNSUB` (auto-unsubscription), `PING/PONG`, `INFO`, and `CONNECT`.
- **NATS Headers**: Support for `HPUB` and `HMSG`, enabling metadata exchange and advanced features.
- **Full JetStream Abstraction**: Durable streams and consumers with retention policies (`Limits`, `WorkQueue`), acknowledgement semantics (`Ack`, `Nack`, `Term`), and Pull/Push delivery models.
- **Per-Message TTL**: Fine-grained message expiration via the `Nats-Msg-TTL` header.

### 🔐 Multi-Tenancy & Security
- **Isolated Accounts**: Multi-tenant isolation with subject scoping (`SubjectPrefix`) and isolated permission spaces.
- **Fine-Grained Auth**: Permission-based PUB/SUB control at the account and user level.
- **Advanced Auth**: Support for **JWT** and **NKEY** (Ed25519) identity derivation.
- **TLS/SSL**: Full encryption via `SslStream` and support for **TLS Client Certificate Authentication**.

### 🌐 Interoperability & Connectivity
- **Multi-Protocol Sniffing**: Support for **NATS**, **MQTT 3.1.1**, and **WebSockets** on the same port via automatic protocol detection.
- **Distributed Clustering**: Full-mesh server-to-server clustering with subscription sharing and automatic node reconnection.
- **Leafnodes**: Bridge local brokers to remote NATS hubs for edge-to-cloud topologies.

### 🛠 Operations & Observability
- **HTTP Monitoring API**: Built-in endpoints for `/varz` (server stats), `/connz` (connection details), and `/jsz` (JetStream metrics).
- **Lame Duck Mode**: Graceful shutdown support, notifying clients to migrate without dropping requests.
- **SQL Persistence**: Native durable storage for streams and consumer offsets using SQLite, Postgres, or SQL Server.

---

## Getting Started

### Basic Setup (Standalone)

```csharp
using CosmoBroker;

// Start the broker with default settings (port 4222, monitor 8222)
var broker = new BrokerServer(port: 4222);
await broker.StartAsync();

Console.WriteLine("CosmoBroker is running. Connect with any NATS client!");
```

### Config File + SQLite JetStream Tuning
Set `COSMOBROKER_CONFIG` to point at a config file and `COSMOBROKER_REPO` to enable SQLite persistence.

Example `broker.conf`:
```
port: 4222
jetstream {
  batch_size: 256
  batch_delay_ms: 1
}
```

Run:
```bash
COSMOBROKER_CONFIG=broker.conf COSMOBROKER_REPO="Data Source=broker.db;" dotnet run --project CosmoBroker.Server -c Release
```

### TLS/Auth Via Config
Example `broker.conf` with TLS + SQL auth + JetStream batching:
```
port: 4222
repo: "Data Source=broker.db;"

tls {
  cert: "server.pfx"
  password: "password"
  client_cert_required: false
}

auth {
  type: "sql"
}

jetstream {
  batch_size: 256
  batch_delay_ms: 1
}
```

### Advanced Setup (JetStream + SQL + TLS)

```csharp
using CosmoBroker;
using CosmoBroker.Persistence;
using CosmoBroker.Auth;
using System.Security.Cryptography.X509Certificates;

// 1. Initialize SQL Persistence
var repo = new MessageRepository("Data Source=broker.db;");
await repo.InitializeAsync();

// 2. Setup JWT/NKEY Security
var authenticator = new JwtAuthenticator();

// 3. Configure TLS
var cert = new X509Certificate2("server.pfx", "password");

// 4. Start Server
var broker = new BrokerServer(
    port: 4222, 
    repo: repo, 
    authenticator: authenticator, 
    serverCertificate: cert
);
await broker.StartAsync();
```

---

## Traffic Shaping Examples

### Subject Mapping (Canary Deployment)
Transform subjects dynamically based on account rules:
```csharp
// Map "orders.*" to "internal.orders.$1"
var mapping = new SubjectMapping { SourcePattern = "orders.*" };
mapping.Destinations.Add(new MapDestination { Subject = "internal.orders.$1", Weight = 1.0 });
account.Mappings.AddMapping(mapping);
```

### Weighted Routing
Split traffic between two versions of a service:
```csharp
var mapping = new SubjectMapping { SourcePattern = "api.v1" };
mapping.Destinations.Add(new MapDestination { Subject = "api.v1.stable", Weight = 0.9 });
mapping.Destinations.Add(new MapDestination { Subject = "api.v1.canary", Weight = 0.1 });
```

---

## Client Compatibility

CosmoBroker is fully compatible with the official NATS ecosystem:

- **Official Clients**: Use any NATS client (`nats.go`, `nats.py`, `nats.js`, `NATS.Client.Core`).
- **MQTT Clients**: Connect using standard MQTT clients (e.g., `Mosquitto`, `MQTTnet`).
- **Web Browsers**: Native WebSocket support for direct browser-to-broker messaging.
- **CLI**: Use the official `nats` CLI tool for management and monitoring.

---

## Architecture

| Component | Responsibility |
| :--- | :--- |
| `BrokerServer` | Orchestrates listeners, clustering, and monitoring. |
| `BrokerConnection` | High-performance `System.IO.Pipelines` handler with protocol sniffing. |
| `TopicTree` | Lock-free Trie for zero-allocation subject matching. |
| `JetStreamService` | Manages durable streams, acks, and consumer state. |
| `ClusterManager` | Handles server-to-server mesh state sync. |
| `MonitoringService` | Exposes the HTTP management and stats API. |
