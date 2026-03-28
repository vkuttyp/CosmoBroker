# CosmoBroker

CosmoBroker is a broker that speaks both NATS and RabbitMQ protocols, exposes a monitor-friendly HTTP/SSR management
surface, and ships production-ready tooling for streaming, retention tuning, and AMQP parity testing.

| Component | Status |
| --- | --- |
| Protocols | NATS JetStream + RabbitMQ Client/Streaming compatibility |
| Management | HTTP + SSR UI powered by [CosmoApiServer](http://github.com/kuttyp/CosmoApiServer) |
| Release | v1.3.0 (NuGet + Docker images via GitHub Actions) |

## Highlights

- **Unified runtime** – the broker handles JetStream, leaf nodes, gateways, and the $RMQ.* compatibility layer simultaneously.
- **RabbitMQ streaming** – the internal RabbitMQ mappings surface streaming queues, super-stream partitions, and per-consumer offsets so you get parity with the RabbitMQ Stream plugin.
- **Stream parity performance** – `/stream` now writes NDJSON through a reusable buffer/`Utf8JsonWriter`, which keeps CosmoApiServer’s streaming latencies (P95 ≈ 0.16 ms, P99 ≈ 0.22 ms, ≈9.5 kops/sec) close to ASP.NET Core’s numbers (P95 ≈ 0.08 ms, P99 ≈ 0.13 ms, ≈18.9 kops/sec) while avoiding the previous regression.
- **Management console** – a standalone `CosmoBroker.Management` app delivers both an SSR dashboard and HTTP API; it also supports Basic auth and retention tuning.
- **Benchmark tooling** – `CosmoBroker.Benchmarks` ships with a `compare-amqp` mode so you can quantify differences against RabbitMQ's own client stack.

## Getting started

1. Restore/build everything (the release binaries land under `bin/Release/net10.0`):

   ```bash
   dotnet restore
   dotnet build CosmoBroker.csproj -c Release
   ```

2. Run the broker (monitor endpoint on `8222`, default NATS port `4222`):

   ```bash
   dotnet run --project CosmoBroker.Server/CosmoBroker.Server.csproj
   ```

3. Enable the RabbitMQ/AMQP layer and the management app once the broker is up:

   ```bash
   COSMOBROKER_ENABLE_AMQP=true \
   COSMOBROKER_AMQP_PORT=5672 \
   COSMOBROKER_MANAGEMENT_PORT=9091 \
   dotnet run --project CosmoBroker.Server/CosmoBroker.Server.csproj
   
   COSMOBROKER_MONITOR_URL=http://127.0.0.1:8222 \
   COSMOBROKER_MANAGEMENT_PORT=9091 \
   dotnet run --project CosmoBroker.Management/CosmoBroker.Management.csproj
   ```

4. Seed demo data (optional, handy for management UI walkthroughs):

   ```bash
   SEED_NATS_URL=nats://127.0.0.1:4222 \
   SEED_AMQP_HOST=127.0.0.1 \
   SEED_AMQP_PORT=5672 \
   tools/ManagementSeeder/bin/Debug/net10.0/ManagementSeeder
   ```

## Management and HTTP API

- See [`docs/management.md`](docs/management.md) for full routing, authentication, retention, and stream-reset instructions.
- The UI bundles `/`, `/connections`, `/jetstream`, and `/rabbitmq`, plus API endpoints such as `/api/overview`, `/api/rabbitmq`, and `/api/rabbitmq/streams/retention`.

## Benchmarking

- Learn how to compare CosmoBroker with RabbitMQ in [`docs/benchmarking.md`](docs/benchmarking.md).
- The `tests/ApiServer.Benchmark` harness reports the latest `/stream` latencies: CosmoApiServer (P50 0.11 ms / P95 0.16 ms / P99 0.22 ms / ≈9.5 kops/sec) versus ASP.NET Core (P50 0.05 ms / P95 0.08 ms / P99 0.13 ms / ≈18.9 kops/sec). That same harness is invoked by `.github/workflows/ci.yml` to catch regressions before merging.

## Publishing & releases

- GitHub Actions (`.github/workflows/publish.yml`) packs and pushes `CosmoBroker`, `CosmoBroker.Client`, and `CosmoBroker.Server` to NuGet, and it builds Docker images for the broker, the management host, and the combined broker/management stack (all targeting `linux/amd64`).
- A release is triggered by tagging the commit: `git tag -f v1.3.0 HEAD && git push --force origin v1.3.0`. The `preflight` job ensures RabbitMQ interop tests and AMQP compare benchmarks pass before packages/images are published.

## Documentation

- Management and API details: [`docs/management.md`](docs/management.md)
- Benchmarking and AMQP comparison: [`docs/benchmarking.md`](docs/benchmarking.md)
- Additional reference docs live under `docs/`.

## Support

- Slack: `#cosmobroker` (if available internally)
- Issues: [https://github.com/vkuttyp/CosmoBroker/issues](https://github.com/vkuttyp/CosmoBroker/issues)
