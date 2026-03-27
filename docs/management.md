# Management UI And HTTP API

CosmoBroker now ships a separate management application built on `CosmoApiServer`. It sits in front of the broker's monitor endpoint and provides:

- a server-rendered dashboard for operations
- a small HTTP API for broker stats
- JetStream and RabbitMQ-specific views

This app does not replace the broker. It reads the broker's monitor data and presents it through a friendlier UI and API layer.

## Project Layout

- management host: [`CosmoBroker.Management`](../CosmoBroker.Management)
- broker monitor source: [`Services/MonitoringService.cs`](../Services/MonitoringService.cs)

## What It Exposes

UI routes:

- `/`
- `/connections`
- `/jetstream`
- `/rabbitmq`

HTTP API routes:

- `/api/health`
- `/api/overview`
- `/api/varz`
- `/api/connections`
- `/api/routes`
- `/api/gateways`
- `/api/leafs`
- `/api/jetstream`
- `/api/rabbitmq`

## Start The Broker

The management app depends on the broker monitor endpoint, which is enabled on `8222` by default.

```bash
dotnet run --project CosmoBroker.Server/CosmoBroker.Server.csproj
```

If you also want RabbitMQ/AMQP enabled on the broker:

```bash
COSMOBROKER_ENABLE_AMQP=true \
COSMOBROKER_AMQP_PORT=5672 \
dotnet run --project CosmoBroker.Server/CosmoBroker.Server.csproj
```

## Start The Management App

```bash
COSMOBROKER_MONITOR_URL=http://127.0.0.1:8222 \
COSMOBROKER_MANAGEMENT_PORT=9091 \
dotnet run --project CosmoBroker.Management/CosmoBroker.Management.csproj
```

Then open:

- UI: `http://127.0.0.1:9091/`
- health: `http://127.0.0.1:9091/api/health`
- overview JSON: `http://127.0.0.1:9091/api/overview`

## Seed Demo Data

If you want the dashboard to show real sample data immediately, start the broker and management UI first, then run:

```bash
SEED_NATS_URL=nats://127.0.0.1:4222 \
SEED_AMQP_HOST=127.0.0.1 \
SEED_AMQP_PORT=5672 \
/Users/kutty/dev/CosmoBroker/tools/ManagementSeeder/bin/Debug/net10.0/ManagementSeeder
```

That helper opens live NATS and RabbitMQ connections, creates a JetStream stream, creates RabbitMQ exchanges and queues, publishes sample messages, and keeps a small amount of queue state alive so the UI is non-empty while you test it.

## Environment Variables

| Variable | Purpose |
|---|---|
| `COSMOBROKER_MANAGEMENT_PORT` | HTTP port used by the management host |
| `COSMOBROKER_MONITOR_URL` | Base URL of the broker monitor endpoint |

## Operational Notes

- The management app is read-only right now. It reports broker state but does not mutate queues, streams, or connections.
- RabbitMQ data in the UI comes from the broker's native monitor stats, not from a separate RabbitMQ management plugin.
- If the broker monitor endpoint is unavailable, the UI shows that health failure instead of stale data.
- The sample seeder under [`tools/ManagementSeeder`](../tools/ManagementSeeder) is intended for local demos and manual testing, not production deployment.
