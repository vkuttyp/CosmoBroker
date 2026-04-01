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
- `/api/rabbitmq/super-streams`
- `/api/rabbitmq/super-stream?vhost=/&name=<exchange>`
- `/api/rabbitmq/super-streams/route?vhost=/&exchange=<exchange>&routing_key=<key>&partition_key=<optional>`
- `/api/rabbitmq/streams/reset-offset`
- `/api/rabbitmq/super-streams/reset-offset`
- `/api/rabbitmq/streams/retention`
- `/api/rabbitmq/super-streams/retention`

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
| `COSMOBROKER_MANAGEMENT_USERNAME` | Enables HTTP Basic auth when set with a password |
| `COSMOBROKER_MANAGEMENT_PASSWORD` | Enables HTTP Basic auth when set with a username |
| `COSMOBROKER_MANAGEMENT_ALLOW_ANONYMOUS_HEALTH` | When `false`, `/api/health` also requires auth |
| `COSMOBROKER_MANAGEMENT_CERT_PATH` | Optional PFX certificate path to enable HTTPS on the management host |
| `COSMOBROKER_MANAGEMENT_CERT_PASSWORD` | Optional PFX certificate password |
| `COSMOBROKER_MANAGEMENT_ENABLE_HTTP3` | When `true`, enables experimental HTTP/3 over QUIC on the HTTPS listener |

## Basic Authentication

The management app supports opt-in HTTP Basic authentication for both the HTML UI and the JSON API.

Example:

```bash
COSMOBROKER_MONITOR_URL=http://127.0.0.1:8222 \
COSMOBROKER_MANAGEMENT_PORT=9091 \
COSMOBROKER_MANAGEMENT_USERNAME=admin \
COSMOBROKER_MANAGEMENT_PASSWORD=change-me \
dotnet run --project CosmoBroker.Management/CosmoBroker.Management.csproj
```

Behavior:

- when username and password are both set, all routes require Basic auth
- `/api/health` remains anonymous by default so container healthchecks keep working
- set `COSMOBROKER_MANAGEMENT_ALLOW_ANONYMOUS_HEALTH=false` if you want health to be protected too

## HTTPS And HTTP/3

The management host can now terminate TLS itself, and can optionally enable experimental HTTP/3.

Example:

```bash
COSMOBROKER_MONITOR_URL=http://127.0.0.1:8222 \
COSMOBROKER_MANAGEMENT_PORT=9091 \
COSMOBROKER_MANAGEMENT_CERT_PATH=/app/certs/management.pfx \
COSMOBROKER_MANAGEMENT_CERT_PASSWORD=change-me \
COSMOBROKER_MANAGEMENT_ENABLE_HTTP3=true \
dotnet run --project CosmoBroker.Management/CosmoBroker.Management.csproj
```

Notes:

- HTTP/3 requires HTTPS. If no certificate path is configured, the management host stays on HTTP/1.1 and HTTP/2 only.
- For most deployments this is a convenience feature, not a major performance lever. The management UI is a lower-throughput control plane compared to the broker itself.

## Stream Operations

The management layer now includes a first operational stream control:

- reset a RabbitMQ-style stream consumer offset
- reset a RabbitMQ-style super-stream consumer offset across all partitions
- update retention limits for stream queues and super streams directly from the UI or JSON API

JSON API example:

```bash
curl -u admin:change-me \
  -X POST http://127.0.0.1:9091/api/rabbitmq/streams/reset-offset \
  -H 'Content-Type: application/json' \
  -d '{
    "vhost": "/",
    "queue": "audit.stream.q",
    "consumer": "audit-reader",
    "offset": "first"
  }'
```

Supported `offset` values:

- `first`
- `last`
- `next`
- a numeric stream offset

The RabbitMQ page also exposes a simple HTML form for the same operation.

Super-stream JSON API example:

```bash
curl -u admin:change-me \
  -X POST http://127.0.0.1:9091/api/rabbitmq/super-streams/reset-offset \
  -H 'Content-Type: application/json' \
  -d '{
    "vhost": "/",
    "exchange": "orders.super",
    "consumer": "orders-reader",
    "offset": "first"
  }'
```

That resets the same consumer tag across every partition queue generated by the super stream and returns the next offset for each partition.

Super-stream route preview JSON API example:

```bash
curl -u admin:change-me \
  "http://127.0.0.1:9091/api/rabbitmq/super-streams/route?vhost=%2F&exchange=orders.super&routing_key=customer-42&partition_key=tenant-alpha"
```

That returns the partition queue the message would land on. If `partition_key` is provided, it overrides the routing-key hash for super-stream partition selection.

## Retention Tuning

The RabbitMQ view now exposes HTML forms and JSON API endpoints to adjust stream retention limits without restarting the broker.

- `POST /api/rabbitmq/streams/retention` accepts `vhost`, `queue`, and optional `max_length_messages`, `max_length_bytes`, `max_age_ms`.
- `POST /api/rabbitmq/super-streams/retention` accepts `vhost`, `exchange`, and the same retention fields and applies them to every partition of the super stream.

The target stream metrics table updates in real time through the next monitor scrape. Use the HTML forms when you want to try values interactively, or call the JSON APIs from scripts or automation.

## Operational Notes

- The management app is mostly read-only right now. The one intentional write operation is persisted stream offset reset.
- RabbitMQ data in the UI comes from the broker's native monitor stats, not from a separate RabbitMQ management plugin.
- Stream queues are visible in the RabbitMQ view with:
  - queue type
  - retained bytes
  - max-length / max-length-bytes / max-age retention settings
  - stream head and tail offsets
  - tracked stream consumer offsets
  - per-consumer lag
- Super streams are visible in the RabbitMQ view as a logical exchange summary plus their generated stream partitions after `x-super-stream` declaration.
- `/api/rabbitmq/super-streams` returns the same logical summary shape the UI uses:
  - partition count
  - aggregate messages and bytes
  - aggregate consumer attachments
  - distinct logical consumer count
  - max lag across partitions
  - aggregate offset range across partitions
  - aggregate retention shape derived from partition queues
  - per-partition messages, bytes, consumers, offset range, lag, and retention
  - per-consumer next-offset and lag summaries aggregated across partitions
- `/api/rabbitmq/super-stream` returns one logical super-stream summary by `vhost` and `name`
- the RabbitMQ UI supports direct super-stream drill-down through `?super=<exchange>&vhost=<vhost>`
- the selected super-stream view shows a logical consumer table with:
  - partition coverage
  - next-offset range
  - total lag
  - max lag
  - per-partition next offset and lag
- If the broker monitor endpoint is unavailable, the UI shows that health failure instead of stale data.
- The sample seeder under [`tools/ManagementSeeder`](../tools/ManagementSeeder) is intended for local demos and manual testing, not production deployment.
