# Performance And Benchmarking

This document summarizes the current built-in benchmark tooling, the latest comparison snapshot against RabbitMQ, and how to reproduce the runs locally.

## Scope

CosmoBroker includes two different kinds of AMQP benchmark tooling:

- functional comparison
  - checks behavior using the standard `RabbitMQ.Client` library against both CosmoBroker and RabbitMQ
  - reports anomalies if scenario behavior differs
- performance matrix
  - measures throughput and latency across several workload shapes
  - useful for tracking concurrency, payload-size, durable, and confirm-path performance

## Latest Functional Comparison Snapshot

Source report: [benchmarks/amqp-compare-report.txt](../benchmarks/amqp-compare-report.txt)

Latest recorded scripted run:

| Metric | CosmoBroker | RabbitMQ |
|---|---:|---:|
| Scenarios executed | 16 | 16 |
| Behavioral anomalies | 0 | 0 |
| Throughput | 742,611 msg/sec | 663,702 msg/sec |
| Drop rate | 0.00% | 0.00% |
| Avg latency | 0.174 ms | 0.394 ms |
| P95 latency | 0.208 ms | 0.313 ms |

That run used:

- `RabbitMQ.Client`
- `1000` messages
- `128B` payload
- `1` producer group
- `1` consumer

## Scenarios Covered In The Functional Comparison

The comparison harness currently exercises these RabbitMQ-style behaviors:

- basic publish/get
- mandatory returns
- passive missing queue
- passive missing exchange
- default exchange bind denial
- unknown ack tag rejection
- tx commit without tx mode
- queue delete with `ifUnused`
- default exchange unbind denial
- duplicate consumer tag rejection
- publisher confirms
- tx commit roundtrip
- tx rollback roundtrip
- exclusive queue access from another connection
- redelivery after channel close
- wrong-password auth rejection

The latest scripted comparison run reported `0` anomalies in this matrix.

## Compact Summary

The latest recorded local benchmark snapshots can be summarized like this:

| Case | CosmoBroker | RabbitMQ | Takeaway |
|---|---:|---:|---|
| Baseline AMQP | `742,611 msg/sec`, `0.174 ms` avg | `663,702 msg/sec`, `0.394 ms` avg | CosmoBroker faster |
| Functional parity | `0` anomalies | `0` anomalies | Match on exercised scenarios |
| Concurrency x4 | `~1.58M msg/sec` | `~1.66M msg/sec` | Roughly comparable |
| 4 KB payload | `~253k msg/sec` | `~236k msg/sec` | Slight CosmoBroker edge |
| 64 KB payload | `~36.7k msg/sec` | `~22.9k msg/sec` | CosmoBroker ahead |
| Durable, repo-backed | `~3.36M msg/sec`, `0.400 ms` avg | `~2.33M msg/sec`, `0.663 ms` avg | CosmoBroker ahead in that run |
| Confirms | `~7.4k msg/sec` | `~3.7k msg/sec` | CosmoBroker ahead |
| Durable + confirms, repo-backed | `~5.1k msg/sec`, `0.659 ms` avg | `~2.85k msg/sec`, `0.820 ms` avg | CosmoBroker ahead |

This is still an engineering benchmark, not a formal independent benchmark. The exact winner can move with workload shape, warmup, payload size, and whether persistence is enabled.

## Performance Matrix Cases

The AMQP matrix mode covers:

- `baseline`
- `concurrency-x4`
- `payload-4k`
- `payload-64k`
- `durable`
- `confirms`
- `durable-confirms`

The matrix is intended for engineering feedback rather than a formal audited benchmark. Results can move around depending on machine state, warmup, and message counts, so repeated runs matter.

## Benchmark Profiles

The benchmark CLI supports two built-in profiles:

| Profile | Purpose | Defaults |
|---|---|---|
| `quick` | fast local smoke test | `count=1000`, `latency=5`, `repeats=1`, `warmup-runs=0`, `timeout=15000` |
| `stable` | slower and more repeatable comparison | `count=10000`, `latency=25`, `repeats=3`, `warmup-runs=1`, `timeout=30000` |

Profiles are just defaults. You can still override individual flags on the same command line.

## Recommended Reproduction Commands

### 1. Functional Comparison

If RabbitMQ is already running locally on `127.0.0.1:5672`:

```bash
bash benchmarks/compare_amqp.sh
```

This script:

- starts a local CosmoBroker process with AMQP enabled
- runs the `compare-amqp` mode
- writes the report to `benchmarks/amqp-compare-report.txt`

Example with overrides:

```bash
COUNT=5000 PAYLOAD=256 LATENCY=200 TIMEOUT_MS=5000 bash benchmarks/compare_amqp.sh
```

### 2. Quick Matrix Run

```bash
dotnet run --project CosmoBroker.Benchmarks/CosmoBroker.Benchmarks.csproj -- \
  --mode compare-amqp-matrix \
  --profile quick \
  --cosmo-url amqp://guest:guest@127.0.0.1:5679/ \
  --rabbit-url amqp://guest:guest@127.0.0.1:5672/
```

### 3. More Stable Matrix Run

```bash
dotnet run --project CosmoBroker.Benchmarks/CosmoBroker.Benchmarks.csproj -- \
  --mode compare-amqp-matrix \
  --profile stable \
  --cosmo-url amqp://guest:guest@127.0.0.1:5679/ \
  --rabbit-url amqp://guest:guest@127.0.0.1:5672/
```

### 4. Repo-Backed Durable Run

To measure durable RabbitMQ-style behavior more realistically, start CosmoBroker with a repository:

```bash
COSMOBROKER_REPO="Data Source=/tmp/cosmobroker-rmq-bench.db" \
COSMOBROKER_AMQP_PORT=5679 \
dotnet run --project CosmoBroker.Server/CosmoBroker.Server.csproj
```

Then run the matrix against that AMQP port:

```bash
dotnet run --project CosmoBroker.Benchmarks/CosmoBroker.Benchmarks.csproj -- \
  --mode compare-amqp-matrix \
  --profile stable \
  --cosmo-url amqp://guest:guest@127.0.0.1:5679/ \
  --rabbit-url amqp://guest:guest@127.0.0.1:5672/
```

## Reading The Results

When looking at matrix output:

- compare throughput and latency together
- treat single small runs as noisy
- prefer repeated runs with warmup
- use repo-backed runs for durable conclusions
- use non-repo-backed runs for in-memory / protocol hot-path tuning

The matrix summary reports a simple verdict per case, but the raw numbers still matter more than the label.

## Current Interpretation

Based on the latest documented comparison state in this repo:

- functional AMQP parity is in good shape for the exercised `RabbitMQ.Client` scenario set
- throughput and latency are competitive on the tested local runs
- benchmark results are sensitive to workload shape
- durable and durable-confirm workloads should be evaluated with a configured repository, not only with in-memory runs

## Notes And Caveats

- Results are machine-specific.
- Local Docker, background CPU load, and disk state can shift the numbers.
- RabbitMQ and CosmoBroker may have different durability internals even when the benchmark scenario names look similar.
- The comparison harness is broad enough to catch mainstream behavioral anomalies, but it is not a claim of full RabbitMQ product parity.

## Useful Files

- [README.md](../README.md)
- [docs/rabbitmq.md](rabbitmq.md)
- [benchmarks/compare_amqp.sh](../benchmarks/compare_amqp.sh)
- [benchmarks/amqp-compare-report.txt](../benchmarks/amqp-compare-report.txt)
- [CosmoBroker.Benchmarks/Program.cs](../CosmoBroker.Benchmarks/Program.cs)
