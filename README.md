# Real-Time Order Processing System

An event-driven order pipeline in Go: HTTP ingress → Kafka → consumer-group workers →
PostgreSQL, with a separate gRPC read path. Built to be **measured, broken, and recovered** —
see [FINDINGS.md](FINDINGS.md) for the numbers and [DECISIONS.md](DECISIONS.md) for why each
piece exists.

There is no UI. This is a backend system: you drive it with `curl`, `grpcurl` and PromQL.

```
  WRITE PATH                                  READ PATH

  POST /orders                                GET /orders/{id}
        |                                            |
        v                                            v
     [ api ] --produce--> [ Kafka: orders, 3 parts ] |
                                   |                 |
                          +--------+--------+        | gRPC
                          v                 v        v
                     [ worker-1 ] ...  [ worker-N ]  [ query ]
                          |                 |        |
                          +--------+--------+        |
                                   v                 |
                            [ PostgreSQL ] <---------+
```

## Requirements

- Go 1.27+
- Docker Desktop (Compose v2)

## Run it

Everything, including the services, runs in Compose:

```sh
docker compose up -d --build --scale worker=2
```

That brings up Kafka (KRaft), PostgreSQL, creates the topics, and starts the API and two
workers. To change the number of workers, re-run with a different `--scale worker=N`.

For iterating on Go code it is often quicker to run a service on the host against the
containerised infrastructure:

```sh
docker compose up -d kafka postgres kafka-init
go run ./cmd/smoke            # acceptance check: talks to both, exits 0
go run ./cmd/api
WORKER_ID=w1 go run ./cmd/worker
```

On Windows without GNU make, `scripts\dev.ps1 <target>` mirrors the Makefile:
`up`, `down`, `reset`, `ps`, `logs`, `topics`, `smoke`, `build`, `test`.

## Ports

| Service          | Host address     | Notes                                        |
|------------------|------------------|----------------------------------------------|
| Kafka            | `localhost:9092` | `kafka:19092` from inside Compose            |
| PostgreSQL       | `localhost:5433` | 5432 inside; 5433 avoids a native PG install |
| api (HTTP)       | `localhost:8080` | Day 2                                        |
| query (gRPC)     | `localhost:9090` | reflection enabled, so `grpcurl` needs no proto |
| Prometheus       | `localhost:9091` | expression browser and API                   |
| metrics          | `:2112` per service | scraped via Docker service discovery      |

## API

### `POST /orders`

```sh
curl -i -X POST http://localhost:8080/orders   -H 'Content-Type: application/json'   -d '{"customer_id":"cust-1","items":[{"sku":"WIDGET-1","quantity":2,"unit_price_cents":1999}]}'
```

```
HTTP/1.1 202 Accepted
{"order_id":"57e3745e-5061-4567-a629-249734e5d9ca","status":"accepted"}
```

**202, not 201** — the event is durably in Kafka, but no row exists yet. See
[DECISIONS.md](DECISIONS.md). Invalid bodies get 400 with every problem listed at once;
if the broker does not acknowledge the record, the client gets 503 rather than a false 202.

### `GET /orders/{id}` and `GET /orders`

Reads take a different route: `api` calls the `query` service over gRPC, which reads Postgres.

```sh
curl -s http://localhost:8080/orders/<order_id>
curl -s "http://localhost:8080/orders?page_size=3&customer_id=cust-1"
```

Or call the gRPC service directly — reflection is enabled, so no `.proto` file is needed:

```sh
grpcurl -plaintext localhost:9090 list order.v1.OrderService
grpcurl -plaintext -d '{"order_id":"<order_id>"}' localhost:9090 order.v1.OrderService/GetOrder
grpcurl -plaintext -d '{"page_size":3}' localhost:9090 order.v1.OrderService/ListOrders
```

A 404 straight after a 202 is correct, not a bug — the write path is asynchronous. Listing uses
keyset pagination with an opaque `next_page_token`, not `OFFSET`; see [DECISIONS.md](DECISIONS.md).

## Load testing

```sh
go run ./cmd/loadgen -rate 1000 -duration 30s
```

Send times are scheduled in advance from the target rate, and latency is measured from the time
a request was *due* rather than from when it was sent. A generator whose goroutines loop
"send, await response, send again" produces a rate of N/latency — it slows down exactly when the
system does, hiding the degradation it exists to find. See [DECISIONS.md](DECISIONS.md).

End-to-end latency is read out of the data rather than traced: every row carries both
`occurred_at` and `processed_at`, so the true pipeline latency is a `percentile_cont` query.

```
=== rate1000 ===
target rate        1000.0/s
achieved rate      1000.0/s   (100.0% of target)
sent / accepted     30001 / 30001

accept latency (client -> 202, measured from scheduled send time)
  p50 0.8 ms   p95 1.2 ms   p99 1.5 ms
end-to-end latency (api accepted -> row committed)
  p50 0.4 ms   p95 1.5 ms   p99 10.7 ms
```

`scheduler behind` in the output means the generator could not dispatch on schedule — a
statement about the harness, not the system. Check it before believing a high-rate result.

## The scaling experiment

```sh
scripts/scaling-matrix.sh 3 1 2 4 8      # 3 partitions, 1/2/4/8 workers
scripts/scaling-matrix.sh 12 4 8         # same again with the ceiling raised
```

Each configuration is deliberately overloaded and the drain is measured, because workers only
run flat out while a backlog exists — offering a rate the system keeps up with would measure
the load generator. Raw per-run reports are in `reports/`.

| Workers | 3 partitions | 12 partitions |
|---------|--------------|---------------|
| 1       | 1828 ev/s    | 1922 ev/s     |
| 2       | 2333 ev/s    | —             |
| 4       | 3348 ev/s    | 1932 ev/s     |
| 8       | 3807 ev/s    | 2410 ev/s     |

Quadrupling the partitions made the system **slower**, which is the opposite of the textbook
expectation. Partition count caps how many consumers can participate, but it only binds if
nothing else saturates first — and here something else did. The full argument, and what the
evidence says the real ceiling is, is in [FINDINGS.md](FINDINGS.md).

## What breaks, and when

Every failure mode was induced deliberately and measured. Full numbers in
[FINDINGS.md](FINDINGS.md); the scripts are `scripts/ramp.sh`, `scripts/chaos-db-outage.sh` and
`scripts/chaos-kafka.sh`.

| Component lost | Result | Correctness | Availability |
|---|---|---|---|
| Worker (graceful stop) | rebalance in 1.35s | intact | intact |
| Worker (killed) | rebalance in 43.2s, lag spike | intact | intact |
| Database, outage < retry budget | latency only | intact | intact |
| Database, outage > retry budget | 6 dead letters per 150001, tagged `retries_exhausted` | intact | intact |
| **Broker** | **384 refusals per 60001, 47s worst-case latency** | **intact** | **degraded** |

Overload is not a failure mode here. At 20,000/s — over 5× what the write path sustains — the
API accepted all 300001 orders and lost none; the excess became 71 seconds of lag. The broker is
the only hard dependency, because it is the one component with no queue in front of it.

## Metrics

Every service exposes `/metrics` on port 2112. Prometheus discovers them through the Docker
API, so `--scale worker=N` needs no configuration change — new workers are scraped within one
refresh interval. Open the expression browser at <http://localhost:9091>.

The two queries worth knowing:

```promql
# p99 write latency right now
histogram_quantile(0.99, sum by (le) (rate(order_processing_duration_seconds_bucket{status="persisted"}[1m])))

# is the consumer group falling behind? (positive = yes)
sum(deriv(kafka_consumergroup_lag{consumergroup="order-processors"}[1m]))
```

Lag is exported twice on purpose: by the workers (what this member sees) and by
`kafka-exporter` (what the broker sees). The workers' gauge goes stale at its last value when
they die — reporting a healthy 0 while a backlog grows — which is exactly when lag matters.
See [FINDINGS.md](FINDINGS.md).

### Regenerating the protobuf code

The generated files are committed, so building needs only Go. To change the contract:

```sh
protoc --proto_path=proto   --go_out=. --go_opt=module=github.com/gigagedianidze/order-pipeline   --go-grpc_out=. --go-grpc_opt=module=github.com/gigagedianidze/order-pipeline   proto/order/v1/order.proto
```

### Watching where events land

```sh
docker exec edp-kafka /opt/kafka/bin/kafka-console-consumer.sh   --bootstrap-server kafka:19092 --topic orders --from-beginning   --property print.partition=true --property print.key=true
```

The key is the `order_id`, so all events for one order sit on one partition and stay ordered.

### Running workers

```sh
go build -o bin/worker.exe ./cmd/worker
WORKER_ID=w1 ./bin/worker.exe    # repeat with w2, w3... to scale the group
```

Each worker logs what it owns after every rebalance (`now_owns=orders:[0 1]`) and a heartbeat
every 15s, so an idle member — more workers than partitions — is visible rather than silent.

```sh
docker exec edp-kafka /opt/kafka/bin/kafka-consumer-groups.sh   --bootstrap-server kafka:19092 --describe --group order-processors
```

## Status

Following the 13-day plan in [PLAN.md](PLAN.md).

- **Day 1** — infrastructure up (Kafka KRaft + Postgres), topics with 3 partitions, services scaffolded
- **Day 2** — `POST /orders` validates, assigns a UUID, produces keyed by `order_id`, returns 202
- **Day 3** — worker joins consumer group `order-processors`; partition assignment and
  rebalancing measured (see [FINDINGS.md](FINDINGS.md): 4 workers, 3 partitions, one idle)
- **Day 4** — idempotent persistence: `UNIQUE (event_id)` + `ON CONFLICT DO NOTHING`, offsets
  committed only after a successful write, failed writes pause their partition rather than
  being committed past
- **Day 5** — graceful shutdown: stop fetching, drain in-flight writes, commit, leave the
  group, close the pool — with a 15s deadline and a hard-exit fallback; services containerised
- **Day 6** — retries with full jitter, SQLSTATE-based transient/poison classification, and a
  dead-letter queue: survived a 31s database outage with 0 lost and 0 dead-lettered
- **Day 7** — gRPC read path: `query` service over Postgres with keyset pagination, called by
  the API for `GET /orders/{id}`; measured 15.6ms end-to-end API-to-database
- **Day 8** — Prometheus metrics with Docker service discovery: latency histograms, outcome
  counters and consumer lag from two independent sources
- **Day 9** — load harness with a scheduled (not emergent) send rate; holds 100/s and 1000/s
  exactly, and reports when the generator itself is the bottleneck
- **Day 10** — scaling matrix over 1/2/4/8 workers at 3 and 12 partitions; throughput rose
  1828 → 3807 ev/s, and raising the partition count made it *worse*, not better
- **Day 11** — chaos and breaking point: nothing broke at 20,000/s; the only availability
  failure in the project is losing the broker
