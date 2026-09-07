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
| query (gRPC)     | `localhost:9090` | Day 7                                        |
| metrics          | `localhost:2112` | Day 8                                        |

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
