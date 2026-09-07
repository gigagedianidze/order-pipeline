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

```sh
docker compose up -d          # Kafka (KRaft) + Postgres + topic creation
go run ./cmd/smoke            # acceptance check: talks to both, exits 0
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

## Status

Following the 13-day plan in [PLAN.md](PLAN.md). Day 1 complete: infrastructure up,
topics created with 3 partitions, Go services scaffolded.
