# Order Pipeline

An event-driven order processing system in Go: HTTP ingress → Kafka → consumer-group workers →
PostgreSQL, with a separate gRPC read path.

It was built to be **measured, broken and recovered**, not just to run. Every claim below is a
number from a recorded experiment — the raw reports are in [`results/`](results/), the analysis
in [FINDINGS.md](FINDINGS.md), and the reasoning behind each design choice in
[DECISIONS.md](DECISIONS.md).

This is a backend system, driven with `curl`, `grpcurl` and PromQL. A web console
([`cmd/console`](cmd/console/)) puts the same experiments behind buttons and the same Prometheus
behind a dashboard — it runs the commands documented below rather than replacing them, so every
number on this page is reproducible without it.

```
  WRITE PATH (asynchronous)                    READ PATH (synchronous)

  POST /orders                                 GET /orders/{id}
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
                                   ^
                          failed   |
                          records  +--> [ Kafka: orders.dlq ]
```

Writes are acknowledged as soon as Kafka has them (`202 Accepted`) and persisted asynchronously.
Reads go through a separate service over gRPC. That asymmetry is the architecture: the two paths
saturate, fail and get tuned independently.

---

## Results

**Scaling** — each configuration is deliberately overloaded and the drain measured, because
workers only run flat out while a backlog exists.

| Workers | 3 partitions | 12 partitions |
|---------|--------------|---------------|
| 1       | 1828 ev/s    | 1922 ev/s     |
| 2       | 2333 ev/s    | —             |
| 4       | 3348 ev/s    | 1932 ev/s     |
| 8       | 3807 ev/s    | 2410 ev/s     |

Quadrupling the partition count made the system **slower**, not faster — the opposite of the
textbook expectation. Partition count caps how many consumers can participate, but only binds if
nothing else saturates first, and here something else did. Per-worker throughput *falls* as
workers are added, and 1828 ev/s for a single worker matches one serial database round trip per
record almost exactly.

**Batching** — the fix the scaling table implied, measured. One worker, draining a 48001-record
backlog, `WORKER_BATCH_SIZE` the only variable.

| Batch size | Throughput | vs. serial |
|-----------:|-----------:|-----------:|
| 1          | 1908 ev/s  | 1.0×       |
| 10         | 13796 ev/s | 7.2×       |
| 50         | 22203 ev/s | 11.6×      |
| 200        | 41082 ev/s | 21.5×      |

Batch=1 reproduces the 1828 ev/s above, which is what makes the rest of the column meaningful.
The ceiling was the worker's per-record round trip, not partition count and not Postgres — the
same database absorbed 21× the write rate once the round trips were amortised. It ships defaulted
to 1 so every measurement on this page stays reproducible; `make batch-matrix` runs the
experiment, and the trade it buys is per-record latency, since a record waits for its whole batch.

**Overload** — 4 workers, 3 partitions, 15s at each rate.

| Offered | 503s | Lost | Accept p99 | End-to-end p99 | Peak lag |
|---------|------|------|-----------|----------------|----------|
| 1,000   | 0    | 0    | 1 ms      | 1 ms           | 2        |
| 5,000   | 0    | 0    | 3 ms      | 5.4 s          | 26908    |
| 20,000  | 0    | 0    | **15 ms** | **71 s**       | 261292   |

At 20,000/s — over 5× the sustainable write rate — all 300001 orders were accepted and persisted.
The entire overload became latency, not errors. **Overload is not a failure mode here.**

**Failure** — every mode induced deliberately and measured.

| Component lost | Result | Correctness | Availability |
|---|---|---|---|
| Worker (graceful stop) | rebalance in 1.35s | intact | intact |
| Worker (killed) | rebalance in 43.2s, lag spike | intact | intact |
| Database, outage < retry budget | latency only | intact | intact |
| Database, outage > retry budget | 6 dead letters per 150001, tagged | intact | intact |
| **Broker** | **384 refusals per 60001** | **intact** | **degraded** |

Correctness survived every experiment. Nothing was lost, duplicated in the database, or silently
dropped. The broker is the only hard dependency, because it is the one component with no queue
in front of it.

A dead letter is a backlog, not a verdict. In the recorded recovery run, a 90-second outage — 45
seconds past the retry budget — parked 4 records out of 150001, and `make replay` sent all 4 back
through the ordinary pipeline: **150001 rows, 150001 distinct `event_id`s**, nothing lost and
nothing duplicated. The rows appeared 0.01s after the replay produced them, because a replayed
record is an ordinary record on the ordinary topic.

---

## Quickstart

Requires Docker Desktop (Compose v2). Go 1.27+ only if you want to run the load tools.

```sh
docker compose up -d --build --scale worker=2
```

That starts Kafka (KRaft), PostgreSQL, creates the topics, and runs the API, workers, query
service and Prometheus. Then:

```sh
curl -i -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"customer_id":"cust-1","items":[{"sku":"WIDGET-1","quantity":2,"unit_price_cents":1999}]}'
# HTTP/1.1 202 Accepted
# {"order_id":"57e3745e-...","status":"accepted"}

curl -s http://localhost:8080/orders/57e3745e-...
```

`make up` / `make load` / `make test` wrap the common commands; on Windows without GNU make,
`scripts\dev.ps1 <target>` does the same.

### Ports

| Service | Address | Notes |
|---------|---------|-------|
| api | `localhost:8080` | HTTP ingress |
| query | `localhost:9090` | gRPC; reflection enabled, so `grpcurl` needs no proto |
| Kafka | `localhost:9092` | `kafka:19092` from inside Compose |
| PostgreSQL | `localhost:5433` | 5432 inside; 5433 avoids clashing with a local install |
| Prometheus | `localhost:9091` | expression browser |
| console | `localhost:8081` | web control panel; loopback only, and behind a password |
| metrics | `:2112` per service | scraped via Docker service discovery |

---

## API

### `POST /orders`

Validates, assigns an `order_id`, produces to Kafka keyed by that id, and returns **202** — not
201, because nothing is persisted yet. Invalid bodies get 400 listing every problem at once; if
the broker does not acknowledge, the client gets 503 rather than a false 202.

Send an **`Idempotency-Key`** header to make your own retries safe. Both ids are derived from the
key, so a retry after a timeout produces an event the worker has already deduplicated on rather
than a second genuine order. Without the key, a retried POST *is* a new order.

```sh
# The same key twice: one order, and the worker logs "duplicate event ignored"
curl -sX POST localhost:8080/orders \
  -H 'Idempotency-Key: checkout-abc-123' \
  -H 'Content-Type: application/json' \
  -d '{"customer_id":"cust-1","items":[{"sku":"A","quantity":2,"unit_price_cents":1999}]}'
```

First write wins: reusing a key with a different body returns the original order, because nothing
stores what the first body was.

### `GET /orders/{id}` and `GET /orders`

The API calls the `query` service over gRPC. A 404 straight after a 202 is correct, not a bug —
the write path is asynchronous. Listing uses keyset pagination with an opaque `next_page_token`,
never `OFFSET`.

```sh
curl -s "http://localhost:8080/orders?page_size=3&customer_id=cust-1"

grpcurl -plaintext localhost:9090 list order.v1.OrderService
grpcurl -plaintext -d '{"order_id":"<id>"}' localhost:9090 order.v1.OrderService/GetOrder
```

### `GET /healthz` and `GET /readyz`

Liveness depends on nothing — restarting a healthy API because the broker is down would turn a
degraded write path into no API at all. Readiness depends on Kafka, because an API that cannot
reach the broker cannot accept a write. Neither depends on the query service, whose absence costs
reads and not writes.

Compose wires `/readyz` as the container healthcheck. The images are distroless, so the probe is
the service binary itself: `/service -healthcheck http://127.0.0.1:8080/readyz`.

---

## Operating it

```sh
# where events land, and which partition
docker exec edp-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:19092 --topic orders --from-beginning \
  --property print.partition=true --property print.key=true

# consumer group: members, offsets, lag
docker exec edp-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:19092 --describe --group order-processors

# what is in the dead-letter queue, and why
docker exec edp-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:19092 --topic orders.dlq --from-beginning \
  --property print.headers=true
```

Dead-lettered records keep their original key and value byte-for-byte so they can be replayed by
the same worker; the diagnosis rides in headers (`dlq_reason`, `dlq_error`, `dlq_attempts`,
`dlq_source_partition`, `dlq_source_offset`, `dlq_failed_at`).

### Recovering the dead letters

A dead letter is not a verdict, it is a backlog. Records tagged `retries_exhausted` were never
wrong — the database was gone when their turn came — so recovery is producing those same bytes
back into `orders` and letting the ordinary worker path finish.

```sh
make dlq      # what is on the dead-letter topic, and what would be replayed. Changes nothing.
make replay   # send the recoverable ones back, then check the rows are in PostgreSQL
```

`cmd/replay` is a dry run unless given `-apply`, and it commits an offset only for a record it
has already produced — the same rule the worker applies to its own writes, for the same reason.
Three things make it safe to run when you are not sure:

- **Idempotence.** The worker deduplicates on `event_id`, so replaying a record that *did* land
  is a no-op rather than a duplicate order.
- **A reason filter.** Poison records failed because of what they contain and would fail
  identically; the default replays only `retries_exhausted`, and `-reason poison` has to be asked
  for by name.
- **A loop guard.** Each replay stamps `replay_count`, the DLQ carries that header forward when a
  record dies again, and `-max-replays` (2 by default) is what stops one permanent failure
  becoming an endless `orders` → `orders.dlq` → `orders` circuit that looks like throughput.

Workers log what they own after every rebalance (`now_owns=orders:[0 1]`) and a heartbeat every
15s, so a worker that owns nothing — more workers than partitions — is visible rather than
silent.

### Metrics

Every service exposes `/metrics` on port 2112. Prometheus discovers them through the Docker API,
so `--scale worker=N` needs no configuration change. The two queries worth knowing:

```promql
# p99 write latency right now
histogram_quantile(0.99, sum by (le) (rate(order_processing_duration_seconds_bucket{status="persisted"}[1m])))

# is the consumer group falling behind? (positive = yes)
sum(deriv(kafka_consumergroup_lag{consumergroup="order-processors"}[1m]))

# is any partition stuck? anything above 0 for more than a scrape or two is an alert
sum(consumer_partitions_paused)
```

`consumer_partitions_paused` is a gauge rather than a counter deliberately. A worker pauses a
partition when a record can be neither persisted nor dead-lettered, and resumes it once the
broker answers again; a cumulative count of pause *events* cannot tell a partition that recovered
from one that never came back.

Lag is exported twice on purpose: by the workers, and by `kafka-exporter` reading the broker.
The workers' gauge goes stale at its last value when they die — reporting a healthy 0 while a
backlog grows — which is exactly when lag matters.

---

## Console

`cmd/console` is a web control panel for the same experiments: buttons that start and stop the
stack, scale the consumer group, drive load at a chosen rate, take Postgres or Kafka away, replay
what that dead-lettered, and run the recorded matrices — with the command's output streaming live
and a dashboard reading the same Prometheus as the PromQL above.

```sh
CONSOLE_PASSWORD='something long and not guessable' make console-hash
# CONSOLE_PASSWORD_HASH=$2a$10$...

CONSOLE_PASSWORD_HASH='$2a$10$...' make console
# -> http://localhost:8081
```

On Windows without GNU make, the same thing in PowerShell. Note the **single** quotes on the
hash: it contains `$` and PowerShell would expand `$2a` to nothing inside double quotes, leaving
a hash that silently never matches any password.

```powershell
$env:CONSOLE_PASSWORD = 'something long and not guessable'
go run ./cmd/console -hash
# CONSOLE_PASSWORD_HASH=$2a$10$...

$env:CONSOLE_PASSWORD_HASH = '$2a$10$...'
go run ./cmd/console
# -> http://localhost:8081
```

It runs on the host rather than in Compose, holds no pipeline credentials, and never connects to
Kafka or Postgres: it shells out to the same commands an operator would type and reads metrics
over HTTP. That separation is the point — it stays up and keeps reporting while the stack it is
pointed at is deliberately being broken, which is the only time a console really has to work.

### Why the buttons are an allowlist

A web page that can stop containers is, mechanically, remote command execution. The only thing
separating this from a shell is that no command is ever assembled from what the client sent:

- Every runnable command is an entry in `actions` in [`cmd/console/action.go`](cmd/console/action.go).
  There is no "run an arbitrary make target" escape hatch, because the moment one exists the
  allowlist is decorative.
- `argv` is built from constants and handed to `exec.Command` as a slice. No shell is involved,
  so there is no quoting to get wrong and no way for a parameter to become a second command.
- Parameters are validated into a number or an enum member *first*, and it is the validated value
  that gets formatted in. `TestValidateRejectsInjection` is the test that matters in that package:
  every case is an attempt to smuggle something past validation, and every one must be refused
  rather than sanitised.

One run at a time, enforced by a lock. That is a correctness requirement, not a politeness one:
these are experiments on a single shared stack, and a load run overlapping a chaos run produces
numbers that describe neither.

Console runs are also namespaced away from [`results/`](results/) proper, under `console-`. Every
script writes to `results/reports/$TAG.txt`, defaulting to the tag its recorded experiment used —
so without this, pressing "Kill the broker" during a demo would silently overwrite the very
measurement the tables on this page cite, and nothing would show it but `git status`. Those files
are gitignored: a button press is a demonstration, not a recorded result.

### Reaching it from somewhere else

`CONSOLE_ADDR` is loopback by default and should stay that way. To demo the system remotely,
tunnel to it rather than binding it to a public interface — the buttons are then never on a
listening port, and the machine needs no inbound firewall rule:

```sh
cloudflared tunnel --url http://localhost:8081
```

For a stable hostname, a named tunnel maps one to this port:

```sh
cloudflared tunnel login
cloudflared tunnel create order-pipeline
cloudflared tunnel route dns order-pipeline pipeline.example.com
cloudflared tunnel run --url http://localhost:8081 order-pipeline
```

Set `CONSOLE_SECURE_COOKIE=true` and `CONSOLE_TRUSTED_PROXY=true` when doing this. The first marks
the session cookie `Secure`; the second makes the login throttle key on `X-Forwarded-For` rather
than on the tunnel's own address, without which every request looks like it came from the same
client and the backoff is meaningless. Trusting that header when there is *no* proxy would be
worse than not reading it at all, which is why it is off by default.

The login is one bcrypt-hashed password with a per-address backoff after two failures. That
backoff table is bounded — expired entries are swept and the oldest are evicted past a cap —
because it is keyed by client address on a service that is reachable from the internet while the
tunnel is up, and an unbounded map keyed by something the caller chooses is a way to exhaust the
process rather than guess the password.

The tunnel only exists while `cloudflared` is running, so the demo is reachable exactly as long
as you choose.

---

## Reproducing the experiments

```sh
go build -o bin/loadgen.exe ./cmd/loadgen

go run ./cmd/loadgen -rate 1000 -duration 30s   # steady load
scripts/scaling-matrix.sh 3 1 2 4 8             # scaling matrix
scripts/ramp.sh 1000 5000 20000                 # ramp until something breaks
scripts/batch-matrix.sh 1 10 50 200             # does batching move the write ceiling?
scripts/chaos-db-outage.sh                      # database gone for 90s
scripts/chaos-kafka.sh                          # broker gone for 45s
scripts/replay-dlq.sh                           # break it past the retry budget, then recover
```

The load generator schedules send times in advance from the target rate and measures latency
from when a request was *due*, not when it was sent. A generator whose goroutines loop "send,
await response, send again" produces a rate of N/latency — it slows down exactly when the system
does, hiding the degradation it exists to find.

`scheduler behind` in the output means the generator could not dispatch on schedule: a statement
about the harness, not the system. Check it before believing a high-rate result.

---

## Layout

```
cmd/api        HTTP ingress, Kafka producer, gRPC client
cmd/worker     consumer group, idempotent persistence, retries, DLQ
cmd/query      gRPC read service over PostgreSQL
cmd/loadgen    load harness with a scheduled send rate
cmd/replay     returns dead-lettered records to the source topic, and verifies they land
cmd/console    web control panel: allowlisted commands, live dashboard
cmd/smoke      connectivity check
internal/      order domain, broker, store, retry policy, metrics, config, healthcheck
proto/         gRPC contract (generated code is committed)
scripts/       experiment harnesses
results/       recorded measurements and raw per-run reports
```

### Development

```sh
make test              # unit tests: no Docker, no network
make test-integration  # store tests against the running Postgres (needs `make up`)
make test-race         # under the race detector (needs cgo and a C toolchain)
make help              # every target
```

Every push runs the same commands in GitHub Actions ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)):
`gofmt`, `go vet`, `go build`, `go test -race`, and the store's integration tests against a real
PostgreSQL service container. A suite that only ever runs on the author's laptop is a claim about
the laptop.

The store's integration tests are gated on `POSTGRES_TEST_DSN` so `make test` stays hermetic.
They exist because some claims only PostgreSQL can settle: that `ON CONFLICT` really makes
redelivery a no-op, that a failed batch leaves no partial rows, and that keyset pagination stays
stable while rows are inserted between pages.

```sh

protoc --proto_path=proto \
  --go_out=. --go_opt=module=github.com/gigagedianidze/order-pipeline \
  --go-grpc_out=. --go-grpc_opt=module=github.com/gigagedianidze/order-pipeline \
  proto/order/v1/order.proto
```

---

## Scope

Deliberately excluded, to keep the system small enough to understand completely: Kubernetes,
cloud deployment, multi-broker clusters, database replicas, deployment pipelines, Kafka
transactions, auth, a schema registry, and a frontend. Each is a project in its own right and none would have made the
measurements above more informative. The reasoning for the choices that *were* made is in
[DECISIONS.md](DECISIONS.md).
