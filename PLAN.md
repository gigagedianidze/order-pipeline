# Real-Time Order Processing System — 13-Day Plan

**Window:** Tue 8 Sep 2026 → Sun 20 Sep 2026 (13 consecutive days)
**Budget:** 5–6 focused hours/day ≈ 65–78 hours total. Cost: $0 (local only).
**Goal:** not "a working app" — a system you have *measured*, *broken*, *recovered*, and can *defend*.

---

## Architecture (final target)

```
  WRITE PATH                                    READ PATH

  POST /orders                                  GET /orders/{id}
        │                                              │
        ▼                                              ▼
  ┌────────────┐                                ┌────────────┐
  │    api     │                                │    api     │
  └─────┬──────┘                                └─────┬──────┘
        │ produce                                     │ gRPC
        ▼                                             ▼
  ┌────────────────────┐                        ┌──────────────┐
  │       Kafka        │                        │    query     │
  │  orders (3 parts)  │                        │   service    │
  └─────────┬──────────┘                        └──────┬───────┘
            │                                          │
    ┌───────┴───────┐                                  │
    ▼               ▼                                  │
┌────────┐     ┌────────┐                              │
│worker-1│ ... │worker-N│                              │
└───┬────┘     └───┬────┘                              │
    └───────┬──────┘                                   │
            ▼                                          │
      ┌──────────────┐                                 │
      │  PostgreSQL  │◀────────────────────────────────┘
      └──────────────┘
```

**Three services**, not fifteen: `api` (HTTP ingress + Kafka producer + gRPC client),
`worker` (Kafka consumer + idempotent persistence), `query` (gRPC read service over Postgres).

**Why gRPC is here and not decoration:** writes go `api → Kafka → worker → Postgres` (async).
Reads go `api → gRPC → query → Postgres` (sync). That is a real internal service boundary
with a real reason to exist, not a query API bolted on for the résumé bullet. If you ever
find yourself unable to justify it in one sentence, delete it and take the time back.

---

## Locked scope — the "no" list

Say no to these for all 13 days. Every one of them is a 6-month project wearing a disguise.

- Kubernetes, service mesh, Helm
- Cloud deployment of any kind (Docker Compose only)
- Multi-broker Kafka cluster / replication factor > 1
- Postgres replicas, failover, connection pooler
- CI/CD pipeline
- Exactly-once semantics / Kafka transactions (at-least-once + idempotent writes instead — and be able to explain why that's the right call)
- Auth, rate limiting, a frontend
- Schema registry / Avro for Kafka payloads (plain JSON is fine; protobuf is only for gRPC)
- Grafana dashboards (Prometheus + PromQL queries are enough; Grafana is a Day 13 stretch)

---

## Phase 1 — Skeleton and happy path (Days 1–4)

### Day 1 — Tue 8 Sep · Scaffold + infrastructure
- Go module, folder layout: `cmd/api`, `cmd/worker`, `cmd/query`, `internal/...`
- `docker-compose.yml`: Kafka in **KRaft mode** (no ZooKeeper) + Postgres 16
- Topic `orders` created with **3 partitions**, replication factor 1
- Config via env vars, structured logging (`log/slog`), `Makefile`

**Done when:** `make up` brings up Kafka + Postgres, and a throwaway Go program
connects to both and exits 0. Nothing else.

**Trap:** you can lose a whole day to Kafka networking in Docker. If `advertised.listeners`
is fighting you past hour 3, take a known-good compose file and move on.

### Day 2 — Wed 9 Sep · Ingress and produce
- `POST /orders` → validate → assign `order_id` (UUID) → produce to `orders`
- **Partition key = `order_id`** (all events for one order stay ordered on one partition)
- Return `202 Accepted` with the `order_id` — not `201`, because you haven't persisted it yet.
  Be ready to explain that distinction; it is an interview question in disguise.

**Done when:** `curl -X POST .../orders` returns 202 and a console consumer shows the
message. You can say which partition it landed on and why.

### Day 3 — Thu 10 Sep · Consumer groups and rebalancing
- Worker joins consumer group `order-processors`, decodes, logs, does nothing else yet
- Run 1, then 2, then 4 worker instances

**Done when:** with 3 partitions and 2 workers you can show the partition split in the logs;
kill one worker and watch the rebalance reassign its partitions. Write down what you see at
4 workers vs 3 partitions — **one worker sits idle**. That observation is Day 10's punchline.

### Day 4 — Fri 11 Sep · Persistence + idempotency **together**
This is the day the original month-plan got wrong by deferring idempotency to week 3.
It is a schema decision. Build it now or rewrite your data layer later.

- `orders` table + `UNIQUE` constraint on `event_id`
- Write with `INSERT ... ON CONFLICT (event_id) DO NOTHING`
- **Manual offset commit, only after the successful DB write** (at-least-once delivery)

**Done when:** replaying the identical event 5 times produces exactly one row, and
`DECISIONS.md` has a paragraph on why at-least-once + idempotent write beats chasing
exactly-once here.

---

## Phase 2 — Correctness under failure (Days 5–8)

### Day 5 — Sat 12 Sep · Graceful shutdown
- `SIGTERM`/`SIGINT` → cancel context → stop fetching → finish in-flight work →
  commit offsets → close producer/consumer/DB pool → exit
- Shutdown deadline (e.g. 15s) with a hard-exit fallback

**Done when:** you `docker stop` a worker mid-load and end with **zero lost and zero
duplicated rows**. Verify by row count, not by vibes.

### Day 6 — Sun 13 Sep · Retries, backoff, DLQ
- Exponential backoff **with jitter** on transient DB errors
- After N attempts → publish to `orders.dlq` with the failure reason; commit and move on
- Classify: transient (retry) vs. poison/malformed (straight to DLQ, never retry)

**Done when:** `docker compose stop postgres` for 30s under sustained load → the worker
survives and drains cleanly on recovery, no data loss. A hand-crafted malformed message
lands in the DLQ instead of causing a crash loop.

### Day 7 — Mon 14 Sep · gRPC read path
- `.proto` for `OrderService`: `GetOrder`, `ListOrders` (with paging)
- `query` service implements it; `api` calls it for `GET /orders/{id}`

**Done when:** `grpcurl` returns an order directly, **and** the HTTP GET returns the same
order through the gRPC hop.

### Day 8 — Tue 15 Sep · Observability (budget this as real work)
Do not treat this as a free line item — it is a full day, and without it Day 10 produces
no numbers worth writing down.

Four metrics minimum:
- `orders_processed_total{status}` — counter
- `order_processing_duration_seconds` — **histogram** (you need p95/p99, not an average)
- `consumer_lag` — per partition; the single most important number in the whole project
- `retries_total`, `dlq_total` — counters

**Done when:** with load running, one PromQL query answers *"what is my p99 processing
latency right now"* and another answers *"is my consumer lag growing or flat"*.

---

## Phase 3 — Measure and prove (Days 9–11)

This phase is the project. Everything before it was setup.

### Day 9 — Wed 16 Sep · Load harness
- A Go producer harness (or k6) with a **configurable, controlled rate** — not a `for` loop
  hammering as fast as it can, which measures your laptop, not your system
- Records: send rate, end-to-end latency (produce → row committed)

**Done when:** you can hold a steady 100/s and a steady 1,000/s, and prove the achieved
rate matches the target rate.

### Day 10 — Thu 17 Sep · The scaling experiment
Run the matrix. **Record every number in `FINDINGS.md` as you go**, not from memory afterwards.

| Workers | Throughput (ev/s) | p50 | p95 | p99 | Peak consumer lag |
|---------|-------------------|-----|-----|-----|-------------------|
| 1       |                   |     |     |     |                   |
| 2       |                   |     |     |     |                   |
| 4       |                   |     |     |     |                   |
| 8       |                   |     |     |     |                   |

**Done when:** you can explain the plateau. (Prediction: throughput stops improving past
3 workers because you have 3 partitions — partition count is the parallelism ceiling.
Then re-run with 12 partitions and show the ceiling move. *That* is the interview answer.)

### Day 11 — Fri 18 Sep · Chaos + find the breaking point
- Kill a worker mid-load → measure rebalance time and the lag spike
- Restart Postgres mid-load → confirm backoff holds and the backlog drains
- Ramp 1k → 5k → 10k/s until something actually breaks

**Done when:** `FINDINGS.md` names **what broke first and why** (DB connection pool?
single-broker fsync? batch size? your own producer?). "It broke somewhere" is not an answer.

---

## Phase 4 — Package it (Days 12–13)

### Day 12 — Sat 19 Sep · Docs and one-command run
- `README.md`: architecture diagram, one-command start, the results table from Day 10
- `DECISIONS.md`: at-least-once vs exactly-once · partition key = order_id · manual offset
  commit · DLQ policy · why gRPC internally
- `make up` / `make load` / `make test`

**Done when:** a stranger clones it and has it running with one command, and can read your
numbers without running anything.

### Day 13 — Sun 20 Sep · Buffer, résumé, self-interview
Half of this day is buffer — you will need it; something on Days 1–11 will overrun.

Write 3–4 bullets using **your real Day 10 numbers**, e.g.:

> Built an event-driven order processing system in Go (Kafka, gRPC, PostgreSQL); scaled
> consumer-group workers from 1→8 and measured throughput from X to Y events/sec at Z ms p99,
> identifying partition count as the parallelism ceiling. Implemented idempotent processing,
> exponential-backoff retries and a dead-letter queue; verified zero message loss across
> worker crashes and a 30-second database outage under sustained load.

Then answer these out loud, without notes:

1. Why at-least-once instead of exactly-once? What did that cost you?
2. What happens if two workers get the same order? Why can't they?
3. Why is `order_id` the partition key? What breaks if you use a random key?
4. When exactly do you commit the offset, and what breaks if you commit earlier?
5. What is consumer lag, and what does a *rising* lag actually mean?
6. What happened at 4 workers with 3 partitions?
7. How do you tell a retryable failure from a poison message?
8. What did your graceful shutdown have to do, in order?
9. Where did the system break first under load, and what would you fix?
10. Why gRPC for reads and Kafka for writes?

If you can answer all ten, the project has done its job — **whether or not you ever deploy it**.

---

## Daily discipline

- Keep `FINDINGS.md` open from Day 1. One line per surprise. Undocumented experiments are
  worthless in an interview.
- Commit daily with a real message.
- If a day overruns, cut *scope inside that day* — never skip the day's acceptance check.
- If you fall two days behind: drop the gRPC read path (Day 7) first, then Grafana.
  Never drop Days 10–11.
