# Findings

One line per surprise, written the day it happened. Undocumented experiments are worthless.

## Day 1 — Tue 8 Sep (started 7 Sep)

- **Port 5432 was already taken by a native PostgreSQL service.** Docker reported the port
  published (`5432/tcp -> 0.0.0.0:5432`) and the container was healthy, yet host clients got
  `FATAL: password authentication failed for user "orders"` — because they were reaching the
  *native* server, which has no such user. In-container `psql` worked fine, which is the
  signal that the problem is the host port, not the database. Moved to host port 5433.
- **Compose shell-splits a plain-string `command`.** With `entrypoint: ["/bin/sh","-c"]`, a
  string `command:` is tokenised, so `sh -c` received only the first word and `kafka-topics.sh`
  ran with no arguments and printed its help text. Fix: pass the command as a **single-element
  list** so it survives as one argv entry.
- Kafka warns that topic names mixing `.` and `_` can collide in metric names. `orders.dlq`
  is fine on its own; just don't also introduce `orders_dlq`.

## Day 2 — Wed 9 Sep (started 7 Sep)

- **Key stickiness verified directly**, not assumed: three records produced with the identical
  key all landed on partition 2, while three different keys spread across 0, 1 and 2. Worth
  doing once by hand — it turns "Kafka hashes the key" from a memorised sentence into an
  observed fact.
- franz-go's default partitioner is murmur2-compatible with the Java client, so records
  produced by the Go API and by `kafka-console-producer.sh` with the same key agree on the
  partition. Handy: CLI tools can be used to inject test traffic that lands where the real
  producer would put it.
- **Deleting and recreating a topic under a running producer did not break it.** franz-go
  refreshed its metadata and kept producing; offsets restarted at 0 on the fresh topic. Useful
  to know before Day 11, when topics get recreated with different partition counts.
- Piping a string into `kafka-console-producer.sh` from PowerShell prepends a UTF-8 BOM to the
  first record's key, silently making it a *different* key. When hand-injecting test records,
  check the first one.

## Day 3 — Thu 10 Sep (started 7 Sep)

### The scaling table, measured

| Workers | Assignment (3 partitions)          | Events of 30 processed |
|---------|------------------------------------|------------------------|
| 1       | `w1:[0 1 2]`                       | 30                     |
| 2       | `w1:[0 1]` `w2:[2]`                | —                      |
| 3       | `w1:[0]` `w2:[2]` `w3:[1]`         | 10 / 10 / 10           |
| 4       | `w1:[0]` `w2:[2]` `w3:[1]` `w4:[]` | 10 / 10 / 10 / **0**   |

**The fourth worker processed exactly zero events.** Partition count is the parallelism
ceiling for a consumer group: a partition is consumed by at most one member, so a fourth
member has nothing to be given. This is the Day 10 experiment's predicted plateau, visible
here for free on Day 3 — adding workers past 3 will not raise throughput until the topic has
more partitions.

The idle worker is not merely quiet, it is *unassigned*: franz-go never fires an assignment
callback for it at all, so it produces no rebalance log line whatsoever. Distinguishing "idle
because unassigned" from "crashed" in a log file is impossible without a heartbeat — which is
why the worker now logs `owns` and `partition_count` every 15 seconds.

### Rebalance after a hard kill took 44.9 seconds

Killed `w2` (owner of partition 2) with `Stop-Process -Force`, the equivalent of `SIGKILL`.
The idle `w4` inherited partition 2 — but only **44.9s later**, matching Kafka's default
`session.timeout.ms` of 45s.

That delay is the group waiting for a heartbeat that will never come. A process killed
outright cannot tell the group it is leaving, so its partitions stay stranded — and their
lag grows — for a full session timeout. A worker that shuts down *gracefully* sends
`LeaveGroup` and triggers rebalance immediately; that is the difference Day 5 is about, and
it is worth ~45 seconds of stalled partitions per uncontrolled crash.

Lowering `session.timeout.ms` shortens the stall but makes the group trigger spurious
rebalances whenever a worker is briefly slow — the usual availability/stability trade.

### franz-go rebalances cooperatively, so callbacks report deltas

The default balancer is cooperative-sticky: adding `w2` to a single-member group produced
`revoked=[2]` on `w1` and `assigned=[2]` on `w2` — partitions 0 and 1 were never revoked and
never stopped being consumed. Empty callbacks (`assigned={}`) also fire routinely as rounds
settle. Delta logs alone are unreadable, so the worker tracks a running total and every line
reports `now_owns`.

Eager rebalancing (the old default elsewhere) would instead have revoked *all* partitions
from every member — a full stop-the-world pause — before reassigning.

### `kafka-consumer-groups.sh --describe` is the ground truth

Three members, one partition each, `LAG 0` on all three. Use it to check the group's own view
rather than trusting the workers' logs about themselves. This is where Day 8's `consumer_lag`
metric comes from.

## Day 4 — Fri 11 Sep (started 7 Sep)

### Idempotency works: 5 identical deliveries, 1 row

Injected the same event (same `event_id`, same key) five times with
`kafka-console-producer.sh`. Result: 1 `order persisted`, 4 `duplicate event ignored`, 0
errors, and exactly 1 row. Acceptance met.

Note what the duplicates are *not*: they are not errors, and they are not silently dropped
either. `RowsAffected() == 0` is a first-class outcome the worker counts and logs, which means
a duplicate rate is observable rather than invisible.

### A conflict target only arbitrates the constraint you name

`ON CONFLICT (event_id) DO NOTHING` does **not** protect against a violation of the
`orders_pkey` constraint on `order_id`. An event with a fresh `event_id` but an `order_id`
that already exists raised:

```
ERROR: duplicate key value violates unique constraint "orders_pkey"
```

This is the desired behaviour — that record is a genuine data conflict, not a redelivery, and
it should surface. But it is worth knowing that a bare `ON CONFLICT DO NOTHING` would have
swallowed it, and that the choice between the two forms is a real semantic decision rather
than a style preference.

### The serious one: stopping at a failed record is not enough to avoid losing it

The first implementation stopped processing a partition at its first failed write and did not
commit that record. That looked correct and was not.

The client's fetch position had already advanced past the failed record. On the next poll the
records *behind* it arrived, were written successfully, and were committed — moving the
group's offset past the failure.

Measured, with a deliberately conflicting record at offset 20:

```
PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
1          22              22              0     <- offset 20 skipped, lost forever
```

Lag 0 and a healthy-looking group, with one message silently gone. This is precisely the class
of bug that makes "at-least-once" a claim rather than a guarantee, and it was invisible from
the outside: no error, no lag, no alert.

**Fix:** `PauseFetchPartitions` on the partition that failed. Nothing behind the failure is
fetched, so nothing behind it can be committed. Same scenario after the fix:

```
PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
0          21              21              0
1          20              22              2     <- stalled at the failure, nothing lost
2          18              18              0
```

Partition 1 stalls at the bad record while 0 and 2 keep processing — a stuck record degrades
one partition, not the system. The stall is visible as lag, which is exactly what Day 8's
`consumer_lag` metric is for. Day 6 converts the indefinite pause into bounded retries plus a
dead-letter queue so the partition can recover on its own.

### A consumer group cannot be deleted while a member is still in it

`--delete --group` fails with `GroupNotEmptyException`, and a hard-killed worker remains a
member for the full 45-second session timeout (Day 3). Resetting state between experiments
therefore means either waiting out the timeout or, faster, running the test under a new
`KAFKA_GROUP_ID`.

### End-to-end, clean run

40 orders posted to the API, 2 workers: 25 + 15 persisted, 0 errors, 40 rows. The split is
uneven because partitions are assigned whole — 2 partitions vs 1 — not because work is
distributed per message.

## Day 5 — Sat 12 Sep (started 7 Sep)

### Graceful shutdown mid-load: zero lost, zero duplicated

4000 orders posted while two workers consumed; `docker stop` on worker-1 partway through.

| | |
|---|---|
| Accepted (202) | 4000 |
| Rows in `orders` | **4000** |
| Distinct `event_id` | **4000** |
| Duplicate events seen by workers | **0** |
| Worker errors | 0 |
| API 503s | 0 |

Acceptance met, and verified by row count rather than by vibes. The worker drained and exited
in **0.19s**, having processed 446 events with 0 duplicates.

**Zero duplicates is the interesting half.** No message was lost — that was never really at
risk, since uncommitted offsets are simply redelivered. What a clean drain buys is that no
message is *reprocessed* either: every record the worker had in hand was written and its
offset committed before the process left. Idempotency would have absorbed the duplicates
silently, so this number only exists because the shutdown path is right.

### Graceful vs. hard kill: 1.35s versus 43.2s

| | `docker stop` (SIGTERM) | `docker kill` (SIGKILL) |
|---|---|---|
| Partitions reassigned after | **1.35s** | **43.2s** |
| Mechanism | `LeaveGroup` sent on the way out | group waits out `session.timeout.ms` |
| In-flight work | drained and committed | abandoned mid-batch |

Killed at 17:24:51.93, reassigned at 17:25:35.12. During those 43 seconds the group still
listed the *dead* worker as the owner of partitions 0 and 1, while the producer kept writing:
lag on those partitions climbed past 300 and kept growing. Partition 2, owned by the surviving
worker, stayed at lag 1 throughout.

That is the concrete cost of an uncontrolled crash, and it is not "some downtime" — it is a
specific number, produced by a specific setting, on partitions that are otherwise perfectly
healthy.

**The hard kill also lost nothing and duplicated nothing: 4000 accepted, 4000 rows, 4000
distinct event ids, 0 duplicates.** That was not the expected result and it is worth stating
plainly rather than reporting the tidier story.

Loss was never at risk — an uncommitted offset is simply redelivered. Duplicates were not
observed because the duplicate window is genuinely narrow: it opens only between a successful
database write and the offset commit that follows it. Offsets are committed once per poll
batch, so SIGKILL has to land inside that gap to cause reprocessing, and in this run it did
not. A busier worker, larger batches, or a slower commit would widen the window.

So the honest summary of graceful shutdown is not "it prevents data loss" — at-least-once plus
an idempotent write already do that. It is: **it collapses recovery from 43s to 1.4s, and it
removes a duplicate-processing race rather than relying on luck to miss it.**

### `restart: unless-stopped` does not restart a `docker kill`ed container

`RestartCount: 0` after SIGKILL. Docker treats an explicit `docker kill` as an operator
decision, the same as `docker stop`, so the restart policy does not fire. A process that dies
on its own — panic, OOM — *is* restarted. Worth knowing before concluding from a chaos
experiment that "the restart policy does not work".

## Day 6 — Sun 13 Sep (started 7 Sep)

### A 31-second database outage under load: nothing lost, nothing dead-lettered

`docker compose stop postgres` mid-load, 31 seconds, then start again.

| | |
|---|---|
| Accepted (202) | 4000 |
| Rows in `orders` | **4000** |
| Distinct `event_id` | **4000** |
| Dead-lettered | **0** |
| Duplicates | 0 |

One record's complete trace through the outage:

```
17:34:57.977  write failed, retrying   attempt 1   FATAL: terminating connection due to
                                                   administrator command (SQLSTATE 57P01)
17:35:06.033  write failed, retrying   attempt 2   hostname resolving error: lookup postgres
17:35:14.152  write failed, retrying   attempt 3   dial tcp 172.18.0.2:5432: connection refused
17:35:22.181  write failed, retrying   attempt 4
17:35:29.072  write failed, retrying   attempt 5
17:35:29.895  order persisted          attempt 6
```

Postgres came back at 17:35:29; the record was written 0.8 seconds later. The first failure is
`57P01` — the server saying goodbye on its way down — and subsequent ones degrade to DNS and
then TCP failures as the container disappears. Three different error shapes for one event, all
correctly classified as transient.

### The retry budget is spent on connect timeouts, not on backoff

Look at the gaps: roughly **8 seconds** between attempts. The backoff delays were 53ms, 116ms,
25ms — microscopic by comparison. Almost the entire interval is the *connection attempt itself*
timing out.

This matters for tuning and would have been invisible without the trace. The 45s elapsed budget
does not buy ~20 attempts as the backoff maths would suggest; it buys about **6**. A 31s outage
survived with two attempts to spare. A 45-second outage would have exhausted the budget and
dead-lettered live orders — not because anything was wrong with them, but because the budget is
measured in wall-clock time that connect timeouts dominate.

Two levers if that margin is too thin: raise `MaxElapsed` (bounded above by the 60s rebalance
timeout — so this cannot go far), or shorten pgx's connect timeout so each attempt fails faster
and more attempts fit in the same budget. The second is the better lever, and it is not obvious
until you look at the timestamps.

### Only 7 retry log lines for a 31-second outage

Not a bug — a consequence of the shape of the retry loop. Each worker blocks on the *one*
record it is retrying, so nothing else is attempted while the database is down. The backlog
accumulates in Kafka as consumer lag rather than as a retry storm.

That is the desirable behaviour: an outage produces a queue, not a thundering herd, and the
queue drains at full speed on recovery. It also means "retries_total" is a poor proxy for
outage severity — consumer lag is the metric that tells that story, which is Day 8's job.

### Poison messages: classified correctly, no wasted retries

Two hand-crafted bad records were injected, and five valid orders behind them still persisted.

| Injected | Classified | Attempts | Why |
|---|---|---|---|
| Truncated JSON | `poison` | **0** | never parsed, so the database was never touched |
| Valid JSON, `order_id: "NOT-A-UUID"` | `poison` | **1** | tried once, `22P02` is not retryable |

`attempts=0` and `attempts=1` are the whole point: neither record burned the 45-second retry
budget on a failure that could never resolve. Both landed in `orders.dlq` with the original key
and value byte-for-byte, and the diagnosis in headers:

```
dlq_reason:poison, dlq_error:unexpected end of JSON input, dlq_attempts:0,
dlq_source_topic:orders, dlq_source_partition:2, dlq_source_offset:0, dlq_failed_at:...
```

The partition never stalled and no crash loop occurred — the Day 4 behaviour where a bad record
paused its partition indefinitely is now reserved for the one case that truly cannot make
progress: Kafka itself being unreachable, so the record can be neither persisted nor parked.

## Day 7 — Mon 14 Sep (started 7 Sep)

### Both read paths return the identical order

`grpcurl` straight to the query service, and `GET /orders/{id}` through the API's gRPC hop,
returned the same order down to the nanosecond timestamps. Acceptance met.

```
occurred_at   2026-09-07T17:45:50.561895Z   (accepted by the API)
processed_at  2026-09-07T17:45:50.577526Z   (persisted by the worker)
```

**15.6ms end-to-end**, API to database, through Kafka. Carrying both timestamps on the row was
worth doing: pipeline latency is now measurable from the data itself, without any instrumentation,
and Day 9's load harness gets a ground truth to check itself against.

### Status codes survive the hop

| Request | gRPC | HTTP |
|---|---|---|
| Unknown but valid UUID | `NotFound` | 404 |
| `NOT-A-UUID` | `InvalidArgument` | 400 |
| Query service down | `Unavailable` | 503 |
| Query service slow | `DeadlineExceeded` | 504 |

The `InvalidArgument` case is the one that is easy to get wrong. A malformed UUID reaches
Postgres and comes back as SQLSTATE `22P02`, which naively becomes a 500 — a caller's typo
reported as a server fault, and real faults then hidden among them. Reusing `store.IsRetryable`
from Day 6 sorts it out: if an error is not retryable it is about the data, so it is the
caller's problem, not ours.

### The read-after-write window is smaller than the client's own overhead

Five runs of "POST, then GET in a tight loop until 200": every single one returned 200 on the
**first** GET, in 38–51ms. The 404 window that the 202 contract explicitly allows never
materialised, because at idle the pipeline persists in ~15ms while `curl` takes ~40ms just to
start.

The window is real and the contract still needs it — under load it widens to whatever the
consumer lag is. But it is worth knowing that a naive read-after-write test at idle will pass
by accident and prove nothing. Day 10's loaded runs are where this gets a real answer.

### `EXPLAIN` confirms the pagination claim, and the SQL syntax matters

```
Limit  (actual time=0.011..0.021 rows=51)  Buffers: shared hit=19
  ->  Index Only Scan using orders_processed_at_order_id_idx
        Index Cond: (ROW(processed_at, order_id) < ROW(now(), '...'::uuid))
```

Index-only scan, condition pushed into the index, no sort, 0.021ms — and the same cost on page
1000 as on page 1.

The detail that matters: the row-value form `(a, b) < ($1, $2)` produces this plan. The
logically identical `a < $1 OR (a = $1 AND b < $2)` does not reliably produce it, because the
planner cannot express that as a single index condition. Writing the "obvious" version would
have given a correct answer with a much worse plan — and it would have looked fine in testing,
where the table is small.

## Day 8 — Tue 15 Sep (started 7 Sep)

### The two acceptance queries answer

```promql
# "what is my p99 processing latency right now"
histogram_quantile(0.99, sum by (le) (rate(order_processing_duration_seconds_bucket{status="persisted"}[1m])))
  -> 0.0024s        (p50: 0.0006s)

# end-to-end, API accept -> row committed
histogram_quantile(0.99, sum by (le) (rate(order_end_to_end_latency_seconds_bucket[1m])))
  -> 0.0050s

# "is my consumer lag growing or flat"
sum(deriv(consumer_lag[1m]))
  -> 0.00 records/sec   (positive = falling behind)
```

`deriv` rather than the raw value is the point. A steady lag of 5000 is a system keeping up with
a backlog; a lag of 500 climbing by 100/s is a system heading for trouble. The value alone
cannot tell those apart.

### The lag metric had a hole exactly where it mattered

The worker exports `consumer_lag`. Stopping every worker while load continued produced this:

| Source | Reported lag |
|---|---|
| `consumer_lag` (from the workers) | **0, 0, 0** |
| Reality (broker-side) | **629, 668, 626 — 1923 records** |

A completely healthy-looking dashboard with a growing backlog behind it. The workers are the
only source of the metric, so when they die the series goes stale at its last value — and
Prometheus serves that stale sample for five minutes. **Silence looked identical to success.**

Fixed by adding `kafka-exporter`, which reads group lag from the broker and does not care
whether any consumer is alive. The worker gauge is kept as well: it is per-worker and shows
what *this* member sees, which is useful for a different question. The rule the mistake
teaches is general — a health signal must not be produced by the thing whose health it reports.

### A rebalance during a backlog produced 626 duplicates, absorbed silently

Restarting two workers against the 1923-record backlog:

| Worker | Persisted | Duplicates |
|---|---|---|
| worker-2 (started first, owned all 3 partitions) | **1923** | 0 |
| worker-1 (joined second, triggering a rebalance) | 0 | **626** |

worker-1 was handed partitions starting from the last *committed* offset, which lagged what
worker-2 had already written — offsets are committed once per poll batch, so a large in-flight
batch is written well before its offset advances. 626 records were therefore processed twice.

This is at-least-once behaving exactly as documented, and it is the first time the project has
actually *observed* it rather than argued for it. Every one of those 626 was absorbed by
`UNIQUE (event_id)` with no duplicate rows and no errors — Day 4's design earning its keep.

It also justifies the metric label. `orders_processed_total{status="duplicate"}` made this
visible; a bare "processed" counter would have hidden it completely, and a counter of "errors"
would have hidden it too, since a duplicate is not an error.

### Docker's embedded DNS cannot be used for Prometheus service discovery

`--scale worker=N` means worker addresses are unknown in advance, so targets have to be
discovered. `dns_sd_configs` on the Compose service name looks like the obvious answer and
fails: Prometheus queries the fully-qualified `worker.` (with the trailing dot) and Docker's
embedded resolver never answers, producing `i/o timeout` rather than an empty result.

`docker_sd_configs` works and picks up scaled workers within one refresh interval. Two details
it needs: containers are discovered once *per network*, so without a `keep` on the network name
every counter appears doubled; and Prometheus reads `/networks` as well as `/containers`.

### Mounting the Docker socket `:ro` is security theatre

Prometheus runs as `nobody` and got `permission denied` on the socket. The quick fix is
`user: root` — and a `:ro` mount looks like it makes that safe. It does not: read-only applies
to the socket *file*, not to the API behind it, so anything that can reach it can still create
a privileged container.

A `docker-socket-proxy` with `CONTAINERS=1`, `NETWORKS=1`, `POST=0` was used instead. That is
the only version of this arrangement where the restriction is real.

## Day 9 — Wed 16 Sep (started 7 Sep)

### Both target rates hold exactly

| Target | Achieved | Accepted | Failed | Accept p99 | End-to-end p50 | End-to-end p99 |
|---|---|---|---|---|---|---|
| 100/s | 100.0/s | 3001 | 0 | 2.5 ms | 0.7 ms | 1.3 ms |
| 1000/s | 1000.0/s | 30001 | 0 | 1.5 ms | 0.4 ms | 10.7 ms |

Acceptance met: the achieved rate matches the target, and it is measured rather than assumed.

Note that end-to-end p50 (0.4–0.7ms) is *lower* than accept latency, which is not a
contradiction: accept latency includes the HTTP round trip from the client, while end-to-end is
measured from the timestamp the API stamps on the event. They measure different spans.

Also worth recording against Day 7: a single cold request measured 15.6ms end-to-end, while
under sustained load the p50 is 0.4ms. A one-shot measurement of an idle system mostly measures
the system waking up.

### At 5000/s the two halves of the system come apart, and that is the whole point

| | 1000/s | 5000/s |
|---|---|---|
| Achieved rate | 1000.0/s | 4999.9/s |
| **Accept latency p50** | 0.8 ms | **0.9 ms** |
| **End-to-end p50** | 0.4 ms | **8179 ms** |
| End-to-end p99 | 10.7 ms | **18042 ms** |
| Accepted → persisted | 30001 → 30001 | 75001 → 75001 |

Ingress did not degrade at all. Accept latency at 5000/s is statistically indistinguishable
from 1000/s, while the write path fell **eighteen seconds** behind — and lost nothing.

This is the async architecture doing exactly what it is for, stated as a number rather than an
opinion. A synchronous design would have pushed that 18 seconds onto the client as timeouts and
5xx. Here the queue absorbs it: the API keeps its promise (202, durably in Kafka) and the
backlog is paid down afterwards. The cost is equally concrete — read-after-write is no longer
milliseconds but potentially many seconds, which is precisely the window Day 7 could not
observe at idle.

2 workers against 3 partitions is the bottleneck being measured here. Day 10 varies it.

### The generator flags its own limits

`scheduler_behind` counted 22630 of 75001 ticks at 5000/s. That is the harness reporting that
*it* could not dispatch on schedule, not a finding about the pipeline — at 5000/s the interval
is 200µs, below Windows timer granularity of roughly 1–15ms.

Aggregate throughput was still accurate (4999.9/s against a 5000/s target), because lateness on
individual ticks averages out. But sub-millisecond per-request timings at those rates should be
read as approximate, and any Day 11 result at high rates has to be checked against this counter
before it is believed. A benchmark that cannot tell "the system is slow" from "my generator is
slow" is not measuring anything.

## Day 10 — Thu 17 Sep (started 7 Sep)

### Method

Each configuration is deliberately overloaded — 5000/s offered for 10s into a system that
cannot persist at that rate — and the drain is measured. Throughput is therefore *capacity*,
not the offered rate: workers only run flat out while a backlog exists.

Every run starts from a deleted and recreated topic and a truncated table, with workers scaled
to zero first and 20 seconds allowed for the group to settle, so no run measures a rebalance or
inherits the previous run's offsets.

### Results: 3 partitions

| Workers | Throughput (ev/s) | p50 | p95 | p99 | Peak lag | Persisted |
|---------|-------------------|------|------|------|----------|-----------|
| 1       | 1828              | 8833 | 16770 | 16920 | 38879 | 50001 |
| 2       | 2333              | 3901 | 12279 | 12709 | 29012 | 50001 |
| 4       | 3348              | 2698 | 4244  | 4338  | 17830 | 50001 |
| 8       | 3807              | 2764 | 4100  | 4218  | 19119 | 50001 |

Latencies in ms, measured from `occurred_at` to `processed_at` on the rows themselves. Every
configuration persisted all 50001 orders — the system never lost anything, it only took longer.

### Results: 12 partitions — the ceiling did **not** move

| Workers | Throughput (ev/s) | p50 | p95 | p99 | Peak lag | Persisted |
|---------|-------------------|------|------|------|----------|-----------|
| 1       | 1922              | 8445 | 14735 | 15535 | 30831 | 50001 |
| 4       | 1932              | 9249 | 16852 | 17042 | 29401 | 50001 |
| 8       | 2410              | 3962 | 11167 | 11940 | 26438 | 50001 |

Side by side:

| Workers | 3 partitions | 12 partitions | Change |
|---------|--------------|---------------|--------|
| 1       | 1828         | 1922          | +5% (noise) |
| 4       | 3348         | **1932**      | **−42%** |
| 8       | 3807         | **2410**      | **−37%** |

**The prediction was wrong, and that is the finding.** The plan expected throughput to plateau
at 3 workers because there are 3 partitions, and to rise once the topic had 12. Quadrupling the
partition count instead made the system *slower* at every worker count above one.

### Why: partition count was never the ceiling here

Three pieces of evidence point the same way.

**Per-worker throughput falls as workers are added.** One worker sustains 1828 ev/s. At 3
partitions and 4 workers only three can consume, and they manage 3348 between them — about 1116
each, well under half what a single worker achieves alone. Adding consumers is not unlocking
parallelism; it is dividing a fixed resource and paying coordination costs on top.

**The single-worker number barely moved with 4× the partitions** (1828 → 1922). If partitions
were the constraint, the one-consumer case is the one that should have been unaffected — and it
was. Everything else got worse.

**More partitions fragment the work.** The same 50001 records spread over 12 partitions instead
of 3 means each partition's batches are a quarter the size, so every poll returns less useful
work per round trip while the worker maintains four times as much fetch state against a single
broker. That is pure overhead when partitions are not the bottleneck.

The likely real ceiling is the shared PostgreSQL instance, and the worker's own design: records
are processed **one at a time, serially** — `EachPartition` walks partitions in turn and each
record is a separate `INSERT` round trip. A single worker's throughput is therefore roughly
1/(database round trip), and 1828 ev/s implies about 0.55ms per write, which matches the
measured p50 write time almost exactly. Owning more partitions cannot help a worker that
processes them sequentially.

**The honest interview answer is therefore not "partition count is the parallelism ceiling"** —
it is: *partition count is the ceiling on how many consumers can participate, but it is only the
binding constraint if nothing else saturates first. Here something else did, so adding
partitions bought nothing and cost batching efficiency.* Day 11 identifies what breaks first.

Two obvious improvements this measurement suggests, neither of which is more partitions:
batch the inserts (one multi-row `INSERT` per poll instead of one per record), and process
partitions concurrently within a worker instead of serially.

### Caveats worth stating before quoting any of these numbers

Single Kafka broker, single PostgreSQL instance, all of it plus the load generator on one
Windows laptop under Docker Desktop. These figures characterise *this* deployment; they are not
a statement about Kafka's or Go's capabilities. What survives the caveats is the *shape* — where
the knee is, which direction each change moved things, and why.

## Day 11 — Fri 18 Sep (started 7 Sep)

### The ramp: nothing broke, up to 20× the sustainable write rate

"Broken" was defined before the experiment (see DECISIONS.md): the API rejecting orders,
accepted orders never reaching the database, or accept latency degrading non-linearly. A
growing backlog explicitly does not count.

4 workers, 3 partitions, 15s at each rate.

| Offered | Achieved | 503s | Failed | Accept p99 | End-to-end p99 | Persisted | Missing | Peak lag |
|---------|----------|------|--------|-----------|----------------|-----------|---------|----------|
| 1,000   | 1000     | 0    | 0      | 1 ms      | 1 ms           | 15001     | **0**   | 2        |
| 2,500   | 2500     | 0    | 0      | 2 ms      | 9 ms           | 37501     | **0**   | 11       |
| 5,000   | 5000     | 0    | 0      | 3 ms      | 5352 ms        | 75001     | **0**   | 26908    |
| 10,000  | 9999     | 0    | 0      | 4 ms      | 27316 ms       | 150001    | **0**   | 103533   |
| 20,000  | 19998    | 0    | 0      | **15 ms** | **70971 ms**   | 300001    | **0**   | 261292   |

At 20,000/s — more than five times what the write path can sustain — the API accepted every one
of 300001 orders, rejected none, and every order reached the database. Accept latency went from
1ms to 15ms while *offered load went up twentyfold*.

The entire overload was converted into **71 seconds of lag**. That is the queue doing precisely
the job it was introduced for, and it is why the definition of "broken" had to exclude a growing
backlog: on any other definition this table reads as a catastrophic failure, when it is in fact
the design working.

The honest conclusion is therefore: **the ingress path has no breaking point within the load
this host can generate.** The write path saturates around 3800 ev/s and everything above that
becomes latency, not errors. Finding a real failure meant attacking the components that can
actually fail, rather than pushing the rate higher.

Caveat: `scheduler_behind` reached 250388 of 300001 at 20,000/s. The aggregate rate held
(19998/s achieved), but per-request timing at that rate is limited by the generator, so the 15ms
accept p99 should be read as approximate.

### Three measurement bugs in one day, all producing plausible numbers

Worth recording as its own finding, because it is the actual lesson of a benchmarking day.

1. **A six-minute lookback window.** `max_over_time(expr[6m:5s])` looks back from *now*, and each
   configuration took about ninety seconds, so runs reported the peak of previous runs. Tell:
   three identical throughput figures and four identical peak lags.
2. **A `rate()` window spanning the load boundary.** `rate(...[15s])` smoothed across the
   boundary between the 10s load phase and the idle period before it, flattening the burst being
   measured. It reported throughput as flat across all worker counts while peak lag showed 4
   workers holding the backlog to a third of what 1 worker allowed — two claims that cannot both
   be true.
3. **Two load generators running at once.** An aborted run survived being cancelled and kept
   generating load for forty minutes alongside its replacement, both appending to the same file.

Every one of these produced numbers that looked reasonable. The first two were caught only by
noticing internal contradictions; the third was caught by an impossible value — `persisted:
-749`, a negative row count.

The lesson is not "be careful". It is that a benchmark needs **cross-checks that can disagree
with each other**: throughput derived independently of latency, lag measured by something other
than the consumer, and a rate the generator reports separately from what the system observed.
Any single number, taken alone, would have been believed.

### Breaking the write path: a 90-second database outage

The ramp could not break the system, so the next question was the boundary of something that
was actually designed to fail: the 45-second retry budget from Day 6. A 31-second outage
survived intact. This one is 90 seconds — deliberately double the budget — under 1000/s.

| | |
|---|---|
| Accepted (202) | 150001 |
| Persisted | 149995 |
| **Dead-lettered** | **6**, all `reason=retries_exhausted` |
| Lost | **0** |
| Accept latency p99 | **1.4 ms** |
| End-to-end p50 / p99 | 37s / 91s |

149995 + 6 = 150001. **The system broke exactly where it was designed to, by exactly as much as
it was designed to, and said so.**

Three things make this the answer to "what broke first and why":

**It is bounded.** Six records out of 150001 — 0.004%. Not "the outage caused data loss", but
"six specific records exceeded a 45-second budget". Only the handful actually in flight per
worker when the database vanished can exceed it; everything behind them waits in Kafka, costing
latency rather than delivery.

**It is attributed.** Each carries `reason=retries_exhausted`, distinguishing it from `poison`.
An operator sees immediately that these are good records that ran out of time, not bad records
that will never work — which is the difference between replaying them and fixing a producer.

**It is recoverable.** The dead-letter records keep their original key and value byte-for-byte,
so replaying them once the database is healthy needs no special tooling.

Meanwhile accept latency was 1.4ms at p99. **The API never noticed the database was gone**, which
is the entire argument for the asynchronous write path, demonstrated by taking the database away
rather than by asserting it.

The tuning conclusion from Day 6 stands and now has a boundary: the budget buys roughly six
attempts because connect timeouts dominate it, and an outage longer than the budget dead-letters
whatever is in flight. Raising it is capped by the 60s rebalance timeout, so the real lever is
shortening the connect timeout so more attempts fit.

### Breaking ingress: a 45-second broker outage

The broker is the one component the architecture cannot absorb the loss of. The API produces
synchronously and answers 202 only on an acknowledgement, so with no broker there is nothing
honest to say. 500/s offered, Kafka stopped for 45 seconds.

| | |
|---|---|
| Offered | 60001 |
| Accepted (202) | 59617 |
| **Rejected (503)** | **256** |
| **Client transport failures** | **128** |
| Persisted | 59617 — **missing 0** |
| Accept latency p50 / p99 / max | 1.2 ms / **46.7 s** / 47.8 s |

59617 + 256 + 128 = 60001. Every order is accounted for.

**This is the first genuine availability failure in the project, and it is the right one.** Only
384 orders out of 60001 were refused — but they were refused *honestly*. Not one false 202 was
issued: the system never told a client an order was safe when it was not. That is the Day 2
decision (`ProduceSync` with all-ISR acks on the request path) being paid for and collecting.

The surprise is how few were refused. A 45-second outage at 500/s means roughly 22500 requests
arrived while the broker was down, yet only 384 failed. franz-go buffers and retries internally
up to `RecordDeliveryTimeout` (10s), so the great majority of those requests simply *waited* and
succeeded once the broker returned.

That is a trade-off, not a free win, and the accept-latency column is where it shows: **p99 of
46.7 seconds**. The API did not fail fast, it held requests open. With a 128-deep in-flight limit
in the generator, queueing pushed the worst case to 47.8s. A real client with a 30-second timeout
would have given up and retried, turning one slow request into two.

So the honest characterisation is: **a broker outage degrades availability, not correctness.**
Whether "hold the request for 10 seconds" or "fail immediately" is right depends on the caller,
and `RecordDeliveryTimeout` is the single knob that decides it. Ten seconds is defensible for a
mobile client that would rather wait than resubmit; it is wrong for a synchronous checkout page.

### Summary: what breaks, in order

| Component lost | Result | Correctness | Availability |
|---|---|---|---|
| Worker (graceful) | rebalance in 1.35s | intact | intact |
| Worker (killed) | rebalance in 43.2s, lag spike | intact | intact |
| Database, < retry budget | latency only | intact | intact |
| Database, > retry budget | 6 dead letters per 150001, tagged | intact | intact |
| **Broker** | **384 refusals per 60001, 47s worst-case latency** | **intact** | **degraded** |

Correctness survived every experiment run in this project. Nothing was ever lost, duplicated in
the database, or silently dropped. The only thing any failure ever cost was time — except the
broker, which costs availability, because it is the one component that has no queue in front of
it.

---

## Day 13 — Mon 22 Sep (started 7 Sep)

Hardening day. Two defects found by re-reading the code rather than by running it, the test
suite extended to cover the invariants that were previously only checked by hand, and the one
open question from Day 10 finally measured.

### A paused partition was never resumed

Day 4 established that a record which can be neither persisted nor dead-lettered must pause its
partition, and Day 6 called that pause temporary — "until Kafka is reachable". Nothing ever made
it temporary. `PauseFetchPartitions` was called and `ResumeFetchPartitions` appeared nowhere in
the tree, and franz-go keeps a paused partition paused across rebalances, so the partition
stayed dead until the process restarted.

Worse, it stayed dead *quietly*. The process is healthy by every measure a supervisor can see,
so `restart: unless-stopped` never fires. The only symptom is lag on one partition, and the only
counter the worker exported was pause *events*, which cannot distinguish a partition that paused
and recovered from one that never came back.

The fix has three parts, and the second and third matter as much as the first:

1. A `resumePaused` goroutine that un-pauses on a 15s tick.
2. It runs on its **own goroutine**, not in the poll loop. `PollFetches` blocks; if every
   partition is paused the poll loop is parked with nothing to fetch and could never resume
   anything. The deadlock is only obvious once written down.
3. It **pings the broker before resuming** rather than resuming blindly. Resuming into a
   continuing outage re-pauses immediately, which is churn with nothing to show for it.

`consumer_partitions_paused` is now a gauge, because the question is "is anything stuck right
now" and no counter can answer it.

### A bad page token was reported as a 500

`ListOrders` with a malformed `page_token` returned `500 internal error`. The cause is a
classifier answering a question it was never asked:

```go
if !store.IsRetryable(err) {          // "is this the caller's fault?"
    return nil, status.Errorf(codes.InvalidArgument, ...)
}
```

`IsRetryable` ends in a catch-all `return true` — anything that is not a `PgError` is assumed to
be a transport failure, which is right for the write path and wrong here. A cursor decode error
is not a `PgError`, so it was classified transient, so `!IsRetryable` was false, so the caller's
mistake was logged as a server fault and returned as a 500.

`TestDecodeCursorRejectsGarbage` passed throughout. It asserted that an error came back; it
could not assert what the client would see.

The fix is to stop conflating two questions. `ErrInvalidArgument` is a sentinel, `IsCallerError`
is its own classifier, and a test pins the relationship that actually matters — every caller
error must also be non-retryable, or the worker burns its whole budget on a record that was
always going to be rejected. The reverse does not hold, and that asymmetry is the bug: plenty of
errors are non-retryable *and* entirely our fault.

### Batching moves the write ceiling by 21×

Day 10 concluded that partition count was never the binding constraint, and that a single
worker's ceiling was roughly 1/(database round trip) because records were processed one at a
time. It named batching as the fix and did not test it. Now measured, one worker throughout,
draining a 48001-record backlog:

| Batch size | Drain span | Throughput | vs. serial |
|-----------:|-----------:|-----------:|-----------:|
| 1          | 25.16s     | 1908 ev/s  | 1.0×       |
| 10         | 3.48s      | 13796 ev/s | 7.2×       |
| 50         | 2.16s      | 22203 ev/s | 11.6×      |
| 200        | 1.17s      | 41082 ev/s | 21.5×      |

The batch=1 figure of 1908 ev/s reproduces Day 10's 1828–1922 ev/s, which is the control that
makes the rest of the column meaningful.

**The Day 10 diagnosis was right.** The ceiling was the per-record round trip, and amortising it
moves that ceiling by more than an order of magnitude with no change to partition count, worker
count, or the database. It is worth being precise about what this does and does not say: it says
the *worker* was the constraint, not Postgres, because the same database absorbed 21× the write
rate the moment the round trips were amortised.

**Caveat on latency.** This measures drain throughput, not steady-state latency: every record sat
in the topic while the backlog built, so end-to-end latency in this experiment is dominated by
backlog age and cannot be compared across rows. Batching genuinely does trade per-record latency
for throughput — a record waits for its whole batch — and `scripts/batch-matrix.sh` measures that
trade properly, under live load rather than against a backlog.

**It ships defaulted to 1.** Every measurement already in this file was taken against one insert
per record, and silently changing the default would invalidate all of them. `WORKER_BATCH_SIZE`
is the knob; `make batch-matrix` is the experiment.

### Batching must not cost poison isolation

A multi-row `INSERT` is all-or-nothing, so one poison record takes its whole chunk down with it.
Without care, that trades a 21× throughput win for the loss of the property Day 6 was built
around.

The worker falls back to one record at a time whenever a batch fails, which isolates the offender
and lets its neighbours through. That also preserves the named conflict target from Day 4: a
genuine `order_id` collision still surfaces against exactly one record instead of being swallowed.
Confirmed against real Postgres — `TestInsertOrdersIsAllOrNothing` checks that no row of a failed
batch survives, which is what makes the fallback safe rather than a double-write.

`RETURNING event_id` rather than a row count is what keeps per-record accounting possible: "7 of
10 inserted" does not say *which* 7, and the duplicate metric is built from exactly that.

### TIMESTAMPTZ is microseconds, and Go time is nanoseconds

Found by an integration test failing on what looked like identical timestamps:

```
occurred_at = 2026-09-09 20:49:32.366048 -0400, want 2026-09-10 00:49:32.3660482 +0000
```

Same instant, one digit shorter. `TIMESTAMPTZ` stores microseconds and truncates the nanosecond
tail. It costs nothing at this system's millisecond scale, and `EndToEndLatency` is measured from
the in-memory event rather than the stored row, so nothing observable changes. Worth asserting
deliberately rather than rediscovering later as a flaky test.

### What the tests cover now, and why that is the point

The suite went from 12 test functions over pure helpers to 72 (146 cases) over the parts that can
actually be wrong. Every reliability claim in this file was previously backed by a manual chaos
script and a reading of `kafka-consumer-groups.sh --describe` — a fine way to *discover* the Day 4
offset bug and a poor way to keep it fixed. A refactor that broke commit ordering would have gone
green.

Making the poll loop testable was most of the work: `processPartition` now takes a slice of
records and returns the last committable one plus whether the partition must stall, with the store
and the dead-letter queue behind interfaces. The invariant is expressible in one sentence and now
in one assertion.

The tests that matter state an invariant rather than an implementation:

- a failure at offset 22 commits offset 21 and no further — the Day 4 bug, now a test
- a dead-lettered record does *not* block its partition, because it is accounted for
- a constraint violation is attempted exactly once; a connection failure is attempted more
- six deliveries of three records produce three rows and zero dead letters
- a cancelled work context commits nothing, because shutdown must not claim work it did not do
- a failed batch leaves no partial rows, which is what makes the per-record fallback safe

The store tests run against real PostgreSQL, gated on `POSTGRES_TEST_DSN` so `make test` stays
hermetic. Some claims only Postgres can answer: whether `ON CONFLICT` really makes redelivery a
no-op, and whether keyset pagination is genuinely stable while rows are inserted underneath it
(`TestListOrdersPaginationIsStableUnderInserts` inserts a row *between* every page, which is
exactly what OFFSET cannot survive).

### A client's retry was a second order

The `UNIQUE (event_id)` constraint makes Kafka *redelivering* an event a no-op. It does nothing
about a client that times out and re-POSTs — that retry minted fresh identifiers and became a
second, genuine order. The gap had been stated for non-database side effects (Day 4) but not
noticed at ingress, where it is the same gap.

`Idempotency-Key` closes it with no new storage and no new code path: both `order_id` and
`event_id` are derived from the key as UUIDv5, so a client's retry produces an event the worker
has *already* deduplicated on. Measured end to end — two POSTs with the same key produce one row,
and the worker logs `duplicate event ignored` for the second, which is the existing machinery
doing the work rather than a new mechanism bolted alongside it.

First write wins, and nothing here records what the first body was, so a client reusing a key with
a different order gets the original back. Detecting that misuse needs the request stored alongside
the key, which is a real design with real costs and is not what this buys.

### The healthcheck problem distroless creates

`/healthz` existed for eight days and no compose service used it, so nothing ever acted on it.
Adding a healthcheck ran straight into the images being distroless: no shell, no curl, nothing to
probe with.

The binary already in the image can do it. `/service -healthcheck http://127.0.0.1:8080/readyz`
exits 0 or 1, which is exactly the contract `HEALTHCHECK` wants, and costs no attack surface — the
alternative is a fatter base image carrying curl solely to ask a question the binary can already
answer.

Liveness and readiness had to be split to make this safe. `/healthz` must **not** depend on Kafka:
restarting a healthy API because the broker is down turns a partial outage into a total one.
`/readyz` must: an API that cannot reach Kafka cannot accept a write. It deliberately says nothing
about the query service, whose absence costs reads and not writes.
