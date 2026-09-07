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
