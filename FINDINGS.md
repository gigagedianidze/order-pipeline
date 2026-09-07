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
