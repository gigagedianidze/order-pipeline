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
