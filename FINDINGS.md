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
