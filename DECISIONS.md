# Design decisions

Each entry answers a question an interviewer can reasonably ask. If a decision cannot be
defended in a paragraph, it should not be in the system.

## Kafka in KRaft mode, single broker, replication factor 1

KRaft removes the ZooKeeper dependency, so the local stack is one container instead of two.
Replication factor 1 means **any broker loss is data loss** — acceptable because this is a
lab for studying consumer-group and failure behaviour, not an availability exercise. A real
deployment would run 3 brokers with RF=3 and `min.insync.replicas=2`.

## Auto-topic-creation disabled; topics created explicitly with 3 partitions

Partition count is the parallelism ceiling for a consumer group, so it must be a deliberate,
visible number rather than a broker default that silently appears on first produce. Day 10
depends on being able to change it and watch the ceiling move.

## franz-go as the Kafka client

Pure Go (no cgo/librdkafka toolchain on Windows), actively maintained, and it exposes
manual offset commits, rebalance callbacks and per-partition control directly — all three
are things this project deliberately wants to observe rather than have hidden.

## pgx over database/sql

Native PostgreSQL protocol driver with a real connection pool. Day 11 needs pool exhaustion
to be an observable, tunable failure mode.

## Host port 5433 for PostgreSQL

A native PostgreSQL service already listens on 5432 on the development machine. Publishing
the container on 5432 appeared to succeed while host clients silently reached the *native*
server instead — an authentication failure that looked like a config bug. See FINDINGS.md.

## `POST /orders` returns 202 Accepted, not 201 Created

201 would be a lie. At the moment the API answers, nothing has been created: the event is
durably in Kafka and no row exists yet. 202 states exactly that — the request was accepted
for processing — and it makes the read-after-write window an explicit part of the contract
rather than a bug report. A client that immediately GETs the returned `order_id` may
legitimately receive 404 for a short interval; that is the cost of the async write path, and
buying it back would mean writing synchronously to Postgres and giving up the queue.

## The API produces synchronously and fails the request if the broker does not ack

`ProduceSync` with `RequiredAcks(AllISR)` sits on the request path deliberately. The entire
meaning of a 202 is "Kafka has this". Fire-and-forget would make the endpoint faster and turn
every broker hiccup into silently lost orders — a fast lie. On produce failure the client
gets 503 and can retry.

## Partition key = `order_id`

Kafka guarantees ordering only *within* a partition. Keying on `order_id` puts every event
for one order on one partition, so those events are processed in order by exactly one
consumer in the group — which is also what makes per-order state safe without locking.

A random key (or no key) would round-robin events across partitions, and two events for the
same order could then be processed concurrently by two workers in any order: an
`order.cancelled` could be applied before the `order.created` it refers to.

Keying on `customer_id` instead would be worse in a different way: a single large customer
would concentrate on one partition and become a hot spot that adding workers cannot relieve.

## `event_id` and `order_id` are separate fields

`order_id` identifies the business entity; `event_id` identifies one specific event. A Kafka
redelivery carries the *same* `event_id`, which is what makes Day 4's `ON CONFLICT (event_id)
DO NOTHING` correct — deduplicating on `order_id` would instead discard legitimate later
events about the same order.

## `DisallowUnknownFields` on request decoding

An unrecognised field means the client and the API disagree about the contract. Silently
ignoring it is how a typo'd `total_cents` gets discarded and a bug takes a week to find.

## One consumer group, `order-processors`, not one group per worker

Members of a *group* divide the partitions between them; separate groups each receive every
message. Workers must share a group or every order would be processed N times. Day 4's
idempotent write protects against duplicate *delivery*, not against a misconfigured topology.

## The worker logs a heartbeat even when it owns nothing

A worker that owns no partitions is silent, and silence is indistinguishable from a crash in
a log file. The 15-second heartbeat reports `owns` and `partition_count`, so "idle because
the group had more members than partitions" is legible at a glance — the exact condition the
Day 10 scaling experiment runs into.

## At-least-once delivery with an idempotent write, not exactly-once

Kafka can offer effectively-once processing through transactions: the consumer's offset commit
and its output are written in one transaction, so a partially-processed batch is rolled back.
It is real, and it is the wrong tool here.

Kafka transactions are exactly-once *within Kafka* — a read-process-write loop where the
output is another Kafka topic. This system's output is a row in PostgreSQL, which is outside
that transaction. Making the pair atomic requires either a distributed transaction across
Kafka and Postgres, or the transactional-outbox pattern: substantial machinery, and a
permanent tax on every future change.

The alternative costs one database constraint. Delivery is at-least-once, the same event may
arrive several times, and `UNIQUE (event_id)` with `ON CONFLICT DO NOTHING` makes the second
and subsequent arrivals no-ops. Correctness is enforced by the database rather than by
application logic that a future refactor can bypass, and the failure mode is a wasted round
trip instead of a duplicated order.

**What it costs:** duplicate *processing* still happens, so any side effect that is not a
database write — sending an email, charging a card, calling a third party — is not covered by
this and needs its own idempotency key. Saying that out loud is the difference between
understanding the guarantee and reciting it.

## Offsets are committed only after the write succeeds

Auto-commit advances offsets on a timer, with no relationship to whether the work was done.
A worker that crashes after an auto-commit but before its write loses those messages
permanently — the group resumes past records that were never persisted.

So: `DisableAutoCommit`, process, then `CommitRecords` for exactly the records that reached
Postgres. Committing after the write means a crash between the two causes *redelivery*,
which the idempotent write absorbs. That asymmetry is the whole design — err toward doing
work twice, never toward skipping it.

## A failed write pauses its partition

Stopping at the first failed record in a poll is *not* sufficient. The client's fetch position
has already moved past that record, so the next poll delivers the records behind it; those
succeed, and committing them advances the offset past the failure — losing it silently. This
was measured, not theorised: a failure at offset 20 ended with the group committed at 22 and
lag 0.

The fix is `PauseFetchPartitions` on the affected partition. Nothing behind the failure is
fetched, so nothing behind it can be committed, and the uncommitted record is redelivered on
the next rebalance or restart. Only the affected partition stalls; the other two keep
processing. Day 6 replaces the indefinite pause with bounded retries and a dead-letter queue.

## `ON CONFLICT (event_id)`, with a named target rather than a bare `DO NOTHING`

A bare `ON CONFLICT DO NOTHING` swallows a violation of *any* unique constraint. That would
also silently discard a genuine anomaly — a different `event_id` carrying an `order_id` that
already exists, which is a data conflict, not a redelivery.

Naming the target means only redelivery is treated as benign; anything else surfaces as an
error, stalls its partition, and gets seen. Verified: an event with a new `event_id` and an
existing `order_id` raises `duplicate key value violates unique constraint "orders_pkey"`
rather than being quietly dropped.

## Schema applied from an embedded `schema.sql` at startup

`CREATE TABLE IF NOT EXISTS` is idempotent, so every worker can run it and the Nth start is a
no-op — no migration container, no ordering requirement between services. It cannot express a
change to an existing column, which is exactly why real systems use versioned migrations
(goose, golang-migrate). For a fixed one-table schema in a 13-day lab, the extra tool would be
ceremony.

## Connection pool capped at 10

Deliberately modest. Day 11 wants pool exhaustion to be a reachable, observable failure mode
rather than something hidden behind a default large enough to never bite locally.
