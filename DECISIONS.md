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
