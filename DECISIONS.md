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

## Two contexts in the worker: one for fetching, one for work

`signal.NotifyContext` gives a context cancelled on SIGTERM. Passing that context to the
database calls is the obvious thing to do and it is wrong: on shutdown it aborts exactly the
in-flight writes the drain exists to finish, so every clean stop produces redelivery.

So `fetchCtx` is cancelled by the signal and stops the worker asking Kafka for more records,
while `workCtx` is independent and lets records already in hand be written and committed.
The signal means *stop taking on new work*, not *drop the work you have*.

## Shutdown order

1. **Stop fetching** — the signal cancels `fetchCtx`, so no new records are pulled.
2. **Finish in-flight work and commit its offsets** — under `workCtx`, which the signal does
   not cancel.
3. **Leave the group** — `client.Close()` blocks until `LeaveGroup` is acknowledged, so the
   remaining members rebalance immediately instead of waiting out the 45-second session
   timeout.
4. **Close the database pool.**

Swapping 2 and 3 is the tempting mistake: leaving the group first hands the partitions to
another member while this one still has uncommitted writes in flight, so the same records are
processed twice. Idempotency would absorb it, but it is wasted work caused by an avoidable
ordering bug.

## A drain deadline with a hard exit, and a second signal that skips the wait

A shutdown that hangs is worse than an abrupt one: the orchestrator kills it anyway, just
later and less predictably. The worker gives itself 15 seconds, then logs and exits non-zero.
A second SIGTERM/SIGINT exits immediately — an operator sending it twice has already decided
not to wait.

`stop_grace_period: 30s` in Compose is set *above* the worker's own 15s deadline. If the
platform's grace period were the shorter of the two, the drain would be cut off by SIGKILL and
the deadline would never be the thing that decides. This pair has to be chosen together;
Kubernetes has exactly the same relationship with `terminationGracePeriodSeconds`.

## What graceful shutdown actually buys

Not "no data loss" — at-least-once delivery plus an idempotent write already guarantee that,
and it was confirmed by a SIGKILL run that also finished with 4000 accepted and 4000 rows.

What it buys, measured: recovery in **1.4s instead of 43s**, because `LeaveGroup` triggers an
immediate rebalance rather than the group waiting out `session.timeout.ms` with the dead
member's partitions stranded and their lag climbing. And it removes a duplicate-processing
race — the window between a successful write and its offset commit — instead of relying on a
crash to miss it.

## Services run in containers now

Not scope creep for its own sake — three things need it. `docker stop` is the only realistic
way to deliver a genuine SIGTERM (Windows has no such signal, so the shutdown path could not
otherwise be tested at all). `docker compose up --scale worker=N` is how Day 10's scaling
matrix gets run. And Day 12 wants one-command startup for a stranger cloning the repo.

The image is multi-stage onto `distroless/static` and runs as non-root: no shell and no
package manager in the final image.

## Retry classification: environment yes, data no

`store.IsRetryable` maps PostgreSQL SQLSTATE classes to a single decision:

| Class | Meaning | Retry? |
|---|---|---|
| 08 | connection exception | yes |
| 53 | insufficient resources | yes |
| 57 | operator intervention (shutdown) | yes |
| 58 | system error | yes |
| 40 | serialization failure, deadlock | yes |
| 23 | integrity constraint violation | **no** |
| 22 | data exception (bad UUID, bad number) | **no** |
| 42 | syntax / access rule violation | **no** |

The rule is: retry anything about the *environment*, never anything about the *data* or our own
SQL. Getting this backwards is expensive in both directions — retrying a constraint violation
burns the whole budget and then dead-letters a record that was never going to succeed, while
not retrying a dropped connection dead-letters thousands of perfectly good orders because the
database restarted.

A stopped database mostly does *not* arrive as a `PgError` at all — it surfaces as a dial or
DNS failure below the protocol — so anything unrecognised defaults to retryable.

## Full jitter, not plain exponential backoff

The delay is `rand(0, min(cap, base·2^n))`, not `min(cap, base·2^n)`.

Jitter is the whole point rather than a refinement. Every worker that fails at the same
instant — which is exactly what a database outage produces — would otherwise retry at the same
instant, and the recovering database gets hit by the entire fleet in lockstep, at the precise
moment it is least able to cope. Full jitter spreads them, and is what AWS's "Exponential
Backoff and Jitter" measured as the best of the simple strategies.

## The retry budget is elapsed time, not an attempt count

`MaxElapsed: 45s` bounds the whole sequence; the attempt count falls out of it.

An attempt count says nothing about how long the worker will be stuck, and "how long will this
block" is the only question that matters to the consumer group: a worker inside a retry loop
cannot respond to a rebalance. The budget is therefore set *below* franz-go's 60s rebalance
timeout. Retrying past that point would trigger a rebalance storm on top of the outage that
caused it — a self-inflicted second failure.

## Two dead-letter reasons, not one

`poison` means the record will never succeed: malformed JSON, a constraint violation, a value
the schema rejects. `retries_exhausted` means it probably would have succeeded, but the
environment stayed broken longer than the budget allowed.

These call for completely different responses — fix the producer versus replay the batch — and
a dead-letter queue that cannot tell them apart is much less useful than one that can.

## The DLQ preserves the original bytes; the diagnosis rides in headers

The dead-letter record carries the source key and value unchanged, with `dlq_reason`,
`dlq_error`, `dlq_attempts`, `dlq_source_topic/partition/offset` and `dlq_failed_at` as Kafka
headers.

Wrapping the payload in an envelope would mean the dead-letter topic has a different shape
from the source topic, so replaying it needs a special consumer that no one writes. That is how
dead-letter queues quietly become write-only. Byte-identical records can be replayed by the
same worker after the cause is fixed.

## Why gRPC is here at all

The one-sentence justification, since the plan says to delete it if there isn't one:

**Writes go `api → Kafka → worker → Postgres` asynchronously; reads go `api → gRPC → query →
Postgres` synchronously. Those are different workloads with different failure modes, and the
gRPC boundary is where they are separated.**

Concretely, the read path can be scaled, tuned, rate-limited and taken down without touching
ingest — a read-heavy hour adds `query` replicas and never risks accepting orders. Read
failures are also *visible* rather than absorbed: the API translates gRPC codes to HTTP ones,
so a read timeout is a 504 rather than a silent empty result.

gRPC specifically, rather than a second HTTP service: this is an internal, schema-first,
service-to-service call. The `.proto` is the contract, both sides are generated from it, and a
field renamed on one side breaks compilation rather than becoming a `null` in production.

## Keyset pagination, not OFFSET

`ListOrders` pages on `(processed_at, order_id) < (cursor)` rather than `OFFSET n`.

`OFFSET` makes the database produce and discard every skipped row, so page 1000 is far slower
than page 1. Worse, it is *wrong* on a table that is being written to: rows inserted while a
client pages will shift every later page, so rows get skipped or returned twice. This table is
written to constantly, so that is not theoretical.

The cursor anchors each page to a fixed point instead. Verified with `EXPLAIN (ANALYZE)`:

```
Limit  (actual time=0.011..0.021 rows=51)
  ->  Index Only Scan using orders_processed_at_order_id_idx
        Index Cond: (ROW(processed_at, order_id) < ROW(now(), '...'::uuid))
```

An index-only scan with the row comparison pushed into the index condition — no sort, no heap
scan of skipped rows, and the cost is the same on page 1000 as on page 1. The row-value syntax
`(a, b) < ($1, $2)` is what makes this a single seek; the equivalent
`a < $1 OR (a = $1 AND b < $2)` is logically identical and does *not* reliably produce this plan.

The token is base64url-encoded JSON, so it is opaque by contract and its shape can change
without breaking clients holding an old one.

## An extra row instead of a COUNT

The page query fetches `pageSize + 1` rows and reports a next-page token only if the extra row
appears. A `COUNT(*)` would scan the whole matching set to answer a question the client did not
ask, and would be out of date by the time it returned.

## The API dials the query service lazily

`grpc.NewClient` does not block on the target being reachable. The API therefore starts and
keeps accepting orders even while the read path is down — coupling ingress availability to a
downstream read service would be a self-inflicted outage. A read attempted during that window
gets a 503 mapped from `codes.Unavailable`, which is the truth.

## gRPC reflection is enabled

`grpcurl` and similar tools can call the service without being handed the `.proto`. On an
internal service that is a real operability win. On a public endpoint it would be an
information leak, and this is the reason it would be turned off there.

## NotFound is normal traffic on the read path

`GET /orders/{id}` returning 404 immediately after a 202 is correct behaviour, not a bug: the
write path is asynchronous. Measured at idle, the gap between `occurred_at` and `processed_at`
is around **15ms**, which is smaller than the round trip needed to observe it. Under load the
window widens to whatever the consumer lag is — which is precisely why lag is the metric that
matters (Day 8).

## Six metrics, chosen to answer specific questions

| Metric | Type | Question it answers |
|---|---|---|
| `consumer_lag` / `kafka_consumergroup_lag` | gauge | Am I keeping up? |
| `order_processing_duration_seconds` | histogram | How long does one write take? |
| `order_end_to_end_latency_seconds` | histogram | What does a user actually wait? |
| `orders_processed_total{status}` | counter | What is happening, and how often? |
| `retries_total` / `dlq_total{reason}` | counter | Is it failing, and is it giving up? |
| `consumer_partitions_owned` | gauge | Is this worker doing anything at all? |

A metric nobody queries is a metric nobody maintains, so the set is deliberately small and each
one has a question attached.

## Histograms, not averages, and custom buckets

The question is "what is my p99", and an average hides exactly the tail that matters: a
p50 of 0.6ms with a p99 of 2.4ms and one with a p99 of 2.4s have the same average.

The buckets are hand-chosen around the measured range (1ms to 30s) rather than left at the
Prometheus defaults, which start at 5ms. With defaults, a system whose p50 is 0.6ms would land
almost every observation in the first bucket and `histogram_quantile` would interpolate inside
it — returning a confident, precise, meaningless number. Buckets have to be chosen after you
know roughly what you are measuring, which is why this came after Day 7 rather than before.

## Lag is exported twice, on purpose

The worker exports `consumer_lag` and `kafka-exporter` exports `kafka_consumergroup_lag`. That
is redundant and both are kept, because they fail differently.

The worker's gauge disappears when workers do — and it does not disappear cleanly, it goes
*stale at its last value*, so Prometheus keeps serving a healthy 0 for five minutes while the
backlog grows. Measured, not theorised; see FINDINGS.md. The exporter asks the broker and does
not care whether any consumer is alive.

**A health signal must not be produced by the thing whose health it reports.** The worker gauge
is still worth keeping for the different question it answers — what *this* member sees, which
is what makes an idle or stalled worker visible.

## Metrics are served on their own port

Port 2112, separate from the service's own traffic. Metrics need to stay scrapeable precisely
when the main listener is saturated, and they should not be exposed wherever the service is —
`/metrics` leaks internal structure and is not something to publish alongside a public API.

## Prometheus discovers targets from Docker, not from a static list

`--scale worker=N` means worker addresses do not exist until the containers do. `dns_sd_configs`
on the Compose service name is the obvious approach and does not work — Docker's embedded DNS
does not answer Prometheus's fully-qualified query. `docker_sd_configs` does, and new workers
appear within one refresh interval.

The daemon is reached through `docker-socket-proxy` with only `CONTAINERS` and `NETWORKS`
enabled. Mounting the socket into Prometheus with `:ro` would *look* equivalent and is not: the
flag applies to the socket file, not to the API, so anything that can reach it can still create
a privileged container.

## No Grafana

Prometheus and PromQL answer the questions this project asks, and its expression browser draws
a graph when one is needed. Grafana would add a container, a provisioning directory and a set
of dashboard JSON files, and would not answer a single question that `histogram_quantile` and
`deriv` do not already answer. It stays on the no-list unless the results in FINDINGS.md turn
out to need a picture.

## The load generator schedules send times; it does not loop

A pool of N goroutines each doing "send, await response, send again" produces a rate of
*N / latency*. The moment the system slows down, the generator slows down with it — so the
offered load silently drops, the queue never builds, and the recorded latencies look fine.
That is coordinated omission, and it is the easiest way to publish a benchmark that is
confidently wrong.

Request start times are therefore fixed in advance from the target rate: request *i* is due at
`start + i·interval`, whatever happened to requests 0..i-1.

## Latency is measured from the time a request was due, not from when it was sent

The corollary, and the half that is usually forgotten. When the in-flight limit is reached, a
request waits for a permit. Timing it from the moment it was finally sent would discard exactly
that wait — which is real, is caused by the system being slow, and is what a client would
actually experience.

Measuring from the due time means saturation shows up as growing latency rather than as a
quietly reduced send rate.

## The generator reports when *it* is the bottleneck

`scheduler_behind` counts ticks that were already more than one interval late before any
request was dispatched. That is a statement about the load generator's own scheduling, not
about the system under test, and conflating the two turns a laptop's limits into a finding
about the architecture.

It fires above roughly 1000/s on this host, where the per-request interval (200µs at 5000/s)
falls below Windows timer granularity. Aggregate throughput stays accurate — 4999.9/s of a
5000/s target — but per-request timing precision does not, so anything sub-millisecond at those
rates should be read as approximate.

## End-to-end latency is read from the data, not instrumented

Every row carries `occurred_at` (when the API accepted it) and `processed_at` (when the worker
persisted it), so the true pipeline latency is a SQL query — `percentile_cont` over
`processed_at - occurred_at` — rather than something the generator has to trace.

This is why both timestamps are on the row. It costs 16 bytes and removes any need for
distributed tracing to answer the project's central question. It is also immune to clock skew
between the client and the services, which a client-side measurement is not.

## The generator waits for the pipeline to drain before measuring

Percentiles taken while a backlog is still draining describe only the orders that happened to
be fast already. The harness polls until the persisted count reaches the accepted count (or the
drain deadline expires) and reports any shortfall as `MISSING`, so an incomplete measurement is
labelled rather than quietly optimistic.

## Throughput is measured while a backlog exists

The scaling runs deliberately overload the pipeline — 5000/s offered into a system that cannot
persist at that rate — and measure how fast it drains.

That is the only condition under which the workers run flat out, so the persisted rate equals
*capacity*. Offering a rate the system comfortably keeps up with measures the load generator
instead: every configuration would report the offered rate and the table would show a flat line
regardless of how many workers were running.

Capacity is read as `max_over_time(sum(rate(orders_processed_total{status="persisted"}[30s])))`
over the run, and peak backlog as the same over `kafka_consumergroup_lag`.

## Each configuration starts from a clean topic

Every run deletes and recreates the topic and truncates the table. Otherwise the second
configuration begins with the first one's committed offsets and a partially warm page cache,
and the numbers drift in a direction that flatters whichever run happened to be later.

Workers are scaled to zero before the topic is touched — deleting a topic under a live consumer
group produces a burst of errors that has nothing to do with the experiment — and the script
then waits 20 seconds for the group to settle, so no run measures a rebalance.

## "Broken" is defined before the experiment, not after

Without a definition fixed in advance, any result can be narrated as breaking or not breaking
once it is on the screen. For the ramp, the system is broken when:

- the API rejects orders (503) or the client sees connection failures;
- accepted orders fail to reach the database within the drain window;
- accept latency degrades non-linearly rather than proportionally.

**A growing backlog is explicitly not breaking.** It is the queue doing precisely what it was
introduced to do, and telling those two apart is most of what this system exists to demonstrate.
A design that absorbs a 5× overload as 18 seconds of lag and zero losses has not failed; one
that returns 503 at the same load has.

## A paused partition is resumed by a separate goroutine, after a broker ping

The pause from Day 4 is only ever meant to last as long as the outage that caused it, and for
eight days nothing ended it. Three details make the resume correct rather than merely present.

**It ticks rather than reacting to an event.** There is no signal that says "Kafka is back";
the only way to find out is to ask.

**It runs on its own goroutine.** `PollFetches` blocks, so if every partition is paused the poll
loop is parked with nothing to fetch and can never resume anything. Putting the resume in the
loop would work in every case except the one that matters.

**It pings the broker before resuming.** Resuming into a continuing outage means the record
fails again instantly and the partition is re-paused: churn, log noise, and no progress. One
metadata round trip buys the difference between recovering and thrashing.

The alternative — exit the process and let the orchestrator restart it — is simpler and would
also work. It is worse because it throws away every other partition's in-flight work to fix one,
and because a crash loop is a blunter signal than a gauge that says exactly how many partitions
are stuck.

## `IsRetryable` and `IsCallerError` are separate questions

"Should I try this again?" and "whose fault is this?" have different answers, and one classifier
answering both produced a real bug: a malformed page token reached the client as a 500.

`IsRetryable` has to end in a permissive catch-all, because on the write path an unrecognised
error is most likely a transport failure and retrying is the safe default. That default is
exactly wrong for status mapping, where an unrecognised error is *not* evidence that the caller
was at fault.

So they are two functions. The invariant tying them together is asserted rather than assumed:
every caller error must also be non-retryable. The converse deliberately does not hold — plenty
of errors are permanent *and* entirely our own doing, and a classifier that cannot say so blames
the caller for our bugs.

## Batching is a knob, defaulted to off

Day 10 measured a single worker at ~1828 ev/s and reasoned that the ceiling was one database
round trip per record. Day 13 measured batching at 41082 ev/s with a batch of 200 — a 21× move
on the same database, which confirms the diagnosis.

It still ships defaulted to 1. Every number in FINDINGS.md was measured against one insert per
record, and changing the default would silently invalidate the whole record. A default of 1
means the documented measurements stay reproducible and the improvement is an experiment
somebody can run (`make batch-matrix`) rather than a claim they have to take on trust.

There is also a real trade to make deliberately: a record waits for its whole batch, so
per-record latency gets worse as throughput gets better. Which side of that trade is right
depends on the workload, which is an argument for a knob rather than for a new default.

## A failed batch falls back to one record at a time

A multi-row `INSERT` is all-or-nothing, so a single poison record fails the whole chunk. Left
there, batching would trade the poison isolation of Day 6 for throughput.

On any batch failure the chunk is replayed record by record. The offender is isolated and
dead-lettered, its neighbours persist, and the partition keeps moving — the Day 6 behaviour
exactly, reached by a slower path only when something has already gone wrong.

This depends on batches being genuinely atomic: if a failed batch could leave partial rows, the
fallback would write them twice. That is asserted against real Postgres rather than assumed.

`RETURNING event_id` is what keeps per-record accounting alive through a batch. A count of 7 out
of 10 does not say *which* 7, and the persisted-versus-duplicate metric is built from precisely
that distinction.

## `Idempotency-Key` derives the identifiers instead of storing them

`UNIQUE (event_id)` makes Kafka redelivering an event a no-op. It does nothing about a client
that times out and retries its POST — that used to mint fresh ids and become a second real order.

The obvious fix is a table of seen keys, which means a database write on the ingress path. That
is exactly what this architecture exists to avoid: the API answers 202 without touching Postgres,
and adding a lookup would put the write path back where it started.

Deriving `order_id` and `event_id` from the key as UUIDv5 needs no storage at all. The retry
produces an event the worker has *already* deduplicated on, so the guarantee is enforced by the
same database constraint as everything else rather than by a second mechanism beside it.

**What it costs:** first write wins. Nothing records what the first body was, so a client that
reuses a key with a different order gets the original back rather than an error. Stripe returns a
422 for that, and doing the same here needs the request stored alongside the key — the storage
this design was chosen to avoid. The honest description is that this makes retries safe, not that
it validates key reuse.

## Services probe themselves; the image gets no curl

The service images are distroless — no shell, no package manager, nothing to run a healthcheck
with. The usual fix is a base image with curl in it.

The binary is already in the image and can answer the question itself:
`/service -healthcheck http://127.0.0.1:8080/readyz` exits 0 or 1, which is the entire
`HEALTHCHECK` contract. No new attack surface, nothing to keep patched, and the probe is
version-locked to the service by construction.

It parses `os.Args` directly instead of using the `flag` package, because it must run before
anything else in `main`. A container asked whether it is healthy should not first connect to
Kafka and Postgres to find out.

## Liveness and readiness answer different questions

`/healthz` deliberately depends on nothing. If it checked Kafka, a broker outage would have the
orchestrator restart every API instance — turning a degraded write path into no API at all, and
adding a thundering herd of reconnects to an already unwell broker.

`/readyz` deliberately depends on Kafka, because the API's job is to accept writes and it cannot
accept a write the broker will not acknowledge. An instance that cannot reach Kafka should leave
the load balancer rather than serve 503s.

It says nothing about the query service. That asymmetry is the same one the whole system is built
on: the query service being down costs reads, and the API stays useful for writes throughout —
which is why it dials the query service lazily in the first place.

## Configuration is validated at startup, not discovered at use

`config.Load` returns an error and the services exit on it. An empty `KAFKA_BROKERS` used to
surface several seconds into startup as a client error three layers from its cause; a DSN in
`key=value` form failed somewhere inside pgx.

The validation is deliberately about what cannot possibly work — no brokers, a broker with no
port, a dead-letter topic equal to the source topic (which would have the worker consume its own
dead letters in a loop) — rather than about what looks unusual. A configuration validator that
rejects things which would have worked is worse than none.
