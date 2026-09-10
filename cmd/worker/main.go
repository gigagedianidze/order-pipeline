// Command worker consumes order events and persists them idempotently.
//
// The two halves of at-least-once processing live here: the database write is
// idempotent (ON CONFLICT DO NOTHING on a unique event_id), and the offset is
// committed only after that write succeeds. Either half alone is insufficient.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gigagedianidze/order-pipeline/internal/broker"
	"github.com/gigagedianidze/order-pipeline/internal/config"
	"github.com/gigagedianidze/order-pipeline/internal/metrics"
	"github.com/gigagedianidze/order-pipeline/internal/order"
	"github.com/gigagedianidze/order-pipeline/internal/retry"
	"github.com/gigagedianidze/order-pipeline/internal/store"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	// shutdownDeadline bounds draining. Compose gives a container 10s by default
	// before SIGKILL, so the service's own deadline must be set against whatever
	// grace period the orchestrator allows (this stack sets 30s).
	shutdownDeadline = 15 * time.Second

	// resumeInterval is how often a paused partition is reconsidered. A pause
	// means "Kafka was unreachable", which is a condition that ends; checking on
	// a timer costs one metadata round trip and is the difference between a
	// partition that recovers and one that needs a restart.
	resumeInterval = 15 * time.Second
)

// counters is the worker's running tally, reported by the heartbeat and on exit.
type counters struct {
	processed  atomic.Int64
	duplicates atomic.Int64
	retries    atomic.Int64
	dlq        atomic.Int64
	paused     atomic.Int64
	resumed    atomic.Int64
}

// orderStore and deadLetterQueue are the two collaborators the processing logic
// actually needs. They are interfaces so the poll-loop body — where every
// at-least-once invariant in this system lives — can be tested without a broker
// and a database.
type orderStore interface {
	InsertOrder(ctx context.Context, evt order.Event) (bool, error)
	InsertOrders(ctx context.Context, evts []order.Event) (map[string]bool, error)
}

type deadLetterQueue interface {
	Send(ctx context.Context, rec *kgo.Record, reason broker.Reason, cause error, attempts int) error
}

// worker is the processing half of the service, separated from the wiring half
// so it can be constructed in a test.
type worker struct {
	db        orderStore
	dlq       deadLetterQueue
	log       *slog.Logger
	policy    retry.Policy
	batchSize int
	c         *counters
}

func main() {
	cfg, err := config.Load()
	if err != nil {
		// No logger yet: configuration is what tells us how to build one.
		fmt.Fprintf(os.Stderr, "invalid configuration: %v\n", err)
		os.Exit(1)
	}
	log := cfg.Logger("worker").With("worker_id", workerID())

	// Two contexts, deliberately.
	//
	// fetchCtx is cancelled the moment a signal arrives: it stops the worker
	// asking Kafka for more records. workCtx is NOT cancelled by the signal, so
	// records already in hand can still be written and committed — cancelling the
	// database context on SIGTERM would abort in-flight writes and guarantee the
	// redelivery that a clean shutdown exists to avoid.
	fetchCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	workCtx, abandonWork := context.WithCancel(context.Background())
	defer abandonWork()

	db, err := store.New(workCtx, cfg.PostgresDSN, cfg.PostgresMaxConns)
	if err != nil {
		log.Error("connect postgres", "err", err)
		os.Exit(1)
	}
	if err := db.Migrate(workCtx); err != nil {
		log.Error("migrate", "err", err)
		os.Exit(1)
	}

	client, owned, err := broker.NewConsumerGroup(broker.ConsumerOptions{
		Brokers:      cfg.KafkaBrokers,
		Topic:        cfg.KafkaTopic,
		GroupID:      cfg.KafkaGroupID,
		Log:          log,
		ManualCommit: true,
	})
	if err != nil {
		log.Error("start consumer", "err", err)
		os.Exit(1)
	}

	dlq, err := broker.NewDLQ(cfg.KafkaBrokers, cfg.KafkaDLQTopic, log)
	if err != nil {
		log.Error("start dlq producer", "err", err)
		os.Exit(1)
	}

	log.Info("joining group",
		"group", cfg.KafkaGroupID, "topic", cfg.KafkaTopic, "batch_size", cfg.WorkerBatchSize)

	var c counters
	w := &worker{
		db:        db,
		dlq:       dlq,
		log:       log,
		policy:    retry.DefaultPolicy(),
		batchSize: cfg.WorkerBatchSize,
		c:         &c,
	}
	paused := newPausedSet()

	go heartbeat(fetchCtx, log, owned, &c, paused)
	go metrics.Serve(fetchCtx, cfg.MetricsAddr, log)
	go pollLag(fetchCtx, client, cfg.KafkaGroupID, owned, log)
	go resumePaused(fetchCtx, client, cfg.KafkaTopic, dlq, paused, log, &c)

	// A shutdown that hangs is worse than one that is abrupt: the container is
	// killed anyway, but later and less predictably. Once the signal arrives the
	// worker has shutdownDeadline to finish, then exits hard.
	go watchdog(fetchCtx, log, abandonWork)

	for {
		fetches := client.PollFetches(fetchCtx)
		if fetches.IsClientClosed() {
			break
		}
		// A signal with nothing in hand means there is no in-flight work to
		// finish. With records in hand, fall through and process them first.
		if fetchCtx.Err() != nil && fetches.NumRecords() == 0 {
			break
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			if !errors.Is(err, context.Canceled) {
				log.Error("fetch error", "topic", topic, "partition", partition, "err", err)
			}
		})

		// Committable holds, per partition, the last record whose write
		// succeeded. franz-go commits the offset *after* the record it is given,
		// so committing this set means "everything up to here is durable".
		var committable []*kgo.Record
		var blocked []int32

		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			last, stall := w.processPartition(workCtx, p.Records)
			if last != nil {
				committable = append(committable, last)
			}
			if stall {
				// Retries and the dead-letter queue have both failed, so a record
				// can neither be persisted nor parked. Stop the partition.
				//
				// Stopping alone would not be enough: the client's fetch position
				// has already moved past that record, so the next poll would
				// deliver the records behind it, succeed, and commit an offset
				// beyond the failure — silently losing it. Measured before the
				// pause existed: a failed record at offset 20 ended with the
				// group committed at 22 and lag 0.
				blocked = append(blocked, p.Partition)
			}
		})

		if len(blocked) > 0 {
			client.PauseFetchPartitions(map[string][]int32{cfg.KafkaTopic: blocked})
			paused.add(blocked)
			c.paused.Add(int64(len(blocked)))
			log.Warn("partition paused: could not persist and could not dead-letter",
				"partitions", blocked,
				"effect", "no further records from these partitions",
				"recheck_in", resumeInterval)
		}

		if len(committable) == 0 {
			if fetchCtx.Err() != nil {
				break
			}
			continue
		}
		// Commit last, and only for records that are already in the database.
		// Committing before the write is what loses messages on a crash.
		if err := client.CommitRecords(workCtx, committable...); err != nil && workCtx.Err() == nil {
			// The records are written but the offsets are not advanced, so they
			// will be redelivered. Idempotency makes that harmless.
			log.Error("commit offsets", "err", err)
		}
	}

	// Shutdown order matters and is the point of the day:
	//   1. stop fetching        (fetchCtx cancelled by the signal, above)
	//   2. finish in-flight work and commit its offsets (the loop above)
	//   3. leave the group      — Close() sends LeaveGroup, so the remaining
	//                             members rebalance immediately instead of
	//                             waiting out the 45s session timeout
	//   4. close the producers and the database pool
	//
	// Reversing 2 and 3 would hand the partitions to another member while this
	// one still had uncommitted writes in progress: duplicate work at best.
	log.Info("draining complete, leaving group",
		"processed_total", c.processed.Load(),
		"duplicates_total", c.duplicates.Load())

	client.Close() // blocks until LeaveGroup is acknowledged
	dlq.Close()
	db.Close()

	log.Info("stopped cleanly",
		"processed_total", c.processed.Load(),
		"duplicates_total", c.duplicates.Load(),
		"retries_total", c.retries.Load(),
		"dlq_total", c.dlq.Load())
}

// processPartition handles one partition's records in offset order.
//
// It returns the last record whose outcome is durable — persisted, recognised as
// a duplicate, or parked in the dead-letter queue — and whether the partition
// must be paused. Returning the *last* durable record rather than a list is what
// makes the commit safe: franz-go commits the offset after the record it is
// given, so everything up to it is accounted for.
//
// It stops at the first record it cannot account for. Nothing behind an
// unaccounted record may be committed, so there is no point processing it.
func (w *worker) processPartition(ctx context.Context, recs []*kgo.Record) (last *kgo.Record, blocked bool) {
	for start := 0; start < len(recs); {
		if ctx.Err() != nil {
			return last, false // deadline exceeded; the watchdog is about to exit
		}
		end := min(start+w.batchSize, len(recs))
		chunk := recs[start:end]

		done, stall := w.processChunk(ctx, chunk)
		if done != nil {
			last = done
		}
		if stall {
			return last, true
		}
		start = end
	}
	return last, false
}

// processChunk persists up to batchSize records in one round trip, falling back
// to one record at a time if the batch cannot be written as a unit.
//
// The fallback is not a nicety. A batch is all-or-nothing, so a single poison
// record inside it would take the whole batch down with it; replaying the chunk
// record by record isolates the offender and lets its neighbours through. It is
// also what preserves the meaning of the named conflict target: a genuine
// order_id collision still surfaces as an error against exactly one record,
// rather than being swallowed the way a bare ON CONFLICT DO NOTHING would.
func (w *worker) processChunk(ctx context.Context, chunk []*kgo.Record) (last *kgo.Record, blocked bool) {
	if len(chunk) == 1 {
		if !w.handle(ctx, chunk[0]) {
			return nil, true
		}
		return chunk[0], false
	}

	evts := make([]order.Event, 0, len(chunk))
	for _, rec := range chunk {
		var evt order.Event
		if err := json.Unmarshal(rec.Value, &evt); err != nil {
			return w.oneByOne(ctx, chunk) // a poison record; isolate it
		}
		evts = append(evts, evt)
	}

	started := time.Now()
	attempts := 0
	var inserted map[string]bool
	err := retry.Do(ctx, w.policy, store.IsRetryable, w.logRetry(chunk[0]), func() error {
		attempts++
		var err error
		inserted, err = w.db.InsertOrders(ctx, evts)
		return err
	})
	if err != nil {
		if ctx.Err() != nil {
			return nil, true // shutting down; leave the offsets uncommitted
		}
		return w.oneByOne(ctx, chunk)
	}

	elapsed := time.Since(started)
	for i, evt := range evts {
		// Every record in the batch genuinely waited for the whole batch, so the
		// batch's elapsed time is each record's honest processing time.
		w.recordOutcome(chunk[i], evt, inserted[evt.EventID], elapsed, attempts)
	}
	return chunk[len(chunk)-1], false
}

// oneByOne replays a chunk record by record after a batch failure.
func (w *worker) oneByOne(ctx context.Context, chunk []*kgo.Record) (last *kgo.Record, blocked bool) {
	for _, rec := range chunk {
		if !w.handle(ctx, rec) {
			return last, true
		}
		last = rec
	}
	return last, false
}

// handle processes one record and reports whether the offset may advance past it.
//
// It returns false only when the worker genuinely cannot make progress — which,
// after retries and the dead-letter queue, means Kafka itself is unreachable.
// Every other outcome, including permanent failure, ends with the record
// accounted for and the partition free to move on.
func (w *worker) handle(ctx context.Context, rec *kgo.Record) bool {
	var evt order.Event
	if err := json.Unmarshal(rec.Value, &evt); err != nil {
		// Poison by definition: no amount of retrying will make this parse.
		return w.deadLetter(ctx, rec, broker.ReasonPoison, err, 0)
	}

	started := time.Now()
	attempts := 0
	err := retry.Do(ctx, w.policy, store.IsRetryable, w.logRetry(rec), func() error {
		attempts++
		inserted, err := w.db.InsertOrder(ctx, evt)
		if err != nil {
			return err
		}
		w.recordOutcome(rec, evt, inserted, time.Since(started), attempts)
		return nil
	})

	if err == nil {
		return true
	}
	if ctx.Err() != nil {
		return false // shutting down; leave the offset uncommitted for redelivery
	}

	// Two different failures, two different stories to tell whoever reads the DLQ.
	// "This record is broken" and "the database was down longer than we were
	// willing to wait" call for completely different responses, and a dead-letter
	// queue that cannot distinguish them is much less useful.
	reason := broker.ReasonRetriesExhausted
	if !store.IsRetryable(err) {
		reason = broker.ReasonPoison
	}
	return w.deadLetter(ctx, rec, reason, err, attempts)
}

// recordOutcome logs and measures one persisted-or-duplicate record.
func (w *worker) recordOutcome(rec *kgo.Record, evt order.Event, inserted bool, elapsed time.Duration, attempts int) {
	if inserted {
		w.c.processed.Add(1)
		metrics.OrdersProcessed.WithLabelValues(metrics.OutcomePersisted).Inc()
		metrics.ProcessingDuration.WithLabelValues(metrics.OutcomePersisted).Observe(elapsed.Seconds())
		// Only measured for genuinely new orders: a duplicate's "latency" would
		// be the age of the original event and would pollute the distribution
		// with irrelevant large values.
		metrics.EndToEndLatency.Observe(time.Since(evt.OccurredAt).Seconds())
		w.log.Info("order persisted",
			"partition", rec.Partition, "offset", rec.Offset,
			"order_id", evt.OrderID, "event_id", evt.EventID,
			"total_cents", evt.TotalCents, "attempts", attempts)
		return
	}
	w.c.duplicates.Add(1)
	metrics.OrdersProcessed.WithLabelValues(metrics.OutcomeDuplicate).Inc()
	metrics.ProcessingDuration.WithLabelValues(metrics.OutcomeDuplicate).Observe(elapsed.Seconds())
	w.log.Info("duplicate event ignored",
		"partition", rec.Partition, "offset", rec.Offset,
		"order_id", evt.OrderID, "event_id", evt.EventID)
}

func (w *worker) logRetry(rec *kgo.Record) func(int, time.Duration, error) {
	return func(attempt int, delay time.Duration, err error) {
		w.c.retries.Add(1)
		metrics.Retries.Inc()
		w.log.Warn("write failed, retrying",
			"attempt", attempt+1,
			"delay", delay,
			"partition", rec.Partition, "offset", rec.Offset,
			"err", err)
	}
}

// deadLetter parks a record and lets the partition continue. If the DLQ write
// itself fails, the record is neither persisted nor parked, so the offset must
// not advance: the caller pauses the partition instead.
func (w *worker) deadLetter(ctx context.Context, rec *kgo.Record,
	reason broker.Reason, cause error, attempts int) bool {

	if err := w.dlq.Send(ctx, rec, reason, cause, attempts); err != nil {
		if ctx.Err() == nil {
			w.log.Error("dead-letter failed, cannot make progress",
				"partition", rec.Partition, "offset", rec.Offset, "err", err)
		}
		return false
	}
	w.c.dlq.Add(1)
	metrics.DeadLettered.WithLabelValues(string(reason)).Inc()
	metrics.OrdersProcessed.WithLabelValues(metrics.OutcomeDeadLetter).Inc()
	return true
}

// pausedSet is the set of partitions the poll loop has stopped fetching from.
// The poll loop adds to it and the resumer goroutine drains it, so it is
// mutex-guarded.
type pausedSet struct {
	mu    sync.Mutex
	parts map[int32]struct{}
}

func newPausedSet() *pausedSet { return &pausedSet{parts: make(map[int32]struct{})} }

func (s *pausedSet) add(parts []int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range parts {
		s.parts[p] = struct{}{}
	}
}

// take empties the set and returns what was in it.
func (s *pausedSet) take() []int32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.parts) == 0 {
		return nil
	}
	out := make([]int32, 0, len(s.parts))
	for p := range s.parts {
		out = append(out, p)
		delete(s.parts, p)
	}
	return out
}

func (s *pausedSet) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.parts)
}

// resumePaused un-pauses partitions once the condition that paused them is gone.
//
// A pause means the dead-letter queue was unreachable, and franz-go keeps a
// paused partition paused across rebalances — so without this the partition
// stalls until the process restarts, invisibly, with the container still
// reporting healthy. The pause is only ever meant to last as long as the outage.
//
// It runs on its own goroutine because PollFetches blocks: if every partition is
// paused the poll loop is parked with nothing to fetch and could never resume
// them itself.
func resumePaused(ctx context.Context, client *kgo.Client, topic string,
	dlq *broker.DLQ, paused *pausedSet, log *slog.Logger, c *counters) {

	ticker := time.NewTicker(resumeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			metrics.PartitionsPaused.Set(float64(paused.count()))
			if paused.count() == 0 {
				continue
			}
			// Ask the broker before resuming rather than resuming blindly on a
			// timer. If Kafka is still down the record fails again immediately
			// and the partition is re-paused, which is churn with nothing to
			// show for it.
			pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := dlq.Ping(pingCtx)
			cancel()
			if err != nil {
				log.Warn("still cannot reach kafka, partitions stay paused",
					"paused", paused.count(), "err", err)
				continue
			}

			parts := paused.take()
			if len(parts) == 0 {
				continue
			}
			client.ResumeFetchPartitions(map[string][]int32{topic: parts})
			c.resumed.Add(int64(len(parts)))
			metrics.PartitionsPaused.Set(float64(paused.count()))
			log.Info("kafka reachable again, partitions resumed",
				"partitions", parts,
				"note", "each partition redelivers from its uncommitted offset, so the record that paused it is retried")
		}
	}
}

// pollLag publishes the consumer group's lag as a gauge.
//
// Lag is read from the group's own committed offsets rather than derived from
// what this worker happens to have fetched. The difference matters exactly when
// it matters most: a worker that has stalled or paused a partition fetches
// nothing, so a fetch-derived gauge would go stale and flat at the precise moment
// lag is climbing.
func pollLag(ctx context.Context, client *kgo.Client, group string, owned *broker.Assignment, log *slog.Logger) {
	admin := kadm.NewClient(client)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			metrics.PartitionsOwned.Set(float64(owned.Count()))

			lags, err := admin.Lag(ctx, group)
			if err != nil {
				if ctx.Err() == nil {
					log.Warn("lag poll failed", "err", err)
				}
				continue
			}
			described, ok := lags[group]
			if !ok {
				continue
			}
			for topic, partitions := range described.Lag {
				for partition, l := range partitions {
					metrics.ConsumerLag.WithLabelValues(topic, strconv.Itoa(int(partition))).Set(float64(l.Lag))
				}
			}
		}
	}
}

func heartbeat(ctx context.Context, log *slog.Logger, owned *broker.Assignment, c *counters, paused *pausedSet) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			log.Info("heartbeat",
				"owns", owned.String(),
				"partition_count", owned.Count(),
				"processed_total", c.processed.Load(),
				"duplicates_total", c.duplicates.Load(),
				"retries_total", c.retries.Load(),
				"dlq_total", c.dlq.Load(),
				"paused_now", paused.count(),
				"pause_events_total", c.paused.Load(),
				"resumed_total", c.resumed.Load())
		}
	}
}

// watchdog bounds the shutdown. On the signal it starts a clock; if draining has
// not finished by shutdownDeadline it cancels in-flight work and exits hard, so
// the process never outlives the orchestrator's own grace period silently.
func watchdog(fetchCtx context.Context, log *slog.Logger, abandonWork context.CancelFunc) {
	<-fetchCtx.Done()
	log.Info("shutdown signal received, draining", "deadline", shutdownDeadline)

	// A second signal means the operator is not willing to wait.
	impatient := make(chan os.Signal, 1)
	signal.Notify(impatient, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-time.After(shutdownDeadline):
		log.Error("shutdown deadline exceeded, exiting hard", "deadline", shutdownDeadline)
	case <-impatient:
		log.Warn("second signal received, exiting hard")
	}
	abandonWork()
	os.Exit(1)
}

// workerID labels log lines so several workers on one machine stay tellable apart.
func workerID() string {
	if v := os.Getenv("WORKER_ID"); v != "" {
		return v
	}
	host, _ := os.Hostname()
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}
