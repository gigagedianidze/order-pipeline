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

// shutdownDeadline bounds draining. Compose gives a container 10s by default
// before SIGKILL, so the service's own deadline must be set against whatever
// grace period the orchestrator allows (this stack sets 30s).
const shutdownDeadline = 15 * time.Second

// counters is the worker's running tally, reported by the heartbeat and on exit.
type counters struct {
	processed  atomic.Int64
	duplicates atomic.Int64
	retries    atomic.Int64
	dlq        atomic.Int64
	paused     atomic.Int64
}

func main() {
	cfg := config.Load()
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

	db, err := store.New(workCtx, cfg.PostgresDSN)
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

	log.Info("joining group", "group", cfg.KafkaGroupID, "topic", cfg.KafkaTopic)

	var c counters
	policy := retry.DefaultPolicy()
	go heartbeat(fetchCtx, log, owned, &c)
	go metrics.Serve(fetchCtx, cfg.MetricsAddr, log)
	go pollLag(fetchCtx, client, cfg.KafkaGroupID, owned, log)

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
			for _, rec := range p.Records {
				if workCtx.Err() != nil {
					return // deadline exceeded; the watchdog is about to exit
				}
				ok := handle(workCtx, db, dlq, log, rec, policy, &c)
				if !ok {
					// Retries and the dead-letter queue have both failed, so this
					// record can neither be persisted nor parked. Stop the
					// partition and pause it.
					//
					// Stopping alone would not be enough: the client's fetch
					// position has already moved past this record, so the next poll
					// would deliver the records behind it, succeed, and commit an
					// offset beyond the failure — silently losing it. Measured
					// before the pause existed: a failed record at offset 20 ended
					// with the group committed at 22 and lag 0.
					blocked = append(blocked, rec.Partition)
					return
				}
				if n := len(committable); n > 0 && committable[n-1].Partition == rec.Partition {
					committable[n-1] = rec
				} else {
					committable = append(committable, rec)
				}
			}
		})

		if len(blocked) > 0 {
			client.PauseFetchPartitions(map[string][]int32{cfg.KafkaTopic: blocked})
			c.paused.Add(int64(len(blocked)))
			log.Warn("partition paused: could not persist and could not dead-letter",
				"partitions", blocked,
				"effect", "no further records from these partitions until Kafka is reachable")
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

// handle processes one record and reports whether the offset may advance past it.
//
// It returns false only when the worker genuinely cannot make progress — which,
// after retries and the dead-letter queue, means Kafka itself is unreachable.
// Every other outcome, including permanent failure, ends with the record
// accounted for and the partition free to move on.
func handle(ctx context.Context, db *store.Store, dlq *broker.DLQ, log *slog.Logger,
	rec *kgo.Record, policy retry.Policy, c *counters) bool {

	var evt order.Event
	if err := json.Unmarshal(rec.Value, &evt); err != nil {
		// Poison by definition: no amount of retrying will make this parse.
		return deadLetter(ctx, dlq, log, rec, broker.ReasonPoison, err, 0, c)
	}

	started := time.Now()
	attempts := 0
	err := retry.Do(ctx, policy, store.IsRetryable,
		func(attempt int, delay time.Duration, err error) {
			c.retries.Add(1)
			metrics.Retries.Inc()
			log.Warn("write failed, retrying",
				"attempt", attempt+1,
				"delay", delay,
				"partition", rec.Partition, "offset", rec.Offset,
				"order_id", evt.OrderID, "err", err)
		},
		func() error {
			attempts++
			inserted, err := db.InsertOrder(ctx, evt)
			if err != nil {
				return err
			}
			if inserted {
				c.processed.Add(1)
				metrics.OrdersProcessed.WithLabelValues(metrics.OutcomePersisted).Inc()
				metrics.ProcessingDuration.WithLabelValues(metrics.OutcomePersisted).Observe(time.Since(started).Seconds())
				// Only measured for genuinely new orders: a duplicate's
				// "latency" would be the age of the original event and would
				// pollute the distribution with irrelevant large values.
				metrics.EndToEndLatency.Observe(time.Since(evt.OccurredAt).Seconds())
				log.Info("order persisted",
					"partition", rec.Partition, "offset", rec.Offset,
					"order_id", evt.OrderID, "event_id", evt.EventID,
					"total_cents", evt.TotalCents, "attempts", attempts)
			} else {
				c.duplicates.Add(1)
				metrics.OrdersProcessed.WithLabelValues(metrics.OutcomeDuplicate).Inc()
				metrics.ProcessingDuration.WithLabelValues(metrics.OutcomeDuplicate).Observe(time.Since(started).Seconds())
				log.Info("duplicate event ignored",
					"partition", rec.Partition, "offset", rec.Offset,
					"order_id", evt.OrderID, "event_id", evt.EventID)
			}
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
	return deadLetter(ctx, dlq, log, rec, reason, err, attempts, c)
}

// deadLetter parks a record and lets the partition continue. If the DLQ write
// itself fails, the record is neither persisted nor parked, so the offset must
// not advance: the caller pauses the partition instead.
func deadLetter(ctx context.Context, dlq *broker.DLQ, log *slog.Logger, rec *kgo.Record,
	reason broker.Reason, cause error, attempts int, c *counters) bool {

	if err := dlq.Send(ctx, rec, reason, cause, attempts); err != nil {
		if ctx.Err() == nil {
			log.Error("dead-letter failed, cannot make progress",
				"partition", rec.Partition, "offset", rec.Offset, "err", err)
		}
		return false
	}
	c.dlq.Add(1)
	metrics.DeadLettered.WithLabelValues(string(reason)).Inc()
	metrics.OrdersProcessed.WithLabelValues(metrics.OutcomeDeadLetter).Inc()
	return true
}

func heartbeat(ctx context.Context, log *slog.Logger, owned *broker.Assignment, c *counters) {
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
				"paused_partitions", c.paused.Load())
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
