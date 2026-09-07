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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gigagedianidze/order-pipeline/internal/broker"
	"github.com/gigagedianidze/order-pipeline/internal/config"
	"github.com/gigagedianidze/order-pipeline/internal/order"
	"github.com/gigagedianidze/order-pipeline/internal/store"

	"github.com/twmb/franz-go/pkg/kgo"
)

// shutdownDeadline bounds draining. Compose gives a container 10s by default
// before SIGKILL, so the service's own deadline must be set against whatever
// grace period the orchestrator allows (this stack sets 30s).
const shutdownDeadline = 15 * time.Second

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
	log.Info("joining group", "group", cfg.KafkaGroupID, "topic", cfg.KafkaTopic)

	var processed, duplicates, paused atomic.Int64
	go heartbeat(fetchCtx, log, owned, &processed, &duplicates, &paused)

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
				ok := handle(workCtx, db, log, rec, &processed, &duplicates)
				if !ok {
					// Stop at the first failure in this partition AND pause it.
					//
					// Stopping alone is not enough: the client's fetch position has
					// already moved past this record, so the next poll would deliver
					// the records behind it, succeed, and commit an offset beyond
					// the failure — silently losing it. Measured: a failed record at
					// offset 20 ended with the group committed at 22 and lag 0.
					//
					// Pausing stops fetching this partition entirely, so nothing
					// behind the failure is processed or committed. The offset stays
					// uncommitted, so the record is redelivered on the next
					// rebalance or restart. Day 6 replaces the indefinite pause with
					// bounded retries and a dead-letter queue.
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
			paused.Add(int64(len(blocked)))
			log.Warn("partition paused after write failure",
				"partitions", blocked,
				"effect", "no further records from these partitions until the failure is resolved")
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
	//   4. close the database pool
	//
	// Reversing 2 and 3 would hand the partitions to another member while this
	// one still had uncommitted writes in progress: duplicate work at best.
	log.Info("draining complete, leaving group",
		"processed_total", processed.Load(),
		"duplicates_total", duplicates.Load())

	client.Close() // blocks until LeaveGroup is acknowledged
	db.Close()

	log.Info("stopped cleanly",
		"processed_total", processed.Load(), "duplicates_total", duplicates.Load())
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

// handle writes one record, returning false if the offset must not advance past it.
func handle(ctx context.Context, db *store.Store, log *slog.Logger, rec *kgo.Record,
	processed, duplicates *atomic.Int64) bool {

	var evt order.Event
	if err := json.Unmarshal(rec.Value, &evt); err != nil {
		// A malformed record will never parse, however many times it is retried.
		// Blocking the partition on it would be worse than skipping it; Day 6
		// routes it to the dead-letter queue instead of merely logging.
		log.Warn("undecodable record, skipping",
			"partition", rec.Partition, "offset", rec.Offset, "err", err)
		return true
	}

	inserted, err := db.InsertOrder(ctx, evt)
	if err != nil {
		if ctx.Err() != nil {
			return false // shutting down, not a real failure
		}
		log.Error("write failed",
			"partition", rec.Partition, "offset", rec.Offset,
			"order_id", evt.OrderID, "err", err)
		return false
	}

	if inserted {
		processed.Add(1)
		log.Info("order persisted",
			"partition", rec.Partition, "offset", rec.Offset,
			"order_id", evt.OrderID, "event_id", evt.EventID, "total_cents", evt.TotalCents)
	} else {
		duplicates.Add(1)
		log.Info("duplicate event ignored",
			"partition", rec.Partition, "offset", rec.Offset,
			"order_id", evt.OrderID, "event_id", evt.EventID)
	}
	return true
}

func heartbeat(ctx context.Context, log *slog.Logger, owned *broker.Assignment,
	processed, duplicates, paused *atomic.Int64) {

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
				"processed_total", processed.Load(),
				"duplicates_total", duplicates.Load(),
				"paused_partitions", paused.Load())
		}
	}
}

// workerID labels log lines so several workers on one machine stay tellable apart.
func workerID() string {
	if v := os.Getenv("WORKER_ID"); v != "" {
		return v
	}
	host, _ := os.Hostname()
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}
