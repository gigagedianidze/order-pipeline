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

func main() {
	cfg := config.Load()
	log := cfg.Logger("worker").With("worker_id", workerID())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := store.New(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Error("connect postgres", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := db.Migrate(ctx); err != nil {
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
	defer client.Close()

	log.Info("joining group", "group", cfg.KafkaGroupID, "topic", cfg.KafkaTopic)

	var processed, duplicates, paused atomic.Int64
	go heartbeat(ctx, log, owned, &processed, &duplicates, &paused)

	for {
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
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
				if ctx.Err() != nil {
					return
				}
				ok := handle(ctx, db, log, rec, &processed, &duplicates)
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
			continue
		}
		// Commit last, and only for records that are already in the database.
		// Committing before the write is what loses messages on a crash.
		if err := client.CommitRecords(ctx, committable...); err != nil && ctx.Err() == nil {
			// The records are written but the offsets are not advanced, so they
			// will be redelivered. Idempotency makes that harmless.
			log.Error("commit offsets", "err", err)
		}
	}

	log.Info("shutdown signal received, leaving group",
		"processed_total", processed.Load(), "duplicates_total", duplicates.Load())
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
