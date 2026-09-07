// Command worker joins the consumer group and processes order events.
//
// Day 3 scope on purpose: join, decode, log. No database, no retries, no manual
// offset commits. The point today is to watch partition assignment and rebalance
// behaviour with nothing else moving.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gigagedianidze/order-pipeline/internal/broker"
	"github.com/gigagedianidze/order-pipeline/internal/config"
	"github.com/gigagedianidze/order-pipeline/internal/order"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	cfg := config.Load()
	log := cfg.Logger("worker").With("worker_id", workerID())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	client, owned, err := broker.NewConsumerGroup(broker.ConsumerOptions{
		Brokers: cfg.KafkaBrokers,
		Topic:   cfg.KafkaTopic,
		GroupID: cfg.KafkaGroupID,
		Log:     log,
	})
	if err != nil {
		log.Error("start consumer", "err", err)
		os.Exit(1)
	}
	defer client.Close()

	log.Info("joining group", "group", cfg.KafkaGroupID, "topic", cfg.KafkaTopic)

	// A heartbeat makes idleness visible. With more members than partitions some
	// worker owns nothing at all, and a silent process looks identical to a busy
	// one in a log file.
	var processed atomic.Int64
	go func() {
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
					"processed_total", processed.Load())
			}
		}
	}()

	for {
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			break
		}
		// Fetch errors are per topic-partition and usually transient (a leader
		// moved, a rebalance is in flight). Log and keep polling.
		fetches.EachError(func(topic string, partition int32, err error) {
			if !errors.Is(err, context.Canceled) {
				log.Error("fetch error", "topic", topic, "partition", partition, "err", err)
			}
		})

		fetches.EachRecord(func(rec *kgo.Record) {
			var evt order.Event
			if err := json.Unmarshal(rec.Value, &evt); err != nil {
				// Day 6 sends this to the dead-letter queue. Today: notice it and
				// carry on, because one bad record must not stop the partition.
				log.Warn("undecodable record",
					"partition", rec.Partition, "offset", rec.Offset, "err", err)
				return
			}
			processed.Add(1)
			log.Info("event received",
				"partition", rec.Partition,
				"offset", rec.Offset,
				"key", string(rec.Key),
				"event_id", evt.EventID,
				"order_id", evt.OrderID,
				"total_cents", evt.TotalCents,
			)
		})
	}

	log.Info("shutdown signal received, leaving group", "processed_total", processed.Load())
}

// workerID labels log lines so several workers on one machine stay tellable apart.
func workerID() string {
	if v := os.Getenv("WORKER_ID"); v != "" {
		return v
	}
	host, _ := os.Hostname()
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}
