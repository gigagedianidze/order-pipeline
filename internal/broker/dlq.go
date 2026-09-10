package broker

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Reason explains why a record was dead-lettered.
type Reason string

const (
	// ReasonPoison: the record will never succeed. Malformed JSON, a constraint
	// violation, a value the schema rejects. Retrying is pure waste.
	ReasonPoison Reason = "poison"
	// ReasonRetriesExhausted: the record might well have succeeded, but the
	// environment stayed broken for longer than the retry budget allowed.
	ReasonRetriesExhausted Reason = "retries_exhausted"
)

// DLQ publishes records that could not be processed.
type DLQ struct {
	producer *Producer
	log      *slog.Logger
}

func NewDLQ(brokers []string, topic string, log *slog.Logger) (*DLQ, error) {
	p, err := NewProducer(brokers, topic, log)
	if err != nil {
		return nil, fmt.Errorf("dlq producer: %w", err)
	}
	return &DLQ{producer: p, log: log}, nil
}

// Send forwards the original record to the dead-letter topic, preserving its key
// and value byte-for-byte so it can be replayed unchanged once the cause is fixed.
//
// The diagnosis travels in headers rather than in the payload. A DLQ whose
// messages have a different shape from the source topic cannot be replayed by the
// same consumer, which is how dead-letter queues quietly become write-only.
func (d *DLQ) Send(ctx context.Context, rec *kgo.Record, reason Reason, cause error, attempts int) error {
	dead := &kgo.Record{
		Key:   rec.Key,
		Value: rec.Value,
		Headers: []kgo.RecordHeader{
			{Key: "dlq_reason", Value: []byte(reason)},
			{Key: "dlq_error", Value: []byte(cause.Error())},
			{Key: "dlq_attempts", Value: []byte(strconv.Itoa(attempts))},
			{Key: "dlq_source_topic", Value: []byte(rec.Topic)},
			{Key: "dlq_source_partition", Value: []byte(strconv.Itoa(int(rec.Partition)))},
			{Key: "dlq_source_offset", Value: []byte(strconv.FormatInt(rec.Offset, 10))},
			{Key: "dlq_failed_at", Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
		},
	}

	partition, offset, err := d.producer.PublishRecord(ctx, dead)
	if err != nil {
		return fmt.Errorf("publish to dlq: %w", err)
	}

	d.log.Warn("record dead-lettered",
		"reason", reason,
		"cause", cause,
		"attempts", attempts,
		"source_partition", rec.Partition,
		"source_offset", rec.Offset,
		"dlq_partition", partition,
		"dlq_offset", offset,
	)
	return nil
}

// Ping reports whether the dead-letter topic's brokers are reachable. The worker
// asks this before un-pausing a partition it paused because the DLQ was down.
func (d *DLQ) Ping(ctx context.Context) error { return d.producer.Ping(ctx) }

func (d *DLQ) Close() { d.producer.Close() }
