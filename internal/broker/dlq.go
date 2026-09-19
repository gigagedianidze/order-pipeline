package broker

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Header keys carried by a dead-lettered record. They are named constants
// because two commands now read them: the worker writes them, and cmd/replay
// decides what to send back on the strength of them. A typo in a string literal
// on either side would silently turn "replay the ones that can succeed" into
// "replay nothing".
const (
	HeaderReason          = "dlq_reason"
	HeaderError           = "dlq_error"
	HeaderAttempts        = "dlq_attempts"
	HeaderSourceTopic     = "dlq_source_topic"
	HeaderSourcePartition = "dlq_source_partition"
	HeaderSourceOffset    = "dlq_source_offset"
	HeaderFailedAt        = "dlq_failed_at"

	// Written by cmd/replay when it returns a record to the source topic. If the
	// record fails again and comes back here, the count is what stops the second
	// replay turning into an endless DLQ -> orders -> DLQ circuit.
	HeaderReplayCount       = "replay_count"
	HeaderReplayAt          = "replay_at"
	HeaderReplayOfPartition = "replay_of_dlq_partition"
	HeaderReplayOfOffset    = "replay_of_dlq_offset"
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
//
// Replay headers on the incoming record are carried forward. They are the only
// evidence that this record has been round-tripped before, and cmd/replay refuses
// a record that has been round-tripped too often — a guard that would be defeated
// by dropping the very header it reads.
func (d *DLQ) Send(ctx context.Context, rec *kgo.Record, reason Reason, cause error, attempts int) error {
	dead := &kgo.Record{
		Key:     rec.Key,
		Value:   rec.Value,
		Headers: carryForward(rec.Headers),
	}
	dead.Headers = append(dead.Headers, []kgo.RecordHeader{
		{Key: HeaderReason, Value: []byte(reason)},
		{Key: HeaderError, Value: []byte(cause.Error())},
		{Key: HeaderAttempts, Value: []byte(strconv.Itoa(attempts))},
		{Key: HeaderSourceTopic, Value: []byte(rec.Topic)},
		{Key: HeaderSourcePartition, Value: []byte(strconv.Itoa(int(rec.Partition)))},
		{Key: HeaderSourceOffset, Value: []byte(strconv.FormatInt(rec.Offset, 10))},
		{Key: HeaderFailedAt, Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
	}...)

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

// carryForward copies the headers a redelivery must not lose. Everything else is
// re-derived at the moment of failure, so keeping a stale copy of it would only
// give a reader two answers to the same question.
func carryForward(headers []kgo.RecordHeader) []kgo.RecordHeader {
	var kept []kgo.RecordHeader
	for _, h := range headers {
		switch h.Key {
		case HeaderReplayCount, HeaderReplayAt, HeaderReplayOfPartition, HeaderReplayOfOffset:
			kept = append(kept, h)
		}
	}
	return kept
}

// Ping reports whether the dead-letter topic's brokers are reachable. The worker
// asks this before un-pausing a partition it paused because the DLQ was down.
func (d *DLQ) Ping(ctx context.Context) error { return d.producer.Ping(ctx) }

func (d *DLQ) Close() { d.producer.Close() }
