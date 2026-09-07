// Package broker wraps the Kafka client so the services deal in events, not in
// client library types.
package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Producer struct {
	client *kgo.Client
	topic  string
	log    *slog.Logger
}

func NewProducer(brokers []string, topic string, log *slog.Logger) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		// Wait for all in-sync replicas before considering a record produced.
		// With one broker this is cheap; the point is that the API must never
		// answer 202 for a record the broker has not acknowledged.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchMaxBytes(1<<20),
		kgo.RecordDeliveryTimeout(10*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	log.Debug("kafka producer ready", "brokers", brokers, "topic", topic)
	return &Producer{client: client, topic: topic, log: log}, nil
}

// Publish sends one event and blocks until the broker acknowledges it, returning
// the partition and offset it landed on.
//
// Synchronous on purpose: this call sits on the HTTP request path, and the whole
// meaning of the 202 response is "Kafka has this". Fire-and-forget here would
// turn every broker hiccup into silently dropped orders.
func (p *Producer) Publish(ctx context.Context, key string, value any) (partition int32, offset int64, err error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return 0, 0, fmt.Errorf("marshal event: %w", err)
	}

	rec := &kgo.Record{
		Topic: p.topic,
		Key:   []byte(key), // partition key: same key -> same partition -> ordered
		Value: payload,
	}

	res := p.client.ProduceSync(ctx, rec)
	if err := res.FirstErr(); err != nil {
		return 0, 0, fmt.Errorf("produce: %w", err)
	}
	r := res[0].Record
	return r.Partition, r.Offset, nil
}

// PublishRecord sends an already-built record, letting the caller set headers
// and reuse the original key and value bytes unchanged.
func (p *Producer) PublishRecord(ctx context.Context, rec *kgo.Record) (partition int32, offset int64, err error) {
	if rec.Topic == "" {
		rec.Topic = p.topic
	}
	res := p.client.ProduceSync(ctx, rec)
	if err := res.FirstErr(); err != nil {
		return 0, 0, fmt.Errorf("produce: %w", err)
	}
	r := res[0].Record
	return r.Partition, r.Offset, nil
}

// Close flushes buffered records and shuts the client down.
func (p *Producer) Close() { p.client.Close() }
