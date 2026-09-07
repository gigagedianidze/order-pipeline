// Command smoke is the Day 1 acceptance check: connect to Postgres and Kafka,
// print what we found, exit 0. It proves the infrastructure and the Go
// toolchain agree with each other before any real service exists.
package main

import (
	"context"
	"os"
	"time"

	"orderpipeline/internal/config"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	cfg := config.Load()
	log := cfg.Logger("smoke")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Error("postgres pool", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	var version string
	if err := pool.QueryRow(ctx, "select version()").Scan(&version); err != nil {
		log.Error("postgres query", "err", err)
		os.Exit(1)
	}
	log.Info("postgres ok", "version", version)

	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.KafkaBrokers...))
	if err != nil {
		log.Error("kafka client", "err", err)
		os.Exit(1)
	}
	defer cl.Close()

	admin := kadm.NewClient(cl)
	topics, err := admin.ListTopics(ctx, cfg.KafkaTopic, cfg.KafkaDLQTopic)
	if err != nil {
		log.Error("kafka list topics", "err", err)
		os.Exit(1)
	}
	if len(topics) == 0 {
		log.Error("kafka reachable but expected topics are missing", "want", cfg.KafkaTopic)
		os.Exit(1)
	}
	for _, t := range topics.Sorted() {
		log.Info("kafka topic ok", "topic", t.Topic, "partitions", len(t.Partitions))
	}

	log.Info("smoke check passed")
}
