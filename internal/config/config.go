// Package config loads service configuration from environment variables.
// Every value has a working local default, so `go run ./cmd/...` needs no setup.
package config

import (
	"fmt"
	"log/slog"
	"math"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	KafkaBrokers    []string
	KafkaTopic      string
	KafkaDLQTopic   string
	KafkaGroupID    string
	PostgresDSN     string
	APIAddr         string
	QueryGRPCAddr   string // where the api dials the query service
	QueryListenAddr string // where the query service listens
	MetricsAddr     string
	LogLevel        slog.Level

	// PostgresMaxConns bounds the pool. Deliberately modest by default so pool
	// exhaustion stays reachable and observable in an experiment rather than
	// hidden behind pgx's core-count-derived default.
	PostgresMaxConns int32

	// WorkerBatchSize is how many records the worker persists per round trip.
	//
	// It defaults to 1 — one INSERT per record, the behaviour every measurement
	// in FINDINGS.md was taken against — so the numbers already recorded stay
	// comparable. Day 10 identified batching as the direct fix for the measured
	// ~1/(database round trip) ceiling; this is the knob that lets that claim be
	// tested rather than asserted.
	WorkerBatchSize int
}

// Load reads the environment and rejects a configuration that cannot work.
//
// Returning an error rather than falling over later is the point: an empty
// KAFKA_BROKERS used to surface as a confusing client error several seconds into
// startup, at which point the cause is three layers away from the symptom.
func Load() (Config, error) {
	cfg := Config{
		KafkaBrokers:    splitAndTrim(env("KAFKA_BROKERS", "localhost:9092")),
		KafkaTopic:      env("KAFKA_TOPIC", "orders"),
		KafkaDLQTopic:   env("KAFKA_DLQ_TOPIC", "orders.dlq"),
		KafkaGroupID:    env("KAFKA_GROUP_ID", "order-processors"),
		PostgresDSN:     env("POSTGRES_DSN", "postgres://orders:orders@localhost:5433/orders?sslmode=disable"),
		APIAddr:         env("API_ADDR", ":8080"),
		QueryGRPCAddr:   env("QUERY_GRPC_ADDR", "localhost:9090"),
		QueryListenAddr: env("QUERY_LISTEN_ADDR", ":9090"),
		MetricsAddr:     env("METRICS_ADDR", ":2112"),
		LogLevel:        level(env("LOG_LEVEL", "info")),
	}

	var err error
	if cfg.PostgresMaxConns, err = envInt32("POSTGRES_MAX_CONNS", 10); err != nil {
		return Config{}, err
	}
	if cfg.WorkerBatchSize, err = envInt("WORKER_BATCH_SIZE", 1); err != nil {
		return Config{}, err
	}
	return cfg, cfg.validate()
}

func (c Config) validate() error {
	var problems []string

	if len(c.KafkaBrokers) == 0 {
		problems = append(problems, "KAFKA_BROKERS must list at least one broker")
	}
	for _, b := range c.KafkaBrokers {
		if !strings.Contains(b, ":") {
			problems = append(problems, fmt.Sprintf("KAFKA_BROKERS entry %q needs a host:port", b))
		}
	}
	if c.KafkaTopic == "" {
		problems = append(problems, "KAFKA_TOPIC must not be empty")
	}
	if c.KafkaDLQTopic == c.KafkaTopic {
		// The worker would consume its own dead letters in a loop.
		problems = append(problems, "KAFKA_DLQ_TOPIC must differ from KAFKA_TOPIC")
	}
	if c.KafkaGroupID == "" {
		problems = append(problems, "KAFKA_GROUP_ID must not be empty")
	}
	if !strings.HasPrefix(c.PostgresDSN, "postgres://") && !strings.HasPrefix(c.PostgresDSN, "postgresql://") {
		problems = append(problems, "POSTGRES_DSN must be a postgres:// URL")
	}
	if c.PostgresMaxConns < 1 {
		problems = append(problems, "POSTGRES_MAX_CONNS must be at least 1")
	}
	if c.WorkerBatchSize < 1 {
		problems = append(problems, "WORKER_BATCH_SIZE must be at least 1")
	}
	if c.WorkerBatchSize > maxBatchSize {
		problems = append(problems, fmt.Sprintf("WORKER_BATCH_SIZE must be at most %d", maxBatchSize))
	}

	if len(problems) > 0 {
		return fmt.Errorf("%s", strings.Join(problems, "; "))
	}
	return nil
}

// maxBatchSize mirrors store.MaxInsertBatch. It is duplicated rather than
// imported so that config stays a leaf package with no dependency on the
// persistence layer; the store enforces the real limit either way.
const maxBatchSize = 1000

// Logger returns the structured logger every service shares.
func (c Config) Logger(service string) *slog.Logger {
	h := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: c.LogLevel})
	return slog.New(h).With("service", service)
}

func env(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) (int, error) {
	raw := env(key, "")
	if raw == "" {
		return fallback, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s=%q is not a number", key, raw)
	}
	return v, nil
}

func envInt32(key string, fallback int32) (int32, error) {
	v, err := envInt(key, int(fallback))
	if err != nil {
		return 0, err
	}
	if v < math.MinInt32 || v > math.MaxInt32 {
		return 0, fmt.Errorf("%s=%d is out of range", key, v)
	}
	return int32(v), nil
}

// splitAndTrim tolerates the spacing people actually write in a compose file:
// "a:1, b:2" is the same list as "a:1,b:2", and a trailing comma is not a broker.
func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func level(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
