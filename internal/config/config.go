// Package config loads service configuration from environment variables.
// Every value has a working local default, so `go run ./cmd/...` needs no setup.
package config

import (
	"log/slog"
	"os"
	"strings"
)

type Config struct {
	KafkaBrokers  []string
	KafkaTopic    string
	KafkaDLQTopic string
	KafkaGroupID  string
	PostgresDSN   string
	APIAddr       string
	QueryGRPCAddr string
	MetricsAddr   string
	LogLevel      slog.Level
}

func Load() Config {
	return Config{
		KafkaBrokers:  strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ","),
		KafkaTopic:    env("KAFKA_TOPIC", "orders"),
		KafkaDLQTopic: env("KAFKA_DLQ_TOPIC", "orders.dlq"),
		KafkaGroupID:  env("KAFKA_GROUP_ID", "order-processors"),
		PostgresDSN:   env("POSTGRES_DSN", "postgres://orders:orders@localhost:5433/orders?sslmode=disable"),
		APIAddr:       env("API_ADDR", ":8080"),
		QueryGRPCAddr: env("QUERY_GRPC_ADDR", "localhost:9090"),
		MetricsAddr:   env("METRICS_ADDR", ":2112"),
		LogLevel:      level(env("LOG_LEVEL", "info")),
	}
}

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
