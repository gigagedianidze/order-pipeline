package config

import (
	"strings"
	"testing"
)

func TestLoadDefaultsAreUsable(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("the defaults must be a valid configuration, got: %v", err)
	}
	if len(cfg.KafkaBrokers) == 0 {
		t.Error("no default broker")
	}
	if cfg.WorkerBatchSize != 1 {
		t.Errorf("default batch size = %d, want 1: the recorded measurements assume one insert per record",
			cfg.WorkerBatchSize)
	}
	if cfg.KafkaTopic == cfg.KafkaDLQTopic {
		t.Error("the default topic and dead-letter topic are the same")
	}
}

func TestLoadRejectsUnusableConfiguration(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want string
	}{
		{"no brokers", map[string]string{"KAFKA_BROKERS": ","}, "at least one broker"},
		{"broker without a port", map[string]string{"KAFKA_BROKERS": "kafka"}, "host:port"},
		{"dlq equals topic", map[string]string{"KAFKA_TOPIC": "orders", "KAFKA_DLQ_TOPIC": "orders"}, "must differ"},
		{"dsn is not a url", map[string]string{"POSTGRES_DSN": "host=localhost user=orders"}, "postgres:// URL"},
		{"batch size zero", map[string]string{"WORKER_BATCH_SIZE": "0"}, "at least 1"},
		{"batch size too large", map[string]string{"WORKER_BATCH_SIZE": "100000"}, "at most"},
		{"batch size not a number", map[string]string{"WORKER_BATCH_SIZE": "lots"}, "not a number"},
		{"max conns zero", map[string]string{"POSTGRES_MAX_CONNS": "0"}, "at least 1"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			_, err := Load()
			if err == nil {
				t.Fatal("accepted an unusable configuration")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

// A compose file written by a human has spaces in it. Treating " kafka:19092" as
// a distinct broker from "kafka:19092" is a confusing way to fail.
func TestBrokerListTolerantOfSpacing(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", " a:1 , b:2,, c:3 ,")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	want := []string{"a:1", "b:2", "c:3"}
	if len(cfg.KafkaBrokers) != len(want) {
		t.Fatalf("brokers = %q, want %q", cfg.KafkaBrokers, want)
	}
	for i, b := range want {
		if cfg.KafkaBrokers[i] != b {
			t.Errorf("broker %d = %q, want %q", i, cfg.KafkaBrokers[i], b)
		}
	}
}

func TestLogLevelParsing(t *testing.T) {
	for env, want := range map[string]string{
		"debug": "DEBUG", "DEBUG": "DEBUG", "warn": "WARN",
		"error": "ERROR", "info": "INFO", "nonsense": "INFO", "": "INFO",
	} {
		t.Run(env, func(t *testing.T) {
			t.Setenv("LOG_LEVEL", env)
			cfg, err := Load()
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			if got := cfg.LogLevel.String(); got != want {
				t.Errorf("LOG_LEVEL=%q gave level %s, want %s", env, got, want)
			}
		})
	}
}
