package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// config is the console's own configuration, deliberately separate from
// internal/config.
//
// internal/config describes a *pipeline service* — brokers, DSNs, topics. The
// console is not one: it never connects to Kafka or Postgres, and it holds a
// password hash, which is not something the pipeline services should be able to
// read out of a shared struct. Keeping the two apart means a mistake here cannot
// widen the blast radius of an api or worker container.
type config struct {
	Addr string

	// PasswordHash is a bcrypt hash. There is no plaintext option and no
	// default: a console with a blank or built-in password is worse than no
	// console, because it looks protected. Load refuses to start without it.
	PasswordHash []byte

	// SessionKey signs session cookies. Absent, a random key is generated, which
	// works but invalidates every session on restart — fine locally, wrong for a
	// deployment, so Load says so rather than leaving it to be discovered.
	SessionKey       []byte
	SessionKeyRandom bool
	SessionTTL       time.Duration

	// SecureCookie marks the session cookie Secure. It must be off for plain
	// http://localhost (the browser would refuse to store the cookie) and on
	// everywhere else.
	SecureCookie bool

	PrometheusURL string

	// ProjectDir is where docker compose and the scripts are run. Every action's
	// working directory, and the only path the console ever touches.
	ProjectDir string

	// MaxLoadRate caps the rate a load run may request. The console exists to be
	// driven by someone else, possibly over the internet, and an uncapped rate
	// field is a self-service denial of service against the box it runs on.
	MaxLoadRate float64

	// MaxLoadDuration caps how long one load run may last, for the same reason.
	MaxLoadDuration time.Duration

	// TrustedProxy makes the console read X-Forwarded-For and X-Forwarded-Proto.
	// Off by default: behind no proxy those headers are attacker-controlled, and
	// trusting them would let a client forge its own address past the login
	// throttle.
	TrustedProxy bool

	LogLevel slog.Level
}

func loadConfig() (config, error) {
	cfg := config{
		Addr:          env("CONSOLE_ADDR", "127.0.0.1:8081"),
		PrometheusURL: strings.TrimRight(env("PROMETHEUS_URL", "http://localhost:9091"), "/"),
		ProjectDir:    env("CONSOLE_PROJECT_DIR", "."),
		SessionTTL:    12 * time.Hour,
		SecureCookie:  env("CONSOLE_SECURE_COOKIE", "") == "true",
		TrustedProxy:  env("CONSOLE_TRUSTED_PROXY", "") == "true",
		LogLevel:      level(env("LOG_LEVEL", "info")),
	}

	hash := env("CONSOLE_PASSWORD_HASH", "")
	if hash == "" {
		return config{}, fmt.Errorf(
			"CONSOLE_PASSWORD_HASH is required; generate one with `go run ./cmd/console -hash`")
	}
	cfg.PasswordHash = []byte(hash)

	if raw := env("CONSOLE_SESSION_KEY", ""); raw != "" {
		key, err := hex.DecodeString(raw)
		if err != nil || len(key) < 32 {
			return config{}, fmt.Errorf("CONSOLE_SESSION_KEY must be at least 32 bytes of hex")
		}
		cfg.SessionKey = key
	} else {
		cfg.SessionKey = make([]byte, 32)
		if _, err := rand.Read(cfg.SessionKey); err != nil {
			return config{}, fmt.Errorf("generate session key: %w", err)
		}
		cfg.SessionKeyRandom = true
	}

	var err error
	if cfg.MaxLoadRate, err = envFloat("CONSOLE_MAX_LOAD_RATE", 20000); err != nil {
		return config{}, err
	}
	if cfg.MaxLoadDuration, err = envDuration("CONSOLE_MAX_LOAD_DURATION", 60*time.Second); err != nil {
		return config{}, err
	}
	if cfg.MaxLoadRate <= 0 {
		return config{}, fmt.Errorf("CONSOLE_MAX_LOAD_RATE must be positive")
	}
	if cfg.MaxLoadDuration <= 0 {
		return config{}, fmt.Errorf("CONSOLE_MAX_LOAD_DURATION must be positive")
	}
	return cfg, nil
}

func (c config) Logger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: c.LogLevel})).
		With("service", "console")
}

func env(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func envFloat(key string, fallback float64) (float64, error) {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return fallback, nil
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be a number, got %q", key, raw)
	}
	return v, nil
}

func envDuration(key string, fallback time.Duration) (time.Duration, error) {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return fallback, nil
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a duration like 30s, got %q", key, raw)
	}
	return v, nil
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
