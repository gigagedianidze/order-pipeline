// Command console is a web control surface for the pipeline: buttons that run
// the same experiments the scripts do, and a dashboard reading the same
// Prometheus the PromQL in the README queries.
//
// It is deliberately not part of the system it drives. It runs on the host
// rather than in Compose, holds no pipeline credentials, and never connects to
// Kafka or Postgres — it shells out to the same commands an operator would type
// and reads metrics over HTTP. That separation is what lets it stay up and keep
// reporting while the stack it is pointed at is deliberately being broken, which
// is the only time a console really has to work.
//
// Nothing here is reachable without logging in, because the buttons stop
// containers and generate load.
package main

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"golang.org/x/crypto/bcrypt"
)

//go:embed ui
var ui embed.FS

var staticOnce struct {
	sync.Once
	fsys fs.FS
}

func staticFS() fs.FS {
	staticOnce.Do(func() {
		sub, err := fs.Sub(ui, "ui")
		if err != nil {
			panic(err) // impossible: the directory is embedded at build time
		}
		staticOnce.fsys = sub
	})
	return staticOnce.fsys
}

func main() {
	// -hash exists so nobody has to go and find a bcrypt tool, and so the
	// plaintext never has to be stored anywhere to be turned into a hash.
	if len(os.Args) > 1 && os.Args[1] == "-hash" {
		if err := printHash(); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.Exit(1)
		}
		return
	}

	cfg, err := loadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid configuration: %v\n", err)
		os.Exit(1)
	}
	log := cfg.Logger()

	if cfg.SessionKeyRandom {
		log.Warn("CONSOLE_SESSION_KEY not set; sessions will not survive a restart",
			"fix", "set it to 32+ bytes of hex")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	stack := newStackWatcher(cfg, log)
	go stack.run(ctx, 3*time.Second)

	s := &server{
		cfg:   cfg,
		log:   log,
		auth:  newAuth(cfg),
		run:   newRunner(cfg, log),
		prom:  newProm(cfg.PrometheusURL),
		stack: stack,
	}

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           s.routes(),
		ReadHeaderTimeout: 5 * time.Second,
		// No WriteTimeout: /api/stream is a long-lived response and any deadline
		// here would cut the output pane off mid-experiment.
	}

	go func() {
		log.Info("console listening",
			"addr", cfg.Addr,
			"prometheus", cfg.PrometheusURL,
			"project", cfg.ProjectDir,
			"max_load_rate", cfg.MaxLoadRate)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutdown signal received")

	// Stop a running experiment rather than orphaning it. A `docker compose up
	// --scale` killed halfway leaves the stack in a state nobody asked for, and
	// the next run would start from it.
	_ = s.run.stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed", "err", err)
	}
	log.Info("stopped")
}

// printHash reads a password from CONSOLE_PASSWORD and prints its bcrypt hash.
//
// Through the environment rather than a flag: a flag would put the password in
// the process list, where every other user on the machine can read it, and in
// the shell history.
func printHash() error {
	pw := os.Getenv("CONSOLE_PASSWORD")
	if len(pw) < 12 {
		return fmt.Errorf(
			"set CONSOLE_PASSWORD to at least 12 characters, then run `go run ./cmd/console -hash`")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(pw), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("hash password: %w", err)
	}
	fmt.Printf("CONSOLE_PASSWORD_HASH=%s\n", hash)
	return nil
}
