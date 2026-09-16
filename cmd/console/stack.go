package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"
)

// container is one row of `docker compose ps`, narrowed to what the page shows.
type container struct {
	Name    string `json:"name"`
	Service string `json:"service"`
	State   string `json:"state"`
	Health  string `json:"health,omitempty"`
	Status  string `json:"status"`
}

type stackSnapshot struct {
	Containers []container `json:"containers"`
	Workers    int         `json:"workers"`
	Up         bool        `json:"up"`
	Err        string      `json:"err,omitempty"`
	CheckedAt  time.Time   `json:"checked_at"`
}

// stackWatcher polls `docker compose ps` in the background and caches the
// answer.
//
// Polling on a timer rather than on each request is what keeps the page honest
// when it is busiest. Every connected browser asking for state would otherwise
// fork a docker CLI per poll per tab, and `docker compose ps` is not free — under
// a load run that contention shows up as a dashboard that stutters exactly when
// it is being watched.
type stackWatcher struct {
	cfg config
	log *slog.Logger

	mu   sync.RWMutex
	snap stackSnapshot
}

func newStackWatcher(cfg config, log *slog.Logger) *stackWatcher {
	return &stackWatcher{cfg: cfg, log: log}
}

func (w *stackWatcher) run(ctx context.Context, every time.Duration) {
	w.poll(ctx)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.poll(ctx)
		}
	}
}

func (w *stackWatcher) poll(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "docker", "compose", "ps", "--format", "json")
	cmd.Dir = w.cfg.ProjectDir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	snap := stackSnapshot{CheckedAt: time.Now()}
	if err := cmd.Run(); err != nil {
		// A stopped stack is not an error, but a missing docker is. Both arrive
		// here the same way, so the stderr text is passed through rather than
		// replaced with a guess about which one it was.
		snap.Err = strings.TrimSpace(stderr.String())
		if snap.Err == "" {
			snap.Err = err.Error()
		}
		w.store(snap)
		return
	}

	snap.Containers = parseComposePS(stdout.Bytes())
	for _, c := range snap.Containers {
		if c.Service == "worker" && strings.EqualFold(c.State, "running") {
			snap.Workers++
		}
		if strings.EqualFold(c.State, "running") {
			snap.Up = true
		}
	}
	sort.Slice(snap.Containers, func(i, j int) bool {
		return snap.Containers[i].Name < snap.Containers[j].Name
	})
	w.store(snap)
}

// parseComposePS handles both shapes Compose v2 has emitted for --format json:
// a single JSON array, and one object per line. Which one you get depends on the
// Compose version, so accepting only the current shape would make the console
// silently show an empty stack on a slightly older Docker Desktop.
func parseComposePS(raw []byte) []container {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return nil
	}

	type row struct {
		Name    string `json:"Name"`
		Service string `json:"Service"`
		State   string `json:"State"`
		Health  string `json:"Health"`
		Status  string `json:"Status"`
	}
	toContainers := func(rows []row) []container {
		out := make([]container, 0, len(rows))
		for _, r := range rows {
			out = append(out, container{
				Name: r.Name, Service: r.Service,
				State: r.State, Health: r.Health, Status: r.Status,
			})
		}
		return out
	}

	if raw[0] == '[' {
		var rows []row
		if err := json.Unmarshal(raw, &rows); err == nil {
			return toContainers(rows)
		}
		return nil
	}

	var rows []row
	for _, l := range bytes.Split(raw, []byte("\n")) {
		l = bytes.TrimSpace(l)
		if len(l) == 0 {
			continue
		}
		var r row
		if err := json.Unmarshal(l, &r); err != nil {
			continue
		}
		rows = append(rows, r)
	}
	return toContainers(rows)
}

func (w *stackWatcher) store(s stackSnapshot) {
	w.mu.Lock()
	w.snap = s
	w.mu.Unlock()
}

func (w *stackWatcher) snapshot() stackSnapshot {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.snap
}
