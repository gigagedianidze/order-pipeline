// Command loadgen drives the pipeline at a controlled rate and reports what
// actually happened.
//
// The rate is *scheduled*, not emergent. A pool of N goroutines each looping
// "send, wait for response, send again" produces a rate of N/latency — so the
// moment the system slows down, the load generator politely slows down with it,
// hides the degradation, and reports latencies that look fine. That is
// coordinated omission, and it is the single easiest way to publish a benchmark
// that is quietly wrong.
//
// Here, request start times are fixed in advance from the target rate. Latency is
// measured from the time a request was *due*, not from the time it was sent, so
// queueing delay caused by the system falling behind is counted rather than
// discarded.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type config struct {
	apiURL      string
	dsn         string
	rate        float64
	duration    time.Duration
	concurrency int
	tag         string
	drain       time.Duration
	jsonOut     string
}

func main() {
	var cfg config
	flag.StringVar(&cfg.apiURL, "api", "http://localhost:8080", "base URL of the api service")
	flag.StringVar(&cfg.dsn, "dsn", "postgres://orders:orders@localhost:5433/orders?sslmode=disable",
		"PostgreSQL DSN, used to measure end-to-end latency; empty to skip")
	flag.Float64Var(&cfg.rate, "rate", 100, "target orders per second")
	flag.DurationVar(&cfg.duration, "duration", 30*time.Second, "how long to send for")
	flag.IntVar(&cfg.concurrency, "concurrency", 128, "maximum requests in flight")
	flag.StringVar(&cfg.tag, "tag", "", "run label; defaults to load-<unix seconds>")
	flag.DurationVar(&cfg.drain, "drain", 60*time.Second, "how long to wait for the pipeline to catch up")
	flag.StringVar(&cfg.jsonOut, "json", "", "also write the report as JSON to this path")
	flag.Parse()

	if cfg.tag == "" {
		cfg.tag = fmt.Sprintf("load-%d", time.Now().Unix())
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rep, err := run(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "loadgen: %v\n", err)
		os.Exit(1)
	}

	rep.print(os.Stdout)
	if cfg.jsonOut != "" {
		if err := rep.writeJSON(cfg.jsonOut); err != nil {
			fmt.Fprintf(os.Stderr, "loadgen: write json: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("\nreport written to %s\n", cfg.jsonOut)
	}
}

func run(ctx context.Context, cfg config) (*report, error) {
	// One connection pool per sender, kept alive. Without this the client opens a
	// fresh TCP connection per request and the benchmark measures connection
	// setup rather than the system under test.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = cfg.concurrency * 2
	transport.MaxIdleConnsPerHost = cfg.concurrency * 2
	transport.MaxConnsPerHost = cfg.concurrency * 2
	client := &http.Client{Transport: transport, Timeout: 30 * time.Second}

	expected := int(cfg.rate * cfg.duration.Seconds())
	res := &results{
		latencies: make([]time.Duration, 0, expected+1024),
		codes:     make(map[int]int64),
	}

	// The permit channel caps requests in flight. When it is empty the system is
	// not keeping up: the ticket waits, its latency grows because it is measured
	// from the due time, and that is exactly the signal we want to see.
	permits := make(chan struct{}, cfg.concurrency)
	for i := 0; i < cfg.concurrency; i++ {
		permits <- struct{}{}
	}

	var wg sync.WaitGroup
	interval := time.Duration(float64(time.Second) / cfg.rate)
	start := time.Now()
	deadline := start.Add(cfg.duration)

	fmt.Printf("sending %.0f/s for %s (tag=%s, max in flight=%d)\n",
		cfg.rate, cfg.duration, cfg.tag, cfg.concurrency)

	var sent int64
	for i := 0; ; i++ {
		due := start.Add(time.Duration(i) * interval)
		if due.After(deadline) {
			break
		}

		if wait := time.Until(due); wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				goto done
			case <-timer.C:
			}
		} else if -wait > interval {
			// The scheduler itself fell behind, not just the system under test.
			// Counted separately so a generator-side bottleneck is never mistaken
			// for a finding about the pipeline.
			res.schedulerBehind.Add(1)
		}

		select {
		case <-ctx.Done():
			goto done
		case <-permits:
		}

		wg.Add(1)
		atomic.AddInt64(&sent, 1)
		go func(seq int, due time.Time) {
			defer wg.Done()
			defer func() { permits <- struct{}{} }()
			send(ctx, client, cfg, seq, due, res)
		}(i, due)
	}

done:
	wg.Wait()
	res.wallClock = time.Since(start)

	rep := res.summarise(cfg)

	if cfg.dsn != "" {
		e2e, err := measureEndToEnd(ctx, cfg, rep.Accepted)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: end-to-end measurement failed: %v\n", err)
		} else {
			rep.EndToEnd = e2e
		}
	}
	return rep, nil
}

type results struct {
	mu        sync.Mutex
	latencies []time.Duration
	codes     map[int]int64

	accepted        atomic.Int64
	failed          atomic.Int64
	schedulerBehind atomic.Int64
	wallClock       time.Duration
}

func send(ctx context.Context, client *http.Client, cfg config, seq int, due time.Time, res *results) {
	body, _ := json.Marshal(map[string]any{
		"customer_id": fmt.Sprintf("%s-%d", cfg.tag, seq),
		"items": []map[string]any{
			{"sku": fmt.Sprintf("SKU-%d", seq%1000), "quantity": 1, "unit_price_cents": 1999},
		},
	})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.apiURL+"/orders", bytes.NewReader(body))
	if err != nil {
		res.failed.Add(1)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	// Latency from the due time, not from now: if this request waited for a
	// permit because the system was saturated, that wait is part of what a client
	// would experience and must not be discarded.
	elapsed := time.Since(due)

	if err != nil {
		res.failed.Add(1)
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body) // drain so the connection is reusable
	resp.Body.Close()

	res.mu.Lock()
	res.latencies = append(res.latencies, elapsed)
	res.codes[resp.StatusCode]++
	res.mu.Unlock()

	if resp.StatusCode == http.StatusAccepted {
		res.accepted.Add(1)
	}
}

func (r *results) summarise(cfg config) *report {
	r.mu.Lock()
	defer r.mu.Unlock()

	sort.Slice(r.latencies, func(i, j int) bool { return r.latencies[i] < r.latencies[j] })

	codes := make(map[string]int64, len(r.codes))
	for code, n := range r.codes {
		codes[fmt.Sprintf("%d", code)] = n
	}

	return &report{
		Tag:             cfg.tag,
		TargetRate:      cfg.rate,
		Duration:        cfg.duration.String(),
		Concurrency:     cfg.concurrency,
		WallClock:       r.wallClock.Seconds(),
		Sent:            int64(len(r.latencies)) + r.failed.Load(),
		Accepted:        r.accepted.Load(),
		Failed:          r.failed.Load(),
		SchedulerBehind: r.schedulerBehind.Load(),
		AchievedRate:    float64(r.accepted.Load()) / r.wallClock.Seconds(),
		Accept:          percentiles(r.latencies),
		Codes:           codes,
	}
}

// measureEndToEnd reads the true pipeline latency out of the data: every row
// carries both the time the API accepted it and the time the worker persisted it.
// No clock skew is possible because both timestamps come from services on the
// same host, and no instrumentation is needed on the read path at all.
func measureEndToEnd(ctx context.Context, cfg config, want int64) (*latencyStats, error) {
	pool, err := pgxpool.New(ctx, cfg.dsn)
	if err != nil {
		return nil, err
	}
	defer pool.Close()

	pattern := cfg.tag + "-%"

	// Wait for the pipeline to catch up before measuring, otherwise the
	// percentiles describe only the orders that happened to be fast.
	deadline := time.Now().Add(cfg.drain)
	var persisted int64
	for time.Now().Before(deadline) {
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM orders WHERE customer_id LIKE $1`, pattern).Scan(&persisted); err != nil {
			return nil, err
		}
		if persisted >= want {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}

	var s latencyStats
	err = pool.QueryRow(ctx, `
		SELECT count(*),
		       coalesce(percentile_cont(0.50) WITHIN GROUP (ORDER BY d), 0),
		       coalesce(percentile_cont(0.95) WITHIN GROUP (ORDER BY d), 0),
		       coalesce(percentile_cont(0.99) WITHIN GROUP (ORDER BY d), 0),
		       coalesce(max(d), 0)
		FROM (
			SELECT extract(epoch FROM (processed_at - occurred_at)) AS d
			FROM orders WHERE customer_id LIKE $1
		) t`, pattern).Scan(&s.Count, &s.P50, &s.P95, &s.P99, &s.Max)
	if err != nil {
		return nil, err
	}
	s.Missing = want - s.Count
	return &s, nil
}

type latencyStats struct {
	Count   int64   `json:"count"`
	Missing int64   `json:"missing"`
	P50     float64 `json:"p50_seconds"`
	P95     float64 `json:"p95_seconds"`
	P99     float64 `json:"p99_seconds"`
	Max     float64 `json:"max_seconds"`
}

type report struct {
	Tag             string           `json:"tag"`
	TargetRate      float64          `json:"target_rate"`
	AchievedRate    float64          `json:"achieved_rate"`
	Duration        string           `json:"duration"`
	WallClock       float64          `json:"wall_clock_seconds"`
	Concurrency     int              `json:"concurrency"`
	Sent            int64            `json:"sent"`
	Accepted        int64            `json:"accepted"`
	Failed          int64            `json:"failed"`
	SchedulerBehind int64            `json:"scheduler_behind"`
	Accept          *latencyStats    `json:"accept_latency"`
	EndToEnd        *latencyStats    `json:"end_to_end_latency,omitempty"`
	Codes           map[string]int64 `json:"status_codes"`
}

func percentiles(sorted []time.Duration) *latencyStats {
	if len(sorted) == 0 {
		return &latencyStats{}
	}
	at := func(q float64) float64 {
		i := int(math.Ceil(q*float64(len(sorted)))) - 1
		if i < 0 {
			i = 0
		}
		if i >= len(sorted) {
			i = len(sorted) - 1
		}
		return sorted[i].Seconds()
	}
	return &latencyStats{
		Count: int64(len(sorted)),
		P50:   at(0.50),
		P95:   at(0.95),
		P99:   at(0.99),
		Max:   sorted[len(sorted)-1].Seconds(),
	}
}

func (r *report) print(w io.Writer) {
	fmt.Fprintf(w, "\n=== %s ===\n", r.Tag)
	fmt.Fprintf(w, "target rate      %8.1f/s\n", r.TargetRate)
	fmt.Fprintf(w, "achieved rate    %8.1f/s   (%.1f%% of target)\n",
		r.AchievedRate, 100*r.AchievedRate/r.TargetRate)
	fmt.Fprintf(w, "wall clock       %8.1fs\n", r.WallClock)
	fmt.Fprintf(w, "sent / accepted  %8d / %d\n", r.Sent, r.Accepted)
	if r.Failed > 0 {
		fmt.Fprintf(w, "failed           %8d\n", r.Failed)
	}
	if r.SchedulerBehind > 0 {
		fmt.Fprintf(w, "scheduler behind %8d   (generator could not keep up; treat results with suspicion)\n",
			r.SchedulerBehind)
	}
	fmt.Fprintf(w, "status codes     %v\n", r.Codes)

	fmt.Fprintf(w, "\naccept latency (client -> 202, measured from scheduled send time)\n")
	printStats(w, r.Accept)

	if r.EndToEnd != nil {
		fmt.Fprintf(w, "\nend-to-end latency (api accepted -> row committed)\n")
		printStats(w, r.EndToEnd)
		if r.EndToEnd.Missing > 0 {
			fmt.Fprintf(w, "  MISSING        %8d rows accepted but not persisted within the drain window\n",
				r.EndToEnd.Missing)
		}
	}
}

func printStats(w io.Writer, s *latencyStats) {
	fmt.Fprintf(w, "  count          %8d\n", s.Count)
	fmt.Fprintf(w, "  p50            %8.1f ms\n", s.P50*1000)
	fmt.Fprintf(w, "  p95            %8.1f ms\n", s.P95*1000)
	fmt.Fprintf(w, "  p99            %8.1f ms\n", s.P99*1000)
	fmt.Fprintf(w, "  max            %8.1f ms\n", s.Max*1000)
}

func (r *report) writeJSON(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}
