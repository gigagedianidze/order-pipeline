package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

// prom is a minimal Prometheus query client.
//
// Minimal on purpose: the official client pulls in a large dependency tree to
// give back the same float this needs two fields of JSON to produce. The console
// asks a fixed list of questions and shows the answers; that does not justify a
// library.
type prom struct {
	base string
	http *http.Client
}

func newProm(base string) *prom {
	return &prom{
		base: base,
		// A timeout well under the UI's poll interval. A Prometheus that has
		// gone away must make the dashboard show gaps, not make it hang: the
		// console has to stay usable precisely when the stack is broken, since
		// breaking the stack is half of what it is for.
		http: &http.Client{Timeout: 2 * time.Second},
	}
}

// tiles are the instant queries behind the numbers on the page.
//
// Counters are shown as totals since the services started, which is what makes
// "accepted 300001 / persisted 299995" a meaningful pair. Rates use a 30s window:
// long enough to be steady at Prometheus's 5s scrape interval, short enough that
// a 15s load run is visible while it is still running.
var tiles = []struct {
	Key   string
	Label string
	Query string
	Unit  string
}{
	{"accepted", "Accepted", `sum(orders_accepted_total)`, "orders"},
	{"persisted", "Persisted", `sum(orders_processed_total{status="persisted"})`, "orders"},
	{"duplicate", "Duplicates", `sum(orders_processed_total{status="duplicate"})`, "orders"},
	{"rejected", "Rejected", `sum(orders_rejected_total)`, "orders"},
	{"dlq", "Dead letters", `sum(dlq_total)`, "records"},
	{"retries", "Retries", `sum(retries_total)`, "attempts"},

	{"accept_rate", "Accept rate", `sum(rate(orders_accepted_total[30s]))`, "/s"},
	{"throughput", "Persist rate", `sum(rate(orders_processed_total{status="persisted"}[30s]))`, "/s"},

	// Broker-side lag, from kafka-exporter rather than from the workers. The
	// workers' own gauge goes stale at its last value when they die, reporting a
	// healthy 0 while a backlog grows, which is exactly when lag matters.
	{"lag", "Consumer lag", `sum(kafka_consumergroup_lag{consumergroup="order-processors"})`, "records"},
	{"partitions_owned", "Partitions owned", `sum(consumer_partitions_owned)`, ""},
	{"partitions_paused", "Partitions paused", `sum(consumer_partitions_paused)`, ""},

	{"p99_accept", "p99 produce", `histogram_quantile(0.99, sum by (le) (rate(kafka_produce_duration_seconds_bucket[1m])))`, "s"},
	{"p99_persist", "p99 persist", `histogram_quantile(0.99, sum by (le) (rate(order_processing_duration_seconds_bucket{status="persisted"}[1m])))`, "s"},
	{"p99_e2e", "p99 end-to-end", `histogram_quantile(0.99, sum by (le) (rate(order_end_to_end_latency_seconds_bucket[1m])))`, "s"},
}

// sparks are the range queries behind the small charts.
var sparks = []struct {
	Key   string
	Query string
}{
	{"lag", `sum(kafka_consumergroup_lag{consumergroup="order-processors"})`},
	{"throughput", `sum(rate(orders_processed_total{status="persisted"}[30s]))`},
	{"accept_rate", `sum(rate(orders_accepted_total[30s]))`},
}

type tileValue struct {
	Label string   `json:"label"`
	Unit  string   `json:"unit"`
	Value *float64 `json:"value"` // nil when Prometheus has no sample
}

type promSnapshot struct {
	Tiles     map[string]tileValue    `json:"tiles"`
	Sparks    map[string][][2]float64 `json:"sparks"`
	Reachable bool                    `json:"reachable"`
	Err       string                  `json:"err,omitempty"`
}

// snapshot runs every query concurrently.
//
// Serially this is fifteen round trips per poll, which at a 2s poll interval
// leaves the dashboard permanently a second behind itself. Prometheus has no
// multi-query endpoint, so concurrency is the only lever.
func (p *prom) snapshot(ctx context.Context) promSnapshot {
	out := promSnapshot{
		Tiles:     make(map[string]tileValue, len(tiles)),
		Sparks:    make(map[string][][2]float64, len(sparks)),
		Reachable: true,
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	var firstErr error

	for _, t := range tiles {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := p.instant(ctx, t.Query)
			mu.Lock()
			defer mu.Unlock()
			if err != nil && firstErr == nil {
				firstErr = err
			}
			out.Tiles[t.Key] = tileValue{Label: t.Label, Unit: t.Unit, Value: v}
		}()
	}

	end := time.Now()
	start := end.Add(-5 * time.Minute)
	for _, s := range sparks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			series, err := p.rangeQuery(ctx, s.Query, start, end, 10*time.Second)
			mu.Lock()
			defer mu.Unlock()
			if err != nil && firstErr == nil {
				firstErr = err
			}
			out.Sparks[s.Key] = series
		}()
	}
	wg.Wait()

	if firstErr != nil {
		out.Reachable = false
		out.Err = firstErr.Error()
	}
	return out
}

func (p *prom) instant(ctx context.Context, q string) (*float64, error) {
	var body struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Value [2]json.RawMessage `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	v := url.Values{"query": {q}}
	if err := p.get(ctx, "/api/v1/query", v, &body); err != nil {
		return nil, err
	}
	if body.Status != "success" || len(body.Data.Result) == 0 {
		// No sample is not an error. A counter that has never been incremented
		// simply does not exist yet, and showing a dash is more honest than
		// showing a zero that claims the query ran and the answer was none.
		return nil, nil
	}
	return parseSample(body.Data.Result[0].Value[1]), nil
}

func (p *prom) rangeQuery(ctx context.Context, q string, start, end time.Time, step time.Duration) ([][2]float64, error) {
	var body struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Values [][2]json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	v := url.Values{
		"query": {q},
		"start": {strconv.FormatInt(start.Unix(), 10)},
		"end":   {strconv.FormatInt(end.Unix(), 10)},
		"step":  {strconv.Itoa(int(step.Seconds())) + "s"},
	}
	if err := p.get(ctx, "/api/v1/query_range", v, &body); err != nil {
		return nil, err
	}
	if body.Status != "success" || len(body.Data.Result) == 0 {
		return [][2]float64{}, nil
	}

	raw := body.Data.Result[0].Values
	out := make([][2]float64, 0, len(raw))
	for _, pair := range raw {
		var ts float64
		if err := json.Unmarshal(pair[0], &ts); err != nil {
			continue
		}
		val := parseSample(pair[1])
		if val == nil {
			continue
		}
		out = append(out, [2]float64{ts, *val})
	}
	return out, nil
}

func (p *prom) get(ctx context.Context, path string, params url.Values, into any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.base+path+"?"+params.Encode(), nil)
	if err != nil {
		return err
	}
	res, err := p.http.Do(req)
	if err != nil {
		return fmt.Errorf("prometheus unreachable at %s", p.base)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("prometheus returned %s", res.Status)
	}
	return json.NewDecoder(res.Body).Decode(into)
}

// parseSample reads Prometheus's value, which is a string rather than a number
// on the wire so that NaN and Inf survive JSON. histogram_quantile returns NaN
// whenever its window holds no observations, which for these queries is most of
// the time — every idle moment between experiments — so NaN has to become "no
// value" rather than being rendered.
func parseSample(raw json.RawMessage) *float64 {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return nil
	}
	return &f
}
