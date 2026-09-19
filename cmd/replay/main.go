// Command replay returns dead-lettered records to the source topic.
//
// The dead-letter queue is where records go when the pipeline cannot account for
// them. It is not where they are supposed to stay. The database-outage
// experiment parked records with reason "retries_exhausted" — records that would
// have been written had the outage been shorter than the retry budget — and
// recovering them means producing those same bytes back into the orders topic and
// letting the ordinary worker path finish the job it could not finish at the time.
// Without this command the DLQ is a place where orders go to be counted, which is
// the write-only dead-letter queue every design document warns about.
//
// Three properties make running it safe, and they are most of the design:
//
//  1. The write is idempotent. The worker deduplicates on event_id, so replaying
//     a record that did in fact land is a no-op, not a duplicate order. That is
//     what makes "when unsure, replay" the correct instinct rather than a risk.
//
//  2. Only records that can succeed are replayed. A poison record failed because
//     of what it contains, so replaying it produces the identical failure and a
//     second dead letter; the default reason filter is retries_exhausted, and
//     poison has to be asked for by name.
//
//  3. A record cannot circulate forever. Each replay stamps replay_count, the DLQ
//     carries that header forward when the record dies again, and a record that
//     has been round-tripped -max-replays times is left alone and reported. A
//     replay tool without this turns one permanent failure into an infinite
//     DLQ -> orders -> DLQ circuit that looks like throughput.
//
// It is a dry run unless -apply is given, because the first thing anyone wants
// from a DLQ at 3am is to see what is in it. Nothing is consumed by a dry run
// either: offsets are committed only for records that were successfully produced,
// which is the same rule the worker applies to its own writes.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gigagedianidze/order-pipeline/internal/broker"
	"github.com/gigagedianidze/order-pipeline/internal/config"
	"github.com/gigagedianidze/order-pipeline/internal/order"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

// joinTimeout bounds the wait for the group's first assignment. Past it, an
// empty poll is reported as an empty dead-letter topic — which is the honest
// answer when the group never formed, and better than a command that never
// returns from a script or a console button.
const joinTimeout = 30 * time.Second

type options struct {
	apply      bool
	reasons    string
	group      string
	idle       time.Duration
	limit      int
	maxReplays int
	verify     time.Duration
	jsonOut    string
}

func main() {
	var opts options
	flag.BoolVar(&opts.apply, "apply", false,
		"actually produce the records back; without it nothing is sent and nothing is committed")
	flag.StringVar(&opts.reasons, "reason", string(broker.ReasonRetriesExhausted),
		`which dlq_reason values to replay: a comma-separated list, or "all"`)
	flag.StringVar(&opts.group, "group", "order-dlq-replay",
		"consumer group for reading the dead-letter topic; a rerun resumes where the last one committed")
	flag.DurationVar(&opts.idle, "idle", 5*time.Second,
		"stop once the dead-letter topic has produced nothing for this long")
	flag.IntVar(&opts.limit, "limit", 0, "stop after replaying this many records; 0 means no limit")
	flag.IntVar(&opts.maxReplays, "max-replays", 2,
		"leave a record alone once it has already been replayed this many times")
	flag.DurationVar(&opts.verify, "verify", 30*time.Second,
		"how long to wait for replayed records to appear in PostgreSQL; 0 to skip the check")
	flag.StringVar(&opts.jsonOut, "json", "", "also write the report as JSON to this path")
	flag.Parse()

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid configuration: %v\n", err)
		os.Exit(1)
	}
	log := cfg.Logger("replay")

	if opts.maxReplays < 1 {
		fmt.Fprintln(os.Stderr, "replay: -max-replays must be at least 1")
		os.Exit(1)
	}
	wanted, err := parseReasons(opts.reasons)
	if err != nil {
		fmt.Fprintf(os.Stderr, "replay: %v\n", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rep, err := run(ctx, cfg, opts, wanted, log)
	if err != nil {
		fmt.Fprintf(os.Stderr, "replay: %v\n", err)
		os.Exit(1)
	}

	rep.print(os.Stdout)
	if opts.jsonOut != "" {
		if err := rep.writeJSON(opts.jsonOut); err != nil {
			fmt.Fprintf(os.Stderr, "replay: write json: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("\nreport written to %s\n", opts.jsonOut)
	}

	// A replay that produced nothing is a successful inspection; a replay that
	// lost records, or whose records never reached the database, is not. Exit
	// codes matter here because this runs from a script.
	if rep.Failed > 0 || (rep.Verify != nil && rep.Verify.Missing > 0) {
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config.Config, opts options, wanted map[string]bool, log *slog.Logger) (*report, error) {
	client, owned, err := broker.NewConsumerGroup(broker.ConsumerOptions{
		Brokers:      cfg.KafkaBrokers,
		Topic:        cfg.KafkaDLQTopic,
		GroupID:      opts.group,
		Log:          log,
		ManualCommit: true,
	})
	if err != nil {
		return nil, err
	}
	defer client.Close()

	producer, err := broker.NewProducer(cfg.KafkaBrokers, cfg.KafkaTopic, log)
	if err != nil {
		return nil, err
	}
	defer producer.Close()

	log.Info("reading dead letters",
		"from", cfg.KafkaDLQTopic, "to", cfg.KafkaTopic, "group", opts.group,
		"reasons", opts.reasons, "apply", opts.apply)

	rep := &report{
		DLQTopic:    cfg.KafkaDLQTopic,
		SourceTopic: cfg.KafkaTopic,
		Reasons:     opts.reasons,
		DryRun:      !opts.apply,
		MaxReplays:  opts.maxReplays,
		Skipped:     map[string]int64{},
	}
	var replayed []string // event ids, for the verification pass
	started := time.Now()

	// Joining a consumer group takes a rebalance, and until it finishes this
	// member owns nothing and every poll comes back empty. An empty poll during
	// the join says nothing about the backlog, so "idle" only starts meaning
	// "drained" once there are partitions to be idle on.
	joinBy := started.Add(joinTimeout)

	for {
		if opts.limit > 0 && rep.Replayed >= int64(opts.limit) {
			break
		}
		// An idle poll is the end of the backlog. The dead-letter topic is not a
		// stream to follow: it holds a finite set of failures, and a run that
		// never returns could not be used from a script or a console button.
		pollCtx, cancel := context.WithTimeout(ctx, opts.idle)
		fetches := client.PollFetches(pollCtx)
		cancel()

		if fetches.IsClientClosed() {
			break
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
				log.Error("fetch error", "topic", topic, "partition", partition, "err", err)
			}
		})
		if fetches.NumRecords() == 0 {
			if ctx.Err() != nil {
				log.Info("interrupted; stopping after committing what was replayed")
				break
			}
			if owned.Count() == 0 && time.Now().Before(joinBy) {
				log.Debug("waiting to join the group", "group", opts.group)
				continue
			}
			break // drained
		}

		stop := false
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if stop {
				return
			}
			last, ids, err := replayPartition(ctx, producer, p.Records, opts, wanted, rep, log)
			replayed = append(replayed, ids...)
			if last != nil && opts.apply {
				// Commit only what is already back on the source topic, for the
				// same reason the worker commits only what is already in the
				// database: an offset committed past a record that was never
				// produced is that record deleted.
				if cerr := client.CommitRecords(ctx, last); cerr != nil {
					log.Error("commit offsets", "err", cerr)
				}
			}
			if err != nil {
				// Producing failed, which means the source topic is refusing
				// writes. Everything behind this record would fail the same way,
				// and continuing would only build a longer list of failures.
				log.Error("stopping: could not produce", "err", err)
				stop = true
			}
		})
		if stop || ctx.Err() != nil {
			break
		}
	}

	rep.Seconds = time.Since(started).Seconds()
	log.Info("replay finished",
		"scanned", rep.Scanned, "replayed", rep.Replayed, "failed", rep.Failed, "dry_run", rep.DryRun)

	if opts.apply && opts.verify > 0 && len(replayed) > 0 {
		v, err := verify(ctx, cfg.PostgresDSN, replayed, opts.verify)
		if err != nil {
			return nil, fmt.Errorf("verify: %w", err)
		}
		rep.Verify = v
	}
	return rep, nil
}

// replayPartition walks one partition's dead letters in offset order and returns
// the last record that is accounted for — replayed, or deliberately skipped.
//
// A skipped record is accounted for: leaving a poison record uncommitted would
// mean every future run re-reads it, and the run after that, until the only thing
// the tool reports is the same record it already decided not to touch.
func replayPartition(
	ctx context.Context,
	producer *broker.Producer,
	recs []*kgo.Record,
	opts options,
	wanted map[string]bool,
	rep *report,
	log *slog.Logger,
) (last *kgo.Record, eventIDs []string, err error) {
	for _, rec := range recs {
		if ctx.Err() != nil {
			return last, eventIDs, nil
		}
		if opts.limit > 0 && rep.Replayed >= int64(opts.limit) {
			return last, eventIDs, nil
		}
		rep.Scanned++

		d := decide(rec, wanted, opts.maxReplays)
		if !d.replay {
			rep.Skipped[d.skip]++
			log.Debug("skipping dead letter",
				"why", d.skip, "dlq_partition", rec.Partition, "dlq_offset", rec.Offset)
			last = rec
			continue
		}

		if !opts.apply {
			rep.Replayed++ // a dry run reports what it would have done, and commits nothing
			continue
		}

		out := &kgo.Record{
			Key:     rec.Key,
			Value:   rec.Value, // byte-for-byte: the worker must see the original event
			Headers: replayHeaders(rec, d.count+1, time.Now()),
		}
		partition, offset, perr := producer.PublishRecord(ctx, out)
		if perr != nil {
			rep.Failed++
			return last, eventIDs, perr
		}

		rep.Replayed++
		last = rec
		if id := eventID(rec.Value); id != "" {
			eventIDs = append(eventIDs, id)
		} else {
			rep.Unreadable++
		}
		log.Info("replayed",
			"dlq_partition", rec.Partition, "dlq_offset", rec.Offset,
			"partition", partition, "offset", offset, "replay_count", d.count+1)
	}
	return last, eventIDs, nil
}

// decision is what to do with one dead letter, and why.
type decision struct {
	replay bool
	skip   string
	count  int // how many times this record has already been replayed
}

// decide applies the reason filter and the loop guard. It reads only headers, so
// the rules that govern what gets sent back into the pipeline can be tested
// without a broker.
func decide(rec *kgo.Record, wanted map[string]bool, maxReplays int) decision {
	reason, hasReason := header(rec, broker.HeaderReason)
	count := replayCount(rec)

	switch {
	case !hasReason:
		// Not written by this pipeline's dead-letter path. Something else put it
		// on the topic, and this tool has no idea whether it is safe to replay.
		return decision{skip: "no_dlq_reason", count: count}
	case !wanted["all"] && !wanted[reason]:
		return decision{skip: "reason:" + reason, count: count}
	case count >= maxReplays:
		return decision{skip: "replay_limit", count: count}
	}
	return decision{replay: true, count: count}
}

// replayHeaders stamps provenance on the record going back to the source topic.
//
// The dlq_ headers are deliberately not carried over: they describe the failure
// this record is being given another chance after, and a fresh set is written if
// it fails again. What survives is the pointer back into the dead-letter topic,
// which is where that diagnosis is still readable at the recorded offset.
func replayHeaders(rec *kgo.Record, count int, now time.Time) []kgo.RecordHeader {
	return []kgo.RecordHeader{
		{Key: broker.HeaderReplayCount, Value: []byte(strconv.Itoa(count))},
		{Key: broker.HeaderReplayAt, Value: []byte(now.UTC().Format(time.RFC3339Nano))},
		{Key: broker.HeaderReplayOfPartition, Value: []byte(strconv.Itoa(int(rec.Partition)))},
		{Key: broker.HeaderReplayOfOffset, Value: []byte(strconv.FormatInt(rec.Offset, 10))},
	}
}

func header(rec *kgo.Record, key string) (string, bool) {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}

// replayCount reads the loop guard's counter. An absent or unreadable header
// means zero: a record that has never been replayed, which is the common case and
// the safe reading — the guard still fires on the next round trip.
func replayCount(rec *kgo.Record) int {
	raw, ok := header(rec, broker.HeaderReplayCount)
	if !ok {
		return 0
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

func eventID(value []byte) string {
	var evt order.Event
	if err := json.Unmarshal(value, &evt); err != nil {
		return "" // a poison record; there is no id to verify against
	}
	return evt.EventID
}

func parseReasons(s string) (map[string]bool, error) {
	out := map[string]bool{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		switch part {
		case "all", string(broker.ReasonPoison), string(broker.ReasonRetriesExhausted):
			out[part] = true
		default:
			return nil, fmt.Errorf("unknown -reason %q: use %q, %q or \"all\"",
				part, broker.ReasonRetriesExhausted, broker.ReasonPoison)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("-reason must name at least one reason")
	}
	return out, nil
}

// verify answers the only question that matters after a replay: are the records
// in the database now?
//
// Producing to Kafka proves the record was handed back to the pipeline, not that
// the pipeline finished with it — the worker still has to pick it up and write
// it. Polling for the rows is the difference between "replay ran" and "the
// outage is recovered", and it is the number the experiment reports.
func verify(ctx context.Context, dsn string, eventIDs []string, wait time.Duration) (*verifyStats, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer pool.Close()

	stats := &verifyStats{Checked: int64(len(eventIDs))}
	started := time.Now()
	deadline := started.Add(wait)

	for {
		var found int64
		err := pool.QueryRow(ctx,
			`SELECT count(*) FROM orders WHERE event_id = ANY($1::uuid[])`, eventIDs).Scan(&found)
		if err != nil {
			return nil, err
		}
		stats.Persisted = found
		if found >= stats.Checked || time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}

	stats.Missing = stats.Checked - stats.Persisted
	stats.Seconds = time.Since(started).Seconds()
	return stats, nil
}

type verifyStats struct {
	Checked   int64   `json:"checked"`
	Persisted int64   `json:"persisted"`
	Missing   int64   `json:"missing"`
	Seconds   float64 `json:"wait_seconds"`
}

type report struct {
	DLQTopic    string           `json:"dlq_topic"`
	SourceTopic string           `json:"source_topic"`
	Reasons     string           `json:"reasons"`
	DryRun      bool             `json:"dry_run"`
	MaxReplays  int              `json:"max_replays"`
	Scanned     int64            `json:"scanned"`
	Replayed    int64            `json:"replayed"`
	Failed      int64            `json:"failed"`
	Unreadable  int64            `json:"unreadable"`
	Skipped     map[string]int64 `json:"skipped"`
	Seconds     float64          `json:"seconds"`
	Verify      *verifyStats     `json:"verify,omitempty"`
}

func (r *report) print(w io.Writer) {
	mode := "APPLIED"
	if r.DryRun {
		mode = "DRY RUN — nothing produced, nothing committed; rerun with -apply"
	}
	fmt.Fprintf(w, "\n=== dlq replay: %s -> %s ===\n", r.DLQTopic, r.SourceTopic)
	fmt.Fprintf(w, "mode             %s\n", mode)
	fmt.Fprintf(w, "reasons          %s   (max %d replays per record)\n", r.Reasons, r.MaxReplays)
	fmt.Fprintf(w, "scanned          %8d\n", r.Scanned)
	fmt.Fprintf(w, "replayed         %8d\n", r.Replayed)
	if r.Failed > 0 {
		fmt.Fprintf(w, "failed           %8d   (could not produce; the rest was left on the dlq)\n", r.Failed)
	}
	if r.Unreadable > 0 {
		fmt.Fprintf(w, "unreadable       %8d   (replayed, but no event_id to verify against)\n", r.Unreadable)
	}
	for _, why := range sortedKeys(r.Skipped) {
		fmt.Fprintf(w, "skipped          %8d   %s\n", r.Skipped[why], why)
	}
	fmt.Fprintf(w, "took             %8.1fs\n", r.Seconds)

	if r.Verify != nil {
		fmt.Fprintf(w, "\npersisted after replay (api -> kafka -> worker -> postgres)\n")
		fmt.Fprintf(w, "  checked        %8d\n", r.Verify.Checked)
		fmt.Fprintf(w, "  persisted      %8d\n", r.Verify.Persisted)
		if r.Verify.Missing > 0 {
			fmt.Fprintf(w, "  MISSING        %8d   replayed but not in the database within the wait\n",
				r.Verify.Missing)
		}
		fmt.Fprintf(w, "  waited         %8.1fs\n", r.Verify.Seconds)
	}
}

func sortedKeys(m map[string]int64) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
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
