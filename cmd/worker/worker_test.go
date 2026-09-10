package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/gigagedianidze/order-pipeline/internal/broker"
	"github.com/gigagedianidze/order-pipeline/internal/order"
	"github.com/gigagedianidze/order-pipeline/internal/retry"
	"github.com/gigagedianidze/order-pipeline/internal/store"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/twmb/franz-go/pkg/kgo"
)

// These tests cover the invariant the whole system rests on: an offset may only
// advance past a record that is durably accounted for. It was previously only
// checked by hand, against a running stack, by reading `kafka-consumer-groups.sh
// --describe` — which is a fine way to discover the bug (FINDINGS.md, Day 4) and
// a poor way to keep it fixed.

// fakeStore is an in-memory stand-in for Postgres with programmable failures.
type fakeStore struct {
	mu       sync.Mutex
	seen     map[string]bool  // event_id -> present
	failWith map[string]error // event_id -> error to return instead of inserting
	batchErr error            // if set, every batch insert fails with this
	calls    int
	batches  int
}

func newFakeStore() *fakeStore {
	return &fakeStore{seen: map[string]bool{}, failWith: map[string]error{}}
}

func (f *fakeStore) InsertOrder(_ context.Context, evt order.Event) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if err, ok := f.failWith[evt.EventID]; ok {
		return false, err
	}
	if f.seen[evt.EventID] {
		return false, nil
	}
	f.seen[evt.EventID] = true
	return true, nil
}

func (f *fakeStore) InsertOrders(_ context.Context, evts []order.Event) (map[string]bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batches++
	if f.batchErr != nil {
		return nil, f.batchErr
	}
	// A batch is all-or-nothing: one bad row and nothing lands.
	for _, evt := range evts {
		if err, ok := f.failWith[evt.EventID]; ok {
			return nil, err
		}
	}
	inserted := map[string]bool{}
	for _, evt := range evts {
		f.calls++
		if !f.seen[evt.EventID] {
			f.seen[evt.EventID] = true
			inserted[evt.EventID] = true
		}
	}
	return inserted, nil
}

func (f *fakeStore) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.seen)
}

// fakeDLQ records what was parked, and can be made unreachable.
type fakeDLQ struct {
	mu      sync.Mutex
	sent    []*kgo.Record
	reasons []broker.Reason
	sendErr error
}

func (d *fakeDLQ) Send(_ context.Context, rec *kgo.Record, reason broker.Reason, _ error, _ int) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.sendErr != nil {
		return d.sendErr
	}
	d.sent = append(d.sent, rec)
	d.reasons = append(d.reasons, reason)
	return nil
}

func (d *fakeDLQ) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.sent)
}

func newTestWorker(db orderStore, dlq deadLetterQueue, batchSize int) *worker {
	return &worker{
		db:  db,
		dlq: dlq,
		log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		// A tiny budget: these tests assert on classification, not on patience.
		policy:    retry.Policy{Base: time.Millisecond, Max: 2 * time.Millisecond, MaxElapsed: 20 * time.Millisecond},
		batchSize: batchSize,
		c:         &counters{},
	}
}

// records builds a partition's worth of records at consecutive offsets.
func records(partition int32, startOffset int64, n int) []*kgo.Record {
	out := make([]*kgo.Record, n)
	for i := range out {
		evt := order.NewEvent(order.CreateRequest{
			CustomerID: "cust",
			Items:      []order.Item{{SKU: "A", Quantity: 1, UnitPriceCents: 100}},
		}, "")
		value, _ := json.Marshal(evt)
		out[i] = &kgo.Record{
			Topic:     "orders",
			Partition: partition,
			Offset:    startOffset + int64(i),
			Key:       []byte(evt.OrderID),
			Value:     value,
		}
	}
	return out
}

func eventID(t *testing.T, rec *kgo.Record) string {
	t.Helper()
	var evt order.Event
	if err := json.Unmarshal(rec.Value, &evt); err != nil {
		t.Fatalf("test record is not a valid event: %v", err)
	}
	return evt.EventID
}

func TestProcessPartitionCommitsEveryRecordOnSuccess(t *testing.T) {
	db, dlq := newFakeStore(), &fakeDLQ{}
	w := newTestWorker(db, dlq, 1)
	recs := records(0, 10, 5)

	last, blocked := w.processPartition(context.Background(), recs)

	if blocked {
		t.Fatal("partition blocked with no failures")
	}
	if last == nil || last.Offset != 14 {
		t.Fatalf("committable offset = %v, want 14", last)
	}
	if db.count() != 5 {
		t.Errorf("persisted %d records, want 5", db.count())
	}
}

// The bug this project was built to understand. A record that can be neither
// persisted nor parked must stop its partition dead, and nothing behind it may
// be committed — otherwise the offset advances past a record that was never
// written and lag reads a healthy zero.
func TestProcessPartitionStopsAtUnaccountableRecord(t *testing.T) {
	db, dlq := newFakeStore(), &fakeDLQ{}
	recs := records(1, 20, 5) // offsets 20..24

	// Offset 22 cannot be written, and the dead-letter queue is unreachable.
	db.failWith[eventID(t, recs[2])] = errors.New("connection refused")
	dlq.sendErr = errors.New("kafka unreachable")

	w := newTestWorker(db, dlq, 1)
	last, blocked := w.processPartition(context.Background(), recs)

	if !blocked {
		t.Fatal("partition not blocked by an unaccountable record")
	}
	if last == nil || last.Offset != 21 {
		t.Fatalf("committable offset = %v, want 21 (the record before the failure)", last)
	}
	if db.count() != 2 {
		t.Errorf("persisted %d records, want 2: nothing behind the failure may be written", db.count())
	}
}

// A record that is dead-lettered *is* accounted for, so its partition keeps
// moving. A poison message degrades one record, not one partition.
func TestPoisonRecordIsParkedAndPartitionContinues(t *testing.T) {
	db, dlq := newFakeStore(), &fakeDLQ{}
	recs := records(0, 0, 3)
	recs[1].Value = []byte("{not json")

	w := newTestWorker(db, dlq, 1)
	last, blocked := w.processPartition(context.Background(), recs)

	if blocked {
		t.Fatal("a dead-lettered record must not block the partition")
	}
	if last == nil || last.Offset != 2 {
		t.Fatalf("committable offset = %v, want 2", last)
	}
	if dlq.count() != 1 {
		t.Fatalf("dead-lettered %d records, want 1", dlq.count())
	}
	if dlq.reasons[0] != broker.ReasonPoison {
		t.Errorf("reason = %q, want %q", dlq.reasons[0], broker.ReasonPoison)
	}
}

// A constraint violation is the data's fault and will never succeed, so it must
// be parked immediately rather than burning the retry budget first.
func TestConstraintViolationIsPoisonNotRetried(t *testing.T) {
	db, dlq := newFakeStore(), &fakeDLQ{}
	recs := records(0, 0, 1)
	db.failWith[eventID(t, recs[0])] = &pgconn.PgError{Code: "23505", Message: "duplicate key"}

	w := newTestWorker(db, dlq, 1)
	if _, blocked := w.processPartition(context.Background(), recs); blocked {
		t.Fatal("a parked record must not block the partition")
	}

	if dlq.reasons[0] != broker.ReasonPoison {
		t.Errorf("reason = %q, want %q", dlq.reasons[0], broker.ReasonPoison)
	}
	if db.calls != 1 {
		t.Errorf("attempted the write %d times, want 1: a constraint violation is not retryable", db.calls)
	}
}

// A transient failure that outlasts the budget is a different story from poison,
// and the dead-letter queue has to be able to tell them apart.
func TestExhaustedRetriesAreLabelledSeparately(t *testing.T) {
	db, dlq := newFakeStore(), &fakeDLQ{}
	recs := records(0, 0, 1)
	db.failWith[eventID(t, recs[0])] = &pgconn.PgError{Code: "08006", Message: "connection failure"}

	w := newTestWorker(db, dlq, 1)
	w.processPartition(context.Background(), recs)

	if dlq.reasons[0] != broker.ReasonRetriesExhausted {
		t.Errorf("reason = %q, want %q", dlq.reasons[0], broker.ReasonRetriesExhausted)
	}
	if db.calls < 2 {
		t.Errorf("attempted the write %d times, want more than 1: a connection failure is retryable", db.calls)
	}
}

// Redelivery is the normal cost of at-least-once. It must be silent, must not
// touch the dead-letter queue, and must still let the offset advance.
func TestRedeliveredRecordsAreDuplicatesNotFailures(t *testing.T) {
	db, dlq := newFakeStore(), &fakeDLQ{}
	w := newTestWorker(db, dlq, 1)
	recs := records(0, 0, 3)

	w.processPartition(context.Background(), recs)
	last, blocked := w.processPartition(context.Background(), recs) // the same records again

	if blocked {
		t.Fatal("redelivery blocked the partition")
	}
	if last == nil || last.Offset != 2 {
		t.Fatalf("committable offset = %v, want 2", last)
	}
	if db.count() != 3 {
		t.Errorf("stored %d rows for 6 deliveries, want 3", db.count())
	}
	if dlq.count() != 0 {
		t.Errorf("dead-lettered %d records, want 0", dlq.count())
	}
	if w.c.duplicates.Load() != 3 {
		t.Errorf("duplicates counted = %d, want 3", w.c.duplicates.Load())
	}
}

func TestBatchInsertPersistsWholeChunk(t *testing.T) {
	db, dlq := newFakeStore(), &fakeDLQ{}
	w := newTestWorker(db, dlq, 10)
	recs := records(0, 0, 25)

	last, blocked := w.processPartition(context.Background(), recs)

	if blocked {
		t.Fatal("partition blocked with no failures")
	}
	if last == nil || last.Offset != 24 {
		t.Fatalf("committable offset = %v, want 24", last)
	}
	if db.count() != 25 {
		t.Errorf("persisted %d records, want 25", db.count())
	}
	if db.batches != 3 { // 10 + 10 + 5
		t.Errorf("made %d batch calls, want 3", db.batches)
	}
}

// Batching must not cost poison isolation. One bad record in a chunk takes the
// batch down, and the fallback has to let its blameless neighbours through.
func TestBatchFallsBackToSingleRecordsOnFailure(t *testing.T) {
	db, dlq := newFakeStore(), &fakeDLQ{}
	recs := records(0, 0, 5)
	db.failWith[eventID(t, recs[3])] = &pgconn.PgError{Code: "23505", Message: "duplicate key"}

	w := newTestWorker(db, dlq, 5)
	last, blocked := w.processPartition(context.Background(), recs)

	if blocked {
		t.Fatal("a parked record must not block the partition")
	}
	if last == nil || last.Offset != 4 {
		t.Fatalf("committable offset = %v, want 4", last)
	}
	if db.count() != 4 {
		t.Errorf("persisted %d records, want 4 (all but the poison one)", db.count())
	}
	if dlq.count() != 1 {
		t.Errorf("dead-lettered %d records, want 1", dlq.count())
	}
}

// A poison record that will not even parse must not abort the batch decode and
// take its whole chunk with it.
func TestBatchIsolatesUnparseableRecord(t *testing.T) {
	db, dlq := newFakeStore(), &fakeDLQ{}
	recs := records(0, 0, 4)
	recs[1].Value = []byte("<xml/>")

	w := newTestWorker(db, dlq, 4)
	last, blocked := w.processPartition(context.Background(), recs)

	if blocked {
		t.Fatal("partition blocked by a record that was successfully parked")
	}
	if last == nil || last.Offset != 3 {
		t.Fatalf("committable offset = %v, want 3", last)
	}
	if db.count() != 3 {
		t.Errorf("persisted %d records, want 3", db.count())
	}
	if dlq.count() != 1 {
		t.Errorf("dead-lettered %d records, want 1", dlq.count())
	}
}

// Shutdown must not commit work it did not do. A cancelled work context means the
// watchdog is about to exit hard, and the offsets left uncommitted are exactly
// what makes the redelivery on restart correct.
func TestCancelledContextCommitsOnlyCompletedWork(t *testing.T) {
	db, dlq := newFakeStore(), &fakeDLQ{}
	w := newTestWorker(db, dlq, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	last, blocked := w.processPartition(ctx, records(0, 0, 5))

	if blocked {
		t.Error("a cancelled context is a shutdown, not a stuck partition")
	}
	if last != nil {
		t.Errorf("committable offset = %v, want nothing committed", last)
	}
	if db.count() != 0 {
		t.Errorf("persisted %d records during shutdown, want 0", db.count())
	}
}

func TestPausedSetIsSafeUnderConcurrency(t *testing.T) {
	s := newPausedSet()
	var wg sync.WaitGroup

	for i := range 8 {
		wg.Add(2)
		go func() { defer wg.Done(); s.add([]int32{int32(i)}) }()
		go func() { defer wg.Done(); _ = s.count(); s.take() }()
	}
	wg.Wait()

	s.take() // drain whatever the racing goroutines left behind
	s.add([]int32{1, 2, 2, 3})
	if got := s.count(); got != 3 {
		t.Errorf("count = %d, want 3: the set must not hold duplicates", got)
	}
	if got := len(s.take()); got != 3 {
		t.Errorf("take returned %d partitions, want 3", got)
	}
	if got := s.count(); got != 0 {
		t.Errorf("count after take = %d, want 0", got)
	}
}

// The store's own classifier decides what the worker retries, so a change there
// is a change to the worker's behaviour. This pins the two together.
func TestRetryClassificationMatchesStore(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		retryable bool
	}{
		{"connection failure", &pgconn.PgError{Code: "08006"}, true},
		{"too many connections", &pgconn.PgError{Code: "53300"}, true},
		{"deadlock", &pgconn.PgError{Code: "40P01"}, true},
		{"unique violation", &pgconn.PgError{Code: "23505"}, false},
		{"invalid uuid", &pgconn.PgError{Code: "22P02"}, false},
		{"syntax error", &pgconn.PgError{Code: "42601"}, false},
		{"caller error", store.ErrInvalidArgument, false},
		{"shutdown", context.Canceled, false},
	}
	for _, tc := range cases {
		if got := store.IsRetryable(tc.err); got != tc.retryable {
			t.Errorf("%s: IsRetryable = %v, want %v", tc.name, got, tc.retryable)
		}
	}
}
