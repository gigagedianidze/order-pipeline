package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/gigagedianidze/order-pipeline/internal/order"

	"github.com/google/uuid"
)

// These exercise the store against a real PostgreSQL, because the things worth
// checking here are things only Postgres can answer: whether ON CONFLICT really
// makes redelivery a no-op, whether a multi-row insert reports exactly which
// rows were new, and whether keyset pagination is actually stable while rows are
// being inserted underneath it. A fake database would only confirm that the fake
// agrees with itself.
//
// Gated on an environment variable rather than skipped by -short so that `make
// test` stays hermetic and `make test-integration` is an explicit choice:
//
//	POSTGRES_TEST_DSN=postgres://orders:orders@localhost:5433/orders?sslmode=disable go test ./internal/store/
//
// The compose stack already serves exactly that DSN.
func testStore(t *testing.T) *Store {
	t.Helper()

	dsn := os.Getenv("POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_TEST_DSN is not set; run `make up` and `make test-integration`")
	}

	ctx := context.Background()
	s, err := New(ctx, dsn, 4)
	if err != nil {
		t.Fatalf("connect to the test database: %v", err)
	}
	t.Cleanup(s.Close)

	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s
}

// event builds an order for a customer unique to this test, so tests can run
// against a shared database — including one with production-shaped load in it —
// without seeing each other's rows.
func event(customerID string) order.Event {
	return order.Event{
		EventID:    uuid.NewString(),
		EventType:  order.EventTypeCreated,
		OrderID:    uuid.NewString(),
		OccurredAt: time.Now().UTC(),
		CustomerID: customerID,
		Items:      []order.Item{{SKU: "A", Quantity: 2, UnitPriceCents: 1999}},
		TotalCents: 3998,
	}
}

func uniqueCustomer(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("test-%s-%s", t.Name(), uuid.NewString()[:8])
}

// The claim the whole design rests on, checked against the constraint that
// actually enforces it rather than against the comment that describes it.
func TestInsertOrderIsIdempotent(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	evt := event(uniqueCustomer(t))

	inserted, err := s.InsertOrder(ctx, evt)
	if err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if !inserted {
		t.Fatal("first insert reported the event as already applied")
	}

	for i := range 4 {
		inserted, err := s.InsertOrder(ctx, evt)
		if err != nil {
			t.Fatalf("redelivery %d: %v", i, err)
		}
		if inserted {
			t.Errorf("redelivery %d was written as a new row", i)
		}
	}
}

// The conflict target is a deliberate choice: a fresh event_id carrying an
// order_id that already exists is a genuine data conflict and must surface, not
// be swallowed the way a bare ON CONFLICT DO NOTHING would swallow it.
func TestInsertOrderSurfacesOrderIDCollision(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	first := event(uniqueCustomer(t))
	if _, err := s.InsertOrder(ctx, first); err != nil {
		t.Fatalf("first insert: %v", err)
	}

	clash := event(uniqueCustomer(t))
	clash.OrderID = first.OrderID // same order, different delivery

	_, err := s.InsertOrder(ctx, clash)
	if err == nil {
		t.Fatal("an order_id collision was silently accepted")
	}
	if IsRetryable(err) {
		t.Error("an order_id collision was classified as retryable; it will conflict forever")
	}
}

func TestInsertOrdersReportsExactlyWhichRowsWereNew(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	customer := uniqueCustomer(t)

	batch := []order.Event{event(customer), event(customer), event(customer)}

	inserted, err := s.InsertOrders(ctx, batch)
	if err != nil {
		t.Fatalf("first batch: %v", err)
	}
	if len(inserted) != 3 {
		t.Fatalf("reported %d new rows, want 3", len(inserted))
	}

	// Replay the batch with two extra records: only the extras are new. This is
	// what a rebalance produces, and getting the count right matters because it
	// is what the duplicate metric is built from.
	replay := append([]order.Event{}, batch...)
	replay = append(replay, event(customer), event(customer))

	inserted, err = s.InsertOrders(ctx, replay)
	if err != nil {
		t.Fatalf("replayed batch: %v", err)
	}
	if len(inserted) != 2 {
		t.Fatalf("reported %d new rows on replay, want 2", len(inserted))
	}
	for _, evt := range batch {
		if inserted[evt.EventID] {
			t.Errorf("event %s reported as new on its second delivery", evt.EventID)
		}
	}
}

// A batch is all-or-nothing. The worker depends on that being true — its
// per-record fallback exists precisely because a poison row takes the batch down
// with it, and if a partial batch could land the fallback would double-write.
func TestInsertOrdersIsAllOrNothing(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	customer := uniqueCustomer(t)

	poison := event(customer)
	poison.OrderID = "not-a-uuid" // 22P02 on the way in

	batch := []order.Event{event(customer), poison, event(customer)}
	if _, err := s.InsertOrders(ctx, batch); err == nil {
		t.Fatal("a batch containing an unusable row was accepted")
	}

	// The blameless rows must not have landed, or the fallback would insert them
	// a second time.
	orders, _, err := s.ListOrders(ctx, 50, "", customer)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(orders) != 0 {
		t.Errorf("%d rows survived a failed batch, want 0", len(orders))
	}
}

// A duplicate event_id inside one statement is tolerated by DO NOTHING and must
// be reported once. (DO UPDATE would fail outright, which is why the target
// matters.)
func TestInsertOrdersToleratesDuplicatesWithinTheBatch(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	customer := uniqueCustomer(t)

	evt := event(customer)
	inserted, err := s.InsertOrders(ctx, []order.Event{evt, evt})
	if err != nil {
		t.Fatalf("batch with an internal duplicate: %v", err)
	}
	if len(inserted) != 1 {
		t.Errorf("reported %d new rows, want 1", len(inserted))
	}
}

func TestInsertOrdersRejectsAnOversizedBatch(t *testing.T) {
	s := testStore(t)
	batch := make([]order.Event, MaxInsertBatch+1)

	_, err := s.InsertOrders(context.Background(), batch)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("error = %v, want ErrInvalidArgument", err)
	}
}

// The single- and multi-row paths must agree, or turning batching on silently
// changes what the system stores.
func TestBatchAndSingleInsertAgree(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	customer := uniqueCustomer(t)

	single := event(customer)
	if _, err := s.InsertOrder(ctx, single); err != nil {
		t.Fatalf("single insert: %v", err)
	}
	batched := event(customer)
	if _, err := s.InsertOrders(ctx, []order.Event{batched}); err != nil {
		t.Fatalf("batch insert: %v", err)
	}

	for _, want := range []order.Event{single, batched} {
		got, err := s.GetOrder(ctx, want.OrderID)
		if err != nil {
			t.Fatalf("get %s: %v", want.OrderID, err)
		}
		if got.EventID != want.EventID || got.TotalCents != want.TotalCents ||
			got.CustomerID != want.CustomerID || len(got.Items) != len(want.Items) {
			t.Errorf("row for %s does not match what was written: %+v", want.OrderID, got)
		}
		// Compared at microsecond resolution: TIMESTAMPTZ stores microseconds, so
		// the nanosecond tail of a Go time.Time does not survive the round trip.
		// It costs nothing here — end-to-end latency is measured in milliseconds —
		// but it is a real truncation and worth asserting deliberately rather than
		// discovering as a flaky test.
		if !got.OccurredAt.Equal(want.OccurredAt.Truncate(time.Microsecond)) {
			t.Errorf("occurred_at = %v, want %v (to the microsecond)",
				got.OccurredAt.UTC(), want.OccurredAt.Truncate(time.Microsecond))
		}
	}
}

func TestGetOrderNotFound(t *testing.T) {
	s := testStore(t)

	_, err := s.GetOrder(context.Background(), uuid.NewString())
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("error = %v, want ErrNotFound", err)
	}
}

// A malformed id reaches Postgres and comes back as 22P02. It is the caller's
// mistake, and the query service reports it as InvalidArgument on that basis.
func TestGetOrderWithMalformedIDIsACallerError(t *testing.T) {
	s := testStore(t)

	_, err := s.GetOrder(context.Background(), "definitely-not-a-uuid")
	if err == nil {
		t.Fatal("a malformed order id was accepted")
	}
	if !IsCallerError(err) {
		t.Errorf("error %v is not classified as the caller's", err)
	}
	if IsRetryable(err) {
		t.Error("a malformed order id was classified as retryable")
	}
}

// The reason for keyset pagination rather than OFFSET. Rows are inserted while
// the client pages, which is the normal state of this system, and no page may
// skip or repeat a row because of it.
func TestListOrdersPaginationIsStableUnderInserts(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	customer := uniqueCustomer(t)

	const total = 25
	for range total {
		if _, err := s.InsertOrder(ctx, event(customer)); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	seen := map[string]int{}
	token := ""
	for page := 0; ; page++ {
		if page > total {
			t.Fatal("pagination did not terminate")
		}
		orders, next, err := s.ListOrders(ctx, 10, token, customer)
		if err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
		for _, o := range orders {
			seen[o.OrderID]++
		}

		// Insert between pages: with OFFSET this is exactly what shifts every
		// subsequent page and makes rows repeat or vanish.
		if _, err := s.InsertOrder(ctx, event(customer)); err != nil {
			t.Fatalf("insert between pages: %v", err)
		}

		if next == "" {
			break
		}
		token = next
	}

	if len(seen) < total {
		t.Errorf("paged over %d distinct rows, want at least the %d seeded", len(seen), total)
	}
	for id, n := range seen {
		if n > 1 {
			t.Errorf("order %s appeared on %d pages", id, n)
		}
	}
}

func TestListOrdersFiltersByCustomer(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	mine, theirs := uniqueCustomer(t), uniqueCustomer(t)

	for range 3 {
		if _, err := s.InsertOrder(ctx, event(mine)); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	if _, err := s.InsertOrder(ctx, event(theirs)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	orders, _, err := s.ListOrders(ctx, 50, "", mine)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(orders) != 3 {
		t.Fatalf("got %d orders, want 3", len(orders))
	}
	for _, o := range orders {
		if o.CustomerID != mine {
			t.Errorf("customer filter leaked an order for %q", o.CustomerID)
		}
	}
}

func TestListOrdersIsNewestFirst(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	customer := uniqueCustomer(t)

	for range 5 {
		if _, err := s.InsertOrder(ctx, event(customer)); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	orders, _, err := s.ListOrders(ctx, 50, "", customer)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	for i := 1; i < len(orders); i++ {
		if orders[i].ProcessedAt.After(orders[i-1].ProcessedAt) {
			t.Errorf("order %d is newer than order %d; the page is not newest-first", i, i-1)
		}
	}
}

func TestListOrdersRejectsABadPageToken(t *testing.T) {
	s := testStore(t)

	_, _, err := s.ListOrders(context.Background(), 10, "not a real token", "")
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("error = %v, want ErrInvalidArgument", err)
	}
	// This is the bug the sentinel exists for: classified as transient, it
	// reached the client as a 500 instead of a 400.
	if !IsCallerError(err) {
		t.Error("a bad page token is not classified as the caller's mistake")
	}
}

func TestListOrdersClampsPageSize(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	customer := uniqueCustomer(t)

	for range 3 {
		if _, err := s.InsertOrder(ctx, event(customer)); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	// Zero means "use the default", not "return nothing".
	orders, _, err := s.ListOrders(ctx, 0, "", customer)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(orders) != 3 {
		t.Errorf("page_size=0 returned %d orders, want the default page of 3", len(orders))
	}

	if _, _, err := s.ListOrders(ctx, maxPageSize*10, "", customer); err != nil {
		t.Errorf("an oversized page size was not clamped: %v", err)
	}
}
