// Package store is the PostgreSQL persistence layer.
package store

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gigagedianidze/order-pipeline/internal/order"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

// insertColumns is the number of columns one row of an insert binds. Postgres
// caps a statement at 65535 parameters, so it also caps how many rows a single
// multi-row insert can carry.
const insertColumns = 6

// MaxInsertBatch is the largest batch InsertOrders will accept. It is well under
// the parameter ceiling (65535/6 ≈ 10922) because the useful range is far below
// it anyway: the gain from batching is in amortising the round trip, and that is
// mostly spent by a few hundred rows.
const MaxInsertBatch = 1000

type Store struct {
	pool *pgxpool.Pool
}

// New opens the pool. maxConns is explicit rather than left to pgx's default
// (which is derived from the machine's core count) so that pool exhaustion is
// reachable and observable in an experiment rather than hidden behind a number
// that changes with the host.
func New(ctx context.Context, dsn string, maxConns int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	cfg.MaxConns = maxConns
	cfg.MaxConnLifetime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Migrate applies the embedded schema.
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, schema); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	return nil
}

// InsertOrder writes an order and reports whether it was new.
//
// ON CONFLICT DO NOTHING is the whole idempotency mechanism. Kafka gives
// at-least-once delivery, so the same event will eventually arrive twice — after
// a rebalance, after a crash between the write and the offset commit, after a
// retry. Making the *write* idempotent is cheaper and more honest than trying to
// make delivery exactly-once, and it is enforced by a database constraint rather
// than by application logic that can be bypassed.
//
// inserted=false means "this event has already been applied" — a normal, expected
// outcome, not an error.
func (s *Store) InsertOrder(ctx context.Context, evt order.Event) (inserted bool, err error) {
	items, err := json.Marshal(evt.Items)
	if err != nil {
		// The event came off the wire as JSON, so this cannot be a transient
		// fault; marking it as the caller's keeps it out of the retry loop.
		return false, fmt.Errorf("%w: marshal items: %v", ErrInvalidArgument, err)
	}

	tag, err := s.pool.Exec(ctx, `
		INSERT INTO orders (order_id, event_id, customer_id, items, total_cents, occurred_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (event_id) DO NOTHING`,
		evt.OrderID, evt.EventID, evt.CustomerID, items, evt.TotalCents, evt.OccurredAt,
	)
	if err != nil {
		return false, fmt.Errorf("insert order: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// InsertOrders writes many orders in one round trip and returns the set of
// event ids that were genuinely new.
//
// Batching exists because a single worker's throughput was measured at roughly
// 1/(database round trip) — one INSERT per record, serially. Amortising that
// round trip over many records is the direct fix, and the conflict target is
// unchanged, so the idempotency guarantee is exactly the one InsertOrder gives.
//
// The batch is all-or-nothing: one bad row fails the statement and no row lands.
// Callers must be able to fall back to inserting the records individually, which
// is what isolates a poison record from its blameless neighbours.
//
// RETURNING event_id, rather than a row count, is what makes per-record
// accounting still possible: a count of 7 out of 10 does not say which 7.
func (s *Store) InsertOrders(ctx context.Context, evts []order.Event) (map[string]bool, error) {
	if len(evts) == 0 {
		return map[string]bool{}, nil
	}
	if len(evts) > MaxInsertBatch {
		return nil, fmt.Errorf("%w: batch of %d exceeds the %d row limit",
			ErrInvalidArgument, len(evts), MaxInsertBatch)
	}

	var sb strings.Builder
	sb.WriteString(`INSERT INTO orders (order_id, event_id, customer_id, items, total_cents, occurred_at) VALUES `)
	args := make([]any, 0, len(evts)*insertColumns)

	for i, evt := range evts {
		items, err := json.Marshal(evt.Items)
		if err != nil {
			return nil, fmt.Errorf("%w: marshal items for event %s: %v", ErrInvalidArgument, evt.EventID, err)
		}
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteByte('(')
		for c := range insertColumns {
			if c > 0 {
				sb.WriteString(", ")
			}
			sb.WriteByte('$')
			sb.WriteString(strconv.Itoa(len(args) + c + 1))
		}
		sb.WriteByte(')')
		args = append(args, evt.OrderID, evt.EventID, evt.CustomerID, items, evt.TotalCents, evt.OccurredAt)
	}
	// A duplicate event_id *within* the batch is fine here: DO NOTHING tolerates
	// a row conflicting with one inserted by the same statement, and only the
	// first appears in RETURNING. (DO UPDATE would fail outright.)
	sb.WriteString(` ON CONFLICT (event_id) DO NOTHING RETURNING event_id`)

	rows, err := s.pool.Query(ctx, sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("insert orders: %w", err)
	}
	defer rows.Close()

	inserted := make(map[string]bool, len(evts))
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan inserted event_id: %w", err)
		}
		inserted[id] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("insert orders: %w", err)
	}
	return inserted, nil
}

// CountOrders is used by the acceptance checks and load tooling.
func (s *Store) CountOrders(ctx context.Context) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM orders`).Scan(&n)
	return n, err
}

// Ping reports whether the database is currently reachable. The readiness probe
// uses it; the write path does not, because a probe's answer is stale the moment
// it is given and the retry loop is the real answer.
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func (s *Store) Close() { s.pool.Close() }
