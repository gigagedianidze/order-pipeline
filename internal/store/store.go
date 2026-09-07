// Package store is the PostgreSQL persistence layer.
package store

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gigagedianidze/order-pipeline/internal/order"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	// Deliberately modest. Day 11 wants pool exhaustion to be reachable and
	// observable rather than hidden behind an enormous default.
	cfg.MaxConns = 10
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
		return false, fmt.Errorf("marshal items: %w", err)
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

// CountOrders is used by the acceptance checks and load tooling.
func (s *Store) CountOrders(ctx context.Context) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM orders`).Scan(&n)
	return n, err
}

func (s *Store) Close() { s.pool.Close() }
