package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gigagedianidze/order-pipeline/internal/order"

	"github.com/jackc/pgx/v5"
)

// ErrNotFound is returned when an order id matches no row. It is a normal
// outcome on this system, not an error condition: the write path is
// asynchronous, so a client that reads immediately after a 202 can legitimately
// find nothing yet.
var ErrNotFound = errors.New("order not found")

// Order is a persisted order, as read back.
type Order struct {
	OrderID     string
	EventID     string
	CustomerID  string
	Items       []order.Item
	TotalCents  int64
	OccurredAt  time.Time
	ProcessedAt time.Time
}

const (
	defaultPageSize = 50
	maxPageSize     = 500
)

const selectColumns = `order_id, event_id, customer_id, items, total_cents, occurred_at, processed_at`

func (s *Store) GetOrder(ctx context.Context, orderID string) (Order, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+selectColumns+` FROM orders WHERE order_id = $1`, orderID)

	o, err := scanOrder(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Order{}, ErrNotFound
	}
	if err != nil {
		return Order{}, fmt.Errorf("get order: %w", err)
	}
	return o, nil
}

// ListOrders returns a page of orders newest-first, with an opaque cursor for
// the next page.
//
// Pagination is keyset ("seek"), not OFFSET. OFFSET makes the database count and
// discard every skipped row, so page 1000 is far slower than page 1 — and worse,
// rows inserted while a client pages through will shift every subsequent page,
// causing rows to be skipped or returned twice. This system inserts constantly,
// so that is not a theoretical concern. A cursor on (processed_at, order_id)
// anchors each page to a fixed point instead, and stays O(page size) whatever
// the offset.
func (s *Store) ListOrders(ctx context.Context, pageSize int, pageToken, customerID string) ([]Order, string, error) {
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	if pageSize > maxPageSize {
		pageSize = maxPageSize
	}

	var (
		sb   strings.Builder
		args []any
	)
	sb.WriteString(`SELECT ` + selectColumns + ` FROM orders WHERE true`)

	if customerID != "" {
		args = append(args, customerID)
		fmt.Fprintf(&sb, " AND customer_id = $%d", len(args))
	}
	if pageToken != "" {
		cur, err := decodeCursor(pageToken)
		if err != nil {
			return nil, "", err
		}
		// Row-value comparison, so this uses the (processed_at, order_id)
		// ordering as a single seek rather than a filter over both columns.
		args = append(args, cur.ProcessedAt, cur.OrderID)
		fmt.Fprintf(&sb, " AND (processed_at, order_id) < ($%d, $%d)", len(args)-1, len(args))
	}

	// One extra row is fetched to discover whether a further page exists, which
	// avoids a second COUNT query that would be wrong by the time it returned.
	args = append(args, pageSize+1)
	fmt.Fprintf(&sb, " ORDER BY processed_at DESC, order_id DESC LIMIT $%d", len(args))

	rows, err := s.pool.Query(ctx, sb.String(), args...)
	if err != nil {
		return nil, "", fmt.Errorf("list orders: %w", err)
	}
	defer rows.Close()

	orders := make([]Order, 0, pageSize)
	for rows.Next() {
		o, err := scanOrder(rows)
		if err != nil {
			return nil, "", fmt.Errorf("scan order: %w", err)
		}
		orders = append(orders, o)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("list orders: %w", err)
	}

	var next string
	if len(orders) > pageSize {
		orders = orders[:pageSize]
		last := orders[len(orders)-1]
		next = encodeCursor(cursor{ProcessedAt: last.ProcessedAt, OrderID: last.OrderID})
	}
	return orders, next, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanOrder(row rowScanner) (Order, error) {
	var (
		o     Order
		items []byte
	)
	if err := row.Scan(&o.OrderID, &o.EventID, &o.CustomerID, &items,
		&o.TotalCents, &o.OccurredAt, &o.ProcessedAt); err != nil {
		return Order{}, err
	}
	if err := json.Unmarshal(items, &o.Items); err != nil {
		return Order{}, fmt.Errorf("decode items: %w", err)
	}
	return o, nil
}

// cursor is the page anchor. It is base64-encoded JSON rather than a raw
// "timestamp:id" string so the shape can change without breaking clients that
// hold an old token — the token is opaque by contract.
type cursor struct {
	ProcessedAt time.Time `json:"p"`
	OrderID     string    `json:"o"`
}

func encodeCursor(c cursor) string {
	b, _ := json.Marshal(c) // cannot fail for this struct
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(token string) (cursor, error) {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return cursor{}, fmt.Errorf("invalid page token: %w", err)
	}
	var c cursor
	if err := json.Unmarshal(b, &c); err != nil {
		return cursor{}, fmt.Errorf("invalid page token: %w", err)
	}
	return c, nil
}
