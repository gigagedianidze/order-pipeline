// Package order holds the domain types shared by every service: the shape of an
// incoming request, the shape of the event that travels through Kafka, and the
// rules that decide whether a request is worth publishing at all.
package order

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// EventTypeCreated is the only event type today. It exists as a named field so a
// consumer can route on it later without a breaking payload change.
const EventTypeCreated = "order.created"

// CreateRequest is the JSON body accepted by POST /orders.
type CreateRequest struct {
	CustomerID string `json:"customer_id"`
	Items      []Item `json:"items"`
}

type Item struct {
	SKU            string `json:"sku"`
	Quantity       int    `json:"quantity"`
	UnitPriceCents int64  `json:"unit_price_cents"`
}

// Event is what gets published to Kafka.
//
// EventID and OrderID are deliberately separate. OrderID identifies the business
// entity and is the partition key, so every event for one order stays ordered on
// one partition. EventID identifies this specific delivery and is what the worker
// deduplicates on (Day 4) — a redelivery of the same event carries the same
// EventID, while a genuinely new event about the same order does not.
type Event struct {
	EventID    string    `json:"event_id"`
	EventType  string    `json:"event_type"`
	OrderID    string    `json:"order_id"`
	OccurredAt time.Time `json:"occurred_at"`
	CustomerID string    `json:"customer_id"`
	Items      []Item    `json:"items"`
	TotalCents int64     `json:"total_cents"`
}

var ErrInvalid = errors.New("invalid order")

// Validate rejects anything that should never reach the topic. Rubbish caught
// here costs one 400; rubbish published becomes a poison message that every
// worker in the group has to cope with (Day 6).
func (r CreateRequest) Validate() error {
	var problems []string

	if strings.TrimSpace(r.CustomerID) == "" {
		problems = append(problems, "customer_id is required")
	}
	if len(r.Items) == 0 {
		problems = append(problems, "at least one item is required")
	}
	for i, it := range r.Items {
		if strings.TrimSpace(it.SKU) == "" {
			problems = append(problems, fmt.Sprintf("items[%d].sku is required", i))
		}
		if it.Quantity <= 0 {
			problems = append(problems, fmt.Sprintf("items[%d].quantity must be > 0", i))
		}
		if it.UnitPriceCents < 0 {
			problems = append(problems, fmt.Sprintf("items[%d].unit_price_cents must be >= 0", i))
		}
	}

	if len(problems) > 0 {
		return fmt.Errorf("%w: %s", ErrInvalid, strings.Join(problems, "; "))
	}
	return nil
}

// idempotencyNamespace seeds the deterministic ids derived from a client's
// Idempotency-Key. It is a fixed, arbitrary UUID: its only job is to keep this
// system's derived ids from colliding with ids derived elsewhere from the same
// key material.
var idempotencyNamespace = uuid.MustParse("6f9619ff-8b86-d011-b42d-00c04fc964ff")

// MaxIdempotencyKeyLen bounds what a client may send. The key is hashed, so
// length costs nothing here — the limit exists so a caller cannot use the header
// as a megabyte-sized channel into our logs.
const MaxIdempotencyKeyLen = 255

// NewEvent turns a validated request into a publishable event. Money is handled
// in integer cents throughout — floats and currency do not mix.
//
// idempotencyKey, when non-empty, makes the identifiers deterministic: the same
// key always produces the same OrderID and EventID. That closes a real hole. The
// UNIQUE (event_id) constraint makes *redelivery* a no-op, but it does nothing
// about a client that times out and retries its POST — that retry used to mint
// fresh ids and become a second, genuine order. Deriving the ids from the
// client's key routes the retry into exactly the same deduplication the worker
// already performs, with no new storage and no new code path.
//
// EventID and OrderID are derived from different strings so they do not collide
// with each other, and both from the same key so a retry lands on both.
//
// First write wins: a client that reuses a key with a different body gets the
// original order, because nothing here records what the first body was. Detecting
// that misuse would need the request stored alongside the key, which is a real
// design with real costs and is not what this buys.
func NewEvent(r CreateRequest, idempotencyKey string) Event {
	var total int64
	for _, it := range r.Items {
		total += it.UnitPriceCents * int64(it.Quantity)
	}

	orderID, eventID := uuid.NewString(), uuid.NewString()
	if idempotencyKey != "" {
		orderID = derive("order:" + idempotencyKey)
		eventID = derive("event:" + idempotencyKey)
	}

	return Event{
		EventID:    eventID,
		EventType:  EventTypeCreated,
		OrderID:    orderID,
		OccurredAt: time.Now().UTC(),
		CustomerID: r.CustomerID,
		Items:      r.Items,
		TotalCents: total,
	}
}

// derive produces a stable UUIDv5 for a string.
func derive(s string) string {
	return uuid.NewSHA1(idempotencyNamespace, []byte(s)).String()
}

// ValidateIdempotencyKey rejects a key the API should not accept. An empty key
// is not an error — it simply means the client is not asking for the guarantee.
func ValidateIdempotencyKey(key string) error {
	if key == "" {
		return nil
	}
	if len(key) > MaxIdempotencyKeyLen {
		return fmt.Errorf("%w: Idempotency-Key must be at most %d characters", ErrInvalid, MaxIdempotencyKeyLen)
	}
	for _, r := range key {
		// Printable ASCII only. The key reaches logs and Kafka headers, and a
		// control character in either is somebody else's bug to debug.
		if r < 0x21 || r > 0x7e {
			return fmt.Errorf("%w: Idempotency-Key must be printable ASCII without spaces", ErrInvalid)
		}
	}
	return nil
}
