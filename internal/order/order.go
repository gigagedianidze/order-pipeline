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

// NewEvent turns a validated request into a publishable event. Money is handled
// in integer cents throughout — floats and currency do not mix.
func NewEvent(r CreateRequest) Event {
	var total int64
	for _, it := range r.Items {
		total += it.UnitPriceCents * int64(it.Quantity)
	}
	return Event{
		EventID:    uuid.NewString(),
		EventType:  EventTypeCreated,
		OrderID:    uuid.NewString(),
		OccurredAt: time.Now().UTC(),
		CustomerID: r.CustomerID,
		Items:      r.Items,
		TotalCents: total,
	}
}
