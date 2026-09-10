package order

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestValidate(t *testing.T) {
	valid := CreateRequest{
		CustomerID: "cust-1",
		Items:      []Item{{SKU: "WIDGET-1", Quantity: 2, UnitPriceCents: 1999}},
	}

	tests := []struct {
		name    string
		req     CreateRequest
		wantErr string // substring expected in the error; empty means valid
	}{
		{"valid", valid, ""},
		{"free item is allowed", CreateRequest{
			CustomerID: "cust-1",
			Items:      []Item{{SKU: "FREEBIE", Quantity: 1, UnitPriceCents: 0}},
		}, ""},
		{"missing customer", CreateRequest{Items: valid.Items}, "customer_id is required"},
		{"whitespace customer", CreateRequest{CustomerID: "   ", Items: valid.Items}, "customer_id is required"},
		{"no items", CreateRequest{CustomerID: "cust-1"}, "at least one item"},
		{"missing sku", CreateRequest{
			CustomerID: "cust-1",
			Items:      []Item{{Quantity: 1, UnitPriceCents: 100}},
		}, "items[0].sku is required"},
		{"zero quantity", CreateRequest{
			CustomerID: "cust-1",
			Items:      []Item{{SKU: "A", Quantity: 0, UnitPriceCents: 100}},
		}, "items[0].quantity must be > 0"},
		{"negative price", CreateRequest{
			CustomerID: "cust-1",
			Items:      []Item{{SKU: "A", Quantity: 1, UnitPriceCents: -1}},
		}, "items[0].unit_price_cents must be >= 0"},
		{"reports the offending index", CreateRequest{
			CustomerID: "cust-1",
			Items:      []Item{{SKU: "A", Quantity: 1, UnitPriceCents: 100}, {SKU: "B", Quantity: -3, UnitPriceCents: 100}},
		}, "items[1].quantity"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.req.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("want valid, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("want error containing %q, got nil", tc.wantErr)
			}
			if !errors.Is(err, ErrInvalid) {
				t.Errorf("error does not wrap ErrInvalid: %v", err)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("want error containing %q, got %q", tc.wantErr, err.Error())
			}
		})
	}
}

func TestNewEventTotal(t *testing.T) {
	evt := NewEvent(CreateRequest{
		CustomerID: "cust-1",
		Items: []Item{
			{SKU: "A", Quantity: 2, UnitPriceCents: 1999}, // 3998
			{SKU: "B", Quantity: 3, UnitPriceCents: 500},  // 1500
		},
	}, "")

	if want := int64(5498); evt.TotalCents != want {
		t.Errorf("total = %d, want %d", evt.TotalCents, want)
	}
	if evt.EventType != EventTypeCreated {
		t.Errorf("event_type = %q, want %q", evt.EventType, EventTypeCreated)
	}
	if evt.OccurredAt.IsZero() {
		t.Error("occurred_at not set")
	}
}

// EventID and OrderID must never be the same value: one identifies the delivery,
// the other the business entity. Day 4's deduplication depends on the difference.
func TestNewEventIDsAreDistinct(t *testing.T) {
	req := CreateRequest{CustomerID: "c", Items: []Item{{SKU: "A", Quantity: 1, UnitPriceCents: 1}}}

	a, b := NewEvent(req, ""), NewEvent(req, "")

	if a.EventID == a.OrderID {
		t.Error("event_id and order_id are identical")
	}
	if a.OrderID == b.OrderID {
		t.Error("two requests produced the same order_id")
	}
	if a.EventID == b.EventID {
		t.Error("two requests produced the same event_id")
	}
}

// An Idempotency-Key must produce the same identifiers every time, because that
// is the entire mechanism: the client's retry becomes a redelivery of an event
// the worker has already deduplicated on, rather than a second genuine order.
func TestIdempotencyKeyProducesStableIDs(t *testing.T) {
	req := CreateRequest{CustomerID: "c", Items: []Item{{SKU: "A", Quantity: 1, UnitPriceCents: 1}}}

	a := NewEvent(req, "checkout-1")
	b := NewEvent(req, "checkout-1")

	if a.OrderID != b.OrderID {
		t.Errorf("order_id differs between two uses of the same key: %s vs %s", a.OrderID, b.OrderID)
	}
	if a.EventID != b.EventID {
		t.Errorf("event_id differs between two uses of the same key: %s vs %s", a.EventID, b.EventID)
	}
	if a.OrderID == a.EventID {
		t.Error("order_id and event_id derived to the same value")
	}
	if _, err := uuid.Parse(a.OrderID); err != nil {
		t.Errorf("derived order_id is not a UUID: %v", err)
	}
	if _, err := uuid.Parse(a.EventID); err != nil {
		t.Errorf("derived event_id is not a UUID: %v", err)
	}
}

func TestDifferentIdempotencyKeysProduceDifferentOrders(t *testing.T) {
	req := CreateRequest{CustomerID: "c", Items: []Item{{SKU: "A", Quantity: 1, UnitPriceCents: 1}}}

	a := NewEvent(req, "checkout-1")
	b := NewEvent(req, "checkout-2")

	if a.OrderID == b.OrderID {
		t.Error("two different keys produced the same order_id")
	}
}

// The key changes the identifiers and nothing else. If it altered the totals or
// the payload it would be a pricing bug waiting to happen.
func TestIdempotencyKeyDoesNotChangeTheOrder(t *testing.T) {
	req := CreateRequest{
		CustomerID: "cust-1",
		Items:      []Item{{SKU: "A", Quantity: 2, UnitPriceCents: 1999}},
	}

	keyed, unkeyed := NewEvent(req, "checkout-1"), NewEvent(req, "")

	if keyed.TotalCents != unkeyed.TotalCents {
		t.Errorf("total differs: %d vs %d", keyed.TotalCents, unkeyed.TotalCents)
	}
	if keyed.CustomerID != unkeyed.CustomerID || keyed.EventType != unkeyed.EventType {
		t.Error("the key altered something other than the identifiers")
	}
}

func TestValidateIdempotencyKey(t *testing.T) {
	valid := []string{"", "abc-123", "order:42", strings.Repeat("k", MaxIdempotencyKeyLen)}
	for _, key := range valid {
		if err := ValidateIdempotencyKey(key); err != nil {
			t.Errorf("ValidateIdempotencyKey(%q) = %v, want nil", key, err)
		}
	}

	invalid := []string{
		strings.Repeat("k", MaxIdempotencyKeyLen+1),
		"has a space",
		"tab\there",
		"null\x00byte",
		"emoji-\U0001F600",
	}
	for _, key := range invalid {
		if err := ValidateIdempotencyKey(key); err == nil {
			t.Errorf("ValidateIdempotencyKey(%q) accepted an unusable key", key)
		}
	}
}
