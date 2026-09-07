package order

import (
	"errors"
	"strings"
	"testing"
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
	})

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

	a, b := NewEvent(req), NewEvent(req)

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
