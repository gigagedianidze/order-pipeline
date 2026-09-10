package store

import (
	"errors"
	"testing"
	"time"
)

func TestCursorRoundTrip(t *testing.T) {
	want := cursor{
		ProcessedAt: time.Date(2026, 9, 7, 17, 45, 50, 577526000, time.UTC),
		OrderID:     "444d5112-cbd4-4fc0-bc33-fa75a2db7b55",
	}

	got, err := decodeCursor(encodeCursor(want))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !got.ProcessedAt.Equal(want.ProcessedAt) {
		t.Errorf("processed_at = %v, want %v", got.ProcessedAt, want.ProcessedAt)
	}
	if got.OrderID != want.OrderID {
		t.Errorf("order_id = %q, want %q", got.OrderID, want.OrderID)
	}
}

// The token must survive being put in a URL unescaped, since that is exactly
// where clients will carry it.
func TestCursorIsURLSafe(t *testing.T) {
	token := encodeCursor(cursor{ProcessedAt: time.Now(), OrderID: "a/b+c=d"})

	for _, c := range token {
		if c == '/' || c == '+' || c == '=' {
			t.Fatalf("token contains URL-unsafe character %q: %s", c, token)
		}
	}
}

func TestDecodeCursorRejectsGarbage(t *testing.T) {
	tokens := []string{
		"not-base64!!",    // not base64 at all
		"aGVsbG8",         // valid base64, not JSON
		"",                // empty
		"e30",             // valid base64 of "{}": JSON, but not a cursor
		"eyJvIjoiYWJjIn0", // has an order id, no timestamp
	}
	for _, token := range tokens {
		_, err := decodeCursor(token)
		if err == nil {
			t.Errorf("decodeCursor(%q) accepted invalid token", token)
			continue
		}
		// The classification is the point: a bad token is the caller's mistake,
		// and reporting it as anything else turns a 400 into a 500.
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("decodeCursor(%q) error is not ErrInvalidArgument: %v", token, err)
		}
		if IsRetryable(err) {
			t.Errorf("decodeCursor(%q) error is classified retryable", token)
		}
	}
}
