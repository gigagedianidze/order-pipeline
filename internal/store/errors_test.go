package store

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestIsRetryable(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"shutdown cancellation", context.Canceled, false},
		{"statement timeout", context.DeadlineExceeded, true},

		// Poison: the data or the query is wrong, and time will not fix either.
		{"unique violation", &pgconn.PgError{Code: "23505"}, false},
		{"not null violation", &pgconn.PgError{Code: "23502"}, false},
		{"invalid text representation", &pgconn.PgError{Code: "22P02"}, false},
		{"undefined column", &pgconn.PgError{Code: "42703"}, false},

		// Transient: the environment is unwell.
		{"connection exception", &pgconn.PgError{Code: "08006"}, true},
		{"too many connections", &pgconn.PgError{Code: "53300"}, true},
		{"admin shutdown", &pgconn.PgError{Code: "57P01"}, true},
		{"cannot connect now", &pgconn.PgError{Code: "57P03"}, true},
		{"io error", &pgconn.PgError{Code: "58030"}, true},
		{"serialization failure", &pgconn.PgError{Code: "40001"}, true},
		{"deadlock detected", &pgconn.PgError{Code: "40P01"}, true},

		// Below the protocol: a stopped database looks like this, not like a PgError.
		{"connection refused", &net.OpError{Op: "dial", Err: errors.New("connection refused")}, true},

		{"wrapped unique violation", fmt.Errorf("insert order: %w", &pgconn.PgError{Code: "23505"}), false},
		{"wrapped connection error", fmt.Errorf("insert order: %w", &pgconn.PgError{Code: "08006"}), true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsRetryable(tc.err); got != tc.want {
				t.Errorf("IsRetryable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
