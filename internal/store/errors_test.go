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

		// The caller's mistake. Retrying an unusable page token just delays the
		// same answer by the full budget.
		{"caller error", ErrInvalidArgument, false},
		{"wrapped caller error", fmt.Errorf("list orders: %w", ErrInvalidArgument), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsRetryable(tc.err); got != tc.want {
				t.Errorf("IsRetryable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// IsCallerError answers a different question from IsRetryable, and the two used
// to be conflated: a bad page token is not a PgError, so !IsRetryable read it as
// transient and the API reported the caller's mistake as a 500.
func TestIsCallerError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"bad page token", fmt.Errorf("%w: page token is not valid base64", ErrInvalidArgument), true},
		{"invalid uuid text", &pgconn.PgError{Code: "22P02"}, true},

		// Not the caller's fault, however non-retryable it may be.
		{"our sql is wrong", &pgconn.PgError{Code: "42703"}, false},
		{"unique violation", &pgconn.PgError{Code: "23505"}, false},
		{"database is down", &net.OpError{Op: "dial", Err: errors.New("connection refused")}, false},
		{"shutdown", context.Canceled, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsCallerError(tc.err); got != tc.want {
				t.Errorf("IsCallerError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// Every caller error must also be non-retryable. If the two classifiers ever
// disagree, the worker burns its whole retry budget on a record that was always
// going to be rejected.
func TestCallerErrorsAreNeverRetryable(t *testing.T) {
	errs := []error{
		ErrInvalidArgument,
		fmt.Errorf("wrapped: %w", ErrInvalidArgument),
		&pgconn.PgError{Code: "22P02"},
	}
	for _, err := range errs {
		if IsCallerError(err) && IsRetryable(err) {
			t.Errorf("%v is both a caller error and retryable", err)
		}
	}
}
