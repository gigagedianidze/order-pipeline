package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

// ErrInvalidArgument marks a failure caused by the caller's input rather than by
// the system: a malformed page token, an order id that is not a UUID.
//
// It exists because "should I retry this?" and "whose fault is this?" are two
// different questions, and answering the second with the first is how a bad page
// token turns into a 500. IsRetryable classifies the *environment*; this
// classifies the *request*. A caller error is never retryable, but plenty of
// non-retryable errors are our fault and must not be blamed on the caller.
var ErrInvalidArgument = errors.New("invalid argument")

// IsRetryable decides whether a failed write is worth attempting again.
//
// This classification is the difference between a system that heals itself and
// one that either loses data or spins forever. Retrying a constraint violation
// wastes the retry budget and then dead-letters a message that was never going to
// succeed; *not* retrying a dropped connection dead-letters perfectly good orders
// because the database restarted.
//
// The rule: retry anything about the *environment*, never anything about the
// *data* or our own SQL.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	// Shutdown is not a failure to retry — the drain deadline decides, not this.
	if errors.Is(err, context.Canceled) {
		return false
	}
	// The caller handed us something unusable. Retrying cannot change that.
	if errors.Is(err, ErrInvalidArgument) {
		return false
	}
	// A statement timeout is the database saying "not now", which is transient.
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch class := pgErr.Code[:2]; class {
		case "23", // integrity constraint violation — the data conflicts; it will conflict again
			"22", // data exception — bad value, e.g. a malformed UUID
			"42": // syntax error or access rule violation — our bug, not the environment's
			return false
		case "08", // connection exception
			"53", // insufficient resources (out of memory, too many connections)
			"57", // operator intervention (database shutting down, admin cancel)
			"58": // system error (disk, file I/O)
			return true
		case "40": // transaction rollback: serialization failure, deadlock detected
			return true
		default:
			// An unrecognised server error is not obviously transient, and
			// retrying it would just delay the dead-letter by the full budget.
			return false
		}
	}

	// Not a PgError at all: the failure happened below the protocol — connection
	// refused, DNS, TCP reset, a closed pool. Those are exactly the transient
	// class. pgx reports a shut-down server this way rather than as a PgError.
	return true
}

// IsCallerError reports whether err is the caller's fault. Transport-level
// failures deliberately do not qualify: an unreachable database is our problem
// to solve, not something to blame on whoever asked.
func IsCallerError(err error) bool {
	if errors.Is(err, ErrInvalidArgument) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// 22 is a data exception — the classic case is 22P02, a value that is not
		// a valid UUID reaching Postgres because the caller sent nonsense.
		return pgErr.Code[:2] == "22"
	}
	return false
}
