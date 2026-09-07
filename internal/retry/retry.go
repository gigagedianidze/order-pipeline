// Package retry implements bounded exponential backoff with full jitter.
package retry

import (
	"context"
	"math"
	"math/rand/v2"
	"time"
)

// Policy bounds a retry loop in two independent ways: how long any single wait
// can be, and how long the whole attempt sequence may take. The elapsed budget
// is the important one — an attempt count alone says nothing about how long the
// worker will be stuck, and "how long will this block" is what actually matters
// to the consumer group.
type Policy struct {
	Base       time.Duration // first delay
	Max        time.Duration // ceiling for any single delay
	MaxElapsed time.Duration // total budget across all attempts
}

func DefaultPolicy() Policy {
	return Policy{
		Base: 100 * time.Millisecond,
		Max:  5 * time.Second,
		// Deliberately below franz-go's 60s rebalance timeout. A worker blocked
		// in a retry loop cannot respond to a rebalance, so the retry budget has
		// to fit inside the window the group is willing to wait, or retrying a
		// database outage causes a rebalance storm on top of it.
		MaxElapsed: 45 * time.Second,
	}
}

// Delay returns the wait before the given attempt (0-based), using full jitter:
// a uniform random value in [0, exponential backoff].
//
// Jitter is not a refinement, it is the point. Without it, every worker that
// failed at the same instant — which is what a database outage produces — retries
// at the same instant, and the recovering database is hit by the entire fleet in
// lockstep. Full jitter spreads them out and is what AWS's "Exponential Backoff
// and Jitter" measured as the best of the simple strategies.
func (p Policy) Delay(attempt int) time.Duration {
	backoff := float64(p.Base) * math.Pow(2, float64(attempt))
	if backoff > float64(p.Max) || math.IsInf(backoff, 1) {
		backoff = float64(p.Max)
	}
	return time.Duration(rand.Int64N(int64(backoff) + 1))
}

// Do calls fn until it succeeds, until it returns an error that retryable rejects,
// or until the elapsed budget runs out. onRetry, if set, is called before each wait.
//
// It returns the last error from fn. Callers distinguish "gave up" from "not
// retryable" by asking retryable about the returned error themselves.
func Do(ctx context.Context, p Policy, retryable func(error) bool,
	onRetry func(attempt int, delay time.Duration, err error), fn func() error) error {

	deadline := time.Now().Add(p.MaxElapsed)

	for attempt := 0; ; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
		if !retryable(err) {
			return err
		}

		delay := p.Delay(attempt)
		if time.Now().Add(delay).After(deadline) {
			return err // budget exhausted
		}
		if onRetry != nil {
			onRetry(attempt, delay, err)
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return err
		case <-timer.C:
		}
	}
}
