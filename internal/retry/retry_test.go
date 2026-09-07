package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDelayRespectsCeilingAndJitter(t *testing.T) {
	p := Policy{Base: 100 * time.Millisecond, Max: time.Second, MaxElapsed: time.Minute}

	// Full jitter means the delay is random in [0, backoff], so the only hard
	// guarantees are the ceiling and that values vary.
	seen := make(map[time.Duration]bool)
	for attempt := 0; attempt < 12; attempt++ {
		for i := 0; i < 50; i++ {
			d := p.Delay(attempt)
			if d < 0 {
				t.Fatalf("attempt %d: negative delay %v", attempt, d)
			}
			if d > p.Max {
				t.Fatalf("attempt %d: delay %v exceeds ceiling %v", attempt, d, p.Max)
			}
			seen[d] = true
		}
	}
	if len(seen) < 10 {
		t.Errorf("delays look non-random: only %d distinct values", len(seen))
	}
}

func TestDoStopsAtNonRetryableError(t *testing.T) {
	poison := errors.New("poison")
	calls := 0

	err := Do(context.Background(), DefaultPolicy(),
		func(error) bool { return false }, nil,
		func() error { calls++; return poison })

	if !errors.Is(err, poison) {
		t.Fatalf("want poison, got %v", err)
	}
	if calls != 1 {
		t.Errorf("non-retryable error was attempted %d times, want 1", calls)
	}
}

func TestDoSucceedsAfterTransientFailures(t *testing.T) {
	calls := 0
	p := Policy{Base: time.Millisecond, Max: 5 * time.Millisecond, MaxElapsed: time.Second}

	err := Do(context.Background(), p, func(error) bool { return true }, nil, func() error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	})

	if err != nil {
		t.Fatalf("want success, got %v", err)
	}
	if calls != 3 {
		t.Errorf("called %d times, want 3", calls)
	}
}

func TestDoGivesUpWhenBudgetExhausted(t *testing.T) {
	p := Policy{Base: 10 * time.Millisecond, Max: 20 * time.Millisecond, MaxElapsed: 60 * time.Millisecond}
	boom := errors.New("still down")

	start := time.Now()
	err := Do(context.Background(), p, func(error) bool { return true }, nil,
		func() error { return boom })
	elapsed := time.Since(start)

	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("took %v, budget was %v", elapsed, p.MaxElapsed)
	}
}

func TestDoStopsOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	p := Policy{Base: 50 * time.Millisecond, Max: time.Second, MaxElapsed: time.Minute}
	calls := 0

	go func() { time.Sleep(20 * time.Millisecond); cancel() }()

	start := time.Now()
	err := Do(ctx, p, func(error) bool { return true }, nil, func() error {
		calls++
		return errors.New("transient")
	})

	if err == nil {
		t.Fatal("want the last error, got nil")
	}
	if time.Since(start) > time.Second {
		t.Error("cancellation did not interrupt the wait")
	}
}
