package broker

import (
	"strings"
	"sync"
	"testing"
)

// Assignment is written by franz-go's rebalance callbacks, which run on the
// client's own goroutine, and read by the poll loop and the metrics ticker. The
// mutex is not decoration, and `go test -race` is what proves it.
func TestAssignmentIsSafeUnderConcurrency(t *testing.T) {
	var a Assignment
	var wg sync.WaitGroup

	for i := range 16 {
		wg.Add(3)
		go func() { defer wg.Done(); a.add(map[string][]int32{"orders": {int32(i)}}) }()
		go func() { defer wg.Done(); _ = a.Count() }()
		go func() { defer wg.Done(); _ = a.String() }()
	}
	wg.Wait()

	a.remove(map[string][]int32{"orders": {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}})
	if got := a.Count(); got != 0 {
		t.Errorf("count after removing everything = %d, want 0", got)
	}
}

// Cooperative-sticky rebalancing reports deltas, so a member's true assignment is
// only ever the running total of what it gained and gave up.
func TestAssignmentTracksTheRunningTotal(t *testing.T) {
	var a Assignment

	a.add(map[string][]int32{"orders": {0, 1, 2}})
	if got := a.Count(); got != 3 {
		t.Fatalf("count = %d, want 3", got)
	}

	a.remove(map[string][]int32{"orders": {2}}) // handed one to a new member
	if got := a.Count(); got != 2 {
		t.Fatalf("count after revoking one = %d, want 2", got)
	}
	if got := a.String(); got != "orders:[0 1]" {
		t.Errorf("owns %q, want %q", got, "orders:[0 1]")
	}

	// A partition assigned twice is still one partition.
	a.add(map[string][]int32{"orders": {1}})
	if got := a.Count(); got != 2 {
		t.Errorf("count after a repeated assignment = %d, want 2", got)
	}
}

// Zero partitions is the observable symptom of more workers than partitions, so
// an empty assignment has to read as "none" rather than as an empty string.
func TestAssignmentEmptyReadsAsNone(t *testing.T) {
	var a Assignment
	if got := a.String(); got != "none" {
		t.Errorf("empty assignment = %q, want %q", got, "none")
	}

	a.add(map[string][]int32{"orders": {0}})
	a.remove(map[string][]int32{"orders": {0}})
	if got := a.String(); got != "none" {
		t.Errorf("emptied assignment = %q, want %q", got, "none")
	}
}

// Log lines from two workers get compared by eye, which only works if the
// ordering is stable rather than whatever the map iteration happened to produce.
func TestFormatIsStablyOrdered(t *testing.T) {
	m := map[string][]int32{
		"orders":     {2, 0, 1},
		"orders.dlq": {5, 3},
	}
	want := "orders:[0 1 2] orders.dlq:[3 5]"

	for range 20 {
		if got := format(m); got != want {
			t.Fatalf("format = %q, want %q", got, want)
		}
	}
}

// format must not reorder the caller's slice: the same map is handed to a log
// line and then reused by the client.
func TestFormatDoesNotMutateItsInput(t *testing.T) {
	parts := []int32{2, 0, 1}
	format(map[string][]int32{"orders": parts})

	if parts[0] != 2 || parts[1] != 0 || parts[2] != 1 {
		t.Errorf("format sorted the caller's slice in place: %v", parts)
	}
}

func TestRemoveOnEmptyAssignmentDoesNotPanic(t *testing.T) {
	var a Assignment
	a.remove(map[string][]int32{"orders": {0, 1}}) // a revoke before any assign
	if got := a.Count(); got != 0 {
		t.Errorf("count = %d, want 0", got)
	}
	if !strings.Contains(a.String(), "none") {
		t.Errorf("owns %q, want none", a.String())
	}
}
