package broker

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// A record that was replayed and died again must arrive on the dead-letter topic
// still carrying its replay count. That header is the only evidence the replay
// tool has that it has seen the record before, so dropping it here would turn the
// loop guard off without anything looking broken.
func TestCarryForwardKeepsTheReplayProvenance(t *testing.T) {
	kept := carryForward([]kgo.RecordHeader{
		{Key: HeaderReplayCount, Value: []byte("1")},
		{Key: HeaderReplayAt, Value: []byte("2026-09-18T10:00:00Z")},
		{Key: HeaderReplayOfPartition, Value: []byte("0")},
		{Key: HeaderReplayOfOffset, Value: []byte("12")},
		// A stale diagnosis from the previous death. Send writes a fresh one, and
		// two answers to "why did this fail" is one too many.
		{Key: HeaderReason, Value: []byte("retries_exhausted")},
		{Key: HeaderError, Value: []byte("connect timeout")},
		{Key: "content-type", Value: []byte("application/json")},
	})

	got := map[string]string{}
	for _, h := range kept {
		got[h.Key] = string(h.Value)
	}
	if got[HeaderReplayCount] != "1" || got[HeaderReplayOfOffset] != "12" {
		t.Errorf("replay provenance lost: %v", got)
	}
	if len(got) != 4 {
		t.Errorf("carried %d headers, want the 4 replay_ ones: %v", len(got), got)
	}
}

func TestCarryForwardKeepsNothingFromAFirstFailure(t *testing.T) {
	if kept := carryForward(nil); len(kept) != 0 {
		t.Errorf("carried %v from a record that has never been replayed", kept)
	}
}
