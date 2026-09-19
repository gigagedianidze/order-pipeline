package main

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gigagedianidze/order-pipeline/internal/broker"

	"github.com/twmb/franz-go/pkg/kgo"
)

// rec builds a dead letter with the headers the worker's DLQ writes.
func rec(reason string, replays int) *kgo.Record {
	r := &kgo.Record{Partition: 2, Offset: 41, Value: []byte(`{"event_id":"e-1"}`)}
	if reason != "" {
		r.Headers = append(r.Headers, kgo.RecordHeader{Key: broker.HeaderReason, Value: []byte(reason)})
	}
	if replays > 0 {
		r.Headers = append(r.Headers,
			kgo.RecordHeader{Key: broker.HeaderReplayCount, Value: []byte(strconv.Itoa(replays))})
	}
	return r
}

// The reason filter is the difference between recovering an outage and putting a
// poison record back into a loop, so every combination is pinned.
func TestDecideAppliesTheReasonFilter(t *testing.T) {
	retries := string(broker.ReasonRetriesExhausted)
	poison := string(broker.ReasonPoison)

	cases := []struct {
		name       string
		record     *kgo.Record
		reasons    string
		wantReplay bool
		wantSkip   string
	}{
		{"default replays an exhausted retry", rec(retries, 0), retries, true, ""},
		{"default leaves poison alone", rec(poison, 0), retries, false, "reason:" + poison},
		{"poison replays when asked for by name", rec(poison, 0), poison, true, ""},
		{"all replays both", rec(poison, 0), "all", true, ""},
		{"a record with no dlq_reason is not ours", rec("", 0), "all", false, "no_dlq_reason"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wanted, err := parseReasons(tc.reasons)
			if err != nil {
				t.Fatalf("parseReasons(%q): %v", tc.reasons, err)
			}
			got := decide(tc.record, wanted, 2)
			if got.replay != tc.wantReplay {
				t.Errorf("replay = %v, want %v", got.replay, tc.wantReplay)
			}
			if got.skip != tc.wantSkip {
				t.Errorf("skip = %q, want %q", got.skip, tc.wantSkip)
			}
		})
	}
}

// The loop guard is the reason a permanent failure cannot become perpetual
// traffic: DLQ -> orders -> DLQ -> orders, each lap looking like work.
func TestDecideStopsRecordsThatHaveCircledEnough(t *testing.T) {
	wanted, err := parseReasons(string(broker.ReasonRetriesExhausted))
	if err != nil {
		t.Fatal(err)
	}
	reason := string(broker.ReasonRetriesExhausted)

	for replays, wantReplay := range map[int]bool{0: true, 1: true, 2: false, 7: false} {
		got := decide(rec(reason, replays), wanted, 2)
		if got.replay != wantReplay {
			t.Errorf("replay_count=%d: replay = %v, want %v", replays, got.replay, wantReplay)
		}
		if !wantReplay && got.skip != "replay_limit" {
			t.Errorf("replay_count=%d: skip = %q, want %q", replays, got.skip, "replay_limit")
		}
		if got.count != replays {
			t.Errorf("replay_count=%d: count = %d", replays, got.count)
		}
	}
}

// An unreadable counter must read as zero rather than as "already at the limit".
// Failing closed here would quietly make the tool refuse to recover anything the
// moment a header was malformed.
func TestReplayCountToleratesRubbish(t *testing.T) {
	for _, raw := range []string{"", "not-a-number", "-3"} {
		r := &kgo.Record{Headers: []kgo.RecordHeader{{Key: broker.HeaderReplayCount, Value: []byte(raw)}}}
		if got := replayCount(r); got != 0 {
			t.Errorf("replayCount(%q) = %d, want 0", raw, got)
		}
	}
}

// A replayed record must be shaped like a source-topic record — original bytes,
// provenance in headers, no stale diagnosis from the death it is recovering from.
func TestReplayHeadersStampProvenanceAndDropTheDiagnosis(t *testing.T) {
	dead := rec(string(broker.ReasonRetriesExhausted), 1)
	dead.Headers = append(dead.Headers,
		kgo.RecordHeader{Key: broker.HeaderError, Value: []byte("connect timeout")})

	headers := replayHeaders(dead, 2, time.Date(2026, 9, 18, 10, 0, 0, 0, time.UTC))

	got := map[string]string{}
	for _, h := range headers {
		if _, dup := got[h.Key]; dup {
			t.Errorf("header %q written twice", h.Key)
		}
		got[h.Key] = string(h.Value)
	}

	want := map[string]string{
		broker.HeaderReplayCount:       "2",
		broker.HeaderReplayAt:          "2026-09-18T10:00:00Z",
		broker.HeaderReplayOfPartition: "2",
		broker.HeaderReplayOfOffset:    "41",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("header %s = %q, want %q", k, got[k], v)
		}
	}
	for _, stale := range []string{broker.HeaderReason, broker.HeaderError} {
		if _, ok := got[stale]; ok {
			t.Errorf("header %s was carried into the source topic", stale)
		}
	}
}

func TestParseReasonsRejectsWhatItCannotHonour(t *testing.T) {
	if _, err := parseReasons("retries_exhaused"); err == nil {
		t.Error("a misspelled reason was accepted; it would have replayed nothing, silently")
	}
	if _, err := parseReasons(" , "); err == nil {
		t.Error("an empty reason list was accepted")
	}
	got, err := parseReasons(" poison , retries_exhausted ")
	if err != nil {
		t.Fatalf("spacing rejected: %v", err)
	}
	if !got[string(broker.ReasonPoison)] || !got[string(broker.ReasonRetriesExhausted)] {
		t.Errorf("parsed %v, want both reasons", got)
	}
}

func TestEventIDIsEmptyForAPoisonRecord(t *testing.T) {
	if id := eventID([]byte(`{"event_id":"abc"}`)); id != "abc" {
		t.Errorf("eventID = %q, want abc", id)
	}
	if id := eventID([]byte(`{ this is not json`)); id != "" {
		t.Errorf("eventID of a poison record = %q, want empty", id)
	}
}

// The dry-run banner is the one line that tells an operator nothing happened.
func TestReportPrintSaysWhenNothingWasDone(t *testing.T) {
	var sb strings.Builder
	(&report{DryRun: true, Scanned: 6, Replayed: 6, Skipped: map[string]int64{}}).print(&sb)
	if !strings.Contains(sb.String(), "DRY RUN") || !strings.Contains(sb.String(), "-apply") {
		t.Errorf("dry-run report does not say it changed nothing:\n%s", sb.String())
	}
}
