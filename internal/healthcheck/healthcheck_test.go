package healthcheck

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRequestedTarget(t *testing.T) {
	cases := []struct {
		name   string
		args   []string
		target string
		asked  bool
	}{
		{"not asked", []string{}, "", false},
		{"other flags", []string{"-v", "-config", "x"}, "", false},
		{"separate value", []string{"-healthcheck", "http://x/readyz"}, "http://x/readyz", true},
		{"inline value", []string{"-healthcheck=http://x/readyz"}, "http://x/readyz", true},
		{"double dash", []string{"--healthcheck", "tcp://x:1"}, "tcp://x:1", true},
		// Asked with nothing to probe must still count as asked: starting the
		// service instead would be a container that never reports unhealthy.
		{"no value", []string{"-healthcheck"}, "", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			target, asked := requestedTarget(tc.args)
			if asked != tc.asked {
				t.Fatalf("asked = %v, want %v", asked, tc.asked)
			}
			if target != tc.target {
				t.Errorf("target = %q, want %q", target, tc.target)
			}
		})
	}
}

func TestProbeHTTP(t *testing.T) {
	ready := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ready.Close()

	notReady := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer notReady.Close()

	if err := Probe(ready.URL + "/readyz"); err != nil {
		t.Errorf("a 200 was reported unhealthy: %v", err)
	}
	if err := Probe(notReady.URL + "/readyz"); err == nil {
		t.Error("a 503 was reported healthy")
	}
}

func TestProbeTCP(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()
	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()

	if err := Probe("tcp://" + lis.Addr().String()); err != nil {
		t.Errorf("a listening port was reported unhealthy: %v", err)
	}

	lis.Close()
	if err := Probe("tcp://" + lis.Addr().String()); err == nil {
		t.Error("a closed port was reported healthy")
	}
}

func TestProbeRejectsUnusableTargets(t *testing.T) {
	for _, target := range []string{"", "localhost:8080", "unix:///tmp/sock", "ftp://x"} {
		if err := Probe(target); err == nil {
			t.Errorf("Probe(%q) reported healthy", target)
		}
	}
}
