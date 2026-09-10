// Package healthcheck lets a service probe itself.
//
// The service images are distroless: no shell, no curl, nothing to run a
// healthcheck with. The one binary guaranteed to be in the image is the service
// itself, so it doubles as its own probe — `/service -healthcheck <target>`
// exits 0 if the target answers and 1 if it does not, which is exactly the
// contract Docker's HEALTHCHECK wants.
//
// The alternative is a fatter base image carrying curl solely to ask a question
// the binary can already answer, which is more attack surface for less.
package healthcheck

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

const probeTimeout = 3 * time.Second

// RunIfRequested handles `-healthcheck <target>` and never returns if it was
// asked to: it exits with the probe's result.
//
// It reads os.Args directly rather than using the flag package because it has to
// run before anything else in main — a service asked to probe itself must not
// first connect to Kafka and Postgres.
//
// Supported targets:
//
//	http://host:port/path — a GET that must answer 2xx
//	tcp://host:port       — a connection that must be accepted
func RunIfRequested() {
	target, ok := requestedTarget(os.Args[1:])
	if !ok {
		return
	}
	if err := Probe(target); err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck failed: %v\n", err)
		os.Exit(1)
	}
	os.Exit(0)
}

func requestedTarget(args []string) (string, bool) {
	for i, arg := range args {
		name, value, inline := strings.Cut(arg, "=")
		if name != "-healthcheck" && name != "--healthcheck" {
			continue
		}
		if inline {
			return value, true
		}
		if i+1 < len(args) {
			return args[i+1], true
		}
		return "", true // asked, but with nothing to probe: fail rather than start
	}
	return "", false
}

// Probe checks one target and reports why it is unhealthy, if it is.
func Probe(target string) error {
	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()

	switch {
	case strings.HasPrefix(target, "http://"), strings.HasPrefix(target, "https://"):
		return probeHTTP(ctx, target)
	case strings.HasPrefix(target, "tcp://"):
		return probeTCP(ctx, strings.TrimPrefix(target, "tcp://"))
	case target == "":
		return fmt.Errorf("no target given")
	default:
		return fmt.Errorf("unsupported target %q: want http:// or tcp://", target)
	}
}

func probeHTTP(ctx context.Context, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("get %s: %w", url, err)
	}
	defer res.Body.Close()

	if res.StatusCode < 200 || res.StatusCode > 299 {
		return fmt.Errorf("%s answered %s", url, res.Status)
	}
	return nil
}

// probeTCP is the check for a gRPC service. Accepting a connection is a weaker
// claim than answering an RPC, but it is an honest one and needs no client: a
// gRPC health service would be the stronger check and is the natural next step
// if this ever needs to distinguish "listening" from "working".
func probeTCP(ctx context.Context, address string) error {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", address)
	if err != nil {
		return fmt.Errorf("dial %s: %w", address, err)
	}
	return conn.Close()
}
