package main

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"
)

// An action is one button. It turns a validated set of parameters into an argv
// slice, and nothing else.
//
// The console's whole security model lives in this file. A web page that can
// stop containers is, mechanically, remote command execution; the only thing
// separating "a demo" from "a shell" is that the command is never assembled from
// what the client sent. So:
//
//   - argv is built here, from constants, and handed to exec.Command as a slice.
//     No shell is involved, so there is no quoting to get wrong and no way for a
//     parameter to become a second command.
//   - Parameters are not strings that get interpolated. They are validated into a
//     number or an enum member first, and it is the validated value that is
//     formatted in. A parameter that does not parse never reaches argv.
//   - The set of runnable commands is this map. There is no "run an arbitrary
//     make target" escape hatch, because the moment one exists the allowlist is
//     decorative.
type action struct {
	ID      string
	Label   string
	Group   string
	Summary string

	// Destructive marks an action that loses data or interrupts service. The UI
	// makes the caller confirm; the server does not care, because a confirmation
	// dialog is advice to the operator, not a security control.
	Destructive bool

	// Timeout kills the process if it outlives it. Every action has one: a chaos
	// script that wedges would otherwise hold the single-run lock forever and
	// leave the console permanently busy with no way back except a restart.
	Timeout time.Duration

	Params []param

	// Report is a file the action writes its real result into, appended to the
	// output pane when the run finishes.
	//
	// The chaos scripts need this and the others do not: they redirect the load
	// generator's whole report to a file and echo only progress, so streaming
	// stdout alone would show "stopping kafka" and never the 384 refusals that
	// are the entire point. A constant path, never anything from the caller.
	Report string

	// Env is added to the child's environment. Constants only, for the same
	// reason argv is: it is another way for a caller's text to reach a process.
	//
	// Its one job here is to move console-driven runs off the tags the recorded
	// experiments use. Every script writes to results/reports/$TAG.txt, so
	// without this a visitor pressing "Kill the broker" silently overwrites the
	// recorded evidence that README.md's tables cite — destroying the thing the
	// demo exists to show off.
	Env []string

	// argv builds the command, and runs only after every param has validated.
	argv func(p values, cfg config) []string
}

type paramKind string

const (
	paramNumber paramKind = "number"
	paramChoice paramKind = "choice"
)

type param struct {
	Name    string
	Label   string
	Kind    paramKind
	Default string
	Choices []string // paramChoice only
	Min     float64  // paramNumber only
	Max     float64  // paramNumber only
	Unit    string
}

// values holds parameters that have already been validated. The types are the
// point: by the time argv sees a rate it is a float64, not text a caller chose.
type values struct {
	numbers map[string]float64
	choices map[string]string
}

func (v values) num(name string) float64 { return v.numbers[name] }
func (v values) str(name string) string  { return v.choices[name] }

// validate turns the raw request into values, rejecting anything that does not
// fit. cfg is passed in because two of the bounds, load rate and duration, are
// deployment limits rather than properties of the action itself.
func (a action) validate(raw map[string]string, cfg config) (values, error) {
	out := values{numbers: map[string]float64{}, choices: map[string]string{}}
	for _, p := range a.Params {
		got, ok := raw[p.Name]
		if !ok || got == "" {
			got = p.Default
		}
		switch p.Kind {
		case paramNumber:
			n, err := strconv.ParseFloat(got, 64)
			if err != nil {
				return values{}, fmt.Errorf("%s must be a number", p.Label)
			}
			// NaN has to be rejected explicitly, before the range check rather
			// than by it. ParseFloat accepts the literal "NaN", and every
			// comparison against NaN is false — so `n < min || n > max` is false
			// too, and a bounds check that looks exhaustive waves it straight
			// through to argv.
			if math.IsNaN(n) || math.IsInf(n, 0) {
				return values{}, fmt.Errorf("%s must be a finite number", p.Label)
			}
			max := a.ceiling(p, cfg)
			if n < p.Min || n > max {
				return values{}, fmt.Errorf("%s must be between %g and %g%s",
					p.Label, p.Min, max, unitSuffix(p.Unit))
			}
			out.numbers[p.Name] = n
		case paramChoice:
			// An enum is checked by membership, never by "looks reasonable".
			if !contains(p.Choices, got) {
				return values{}, fmt.Errorf("%s must be one of %v", p.Label, p.Choices)
			}
			out.choices[p.Name] = got
		}
	}
	return out, nil
}

// ceiling is the effective upper bound for a numeric parameter: the action's own
// limit, lowered by the deployment's if it is stricter. Lowered only, never
// raised, so a permissive environment variable cannot talk an action into a
// value it was not written to accept.
func (a action) ceiling(p param, cfg config) float64 {
	max := p.Max
	if a.ID == "load.send" {
		switch p.Name {
		case "rate":
			max = min(max, cfg.MaxLoadRate)
		case "duration":
			max = min(max, cfg.MaxLoadDuration.Seconds())
		}
	}
	return max
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

func unitSuffix(u string) string {
	if u == "" {
		return ""
	}
	return " " + u
}

// actions is the allowlist. Adding a button means adding an entry here; there is
// no other route from an HTTP request to a process.
var actions = map[string]action{
	"stack.up": {
		ID:      "stack.up",
		Label:   "Start stack",
		Group:   "stack",
		Summary: "Bring up Kafka, Postgres, the services and Prometheus.",
		Timeout: 5 * time.Minute,
		argv: func(values, config) []string {
			return []string{"docker", "compose", "up", "-d"}
		},
	},
	"stack.down": {
		ID:          "stack.down",
		Label:       "Stop stack",
		Group:       "stack",
		Summary:     "Stop every container. Volumes, and so all data, are kept.",
		Destructive: true,
		Timeout:     3 * time.Minute,
		argv: func(values, config) []string {
			return []string{"docker", "compose", "down"}
		},
	},
	"stack.reset": {
		ID:          "stack.reset",
		Label:       "Reset (deletes data)",
		Group:       "stack",
		Summary:     "Stop everything and delete the Kafka and Postgres volumes.",
		Destructive: true,
		Timeout:     3 * time.Minute,
		argv: func(values, config) []string {
			return []string{"docker", "compose", "down", "-v"}
		},
	},
	"workers.scale": {
		ID:      "workers.scale",
		Label:   "Scale workers",
		Group:   "stack",
		Summary: "Change the consumer group size and watch Kafka rebalance.",
		Timeout: 3 * time.Minute,
		Params: []param{{
			Name: "n", Label: "Workers", Kind: paramChoice,
			Choices: []string{"0", "1", "2", "4", "8"}, Default: "2",
		}},
		argv: func(p values, _ config) []string {
			return []string{"docker", "compose", "up", "-d", "--scale", "worker=" + p.str("n")}
		},
	},
	"load.send": {
		ID:      "load.send",
		Label:   "Send load",
		Group:   "load",
		Summary: "Drive the API at a scheduled rate and report what actually happened.",
		Timeout: 10 * time.Minute,
		Params: []param{
			{Name: "rate", Label: "Rate", Kind: paramNumber, Min: 1, Max: 20000, Default: "1000", Unit: "orders/s"},
			{Name: "duration", Label: "Duration", Kind: paramNumber, Min: 1, Max: 120, Default: "15", Unit: "s"},
		},
		argv: func(p values, _ config) []string {
			// Formatted from a validated float64, not from the caller's text.
			// Even if the formatting were wrong the result is still a single
			// argv element, and loadgen's own flag parser rejects it.
			return []string{
				"go", "run", "./cmd/loadgen",
				"-rate", strconv.FormatFloat(p.num("rate"), 'f', -1, 64),
				"-duration", strconv.FormatInt(int64(p.num("duration")), 10) + "s",
			}
		},
	},
	"chaos.db": {
		ID:          "chaos.db",
		Label:       "Kill Postgres",
		Group:       "chaos",
		Summary:     "Remove the database for 90s under load. It comes back by itself.",
		Destructive: true,
		Timeout:     10 * time.Minute,
		Report:      "results/reports/console-chaos-db.txt",
		Env:         []string{"TAG=console-chaos-db"},
		argv: func(values, config) []string {
			return []string{"bash", "scripts/chaos-db-outage.sh"}
		},
	},
	"chaos.kafka": {
		ID:          "chaos.kafka",
		Label:       "Kill the broker",
		Group:       "chaos",
		Summary:     "Remove Kafka for 45s under load. Writes are refused while it is gone.",
		Destructive: true,
		Timeout:     10 * time.Minute,
		Report:      "results/reports/console-chaos-kafka.txt",
		Env:         []string{"TAG=console-chaos-kafka"},
		argv: func(values, config) []string {
			return []string{"bash", "scripts/chaos-kafka.sh"}
		},
	},
	"dlq.inspect": {
		ID:      "dlq.inspect",
		Label:   "Inspect dead letters",
		Group:   "recovery",
		Summary: "What is on the dead-letter topic, and what would be replayed. Changes nothing.",
		Timeout: 3 * time.Minute,
		argv: func(values, config) []string {
			return []string{"go", "run", "./cmd/replay"}
		},
	},
	"dlq.replay": {
		ID:    "dlq.replay",
		Label: "Replay dead letters",
		Group: "recovery",
		Summary: "Send the recoverable dead letters back through the pipeline, " +
			"then check the rows reached PostgreSQL.",
		// It writes to the source topic. Nothing it does is unsafe — the write is
		// idempotent and poison records are left alone — but a button that puts
		// records back into a running system should say so before it does.
		Destructive: true,
		Timeout:     10 * time.Minute,
		argv: func(values, config) []string {
			return []string{"go", "run", "./cmd/replay", "-apply", "-verify", "60s"}
		},
	},
	"experiment.matrix": {
		ID:      "experiment.matrix",
		Label:   "Scaling matrix",
		Group:   "experiment",
		Summary: "3 partitions, 1/2/4/8 workers, drain measured at each. Minutes, not seconds.",
		Timeout: 45 * time.Minute,
		Env:     []string{"OUT=results/console-scaling.tsv"},
		argv: func(values, config) []string {
			return []string{"bash", "scripts/scaling-matrix.sh", "3", "1", "2", "4", "8"}
		},
	},
	"experiment.batch": {
		ID:      "experiment.batch",
		Label:   "Batch matrix",
		Group:   "experiment",
		Summary: "1/10/50/200 records per round trip, one worker. Minutes, not seconds.",
		Timeout: 45 * time.Minute,
		Env:     []string{"OUT=results/console-batching.tsv"},
		argv: func(values, config) []string {
			return []string{"bash", "scripts/batch-matrix.sh", "1", "10", "50", "200"}
		},
	},
	"experiment.ramp": {
		ID:      "experiment.ramp",
		Label:   "Overload ramp",
		Group:   "experiment",
		Summary: "1k to 20k orders/s until something gives. Minutes, not seconds.",
		Timeout: 45 * time.Minute,
		Env:     []string{"OUT=results/console-ramp.tsv"},
		argv: func(values, config) []string {
			return []string{"bash", "scripts/ramp.sh", "1000", "2500", "5000", "10000", "20000"}
		},
	},
}

// actionList returns the allowlist in a stable order, for the UI to render.
func actionList() []action {
	out := make([]action, 0, len(actions))
	for _, a := range actions {
		out = append(out, a)
	}
	groupRank := map[string]int{"stack": 0, "load": 1, "chaos": 2, "experiment": 3}
	sort.Slice(out, func(i, j int) bool {
		if groupRank[out[i].Group] != groupRank[out[j].Group] {
			return groupRank[out[i].Group] < groupRank[out[j].Group]
		}
		return out[i].ID < out[j].ID
	})
	return out
}
