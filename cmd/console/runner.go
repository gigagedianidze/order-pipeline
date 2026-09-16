package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

// errBusy is returned when a run is already in progress.
var errBusy = errors.New("an experiment is already running")

// line is one piece of output, or one state change, as the UI sees it.
type line struct {
	Seq    int64     `json:"seq"`
	At     time.Time `json:"at"`
	Stream string    `json:"stream"` // stdout | stderr | console
	Text   string    `json:"text"`
}

// runState is what the UI needs to know about the current or last run.
type runState struct {
	ActionID string    `json:"action_id"`
	Label    string    `json:"label"`
	Command  string    `json:"command"`
	Running  bool      `json:"running"`
	Started  time.Time `json:"started"`
	Ended    time.Time `json:"ended,omitempty"`
	ExitCode int       `json:"exit_code"`
	Err      string    `json:"err,omitempty"`
}

// runner executes allowlisted actions, one at a time, and broadcasts their
// output.
//
// One at a time is a correctness requirement, not a politeness one. These
// actions are experiments on a single shared stack: a load run overlapping a
// chaos run produces numbers that describe neither, and two `docker compose up
// --scale` calls racing each other leave the worker count anybody's guess. The
// lock is what lets a reported result mean something.
type runner struct {
	cfg config
	log *slog.Logger

	mu      sync.Mutex
	state   runState
	cancel  context.CancelFunc
	ring    []line // the last ringSize lines, so a late subscriber sees context
	seq     int64
	subs    map[chan line]struct{}
	subsMu  sync.Mutex
	maxRing int
}

func newRunner(cfg config, log *slog.Logger) *runner {
	return &runner{
		cfg:     cfg,
		log:     log,
		subs:    map[chan line]struct{}{},
		maxRing: 500,
	}
}

// start runs an action. It returns as soon as the process has been launched;
// output arrives through subscribe.
func (r *runner) start(a action, p values) error {
	r.mu.Lock()
	if r.state.Running {
		r.mu.Unlock()
		return errBusy
	}

	argv := a.argv(p, r.cfg)
	ctx, cancel := context.WithTimeout(context.Background(), a.Timeout)
	r.cancel = cancel
	r.state = runState{
		ActionID: a.ID,
		Label:    a.Label,
		Command:  shellish(argv),
		Running:  true,
		Started:  time.Now(),
		ExitCode: -1,
	}
	r.mu.Unlock()

	// Clear the previous run's output rather than appending to it. Two runs'
	// logs interleaved in one pane is how a demo turns into a confusing wall of
	// text; the previous run's report is in results/ if it mattered.
	r.resetRing()
	r.emit("console", "$ "+shellish(argv))

	go r.supervise(ctx, cancel, argv, a.Report, a.Env)
	return nil
}

func (r *runner) supervise(ctx context.Context, cancel context.CancelFunc, argv []string, report string, extraEnv []string) {
	defer cancel()

	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Dir = r.cfg.ProjectDir
	if len(extraEnv) > 0 {
		cmd.Env = append(os.Environ(), extraEnv...)
	}
	configureProcessGroup(cmd)

	// Cancel replaces CommandContext's default, which kills the direct child and
	// nothing else. Every action here starts grandchildren — `go run` compiles
	// and runs a separate binary, and each chaos script is a bash process whose
	// real work is the docker commands under it — so the default would leave a
	// load generator running with nothing supervising it.
	cmd.Cancel = func() error { return killTree(cmd) }

	// WaitDelay is what stops a stopped run from wedging the console.
	//
	// Wait cannot return while anything still holds the output pipes, and an
	// orphaned grandchild holds them indefinitely. Without this, cancelling a run
	// left Wait blocked forever, the single-run lock held, and the console
	// permanently busy with no way back except a restart. After the delay, Wait
	// closes the pipes and returns regardless.
	cmd.WaitDelay = 10 * time.Second

	// Writers rather than StdoutPipe: a pipe would have to be fully drained
	// before Wait may be called, which is the deadlock above. With writers, Wait
	// owns the lifetime of the copying goroutines and WaitDelay bounds it.
	stdout := &lineWriter{emit: func(s string) { r.emit("stdout", s) }}
	stderr := &lineWriter{emit: func(s string) { r.emit("stderr", s) }}
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	if err := cmd.Start(); err != nil {
		// Almost always "executable file not found": docker, bash or go is not
		// on this machine's PATH. Name the executable, because the generic
		// message sends people looking at the console instead of their PATH.
		r.finish(-1, fmt.Errorf("could not start %q: %w", argv[0], err))
		return
	}

	waitErr := cmd.Wait()

	// A process that exited without a trailing newline still has a last line
	// worth showing, and it is usually the error message.
	stdout.flush()
	stderr.flush()

	r.appendReport(report)

	code := cmd.ProcessState.ExitCode()
	switch {
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		r.finish(code, errors.New("timed out and was killed"))
	case errors.Is(ctx.Err(), context.Canceled):
		r.finish(code, errors.New("stopped"))
	case waitErr != nil:
		r.finish(code, fmt.Errorf("exited %d", code))
	default:
		r.finish(code, nil)
	}
}

// appendReport streams a file the action wrote into the output pane.
//
// Read after the process has exited, so there is no question of catching it
// half-written. A missing file is not an error worth surfacing: a cancelled run
// legitimately never produces one.
func (r *runner) appendReport(path string) {
	if path == "" {
		return
	}
	full := filepath.Join(r.cfg.ProjectDir, filepath.FromSlash(path))
	f, err := os.Open(full)
	if err != nil {
		return
	}
	defer f.Close()

	r.emit("console", "-- "+path+" --")

	// Bounded, because this is a file the console did not write and a runaway
	// report would otherwise be copied into every connected browser.
	sc := bufio.NewScanner(io.LimitReader(f, maxReport))
	sc.Buffer(make([]byte, 0, 64*1024), maxLine)
	for sc.Scan() {
		r.emit("stdout", sc.Text())
	}
}

// maxReport bounds how much of an action's report file is streamed back.
const maxReport = 256 * 1024

// maxLine caps a single line of output.
//
// `docker compose up` redraws progress with carriage returns and no newline for
// as long as a pull takes, so a buffer that only ever grows on a missing newline
// is a slow memory leak with a plausible trigger.
const maxLine = 64 * 1024

// lineWriter splits a process's output into lines as it arrives.
//
// Writes come from one goroutine per stream, serially, so the buffer needs no
// lock of its own; emit does its own locking.
type lineWriter struct {
	emit func(string)
	buf  []byte
}

func (w *lineWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			break
		}
		// Trailing \r, because Compose and the scripts both emit CRLF on Windows
		// and a stray carriage return renders as a blank line in the browser.
		w.emit(string(bytes.TrimRight(w.buf[:i], "\r")))
		w.buf = w.buf[i+1:]
	}
	if len(w.buf) > maxLine {
		w.emit(string(w.buf[:maxLine]))
		w.buf = w.buf[:0]
	}
	return len(p), nil
}

func (w *lineWriter) flush() {
	if len(w.buf) > 0 {
		w.emit(string(bytes.TrimRight(w.buf, "\r")))
		w.buf = nil
	}
}

func (r *runner) stop() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.state.Running || r.cancel == nil {
		return errors.New("nothing is running")
	}
	r.emit("console", "-- stop requested --")
	r.cancel()
	return nil
}

func (r *runner) finish(code int, err error) {
	r.mu.Lock()
	r.state.Running = false
	r.state.Ended = time.Now()
	r.state.ExitCode = code
	if err != nil {
		r.state.Err = err.Error()
	}
	label := r.state.Label
	r.mu.Unlock()

	if err != nil {
		r.emit("console", fmt.Sprintf("-- %s failed: %v --", label, err))
		r.log.Warn("action failed", "action", label, "exit", code, "err", err)
		return
	}
	r.emit("console", fmt.Sprintf("-- %s finished --", label))
	r.log.Info("action finished", "action", label, "exit", code)
}

func (r *runner) snapshot() (runState, []line) {
	r.mu.Lock()
	st := r.state
	r.mu.Unlock()

	r.subsMu.Lock()
	out := make([]line, len(r.ring))
	copy(out, r.ring)
	r.subsMu.Unlock()
	return st, out
}

func (r *runner) resetRing() {
	r.subsMu.Lock()
	r.ring = nil
	r.subsMu.Unlock()
}

// emit appends to the ring and fans out to subscribers.
//
// The send is non-blocking. A browser tab that has been suspended stops reading
// its channel, and a blocking send would stall the process's output pump behind
// it — the observer would be throttling the thing it is observing, which is the
// one property a console must not have.
func (r *runner) emit(stream, text string) {
	r.subsMu.Lock()
	r.seq++
	l := line{Seq: r.seq, At: time.Now(), Stream: stream, Text: text}
	r.ring = append(r.ring, l)
	if len(r.ring) > r.maxRing {
		r.ring = r.ring[len(r.ring)-r.maxRing:]
	}
	for ch := range r.subs {
		select {
		case ch <- l:
		default:
		}
	}
	r.subsMu.Unlock()
}

func (r *runner) subscribe() (<-chan line, func()) {
	ch := make(chan line, 256)
	r.subsMu.Lock()
	r.subs[ch] = struct{}{}
	r.subsMu.Unlock()

	return ch, func() {
		r.subsMu.Lock()
		delete(r.subs, ch)
		close(ch)
		r.subsMu.Unlock()
	}
}

// shellish renders argv for display only. It is never parsed back into a
// command, which is why naive quoting is acceptable here and would not be
// anywhere else.
func shellish(argv []string) string {
	out := ""
	for i, a := range argv {
		if i > 0 {
			out += " "
		}
		out += a
	}
	return out
}
