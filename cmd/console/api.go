package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

// server wires the allowlist, the runner and the two pollers to HTTP.
type server struct {
	cfg   config
	log   *slog.Logger
	auth  *auth
	run   *runner
	prom  *prom
	stack *stackWatcher
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /{$}", s.page)
	mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServer(http.FS(staticFS()))))

	mux.HandleFunc("GET /api/session", s.session)
	mux.HandleFunc("POST /api/login", s.csrf(s.login))
	mux.HandleFunc("POST /api/logout", s.csrf(s.logout))

	mux.HandleFunc("GET /api/actions", s.auth.require(s.listActions))
	mux.HandleFunc("GET /api/state", s.auth.require(s.state))
	mux.HandleFunc("GET /api/stream", s.auth.require(s.stream))
	mux.HandleFunc("POST /api/run", s.csrf(s.auth.require(s.runAction)))
	mux.HandleFunc("POST /api/stop", s.csrf(s.auth.require(s.stopAction)))

	return mux
}

// csrf rejects state-changing requests that did not come from the console's own
// page.
//
// The session is a cookie, so a form on any other site could otherwise POST to
// /api/run and the browser would attach it. SameSite=Lax already blocks the
// obvious version of that; this header is the belt to its braces, and costs one
// line in the fetch call. A cross-origin <form> cannot set a custom header at
// all, and a cross-origin fetch that tries turns the request into a preflight
// this server never approves.
func (s *server) csrf(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Console-Request") != "1" {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "bad request origin"})
			return
		}
		next(w, r)
	}
}

func (s *server) page(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// The page is one file with no third-party anything, so the policy can be
	// this tight. 'unsafe-inline' for style only, because the sparkline bars are
	// sized from data.
	w.Header().Set("Content-Security-Policy",
		"default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	http.ServeFileFS(w, r, staticFS(), "index.html")
}

func (s *server) session(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"authenticated": s.auth.valid(r)})
}

func (s *server) login(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Password string `json:"password"`
	}
	if err := decode(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := s.auth.login(w, r, body.Password); err != nil {
		var he *httpError
		if errors.As(err, &he) {
			s.log.Warn("login rejected", "ip", s.auth.clientIP(r), "reason", he.msg)
			writeJSON(w, he.status, map[string]string{"error": he.msg})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "login failed"})
		return
	}
	s.log.Info("login accepted", "ip", s.auth.clientIP(r))
	writeJSON(w, http.StatusOK, map[string]any{"authenticated": true})
}

func (s *server) logout(w http.ResponseWriter, r *http.Request) {
	s.auth.logout(w, r)
	writeJSON(w, http.StatusOK, map[string]any{"authenticated": false})
}

func (s *server) listActions(w http.ResponseWriter, r *http.Request) {
	list := actionList()
	out := make([]map[string]any, 0, len(list))
	for _, a := range list {
		params := make([]map[string]any, 0, len(a.Params))
		for _, p := range a.Params {
			params = append(params, map[string]any{
				"name": p.Name, "label": p.Label, "kind": string(p.Kind),
				"default": p.Default, "choices": p.Choices,
				"min": p.Min, "max": a.ceiling(p, s.cfg), "unit": p.Unit,
			})
		}
		out = append(out, map[string]any{
			"id": a.ID, "label": a.Label, "group": a.Group,
			"summary": a.Summary, "destructive": a.Destructive, "params": params,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"actions": out})
}

func (s *server) state(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
	defer cancel()

	runState, _ := s.run.snapshot()
	writeJSON(w, http.StatusOK, map[string]any{
		"stack":   s.stack.snapshot(),
		"metrics": s.prom.snapshot(ctx),
		"run":     runState,
	})
}

func (s *server) runAction(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Action string            `json:"action"`
		Params map[string]string `json:"params"`
	}
	if err := decode(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	a, ok := actions[body.Action]
	if !ok {
		// The unknown id is not echoed back. Reflecting caller-controlled text
		// into a response is a habit worth not having, and the caller already
		// knows what it sent.
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such action"})
		return
	}

	p, err := a.validate(body.Params, s.cfg)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	if err := s.run.start(a, p); err != nil {
		if errors.Is(err, errBusy) {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.log.Info("action started", "action", a.ID, "ip", s.auth.clientIP(r))

	st, _ := s.run.snapshot()
	writeJSON(w, http.StatusAccepted, st)
}

func (s *server) stopAction(w http.ResponseWriter, r *http.Request) {
	if err := s.run.stop(); err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"stopping": true})
}

// stream is the output pane's feed.
//
// Server-sent events rather than a WebSocket: the traffic is one-directional and
// text, SSE reconnects by itself, and it needs no protocol upgrade — which
// matters when the console is reached through a tunnel or reverse proxy that
// would otherwise have to be configured to allow one.
func (s *server) stream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// Proxies that buffer would hold the output until the run ended, which is
	// the opposite of the point.
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	ch, unsubscribe := s.run.subscribe()
	defer unsubscribe()

	// Replay the current buffer before streaming, so a tab opened mid-run shows
	// the run so far instead of starting blank.
	_, backlog := s.run.snapshot()
	for _, l := range backlog {
		if !writeSSE(w, flusher, l) {
			return
		}
	}

	// A comment every 20s. Idle connections are otherwise reaped by whatever sits
	// in the middle, and the experiments have long quiet stretches by design.
	ping := time.NewTicker(20 * time.Second)
	defer ping.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case l, ok := <-ch:
			if !ok || !writeSSE(w, flusher, l) {
				return
			}
		case <-ping.C:
			if _, err := w.Write([]byte(": ping\n\n")); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func writeSSE(w http.ResponseWriter, f http.Flusher, l line) bool {
	payload, err := json.Marshal(l)
	if err != nil {
		return true
	}
	if _, err := w.Write([]byte("data: " + string(payload) + "\n\n")); err != nil {
		return false
	}
	f.Flush()
	return true
}

func decode(r *http.Request, into any) error {
	r.Body = http.MaxBytesReader(nil, r.Body, 64*1024)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(into); err != nil {
		return errors.New("malformed request body")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
