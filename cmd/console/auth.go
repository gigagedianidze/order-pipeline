package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
)

const sessionCookie = "console_session"

// auth is the console's login.
//
// One shared password, not accounts: there is exactly one person who should be
// able to press these buttons, and a user table would be more code protecting
// the same single secret. What it does take seriously is the two ways a
// one-password login actually fails — a guessable password, and an attacker who
// is allowed to keep guessing. bcrypt answers the first; the throttle answers
// the second.
type auth struct {
	cfg config

	mu       sync.Mutex
	failures map[string]*failure
}

type failure struct {
	count int
	until time.Time
}

func newAuth(cfg config) *auth {
	return &auth{cfg: cfg, failures: map[string]*failure{}}
}

// login verifies the password and issues a session cookie.
func (a *auth) login(w http.ResponseWriter, r *http.Request, password string) error {
	ip := a.clientIP(r)

	if wait := a.lockedFor(ip); wait > 0 {
		return &httpError{http.StatusTooManyRequests,
			"too many attempts, try again in " + wait.Round(time.Second).String()}
	}

	// CompareHashAndPassword is constant-time in the comparison and deliberately
	// slow in the hashing, which is the whole point of bcrypt: a correct guess
	// and a wrong one cost the attacker the same, and both cost enough that
	// guessing at scale is not worth it.
	if err := bcrypt.CompareHashAndPassword(a.cfg.PasswordHash, []byte(password)); err != nil {
		a.recordFailure(ip)
		return &httpError{http.StatusUnauthorized, "wrong password"}
	}

	a.clearFailures(ip)
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    a.issue(time.Now().Add(a.cfg.SessionTTL)),
		Path:     "/",
		HttpOnly: true,
		Secure:   a.secure(r),
		SameSite: http.SameSiteLaxMode,
		MaxAge:   int(a.cfg.SessionTTL.Seconds()),
	})
	return nil
}

func (a *auth) logout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		Secure:   a.secure(r),
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
	})
}

// issue builds a signed cookie value: the expiry, and an HMAC over it.
//
// Nothing is stored server-side, so a restart does not log the operator out —
// provided CONSOLE_SESSION_KEY is set. The cookie carries no identity because
// there is only one, and no privileges, so the only thing forging it could gain
// is a longer session, which the signature prevents anyway.
func (a *auth) issue(expiry time.Time) string {
	payload := strconv.FormatInt(expiry.Unix(), 10)
	return payload + "." + base64.RawURLEncoding.EncodeToString(a.sign(payload))
}

func (a *auth) sign(payload string) []byte {
	m := hmac.New(sha256.New, a.cfg.SessionKey)
	m.Write([]byte(payload))
	return m.Sum(nil)
}

// valid reports whether the request carries a live session.
func (a *auth) valid(r *http.Request) bool {
	c, err := r.Cookie(sessionCookie)
	if err != nil {
		return false
	}
	payload, sig, ok := strings.Cut(c.Value, ".")
	if !ok {
		return false
	}
	want, err := base64.RawURLEncoding.DecodeString(sig)
	if err != nil {
		return false
	}
	// Signature first, then expiry. Reading the expiry out of an unverified
	// cookie and acting on it would be trusting the attacker's own number.
	if subtle.ConstantTimeCompare(want, a.sign(payload)) != 1 {
		return false
	}
	expiry, err := strconv.ParseInt(payload, 10, 64)
	if err != nil {
		return false
	}
	return time.Now().Unix() < expiry
}

// require wraps a handler so it is only reachable with a session.
func (a *auth) require(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !a.valid(r) {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "not logged in"})
			return
		}
		next(w, r)
	}
}

// lockedFor returns how long this address must wait, backing off as failures
// accumulate. Capped at a minute: long enough to make online guessing useless,
// short enough that the operator's own typo does not lock them out of a demo.
func (a *auth) lockedFor(ip string) time.Duration {
	a.mu.Lock()
	defer a.mu.Unlock()
	f, ok := a.failures[ip]
	if !ok {
		return 0
	}
	if d := time.Until(f.until); d > 0 {
		return d
	}
	return 0
}

func (a *auth) recordFailure(ip string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	f, ok := a.failures[ip]
	if !ok {
		f = &failure{}
		a.failures[ip] = f
	}
	f.count++
	// The first two attempts are free; a typo should not be punished.
	if f.count < 3 {
		return
	}
	backoff := time.Duration(1<<min(f.count-3, 6)) * time.Second
	if backoff > time.Minute {
		backoff = time.Minute
	}
	f.until = time.Now().Add(backoff)
}

func (a *auth) clearFailures(ip string) {
	a.mu.Lock()
	delete(a.failures, ip)
	a.mu.Unlock()
}

// clientIP is the throttle's key.
//
// X-Forwarded-For is only read when the console has been told it is behind a
// proxy. Unconditionally trusting it would hand every client a free reset of its
// own throttle: set the header to a new value each attempt and the backoff never
// fires.
func (a *auth) clientIP(r *http.Request) string {
	if a.cfg.TrustedProxy {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			first, _, _ := strings.Cut(xff, ",")
			if ip := strings.TrimSpace(first); ip != "" {
				return ip
			}
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// secure decides whether the session cookie gets the Secure flag.
//
// It has to be off for plain http://localhost, because a browser will not store
// a Secure cookie from an insecure origin and the operator would appear to log
// in successfully and stay logged out.
func (a *auth) secure(r *http.Request) bool {
	if a.cfg.SecureCookie {
		return true
	}
	if a.cfg.TrustedProxy && r.Header.Get("X-Forwarded-Proto") == "https" {
		return true
	}
	return r.TLS != nil
}

type httpError struct {
	status int
	msg    string
}

func (e *httpError) Error() string { return e.msg }
