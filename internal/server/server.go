// Package server implements the ratelimiterd HTTP API.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/HOFFMAN-ALTERNATIVE/distributed-rate-limiter/middleware"
	"github.com/HOFFMAN-ALTERNATIVE/distributed-rate-limiter/ratelimit"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// CheckResponse is the JSON body returned by /check.
type CheckResponse struct {
	Allowed      bool   `json:"allowed"`
	Key          string `json:"key"`
	Limit        int    `json:"limit"`
	Remaining    int    `json:"remaining"`
	RetryAfterMS int64  `json:"retry_after_ms"`
}

// Server wires a limiter to HTTP handlers.
type Server struct {
	Limiter  ratelimit.Limiter
	Ready    func(context.Context) error // readiness probe, e.g. Redis PING
	Gatherer prometheus.Gatherer
	Timeout  time.Duration // per-decision backend deadline
}

// Handler returns the HTTP routes:
//
//	GET|POST /check?key=<key>&cost=<n>  decision as JSON (200 allowed, 429 limited)
//	GET      /demo                       sample endpoint protected by the middleware (keyed by client IP)
//	GET      /healthz                    liveness
//	GET      /readyz                     readiness (backend reachable)
//	GET      /metrics                    Prometheus metrics
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/check", s.check)
	mux.Handle("/demo", middleware.RateLimit(s.Limiter, middleware.Options{})(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("ok\n"))
		})))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if s.Ready != nil {
			if err := s.Ready(r.Context()); err != nil {
				http.Error(w, "not ready: "+err.Error(), http.StatusServiceUnavailable)
				return
			}
		}
		_, _ = w.Write([]byte("ready\n"))
	})
	if s.Gatherer != nil {
		mux.Handle("/metrics", promhttp.HandlerFor(s.Gatherer, promhttp.HandlerOpts{}))
	}
	return mux
}

func (s *Server) check(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		w.Header().Set("Allow", "GET, POST")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	key := r.FormValue("key")
	cost := 1
	if c := r.FormValue("cost"); c != "" {
		n, err := strconv.Atoi(c)
		if err != nil {
			writeError(w, http.StatusBadRequest, "cost must be an integer")
			return
		}
		cost = n
	}
	ctx := r.Context()
	if s.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.Timeout)
		defer cancel()
	}
	res, err := s.Limiter.AllowN(ctx, key, cost)
	switch {
	case errors.Is(err, ratelimit.ErrEmptyKey), errors.Is(err, ratelimit.ErrInvalidCost):
		writeError(w, http.StatusBadRequest, err.Error())
		return
	case err != nil:
		writeError(w, http.StatusServiceUnavailable, "rate limiter backend unavailable")
		return
	}
	middleware.SetHeaders(w.Header(), res)
	status := http.StatusOK
	if !res.Allowed {
		status = http.StatusTooManyRequests
	}
	writeJSON(w, status, CheckResponse{
		Allowed:      res.Allowed,
		Key:          key,
		Limit:        res.Limit,
		Remaining:    res.Remaining,
		RetryAfterMS: res.RetryAfter.Milliseconds(),
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
