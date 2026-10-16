package middleware

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/HOFFMAN-ALTERNATIVE/distributed-rate-limiter/ratelimit"
)

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
}

func TestRateLimit_AllowsThenRejects(t *testing.T) {
	l, _ := ratelimit.NewMemoryTokenBucket(ratelimit.TokenBucket{Rate: 0.5, Burst: 2})
	h := RateLimit(l, Options{})(okHandler())

	codes := []int{}
	var last *httptest.ResponseRecorder
	for i := 0; i < 3; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = "203.0.113.7:5555"
		h.ServeHTTP(rec, req)
		codes = append(codes, rec.Code)
		last = rec
	}
	if codes[0] != 200 || codes[1] != 200 || codes[2] != 429 {
		t.Fatalf("codes = %v, want [200 200 429]", codes)
	}
	if got := last.Header().Get("Retry-After"); got != "2" {
		t.Fatalf("Retry-After = %q, want 2", got)
	}
	if got := last.Header().Get("RateLimit-Remaining"); got != "0" {
		t.Fatalf("RateLimit-Remaining = %q, want 0", got)
	}

	// A different client has its own budget.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "198.51.100.1:1"
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("other client got %d", rec.Code)
	}
}

func TestRateLimit_EmptyKeySkips(t *testing.T) {
	l, _ := ratelimit.NewMemoryTokenBucket(ratelimit.TokenBucket{Rate: 1, Burst: 1})
	h := RateLimit(l, Options{Key: KeyByHeader("X-API-Key")})(okHandler())
	for i := 0; i < 5; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		if rec.Code != 200 {
			t.Fatalf("request without key should bypass limiting, got %d", rec.Code)
		}
	}
}

type failing struct{}

func (failing) Allow(ctx context.Context, k string) (ratelimit.Result, error) {
	return failing{}.AllowN(ctx, k, 1)
}
func (failing) AllowN(context.Context, string, int) (ratelimit.Result, error) {
	return ratelimit.Result{}, errors.New("backend down")
}

func TestRateLimit_ErrorHandling(t *testing.T) {
	rec := httptest.NewRecorder()
	RateLimit(failing{}, Options{})(okHandler()).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("fail-closed: got %d, want 503", rec.Code)
	}
	rec = httptest.NewRecorder()
	RateLimit(ratelimit.FailOpen(failing{}, nil), Options{})(okHandler()).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("fail-open: got %d, want 200", rec.Code)
	}
}

func TestKeyFuncs(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = "[2001:db8::1]:443"
	if got := KeyByRemoteIP(r); got != "2001:db8::1" {
		t.Errorf("KeyByRemoteIP = %q", got)
	}
	r.Header.Set("X-Forwarded-For", " 192.0.2.10 , 10.0.0.1")
	if got := KeyByHeader("X-Forwarded-For")(r); got != "192.0.2.10" {
		t.Errorf("KeyByHeader = %q", got)
	}
	if got := RetryAfterSeconds(1500 * time.Millisecond); got != 2 {
		t.Errorf("RetryAfterSeconds = %d", got)
	}
}
