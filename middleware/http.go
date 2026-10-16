// Package middleware provides net/http rate-limiting middleware.
package middleware

import (
	"errors"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/HOFFMAN-ALTERNATIVE/distributed-rate-limiter/ratelimit"
)

// KeyFunc extracts the rate-limit key from a request. Returning an empty key
// skips limiting for that request.
type KeyFunc func(*http.Request) string

// Options configures the middleware.
type Options struct {
	// Key extracts the limiting key. Default: KeyByRemoteIP.
	Key KeyFunc
	// Cost returns how many units a request consumes. Default: 1.
	Cost func(*http.Request) int
	// OnLimited writes the response for rejected requests.
	// Default: 429 with a plain-text body.
	OnLimited http.HandlerFunc
	// OnError is called when the limiter fails. Default: 503.
	// Wrap the limiter with ratelimit.FailOpen to allow on errors instead.
	OnError func(http.ResponseWriter, *http.Request, error)
}

// RateLimit returns middleware that enforces l on every request and sets the
// RateLimit-Limit / RateLimit-Remaining / Retry-After headers.
func RateLimit(l ratelimit.Limiter, opts Options) func(http.Handler) http.Handler {
	if opts.Key == nil {
		opts.Key = KeyByRemoteIP
	}
	if opts.Cost == nil {
		opts.Cost = func(*http.Request) int { return 1 }
	}
	if opts.OnLimited == nil {
		opts.OnLimited = func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		}
	}
	if opts.OnError == nil {
		opts.OnError = func(w http.ResponseWriter, _ *http.Request, err error) {
			status := http.StatusServiceUnavailable
			if errors.Is(err, ratelimit.ErrInvalidCost) {
				status = http.StatusBadRequest
			}
			http.Error(w, http.StatusText(status), status)
		}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := opts.Key(r)
			if key == "" {
				next.ServeHTTP(w, r)
				return
			}
			res, err := l.AllowN(r.Context(), key, opts.Cost(r))
			if err != nil {
				opts.OnError(w, r, err)
				return
			}
			SetHeaders(w.Header(), res)
			if !res.Allowed {
				opts.OnLimited(w, r)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// SetHeaders writes standard rate-limit headers for res.
func SetHeaders(h http.Header, res ratelimit.Result) {
	if res.Limit > 0 {
		h.Set("RateLimit-Limit", strconv.Itoa(res.Limit))
		h.Set("RateLimit-Remaining", strconv.Itoa(res.Remaining))
	}
	if !res.Allowed && res.RetryAfter > 0 {
		h.Set("Retry-After", strconv.Itoa(RetryAfterSeconds(res.RetryAfter)))
	}
}

// RetryAfterSeconds rounds d up to whole seconds, as Retry-After requires.
func RetryAfterSeconds(d time.Duration) int { return int(math.Ceil(d.Seconds())) }

// KeyByRemoteIP keys requests by the client IP from RemoteAddr. It does not
// trust X-Forwarded-For; use KeyByHeader behind a trusted proxy.
func KeyByRemoteIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// KeyByHeader keys requests by the value of header (for example an API key or
// a header set by a trusted reverse proxy). For comma-separated values such
// as X-Forwarded-For, the first element is used.
func KeyByHeader(header string) KeyFunc {
	return func(r *http.Request) string {
		v := r.Header.Get(header)
		if i := strings.IndexByte(v, ','); i >= 0 {
			v = v[:i]
		}
		return strings.TrimSpace(v)
	}
}
