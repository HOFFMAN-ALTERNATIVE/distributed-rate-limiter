// Package ratelimit implements distributed and in-process rate limiting.
//
// Two algorithms are provided:
//
//   - Token bucket: smooth refill at a fixed rate with a configurable burst.
//   - Sliding window log: exact "at most N events in any window W" semantics.
//
// Each algorithm has an in-memory backend (single process, lock-sharded) and a
// Redis backend (multi-instance, one atomic Lua script per decision). All
// backends implement the same Limiter interface, so callers can swap them via
// configuration without code changes.
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Result describes the outcome of a single rate-limit decision.
type Result struct {
	// Allowed reports whether the request may proceed.
	Allowed bool
	// Limit is the configured capacity (burst for token bucket, max events
	// for sliding window).
	Limit int
	// Remaining is the capacity left after this decision (never negative).
	Remaining int
	// RetryAfter is how long the caller should wait before the same request
	// could succeed. Zero when Allowed is true.
	RetryAfter time.Duration
}

// Limiter decides whether an event identified by key may happen now.
type Limiter interface {
	// Allow is shorthand for AllowN(ctx, key, 1).
	Allow(ctx context.Context, key string) (Result, error)
	// AllowN atomically consumes n units of capacity for key if available.
	AllowN(ctx context.Context, key string, n int) (Result, error)
}

// Algorithm identifies a rate-limiting algorithm.
type Algorithm string

const (
	// AlgorithmTokenBucket refills Rate tokens per second up to Burst.
	AlgorithmTokenBucket Algorithm = "token_bucket"
	// AlgorithmSlidingWindow allows at most Limit events per Window.
	AlgorithmSlidingWindow Algorithm = "sliding_window"
)

// Errors returned by limiters.
var (
	ErrInvalidConfig = errors.New("ratelimit: invalid config")
	ErrInvalidCost   = errors.New("ratelimit: cost must be between 1 and the limit")
	ErrEmptyKey      = errors.New("ratelimit: key must not be empty")
)

// TokenBucket configures the token bucket algorithm.
type TokenBucket struct {
	// Rate is the refill rate in tokens per second. Must be > 0.
	Rate float64
	// Burst is the bucket capacity. Must be >= 1.
	Burst int
}

// Validate checks the configuration.
func (c TokenBucket) Validate() error {
	if c.Rate <= 0 || c.Burst < 1 {
		return fmt.Errorf("%w: token bucket needs rate > 0 and burst >= 1 (got rate=%v burst=%d)", ErrInvalidConfig, c.Rate, c.Burst)
	}
	return nil
}

// SlidingWindow configures the sliding window log algorithm.
type SlidingWindow struct {
	// Limit is the maximum number of events in any Window. Must be >= 1.
	Limit int
	// Window is the length of the sliding window. Must be >= 1ms.
	Window time.Duration
}

// Validate checks the configuration.
func (c SlidingWindow) Validate() error {
	if c.Limit < 1 || c.Window < time.Millisecond {
		return fmt.Errorf("%w: sliding window needs limit >= 1 and window >= 1ms (got limit=%d window=%s)", ErrInvalidConfig, c.Limit, c.Window)
	}
	return nil
}

func checkArgs(key string, n, limit int) error {
	if key == "" {
		return ErrEmptyKey
	}
	if n < 1 || n > limit {
		return fmt.Errorf("%w (cost=%d limit=%d)", ErrInvalidCost, n, limit)
	}
	return nil
}

// FailOpen wraps a Limiter so that backend errors (for example Redis being
// unreachable) allow the request instead of failing it. Argument errors such
// as an empty key are still returned. onError, if non-nil, is called with every
// swallowed error so it can be logged or counted.
func FailOpen(l Limiter, onError func(error)) Limiter {
	return &failOpen{next: l, onError: onError}
}

type failOpen struct {
	next    Limiter
	onError func(error)
}

func (f *failOpen) Allow(ctx context.Context, key string) (Result, error) {
	return f.AllowN(ctx, key, 1)
}

func (f *failOpen) AllowN(ctx context.Context, key string, n int) (Result, error) {
	res, err := f.next.AllowN(ctx, key, n)
	if err == nil || errors.Is(err, ErrEmptyKey) || errors.Is(err, ErrInvalidCost) {
		return res, err
	}
	if f.onError != nil {
		f.onError(err)
	}
	return Result{Allowed: true}, nil
}
