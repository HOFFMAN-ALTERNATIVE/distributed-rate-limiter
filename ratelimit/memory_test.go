package ratelimit

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Unix(1_700_000_000, 0)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func mustAllow(t *testing.T, l Limiter, key string, n int, want bool) Result {
	t.Helper()
	res, err := l.AllowN(context.Background(), key, n)
	if err != nil {
		t.Fatalf("AllowN(%q, %d): unexpected error: %v", key, n, err)
	}
	if res.Allowed != want {
		t.Fatalf("AllowN(%q, %d).Allowed = %v, want %v (res=%+v)", key, n, res.Allowed, want, res)
	}
	return res
}

func TestConfigValidation(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"tb zero rate", TokenBucket{Rate: 0, Burst: 1}.Validate()},
		{"tb zero burst", TokenBucket{Rate: 1, Burst: 0}.Validate()},
		{"sw zero limit", SlidingWindow{Limit: 0, Window: time.Second}.Validate()},
		{"sw tiny window", SlidingWindow{Limit: 1, Window: time.Microsecond}.Validate()},
	}
	for _, tc := range cases {
		if !errors.Is(tc.err, ErrInvalidConfig) {
			t.Errorf("%s: got %v, want ErrInvalidConfig", tc.name, tc.err)
		}
	}
	if err := (TokenBucket{Rate: 1, Burst: 1}).Validate(); err != nil {
		t.Errorf("valid token bucket rejected: %v", err)
	}
}

func TestArgumentErrors(t *testing.T) {
	l, _ := NewMemoryTokenBucket(TokenBucket{Rate: 1, Burst: 5})
	if _, err := l.Allow(context.Background(), ""); !errors.Is(err, ErrEmptyKey) {
		t.Errorf("empty key: got %v", err)
	}
	if _, err := l.AllowN(context.Background(), "k", 0); !errors.Is(err, ErrInvalidCost) {
		t.Errorf("zero cost: got %v", err)
	}
	if _, err := l.AllowN(context.Background(), "k", 6); !errors.Is(err, ErrInvalidCost) {
		t.Errorf("cost > burst: got %v", err)
	}
}

func TestMemoryTokenBucket_BurstThenRefill(t *testing.T) {
	clk := newFakeClock()
	l, err := NewMemoryTokenBucket(TokenBucket{Rate: 2, Burst: 3}, WithClock(clk.Now))
	if err != nil {
		t.Fatal(err)
	}
	for i := 2; i >= 0; i-- {
		res := mustAllow(t, l, "a", 1, true)
		if res.Remaining != i || res.Limit != 3 {
			t.Fatalf("remaining=%d limit=%d, want %d/3", res.Remaining, res.Limit, i)
		}
	}
	res := mustAllow(t, l, "a", 1, false)
	if res.RetryAfter != 500*time.Millisecond {
		t.Fatalf("RetryAfter = %s, want 500ms", res.RetryAfter)
	}
	// Other keys are independent.
	mustAllow(t, l, "b", 3, true)

	clk.Advance(499 * time.Millisecond)
	mustAllow(t, l, "a", 1, false)
	clk.Advance(time.Millisecond)
	mustAllow(t, l, "a", 1, true)

	// Refill is capped at burst.
	clk.Advance(time.Hour)
	mustAllow(t, l, "a", 3, true)
	mustAllow(t, l, "a", 1, false)
}

func TestMemoryTokenBucket_CostAndSweep(t *testing.T) {
	clk := newFakeClock()
	l, _ := NewMemoryTokenBucket(TokenBucket{Rate: 1, Burst: 10}, WithClock(clk.Now))
	mustAllow(t, l, "k", 7, true)
	res := mustAllow(t, l, "k", 5, false)
	if res.Remaining != 3 || res.RetryAfter != 2*time.Second {
		t.Fatalf("got %+v, want remaining=3 retry=2s", res)
	}
	if n := l.Sweep(); n != 0 {
		t.Fatalf("Sweep removed %d non-full buckets", n)
	}
	clk.Advance(10 * time.Second)
	if n := l.Sweep(); n != 1 {
		t.Fatalf("Sweep removed %d, want 1", n)
	}
	mustAllow(t, l, "k", 10, true)
}

func TestMemorySlidingWindow_ExactWindow(t *testing.T) {
	clk := newFakeClock()
	l, err := NewMemorySlidingWindow(SlidingWindow{Limit: 3, Window: time.Second}, WithClock(clk.Now))
	if err != nil {
		t.Fatal(err)
	}
	mustAllow(t, l, "k", 1, true) // t=0
	clk.Advance(300 * time.Millisecond)
	mustAllow(t, l, "k", 1, true) // t=300
	clk.Advance(300 * time.Millisecond)
	res := mustAllow(t, l, "k", 1, true) // t=600
	if res.Remaining != 0 {
		t.Fatalf("remaining = %d, want 0", res.Remaining)
	}
	res = mustAllow(t, l, "k", 1, false)
	if res.RetryAfter != 400*time.Millisecond {
		t.Fatalf("RetryAfter = %s, want 400ms", res.RetryAfter)
	}
	// Needing two slots means waiting for the second-oldest event too.
	res = mustAllow(t, l, "k", 2, false)
	if res.RetryAfter != 700*time.Millisecond {
		t.Fatalf("RetryAfter(2) = %s, want 700ms", res.RetryAfter)
	}
	clk.Advance(400 * time.Millisecond) // t=1000: first event expires
	mustAllow(t, l, "k", 1, true)
	mustAllow(t, l, "k", 1, false)
}

func TestMemorySlidingWindow_NoBoundaryBurst(t *testing.T) {
	// A fixed window would allow 2*limit around a boundary; a sliding log must not.
	clk := newFakeClock()
	l, _ := NewMemorySlidingWindow(SlidingWindow{Limit: 5, Window: time.Second}, WithClock(clk.Now))
	clk.Advance(900 * time.Millisecond)
	mustAllow(t, l, "k", 5, true)
	clk.Advance(200 * time.Millisecond)
	mustAllow(t, l, "k", 1, false)
}

func TestMemorySlidingWindow_Sweep(t *testing.T) {
	clk := newFakeClock()
	l, _ := NewMemorySlidingWindow(SlidingWindow{Limit: 2, Window: time.Second}, WithClock(clk.Now))
	mustAllow(t, l, "a", 1, true)
	mustAllow(t, l, "b", 1, true)
	clk.Advance(500 * time.Millisecond)
	mustAllow(t, l, "b", 1, true)
	clk.Advance(600 * time.Millisecond)
	if n := l.Sweep(); n != 1 {
		t.Fatalf("Sweep removed %d, want 1 (only a)", n)
	}
}

// TestConcurrentExactness hammers one key from many goroutines and checks the
// limiter never admits more than its capacity. Run with -race.
func TestConcurrentExactness(t *testing.T) {
	clk := newFakeClock() // frozen clock => no refill during the test
	tb, _ := NewMemoryTokenBucket(TokenBucket{Rate: 1, Burst: 100}, WithClock(clk.Now))
	sw, _ := NewMemorySlidingWindow(SlidingWindow{Limit: 100, Window: time.Minute}, WithClock(clk.Now))
	for name, l := range map[string]Limiter{"token_bucket": tb, "sliding_window": sw} {
		t.Run(name, func(t *testing.T) {
			var allowed atomic.Int64
			var wg sync.WaitGroup
			for g := 0; g < 32; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < 50; i++ {
						res, err := l.Allow(context.Background(), "hot")
						if err != nil {
							t.Error(err)
							return
						}
						if res.Allowed {
							allowed.Add(1)
						}
					}
				}()
			}
			wg.Wait()
			if got := allowed.Load(); got != 100 {
				t.Fatalf("allowed %d requests, want exactly 100", got)
			}
		})
	}
}

type errLimiter struct{ err error }

func (e errLimiter) Allow(ctx context.Context, key string) (Result, error) {
	return e.AllowN(ctx, key, 1)
}
func (e errLimiter) AllowN(context.Context, string, int) (Result, error) { return Result{}, e.err }

func TestFailOpen(t *testing.T) {
	var seen error
	l := FailOpen(errLimiter{err: errors.New("redis down")}, func(err error) { seen = err })
	res, err := l.Allow(context.Background(), "k")
	if err != nil || !res.Allowed || seen == nil {
		t.Fatalf("fail-open: res=%+v err=%v seen=%v", res, err, seen)
	}
	l = FailOpen(errLimiter{err: ErrEmptyKey}, nil)
	if _, err := l.Allow(context.Background(), ""); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("argument errors must not be swallowed, got %v", err)
	}
}
