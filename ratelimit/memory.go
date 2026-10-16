package ratelimit

import (
	"context"
	"hash/maphash"
	"math"
	"sync"
	"time"
)

const shardCount = 64

// MemoryOption configures in-memory limiters.
type MemoryOption func(*memoryOptions)

type memoryOptions struct {
	now func() time.Time
}

// WithClock overrides the time source. Intended for tests.
func WithClock(now func() time.Time) MemoryOption {
	return func(o *memoryOptions) { o.now = now }
}

func buildMemoryOptions(opts []MemoryOption) memoryOptions {
	o := memoryOptions{now: time.Now}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// shardedMap is a lock-striped map. Striping keeps contention low under
// parallel load while staying far simpler than a lock-free structure.
type shardedMap[V any] struct {
	seed   maphash.Seed
	shards [shardCount]struct {
		mu sync.Mutex
		m  map[string]V
	}
}

func newShardedMap[V any]() *shardedMap[V] {
	s := &shardedMap[V]{seed: maphash.MakeSeed()}
	for i := range s.shards {
		s.shards[i].m = make(map[string]V)
	}
	return s
}

// with runs fn while holding the lock of the shard owning key.
func (s *shardedMap[V]) with(key string, fn func(m map[string]V)) {
	sh := &s.shards[maphash.String(s.seed, key)%shardCount]
	sh.mu.Lock()
	fn(sh.m)
	sh.mu.Unlock()
}

// sweep deletes entries for which expired returns true.
func (s *shardedMap[V]) sweep(expired func(V) bool) int {
	n := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		for k, v := range sh.m {
			if expired(v) {
				delete(sh.m, k)
				n++
			}
		}
		sh.mu.Unlock()
	}
	return n
}

// ---------------------------------------------------------------- token bucket

type bucket struct {
	tokens float64
	last   time.Time
}

// MemoryTokenBucket is an in-process token bucket limiter.
type MemoryTokenBucket struct {
	cfg  TokenBucket
	opts memoryOptions
	m    *shardedMap[*bucket]
}

// NewMemoryTokenBucket creates an in-memory token bucket limiter.
func NewMemoryTokenBucket(cfg TokenBucket, opts ...MemoryOption) (*MemoryTokenBucket, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &MemoryTokenBucket{cfg: cfg, opts: buildMemoryOptions(opts), m: newShardedMap[*bucket]()}, nil
}

// Allow implements Limiter.
func (l *MemoryTokenBucket) Allow(ctx context.Context, key string) (Result, error) {
	return l.AllowN(ctx, key, 1)
}

// AllowN implements Limiter.
func (l *MemoryTokenBucket) AllowN(_ context.Context, key string, n int) (Result, error) {
	if err := checkArgs(key, n, l.cfg.Burst); err != nil {
		return Result{}, err
	}
	now := l.opts.now()
	burst := float64(l.cfg.Burst)
	var res Result
	l.m.with(key, func(m map[string]*bucket) {
		b, ok := m[key]
		if !ok {
			b = &bucket{tokens: burst, last: now}
			m[key] = b
		}
		if elapsed := now.Sub(b.last); elapsed > 0 {
			b.tokens = math.Min(burst, b.tokens+elapsed.Seconds()*l.cfg.Rate)
			b.last = now
		}
		res.Limit = l.cfg.Burst
		if b.tokens >= float64(n) {
			b.tokens -= float64(n)
			res.Allowed = true
		} else {
			missing := float64(n) - b.tokens
			res.RetryAfter = time.Duration(math.Ceil(missing / l.cfg.Rate * float64(time.Second)))
		}
		res.Remaining = int(math.Floor(b.tokens))
	})
	return res, nil
}

// Sweep removes buckets that have fully refilled (they are equivalent to
// absent ones) and returns how many were removed. Call it periodically to
// bound memory when keys are high-cardinality.
func (l *MemoryTokenBucket) Sweep() int {
	now := l.opts.now()
	full := time.Duration(float64(l.cfg.Burst) / l.cfg.Rate * float64(time.Second))
	return l.m.sweep(func(b *bucket) bool { return now.Sub(b.last) >= full })
}

// -------------------------------------------------------------- sliding window

// eventLog is a ring buffer of event timestamps, oldest first.
type eventLog struct {
	ts    []time.Time
	head  int
	count int
}

func (e *eventLog) at(i int) time.Time { return e.ts[(e.head+i)%len(e.ts)] }

func (e *eventLog) evictBefore(cutoff time.Time) {
	for e.count > 0 && !e.at(0).After(cutoff) {
		e.head = (e.head + 1) % len(e.ts)
		e.count--
	}
}

func (e *eventLog) push(t time.Time) {
	e.ts[(e.head+e.count)%len(e.ts)] = t
	e.count++
}

// MemorySlidingWindow is an in-process sliding window log limiter. Each key
// stores at most Limit timestamps in a fixed-size ring buffer, so memory per
// key is bounded and no allocation happens on the hot path.
type MemorySlidingWindow struct {
	cfg  SlidingWindow
	opts memoryOptions
	m    *shardedMap[*eventLog]
}

// NewMemorySlidingWindow creates an in-memory sliding window log limiter.
func NewMemorySlidingWindow(cfg SlidingWindow, opts ...MemoryOption) (*MemorySlidingWindow, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &MemorySlidingWindow{cfg: cfg, opts: buildMemoryOptions(opts), m: newShardedMap[*eventLog]()}, nil
}

// Allow implements Limiter.
func (l *MemorySlidingWindow) Allow(ctx context.Context, key string) (Result, error) {
	return l.AllowN(ctx, key, 1)
}

// AllowN implements Limiter.
func (l *MemorySlidingWindow) AllowN(_ context.Context, key string, n int) (Result, error) {
	if err := checkArgs(key, n, l.cfg.Limit); err != nil {
		return Result{}, err
	}
	now := l.opts.now()
	var res Result
	l.m.with(key, func(m map[string]*eventLog) {
		e, ok := m[key]
		if !ok {
			e = &eventLog{ts: make([]time.Time, l.cfg.Limit)}
			m[key] = e
		}
		e.evictBefore(now.Add(-l.cfg.Window))
		res.Limit = l.cfg.Limit
		if e.count+n <= l.cfg.Limit {
			for i := 0; i < n; i++ {
				e.push(now)
			}
			res.Allowed = true
		} else {
			// The request fits once the (count+n-limit) oldest events expire.
			oldest := e.at(e.count + n - l.cfg.Limit - 1)
			res.RetryAfter = oldest.Add(l.cfg.Window).Sub(now)
		}
		res.Remaining = l.cfg.Limit - e.count
	})
	return res, nil
}

// Sweep removes keys whose whole log has expired and returns how many were
// removed.
func (l *MemorySlidingWindow) Sweep() int {
	cutoff := l.opts.now().Add(-l.cfg.Window)
	return l.m.sweep(func(e *eventLog) bool {
		return e.count == 0 || !e.at(e.count-1).After(cutoff)
	})
}
