package ratelimit

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	//go:embed scripts/token_bucket.lua
	tokenBucketLua string
	//go:embed scripts/sliding_window.lua
	slidingWindowLua string

	tokenBucketScript   = redis.NewScript(tokenBucketLua)
	slidingWindowScript = redis.NewScript(slidingWindowLua)
)

// RedisOption configures Redis-backed limiters.
type RedisOption func(*redisOptions)

type redisOptions struct {
	prefix string
}

// WithKeyPrefix sets the prefix prepended to every Redis key
// (default "rl:"). Use it to namespace several limiters in one Redis.
func WithKeyPrefix(p string) RedisOption {
	return func(o *redisOptions) { o.prefix = p }
}

func buildRedisOptions(opts []RedisOption) redisOptions {
	o := redisOptions{prefix: "rl:"}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// RedisTokenBucket is a token bucket shared by every process using the same
// Redis. Each decision is a single EVALSHA round-trip.
type RedisTokenBucket struct {
	client redis.Scripter
	cfg    TokenBucket
	opts   redisOptions
}

// NewRedisTokenBucket creates a Redis-backed token bucket limiter. client may
// be a *redis.Client, *redis.ClusterClient or *redis.Ring.
func NewRedisTokenBucket(client redis.Scripter, cfg TokenBucket, opts ...RedisOption) (*RedisTokenBucket, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &RedisTokenBucket{client: client, cfg: cfg, opts: buildRedisOptions(opts)}, nil
}

// Allow implements Limiter.
func (l *RedisTokenBucket) Allow(ctx context.Context, key string) (Result, error) {
	return l.AllowN(ctx, key, 1)
}

// AllowN implements Limiter.
func (l *RedisTokenBucket) AllowN(ctx context.Context, key string, n int) (Result, error) {
	if err := checkArgs(key, n, l.cfg.Burst); err != nil {
		return Result{}, err
	}
	raw, err := tokenBucketScript.Run(ctx, l.client,
		[]string{l.opts.prefix + "tb:{" + key + "}"},
		strconv.FormatFloat(l.cfg.Rate, 'f', -1, 64), l.cfg.Burst, n,
	).Int64Slice()
	if err != nil {
		return Result{}, fmt.Errorf("ratelimit: redis token bucket: %w", err)
	}
	return parseScriptResult(raw, l.cfg.Burst)
}

// RedisSlidingWindow is a sliding window log shared by every process using
// the same Redis. Events are stored in a sorted set scored by Redis time.
type RedisSlidingWindow struct {
	client redis.Scripter
	cfg    SlidingWindow
	opts   redisOptions
}

// NewRedisSlidingWindow creates a Redis-backed sliding window log limiter.
func NewRedisSlidingWindow(client redis.Scripter, cfg SlidingWindow, opts ...RedisOption) (*RedisSlidingWindow, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &RedisSlidingWindow{client: client, cfg: cfg, opts: buildRedisOptions(opts)}, nil
}

// Allow implements Limiter.
func (l *RedisSlidingWindow) Allow(ctx context.Context, key string) (Result, error) {
	return l.AllowN(ctx, key, 1)
}

// AllowN implements Limiter.
func (l *RedisSlidingWindow) AllowN(ctx context.Context, key string, n int) (Result, error) {
	if err := checkArgs(key, n, l.cfg.Limit); err != nil {
		return Result{}, err
	}
	raw, err := slidingWindowScript.Run(ctx, l.client,
		[]string{l.opts.prefix + "sw:{" + key + "}"},
		l.cfg.Limit, l.cfg.Window.Microseconds(), n, uniqueID(),
	).Int64Slice()
	if err != nil {
		return Result{}, fmt.Errorf("ratelimit: redis sliding window: %w", err)
	}
	return parseScriptResult(raw, l.cfg.Limit)
}

func parseScriptResult(raw []int64, limit int) (Result, error) {
	if len(raw) != 3 {
		return Result{}, fmt.Errorf("ratelimit: unexpected script reply %v", raw)
	}
	remaining := int(raw[1])
	if remaining < 0 {
		remaining = 0
	}
	return Result{
		Allowed:    raw[0] == 1,
		Limit:      limit,
		Remaining:  remaining,
		RetryAfter: time.Duration(raw[2]) * time.Microsecond,
	}, nil
}

var (
	idPrefix  = randomHex(8)
	idCounter atomic.Uint64
)

// uniqueID returns a process-unique, cluster-unique-with-high-probability id
// used to keep sorted-set members distinct when timestamps collide.
func uniqueID() string {
	return idPrefix + strconv.FormatUint(idCounter.Add(1), 36)
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(b)
}
