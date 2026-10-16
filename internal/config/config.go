// Package config loads service configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/HOFFMAN-ALTERNATIVE/distributed-rate-limiter/ratelimit"
)

// Config is the runtime configuration of the ratelimiterd service.
type Config struct {
	Addr      string              // RL_ADDR            listen address
	Backend   string              // RL_BACKEND         "memory" | "redis"
	Algorithm ratelimit.Algorithm // RL_ALGORITHM       "token_bucket" | "sliding_window"
	RedisAddr string              // RL_REDIS_ADDR
	RedisDB   int                 // RL_REDIS_DB
	KeyPrefix string              // RL_KEY_PREFIX
	FailOpen  bool                // RL_FAIL_OPEN       allow requests when the backend errors
	Timeout   time.Duration       // RL_BACKEND_TIMEOUT per-decision deadline
	Bucket    ratelimit.TokenBucket
	Window    ratelimit.SlidingWindow
}

// Load reads the configuration using getenv (os.Getenv when nil).
func Load(getenv func(string) string) (Config, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	p := parser{getenv: getenv}
	c := Config{
		Addr:      p.str("RL_ADDR", ":8080"),
		Backend:   p.str("RL_BACKEND", "memory"),
		Algorithm: ratelimit.Algorithm(p.str("RL_ALGORITHM", string(ratelimit.AlgorithmTokenBucket))),
		RedisAddr: p.str("RL_REDIS_ADDR", "localhost:6379"),
		RedisDB:   p.int("RL_REDIS_DB", 0),
		KeyPrefix: p.str("RL_KEY_PREFIX", "rl:"),
		FailOpen:  p.bool("RL_FAIL_OPEN", false),
		Timeout:   p.dur("RL_BACKEND_TIMEOUT", 50*time.Millisecond),
		Bucket: ratelimit.TokenBucket{
			Rate:  p.float("RL_RATE", 10),
			Burst: p.int("RL_BURST", 20),
		},
		Window: ratelimit.SlidingWindow{
			Limit:  p.int("RL_LIMIT", 100),
			Window: p.dur("RL_WINDOW", time.Minute),
		},
	}
	if p.err != nil {
		return Config{}, p.err
	}
	return c, c.Validate()
}

// Validate checks cross-field constraints.
func (c Config) Validate() error {
	switch c.Backend {
	case "memory", "redis":
	default:
		return fmt.Errorf("config: RL_BACKEND must be memory or redis, got %q", c.Backend)
	}
	switch c.Algorithm {
	case ratelimit.AlgorithmTokenBucket:
		return c.Bucket.Validate()
	case ratelimit.AlgorithmSlidingWindow:
		return c.Window.Validate()
	default:
		return fmt.Errorf("config: RL_ALGORITHM must be token_bucket or sliding_window, got %q", c.Algorithm)
	}
}

type parser struct {
	getenv func(string) string
	err    error
}

func (p *parser) raw(k string) (string, bool) {
	v := p.getenv(k)
	return v, v != ""
}

func (p *parser) fail(k, v string, err error) {
	if p.err == nil {
		p.err = fmt.Errorf("config: invalid %s=%q: %w", k, v, err)
	}
}

func (p *parser) str(k, def string) string {
	if v, ok := p.raw(k); ok {
		return v
	}
	return def
}

func (p *parser) int(k string, def int) int {
	v, ok := p.raw(k)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		p.fail(k, v, err)
	}
	return n
}

func (p *parser) float(k string, def float64) float64 {
	v, ok := p.raw(k)
	if !ok {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		p.fail(k, v, err)
	}
	return f
}

func (p *parser) bool(k string, def bool) bool {
	v, ok := p.raw(k)
	if !ok {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		p.fail(k, v, err)
	}
	return b
}

func (p *parser) dur(k string, def time.Duration) time.Duration {
	v, ok := p.raw(k)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		p.fail(k, v, err)
	}
	return d
}
