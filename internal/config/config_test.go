package config

import (
	"testing"
	"time"

	"github.com/HOFFMAN-ALTERNATIVE/distributed-rate-limiter/ratelimit"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestDefaults(t *testing.T) {
	c, err := Load(env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if c.Addr != ":8080" || c.Backend != "memory" || c.Algorithm != ratelimit.AlgorithmTokenBucket || c.Bucket.Burst != 20 {
		t.Fatalf("unexpected defaults: %+v", c)
	}
}

func TestOverrides(t *testing.T) {
	c, err := Load(env(map[string]string{
		"RL_BACKEND": "redis", "RL_ALGORITHM": "sliding_window",
		"RL_LIMIT": "5", "RL_WINDOW": "10s", "RL_FAIL_OPEN": "true",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if c.Backend != "redis" || c.Window.Limit != 5 || c.Window.Window != 10*time.Second || !c.FailOpen {
		t.Fatalf("overrides not applied: %+v", c)
	}
}

func TestInvalid(t *testing.T) {
	for _, m := range []map[string]string{
		{"RL_BURST": "lots"},
		{"RL_BACKEND": "etcd"},
		{"RL_ALGORITHM": "leaky"},
		{"RL_RATE": "0"},
		{"RL_WINDOW": "forever", "RL_ALGORITHM": "sliding_window"},
	} {
		if _, err := Load(env(m)); err == nil {
			t.Errorf("%v: expected error", m)
		}
	}
}
