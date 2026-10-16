package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// These tests run the real Lua scripts against miniredis (an in-process Redis
// with a Lua interpreter). The integration suite (build tag "integration")
// runs the same contract against a real Redis server.

func newMiniredis(t *testing.T) *redis.Client {
	t.Helper()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestRedisTokenBucket_Miniredis(t *testing.T) {
	testRedisTokenBucket(t, newMiniredis(t))
}

func TestRedisSlidingWindow_Miniredis(t *testing.T) {
	testRedisSlidingWindow(t, newMiniredis(t))
}

func TestRedisBackendError(t *testing.T) {
	c := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 50 * time.Millisecond, MaxRetries: -1})
	defer c.Close()
	l, _ := NewRedisTokenBucket(c, TokenBucket{Rate: 1, Burst: 1})
	if _, err := l.Allow(context.Background(), "k"); err == nil {
		t.Fatal("expected error from unreachable redis")
	}
	res, err := FailOpen(l, nil).Allow(context.Background(), "k")
	if err != nil || !res.Allowed {
		t.Fatalf("fail-open should allow: res=%+v err=%v", res, err)
	}
}

// testRedisTokenBucket is shared by the miniredis and real-Redis suites. It
// uses a slow refill so wall-clock time during the test cannot affect results.
func testRedisTokenBucket(t *testing.T, c redis.Scripter) {
	t.Helper()
	ctx := context.Background()
	l, err := NewRedisTokenBucket(c, TokenBucket{Rate: 0.01, Burst: 5}, WithKeyPrefix("test:"+t.Name()+":"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 4; i >= 0; i-- {
		res := mustAllow(t, l, "user", 1, true)
		if res.Remaining != i {
			t.Fatalf("remaining = %d, want %d", res.Remaining, i)
		}
	}
	res := mustAllow(t, l, "user", 1, false)
	if res.RetryAfter < 90*time.Second || res.RetryAfter > 100*time.Second {
		t.Fatalf("RetryAfter = %s, want ~100s", res.RetryAfter)
	}
	mustAllow(t, l, "other", 5, true)
	if _, err := l.AllowN(ctx, "user", 6); err == nil {
		t.Fatal("cost > burst must be rejected")
	}
}

func testRedisSlidingWindow(t *testing.T, c redis.Scripter) {
	t.Helper()
	l, err := NewRedisSlidingWindow(c, SlidingWindow{Limit: 4, Window: time.Minute}, WithKeyPrefix("test:"+t.Name()+":"))
	if err != nil {
		t.Fatal(err)
	}
	mustAllow(t, l, "ip", 3, true)
	res := mustAllow(t, l, "ip", 1, true)
	if res.Remaining != 0 {
		t.Fatalf("remaining = %d, want 0", res.Remaining)
	}
	res = mustAllow(t, l, "ip", 1, false)
	if res.RetryAfter <= 0 || res.RetryAfter > time.Minute {
		t.Fatalf("RetryAfter = %s, want (0, 1m]", res.RetryAfter)
	}
	mustAllow(t, l, "other-ip", 4, true)
}
