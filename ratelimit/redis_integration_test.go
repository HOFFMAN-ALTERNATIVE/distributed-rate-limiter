//go:build integration

package ratelimit

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Run with: REDIS_ADDR=localhost:6379 go test -tags integration ./...

func realRedis(tb testing.TB) *redis.Client {
	tb.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	c := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		tb.Fatalf("redis at %s not reachable: %v", addr, err)
	}
	if err := c.FlushDB(ctx).Err(); err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = c.Close() })
	return c
}

func TestIntegration_TokenBucket(t *testing.T)   { testRedisTokenBucket(t, realRedis(t)) }
func TestIntegration_SlidingWindow(t *testing.T) { testRedisSlidingWindow(t, realRedis(t)) }

// TestIntegration_DistributedExactness simulates several service instances
// (independent clients and connection pools) sharing one Redis and checks
// the global limit holds exactly.
func TestIntegration_DistributedExactness(t *testing.T) {
	base := realRedis(t)
	opts := base.Options()
	const instances, perInstance, limit = 4, 100, 150
	for _, alg := range []Algorithm{AlgorithmTokenBucket, AlgorithmSlidingWindow} {
		t.Run(string(alg), func(t *testing.T) {
			var allowed atomic.Int64
			var wg sync.WaitGroup
			for i := 0; i < instances; i++ {
				c := redis.NewClient(opts)
				defer c.Close()
				var l Limiter
				if alg == AlgorithmTokenBucket {
					l, _ = NewRedisTokenBucket(c, TokenBucket{Rate: 0.001, Burst: limit}, WithKeyPrefix("dist:"))
				} else {
					l, _ = NewRedisSlidingWindow(c, SlidingWindow{Limit: limit, Window: time.Hour}, WithKeyPrefix("dist:"))
				}
				for g := 0; g < 4; g++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for j := 0; j < perInstance/4; j++ {
							res, err := l.Allow(context.Background(), "global")
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
			}
			wg.Wait()
			if got := allowed.Load(); got != limit {
				t.Fatalf("allowed %d across %d instances, want exactly %d", got, instances, limit)
			}
		})
	}
}

func TestIntegration_TokenBucketRefill(t *testing.T) {
	l, _ := NewRedisTokenBucket(realRedis(t), TokenBucket{Rate: 20, Burst: 1})
	mustAllow(t, l, "r", 1, true)
	mustAllow(t, l, "r", 1, false)
	time.Sleep(80 * time.Millisecond) // 20/s => one token every 50ms
	mustAllow(t, l, "r", 1, true)
}

func TestIntegration_SlidingWindowExpiry(t *testing.T) {
	l, _ := NewRedisSlidingWindow(realRedis(t), SlidingWindow{Limit: 1, Window: 100 * time.Millisecond})
	mustAllow(t, l, "e", 1, true)
	mustAllow(t, l, "e", 1, false)
	time.Sleep(150 * time.Millisecond)
	mustAllow(t, l, "e", 1, true)
}

func BenchmarkRedisTokenBucket(b *testing.B) {
	l, _ := NewRedisTokenBucket(realRedis(b), TokenBucket{Rate: 1e9, Burst: 1 << 30})
	ctx := context.Background()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := l.Allow(ctx, "bench"); err != nil {
				b.Fatal(err)
			}
		}
	})
}
