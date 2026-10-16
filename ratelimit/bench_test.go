package ratelimit

import (
	"context"
	"strconv"
	"testing"
	"time"
)

func benchLimiter(b *testing.B, l Limiter, keys int) {
	names := make([]string, keys)
	for i := range names {
		names[i] = "key-" + strconv.Itoa(i)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, err := l.Allow(ctx, names[i%keys]); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

func BenchmarkMemoryTokenBucket_HotKey(b *testing.B) {
	l, _ := NewMemoryTokenBucket(TokenBucket{Rate: 1e9, Burst: 1 << 30})
	benchLimiter(b, l, 1)
}

func BenchmarkMemoryTokenBucket_10kKeys(b *testing.B) {
	l, _ := NewMemoryTokenBucket(TokenBucket{Rate: 1e9, Burst: 1 << 30})
	benchLimiter(b, l, 10_000)
}

func BenchmarkMemorySlidingWindow_10kKeys(b *testing.B) {
	l, _ := NewMemorySlidingWindow(SlidingWindow{Limit: 100, Window: time.Second})
	benchLimiter(b, l, 10_000)
}
