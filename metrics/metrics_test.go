package metrics

import (
	"context"
	"testing"

	"github.com/HOFFMAN-ALTERNATIVE/distributed-rate-limiter/ratelimit"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestInstrument(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := NewCollectors(reg)
	base, _ := ratelimit.NewMemoryTokenBucket(ratelimit.TokenBucket{Rate: 0.1, Burst: 2})
	l := c.Instrument("api", base)
	for i := 0; i < 5; i++ {
		if _, err := l.Allow(context.Background(), "k"); err != nil {
			t.Fatal(err)
		}
	}
	if got := testutil.ToFloat64(c.Decisions.WithLabelValues("api", "allowed")); got != 2 {
		t.Errorf("allowed = %v, want 2", got)
	}
	if got := testutil.ToFloat64(c.Decisions.WithLabelValues("api", "limited")); got != 3 {
		t.Errorf("limited = %v, want 3", got)
	}
	if n := testutil.CollectAndCount(c.Latency); n != 1 {
		t.Errorf("latency series = %d, want 1", n)
	}
}
