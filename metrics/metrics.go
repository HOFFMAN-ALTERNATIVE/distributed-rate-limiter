// Package metrics instruments a ratelimit.Limiter with Prometheus metrics.
package metrics

import (
	"context"
	"time"

	"github.com/HOFFMAN-ALTERNATIVE/distributed-rate-limiter/ratelimit"
	"github.com/prometheus/client_golang/prometheus"
)

// Collectors holds the Prometheus collectors used by Instrument.
type Collectors struct {
	Decisions *prometheus.CounterVec
	Errors    *prometheus.CounterVec
	Latency   *prometheus.HistogramVec
}

// NewCollectors creates and registers the collectors on reg.
//
// Metrics are labelled by limiter name and decision only, never by key:
// per-key labels would explode cardinality.
func NewCollectors(reg prometheus.Registerer) *Collectors {
	c := &Collectors{
		Decisions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "ratelimit",
			Name:      "decisions_total",
			Help:      "Rate-limit decisions by limiter and outcome.",
		}, []string{"limiter", "decision"}),
		Errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "ratelimit",
			Name:      "errors_total",
			Help:      "Backend errors while evaluating a rate limit.",
		}, []string{"limiter"}),
		Latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "ratelimit",
			Name:      "decision_duration_seconds",
			Help:      "Time to reach a rate-limit decision.",
			Buckets:   []float64{.00001, .00005, .0001, .00025, .0005, .001, .0025, .005, .01, .025, .1},
		}, []string{"limiter"}),
	}
	reg.MustRegister(c.Decisions, c.Errors, c.Latency)
	return c
}

// Instrument wraps l so every decision is recorded under the given name.
func (c *Collectors) Instrument(name string, l ratelimit.Limiter) ratelimit.Limiter {
	return &instrumented{
		next:    l,
		allowed: c.Decisions.WithLabelValues(name, "allowed"),
		limited: c.Decisions.WithLabelValues(name, "limited"),
		errors:  c.Errors.WithLabelValues(name),
		latency: c.Latency.WithLabelValues(name),
	}
}

type instrumented struct {
	next             ratelimit.Limiter
	allowed, limited prometheus.Counter
	errors           prometheus.Counter
	latency          prometheus.Observer
}

func (i *instrumented) Allow(ctx context.Context, key string) (ratelimit.Result, error) {
	return i.AllowN(ctx, key, 1)
}

func (i *instrumented) AllowN(ctx context.Context, key string, n int) (ratelimit.Result, error) {
	start := time.Now()
	res, err := i.next.AllowN(ctx, key, n)
	i.latency.Observe(time.Since(start).Seconds())
	switch {
	case err != nil:
		i.errors.Inc()
	case res.Allowed:
		i.allowed.Inc()
	default:
		i.limited.Inc()
	}
	return res, err
}
