// Command ratelimiterd is a standalone HTTP rate-limiting service.
//
// Other services call GET /check?key=<key> and receive a JSON decision, so
// the limiting policy lives in one place regardless of the caller's language.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/HOFFMAN-ALTERNATIVE/distributed-rate-limiter/internal/config"
	"github.com/HOFFMAN-ALTERNATIVE/distributed-rate-limiter/internal/server"
	"github.com/HOFFMAN-ALTERNATIVE/distributed-rate-limiter/metrics"
	"github.com/HOFFMAN-ALTERNATIVE/distributed-rate-limiter/ratelimit"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/redis/go-redis/v9"
)

func main() {
	// `ratelimiterd healthcheck` lets a distroless image (no shell, no curl)
	// run a Docker HEALTHCHECK with the binary it already has.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		if err := healthcheck(); err != nil {
			fmt.Fprintln(os.Stderr, "unhealthy:", err)
			os.Exit(1)
		}
		return
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load(nil)
	if err != nil {
		return err
	}

	limiter, ready, closeFn, err := buildLimiter(cfg)
	if err != nil {
		return err
	}
	defer closeFn()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	limiter = metrics.NewCollectors(reg).Instrument(string(cfg.Algorithm), limiter)
	if cfg.FailOpen {
		limiter = ratelimit.FailOpen(limiter, func(err error) { log.Warn("backend error, failing open", "err", err) })
	}

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           (&server.Server{Limiter: limiter, Ready: ready, Gatherer: reg, Timeout: cfg.Timeout}).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errCh := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", cfg.Addr, "backend", cfg.Backend, "algorithm", cfg.Algorithm)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		log.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
	return nil
}

func buildLimiter(cfg config.Config) (ratelimit.Limiter, func(context.Context) error, func(), error) {
	if cfg.Backend == "memory" {
		var l interface {
			ratelimit.Limiter
			Sweep() int
		}
		var err error
		if cfg.Algorithm == ratelimit.AlgorithmSlidingWindow {
			l, err = ratelimit.NewMemorySlidingWindow(cfg.Window)
		} else {
			l, err = ratelimit.NewMemoryTokenBucket(cfg.Bucket)
		}
		if err != nil {
			return nil, nil, nil, err
		}
		// Periodically drop idle keys so memory stays bounded.
		done := make(chan struct{})
		go func() {
			t := time.NewTicker(time.Minute)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					l.Sweep()
				case <-done:
					return
				}
			}
		}()
		return l, nil, func() { close(done) }, nil
	}

	// Explicit timeouts: the per-decision context deadline bounds each call,
	// these bound connection setup and pool waits underneath it.
	client := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		DB:           cfg.RedisDB,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
		PoolTimeout:  time.Second,
	})
	ready := func(ctx context.Context) error { return client.Ping(ctx).Err() }
	closeFn := func() { _ = client.Close() }
	var (
		l   ratelimit.Limiter
		err error
	)
	if cfg.Algorithm == ratelimit.AlgorithmSlidingWindow {
		l, err = ratelimit.NewRedisSlidingWindow(client, cfg.Window, ratelimit.WithKeyPrefix(cfg.KeyPrefix))
	} else {
		l, err = ratelimit.NewRedisTokenBucket(client, cfg.Bucket, ratelimit.WithKeyPrefix(cfg.KeyPrefix))
	}
	if err != nil {
		closeFn()
		return nil, nil, nil, err
	}
	return l, ready, closeFn, nil
}

// healthcheck probes /healthz on the configured port over loopback.
func healthcheck() error {
	addr := os.Getenv("RL_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://127.0.0.1:" + port + "/healthz")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}
