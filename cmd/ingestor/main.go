// Command ingestor runs the HTTP API, the stream worker, or both.
//
//	ingestor api     # accept events over HTTP
//	ingestor worker  # drain the Redis Stream into Postgres
//	ingestor all     # both in one process (local development)
//	ingestor dlq-replay [n]  # move up to n dead-lettered messages back into the stream
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"

	"github.com/msalimcengiz/go-event-ingestor/internal/api"
	"github.com/msalimcengiz/go-event-ingestor/internal/config"
	"github.com/msalimcengiz/go-event-ingestor/internal/event"
	"github.com/msalimcengiz/go-event-ingestor/internal/store"
	"github.com/msalimcengiz/go-event-ingestor/internal/stream"
	"github.com/msalimcengiz/go-event-ingestor/internal/worker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel()}))
	role := "all"
	if len(os.Args) > 1 {
		role = os.Args[1]
	}
	if err := run(role, logger); err != nil {
		logger.Error("exit", "err", err)
		os.Exit(1)
	}
}

func run(role string, logger *slog.Logger) error {
	if role != "api" && role != "worker" && role != "all" && role != "dlq-replay" {
		return fmt.Errorf("unknown role %q (want api, worker, all or dlq-replay)", role)
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pg, err := store.NewPostgres(ctx, cfg.PostgresURL)
	if err != nil {
		return err
	}
	defer pg.Close()
	if err := pg.Migrate(ctx); err != nil {
		return err
	}

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, PoolSize: 64})
	defer func() { _ = rdb.Close() }()
	rs := stream.NewRedis(rdb, cfg.Stream, cfg.ConsumerGroup)

	if role == "dlq-replay" {
		n := int64(1000)
		if len(os.Args) > 2 {
			if _, err := fmt.Sscan(os.Args[2], &n); err != nil {
				return fmt.Errorf("dlq-replay count: %w", err)
			}
		}
		moved, err := rs.Replay(ctx, n)
		logger.Info("replayed dead letters", "moved", moved)
		return err
	}

	g, ctx := errgroup.WithContext(ctx)

	if role == "api" || role == "all" {
		g.Go(func() error { return serveAPI(ctx, cfg, pg, rdb, rs, logger) })
	}
	if role == "worker" || role == "all" {
		w := &worker.Worker{
			Stream:        rs,
			Store:         pg,
			Consumer:      cfg.ConsumerName,
			BatchSize:     cfg.BatchSize,
			FlushInterval: cfg.FlushInterval,
			ClaimMinIdle:  cfg.ClaimMinIdle,
			MaxDeliveries: cfg.MaxDeliveries,
			Logger:        logger.With("role", "worker"),
		}
		g.Go(func() error { return w.Run(ctx) })
		if role == "worker" {
			g.Go(func() error { return serveMetricsOnly(ctx, cfg.HTTPAddr) })
		}
	}

	logger.Info("started", "role", role, "mode", cfg.WriteMode, "batch_size", cfg.BatchSize, "flush_interval", cfg.FlushInterval)
	return g.Wait()
}

func serveAPI(ctx context.Context, cfg config.Config, pg *store.Postgres, rdb *redis.Client, rs *stream.Redis, logger *slog.Logger) error {
	var sink api.Sink
	switch cfg.WriteMode {
	case config.ModeDirect:
		sink = func(ctx context.Context, events []event.Event) error {
			for _, e := range events {
				if err := pg.InsertOne(ctx, e); err != nil {
					return err
				}
			}
			return nil
		}
	default:
		sink = rs.Publish
	}

	if len(cfg.APIKeys) == 0 {
		logger.Warn("API_KEYS is empty: authentication is disabled")
	}
	draining := &atomic.Bool{}
	srv := &api.Server{
		Sink:         sink,
		Mode:         string(cfg.WriteMode),
		MaxBodyBytes: cfg.MaxBodyBytes,
		Admit:        admission(ctx, rs, cfg.MaxBacklog, logger),
		APIKeys:      cfg.APIKeys,
		Draining:     draining,
		Lookup:       pg.Find,
		Logger:       logger.With("role", "api"),
		Now:          time.Now,
		Ready: func(ctx context.Context) error {
			if err := pg.Ping(ctx); err != nil {
				return fmt.Errorf("postgres: %w", err)
			}
			if err := rdb.Ping(ctx).Err(); err != nil {
				return fmt.Errorf("redis: %w", err)
			}
			return nil
		},
	}
	return listenDraining(ctx, draining, cfg.ShutdownDelay, &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           srv.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	})
}

// admission samples the stream backlog in the background and rejects new events while
// it is above maxBacklog, so a slow database turns into client retries instead of an
// unbounded Redis memory footprint. Fails open if Redis cannot be read (the publish
// itself will then fail and return 503).
func admission(ctx context.Context, rs *stream.Redis, maxBacklog int64, logger *slog.Logger) func() error {
	var over atomic.Bool
	go func() {
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()
		for {
			if b, err := rs.Backlog(ctx); err == nil {
				was := over.Swap(b.Total() > maxBacklog)
				if was != over.Load() {
					logger.Warn("backpressure changed", "on", over.Load(), "backlog", b.Total(), "limit", maxBacklog)
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	return func() error {
		if over.Load() {
			return api.ErrBackpressure
		}
		return nil
	}
}

// listenDraining marks the server not-ready on shutdown, keeps serving for delay so
// load balancers notice, then drains in-flight requests.
func listenDraining(ctx context.Context, draining *atomic.Bool, delay time.Duration, srv *http.Server) error {
	stop, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-ctx.Done()
		draining.Store(true)
		time.Sleep(delay)
		cancel()
	}()
	return listen(stop, srv)
}

func serveMetricsOnly(ctx context.Context, addr string) error {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return listen(ctx, &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second})
}

// listen serves until ctx is cancelled, then drains in-flight requests.
func listen(ctx context.Context, srv *http.Server) error {
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return err
		}
		if err := <-errc; !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}

func logLevel() slog.Level {
	if os.Getenv("LOG_LEVEL") == "debug" {
		return slog.LevelDebug
	}
	return slog.LevelInfo
}
