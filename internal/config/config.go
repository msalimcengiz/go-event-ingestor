// Package config loads runtime settings from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type WriteMode string

const (
	// ModeBuffered pushes events to a Redis Stream; workers flush them to Postgres in batches.
	ModeBuffered WriteMode = "buffered"
	// ModeDirect writes every event to Postgres inside the HTTP request (baseline for benchmarks).
	ModeDirect WriteMode = "direct"
)

type Config struct {
	HTTPAddr      string
	PostgresURL   string
	RedisAddr     string
	Stream        string
	ConsumerGroup string
	ConsumerName  string
	WriteMode     WriteMode
	BatchSize     int
	FlushInterval time.Duration
	ClaimMinIdle  time.Duration
	MaxBodyBytes  int64
	// MaxBacklog is the undelivered+pending stream size above which the API answers 503.
	MaxBacklog int64
	// MaxDeliveries is how often a message may be delivered before it is dead-lettered.
	MaxDeliveries int64
	// APIKeys accepted as "Authorization: Bearer <key>". Empty disables auth (local dev only).
	APIKeys []string
	// ShutdownDelay keeps serving after SIGTERM while /readyz reports not ready, so load
	// balancers stop routing to the pod before connections are closed.
	ShutdownDelay time.Duration
}

func Load() (Config, error) {
	host, _ := os.Hostname()
	c := Config{
		HTTPAddr:      env("HTTP_ADDR", ":8080"),
		PostgresURL:   env("POSTGRES_URL", "postgres://ingestor:ingestor@localhost:5432/ingestor?sslmode=disable"),
		RedisAddr:     env("REDIS_ADDR", "localhost:6379"),
		Stream:        env("STREAM_NAME", "events"),
		ConsumerGroup: env("CONSUMER_GROUP", "pg-writers"),
		ConsumerName:  env("CONSUMER_NAME", host),
		WriteMode:     WriteMode(env("WRITE_MODE", string(ModeBuffered))),
	}

	var err error
	if c.BatchSize, err = envInt("BATCH_SIZE", 1000); err != nil {
		return c, err
	}
	if c.FlushInterval, err = envDuration("FLUSH_INTERVAL", 500*time.Millisecond); err != nil {
		return c, err
	}
	if c.ClaimMinIdle, err = envDuration("CLAIM_MIN_IDLE", 30*time.Second); err != nil {
		return c, err
	}
	maxBacklog, err := envInt("MAX_BACKLOG", 2_000_000)
	if err != nil {
		return c, err
	}
	c.MaxBacklog = int64(maxBacklog)
	maxDeliveries, err := envInt("MAX_DELIVERIES", 10)
	if err != nil {
		return c, err
	}
	c.MaxDeliveries = int64(maxDeliveries)
	if c.ShutdownDelay, err = envDuration("SHUTDOWN_DELAY", 5*time.Second); err != nil {
		return c, err
	}
	for _, k := range strings.Split(env("API_KEYS", ""), ",") {
		if k = strings.TrimSpace(k); k != "" {
			c.APIKeys = append(c.APIKeys, k)
		}
	}
	maxBody, err := envInt("MAX_BODY_BYTES", 1<<20)
	if err != nil {
		return c, err
	}
	c.MaxBodyBytes = int64(maxBody)

	if c.WriteMode != ModeBuffered && c.WriteMode != ModeDirect {
		return c, fmt.Errorf("WRITE_MODE must be %q or %q, got %q", ModeBuffered, ModeDirect, c.WriteMode)
	}
	if c.BatchSize <= 0 {
		return c, fmt.Errorf("BATCH_SIZE must be positive")
	}
	return c, nil
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) (int, error) {
	v := env(key, "")
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

func envDuration(key string, def time.Duration) (time.Duration, error) {
	v := env(key, "")
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}
