//go:build integration

package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/msalimcengiz/go-event-ingestor/internal/event"
	"github.com/msalimcengiz/go-event-ingestor/internal/store"
	"github.com/msalimcengiz/go-event-ingestor/internal/stream"
)

// Run with: make test-integration (needs Postgres and Redis from docker compose).

func setup(t *testing.T) (*store.Postgres, *redis.Client, *stream.Redis) {
	t.Helper()
	ctx := context.Background()
	pg, err := store.NewPostgres(ctx, os.Getenv("POSTGRES_URL"))
	if err != nil {
		t.Fatalf("postgres: %v", err)
	}
	t.Cleanup(pg.Close)
	if err := pg.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: os.Getenv("REDIS_ADDR")})
	t.Cleanup(func() { _ = rdb.Close() })

	name := "test-" + uuid.NewString()
	t.Cleanup(func() { rdb.Del(context.Background(), name, name+":dead") })
	rs := stream.NewRedis(rdb, name, "g")
	if err := rs.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	return pg, rdb, rs
}

func newEvents(n int) []event.Event {
	out := make([]event.Event, n)
	for i := range out {
		out[i] = event.Event{ID: uuid.New(), Type: "it", UserID: fmt.Sprintf("u%d", i%10), Payload: json.RawMessage(`{}`)}
		_ = out[i].Normalize(time.Now())
	}
	return out
}

func newWorker(rs *stream.Redis, st Store, consumer string) *Worker {
	return &Worker{
		Stream: rs, Store: st, Consumer: consumer,
		BatchSize: 100, FlushInterval: 50 * time.Millisecond, ClaimMinIdle: 300 * time.Millisecond, MaxDeliveries: 3,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func countIDs(t *testing.T, pg *store.Postgres, events []event.Event) int {
	t.Helper()
	ids := make([]uuid.UUID, len(events))
	for i, e := range events {
		ids[i] = e.ID
	}
	n, err := pg.CountIDs(context.Background(), ids)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func pendingCount(t *testing.T, rdb *redis.Client, rs *stream.Redis) int64 {
	t.Helper()
	p, err := rdb.XPending(context.Background(), rs.Name(), "g").Result()
	if err != nil {
		t.Fatal(err)
	}
	return p.Count
}

func TestInsertBatchIsIdempotent(t *testing.T) {
	pg, _, _ := setup(t)
	ctx := context.Background()
	events := newEvents(500)

	n, err := pg.InsertBatch(ctx, events)
	if err != nil || n != 500 {
		t.Fatalf("first insert: n=%d err=%v", n, err)
	}
	// Same batch again plus a duplicate inside the batch: nothing new is written.
	n, err = pg.InsertBatch(ctx, append(events, events[0]))
	if err != nil || n != 0 {
		t.Fatalf("redelivery: n=%d err=%v, want 0 new rows", n, err)
	}
}

func TestDrainsStreamAndDeadLettersUndecodable(t *testing.T) {
	pg, rdb, rs := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := newEvents(2_500)
	if err := rs.Publish(ctx, events); err != nil {
		t.Fatal(err)
	}
	rdb.XAdd(ctx, &redis.XAddArgs{Stream: rs.Name(), Values: map[string]any{"d": "not json"}})

	done := make(chan error, 1)
	go func() { done <- newWorker(rs, pg, "c1").Run(ctx) }()

	waitFor(t, 10*time.Second, "all rows", func() bool { return countIDs(t, pg, events) == len(events) })
	waitFor(t, 5*time.Second, "dead letter", func() bool { return rdb.XLen(ctx, rs.DeadLetterStream()).Val() == 1 })
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if n := pendingCount(t, rdb, rs); n != 0 {
		t.Fatalf("%d messages left pending", n)
	}
}

func TestBadRowIsIsolatedAndTheRestOfTheBatchIsStored(t *testing.T) {
	pg, rdb, rs := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	good := newEvents(99)
	// Valid JSON that Postgres jsonb rejects (SQLSTATE 22P05). The API refuses it, so
	// write it straight to the stream as if an older producer had sent it.
	bad := event.Event{ID: uuid.New(), Type: "it", UserID: "u1", OccurredAt: time.Now().UTC(), Payload: json.RawMessage(`{"x":"\u0000"}`)}
	all := append(append([]event.Event{}, good[:50]...), append([]event.Event{bad}, good[50:]...)...)
	if err := rs.Publish(ctx, all); err != nil {
		t.Fatal(err)
	}

	go newWorker(rs, pg, "c1").Run(ctx) //nolint:errcheck

	waitFor(t, 10*time.Second, "good rows", func() bool { return countIDs(t, pg, good) == len(good) })
	waitFor(t, 5*time.Second, "dead letter", func() bool { return rdb.XLen(ctx, rs.DeadLetterStream()).Val() == 1 })
	dl := rdb.XRange(ctx, rs.DeadLetterStream(), "-", "+").Val()[0]
	if reason, _ := dl.Values["reason"].(string); reason == "" || dl.Values["d"] == nil {
		t.Fatalf("dead letter lacks reason or payload: %v", dl.Values)
	}
	waitFor(t, 2*time.Second, "no pending", func() bool { return pendingCount(t, rdb, rs) == 0 })
}

// flakyStore fails with a transient error until healthy is set.
type flakyStore struct {
	inner   Store
	healthy atomic.Bool
	calls   atomic.Int64
}

func (f *flakyStore) InsertBatch(ctx context.Context, events []event.Event) (int64, error) {
	f.calls.Add(1)
	if !f.healthy.Load() {
		return 0, errors.New("connection refused")
	}
	return f.inner.InsertBatch(ctx, events)
}

func TestTransientFailuresAreRetriedWithoutLoss(t *testing.T) {
	pg, rdb, rs := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := newEvents(300)
	if err := rs.Publish(ctx, events); err != nil {
		t.Fatal(err)
	}
	st := &flakyStore{inner: pg}
	w := newWorker(rs, st, "c1")
	w.MaxDeliveries = 100
	go w.Run(ctx) //nolint:errcheck

	waitFor(t, 5*time.Second, "failed flushes", func() bool { return st.calls.Load() >= 3 })
	if countIDs(t, pg, events) != 0 || pendingCount(t, rdb, rs) == 0 {
		t.Fatal("messages must stay pending while the database is down")
	}
	st.healthy.Store(true) // database is back
	waitFor(t, 10*time.Second, "all rows after recovery", func() bool { return countIDs(t, pg, events) == len(events) })
	waitFor(t, 2*time.Second, "no pending", func() bool { return pendingCount(t, rdb, rs) == 0 })
	if rdb.XLen(ctx, rs.DeadLetterStream()).Val() != 0 {
		t.Fatal("transient failures must not dead-letter")
	}
}

func TestMessagesOutOfDeliveriesAreDeadLetteredAndReplayable(t *testing.T) {
	pg, rdb, rs := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := newEvents(20)
	if err := rs.Publish(ctx, events); err != nil {
		t.Fatal(err)
	}
	st := &flakyStore{inner: pg} // never recovers during the first phase
	w := newWorker(rs, st, "c1")
	runCtx, stop := context.WithCancel(ctx)
	go w.Run(runCtx) //nolint:errcheck

	waitFor(t, 15*time.Second, "dead letters", func() bool { return rdb.XLen(ctx, rs.DeadLetterStream()).Val() == 20 })
	stop()
	if n := pendingCount(t, rdb, rs); n != 0 {
		t.Fatalf("%d still pending", n)
	}

	// After the fix: replay the dead letters and let a healthy worker store them.
	moved, err := rs.Replay(ctx, 100)
	if err != nil || moved != 20 {
		t.Fatalf("replay: moved=%d err=%v", moved, err)
	}
	go newWorker(rs, pg, "c2").Run(ctx) //nolint:errcheck
	waitFor(t, 10*time.Second, "replayed rows", func() bool { return countIDs(t, pg, events) == len(events) })
}

func TestCrashedConsumerMessagesAreReclaimed(t *testing.T) {
	pg, _, rs := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := newEvents(300)
	if err := rs.Publish(ctx, events); err != nil {
		t.Fatal(err)
	}
	// A consumer reads the messages and "crashes" before writing or acking them.
	if _, _, err := rs.Read(ctx, "crashed", 1000, time.Second); err != nil {
		t.Fatal(err)
	}
	go newWorker(rs, pg, "survivor").Run(ctx) //nolint:errcheck
	waitFor(t, 10*time.Second, "reclaimed rows", func() bool { return countIDs(t, pg, events) == len(events) })
}

func TestTrimNeverRemovesUnacknowledgedEntries(t *testing.T) {
	_, rdb, rs := setup(t)
	ctx := context.Background()

	if err := rs.Publish(ctx, newEvents(3_000)); err != nil {
		t.Fatal(err)
	}
	// Deliver everything, acknowledge only the first 1000.
	msgs, _, err := rs.Read(ctx, "c1", 3_000, time.Second)
	if err != nil || len(msgs) != 3_000 {
		t.Fatalf("read %d: %v", len(msgs), err)
	}
	ids := make([]string, 1_000)
	for i := range ids {
		ids[i] = msgs[i].StreamID
	}
	if err := rs.Ack(ctx, ids...); err != nil {
		t.Fatal(err)
	}

	if _, err := rs.Trim(ctx); err != nil {
		t.Fatal(err)
	}
	remaining := rdb.XRange(ctx, rs.Name(), "-", "+").Val()
	present := make(map[string]bool, len(remaining))
	for _, m := range remaining {
		present[m.ID] = true
	}
	for _, m := range msgs[1_000:] {
		if !present[m.StreamID] {
			t.Fatalf("unacknowledged entry %s was trimmed", m.StreamID)
		}
	}
	if len(remaining) == 3_000 {
		t.Fatal("expected acknowledged entries to be trimmed")
	}
}
