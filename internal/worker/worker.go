// Package worker drains the Redis Stream into Postgres in batches.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/msalimcengiz/go-event-ingestor/internal/batch"
	"github.com/msalimcengiz/go-event-ingestor/internal/event"
	"github.com/msalimcengiz/go-event-ingestor/internal/metrics"
	"github.com/msalimcengiz/go-event-ingestor/internal/stream"
)

// Store is the part of the Postgres store the worker needs.
type Store interface {
	InsertBatch(ctx context.Context, events []event.Event) (int64, error)
}

type Worker struct {
	Stream        *stream.Redis
	Store         Store
	Consumer      string
	BatchSize     int
	FlushInterval time.Duration
	ClaimMinIdle  time.Duration
	// MaxDeliveries is how many times a message may be delivered before it is dead-lettered.
	MaxDeliveries int64
	Logger        *slog.Logger
}

func (w *Worker) Run(ctx context.Context) error {
	if err := w.Stream.EnsureGroup(ctx); err != nil {
		return err
	}

	in := make(chan stream.Message, w.BatchSize*2)
	b := &batch.Batcher[stream.Message]{
		Size:     w.BatchSize,
		Interval: w.FlushInterval,
		Flush:    w.flush,
		OnError: func(err error) {
			metrics.FlushErrors.Inc()
			w.Logger.Error("flush failed; messages stay pending and will be reclaimed", "err", err)
		},
	}
	batcherDone := make(chan struct{})
	go func() { b.Run(ctx, in); close(batcherDone) }()

	go w.maintain(ctx)
	go w.reclaimLoop(ctx, in)

	backoff := newBackoff(100*time.Millisecond, 5*time.Second)
	for ctx.Err() == nil {
		msgs, dead, err := w.Stream.Read(ctx, w.Consumer, int64(w.BatchSize), w.FlushInterval)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			d := backoff.next()
			w.Logger.Warn("read failed", "err", err, "retry_in", d)
			sleep(ctx, d)
			continue
		}
		backoff.reset()
		w.deadLetter(ctx, dead)
		for _, m := range msgs {
			select {
			case in <- m:
			case <-ctx.Done():
			}
		}
	}
	// Anything read but not yet handed over stays pending in Redis and is reclaimed by
	// another worker; the batcher flushes what it already holds before Run returns.
	<-batcherDone
	return nil
}

// flush writes a batch and acknowledges it only after the commit. A crash between the
// commit and the ack causes a redelivery, which ON CONFLICT DO NOTHING turns into a no-op.
func (w *Worker) flush(ctx context.Context, msgs []stream.Message) error {
	start := time.Now()
	err := w.write(ctx, msgs)
	metrics.FlushDuration.Observe(time.Since(start).Seconds())
	return err
}

// write inserts msgs. When the batch fails because of the data in it (a constraint or
// data exception), it splits the batch in halves and retries each, so one bad message
// is isolated in log2(n) steps and dead-lettered while every good message is stored.
// Transient failures (connection lost, timeout) are returned as errors and the whole
// batch stays pending for a later retry.
func (w *Worker) write(ctx context.Context, msgs []stream.Message) error {
	events := make([]event.Event, len(msgs))
	ids := make([]string, len(msgs))
	for i, m := range msgs {
		events[i] = m.Event
		ids[i] = m.StreamID
	}

	inserted, err := w.Store.InsertBatch(ctx, events)
	if err == nil {
		if err := w.Stream.Ack(ctx, ids...); err != nil {
			return fmt.Errorf("ack after commit: %w", err)
		}
		metrics.BatchSize.Observe(float64(len(msgs)))
		metrics.EventsPersisted.Add(float64(inserted))
		return nil
	}
	if !isPermanent(err) {
		return err
	}
	if len(msgs) == 1 {
		w.Logger.Warn("dead-lettering a message the database rejects", "id", msgs[0].StreamID, "err", err)
		w.deadLetter(ctx, []stream.Dead{{StreamID: msgs[0].StreamID, Raw: msgs[0].Raw, Reason: "rejected by database: " + err.Error()}})
		return nil
	}
	metrics.BatchSplits.Inc()
	mid := len(msgs) / 2
	return errors.Join(w.write(ctx, msgs[:mid]), w.write(ctx, msgs[mid:]))
}

// isPermanent reports whether retrying the same rows can never succeed:
// SQLSTATE class 22 (data exception) or 23 (integrity constraint violation).
func isPermanent(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && len(pgErr.Code) == 5 {
		class := pgErr.Code[:2]
		return class == "22" || class == "23"
	}
	return false
}

func (w *Worker) reclaimLoop(ctx context.Context, in chan<- stream.Message) {
	t := time.NewTicker(w.ClaimMinIdle / 2)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		msgs, dead, err := w.Stream.Reclaim(ctx, w.Consumer, w.ClaimMinIdle, int64(w.BatchSize), w.MaxDeliveries)
		if err != nil {
			w.Logger.Warn("reclaim failed", "err", err)
			continue
		}
		w.deadLetter(ctx, dead)
		if len(msgs) > 0 {
			metrics.Reclaimed.Add(float64(len(msgs)))
			w.Logger.Info("reclaimed pending messages", "count", len(msgs))
		}
		for _, m := range msgs {
			select {
			case in <- m:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (w *Worker) deadLetter(ctx context.Context, dead []stream.Dead) {
	if len(dead) == 0 {
		return
	}
	if err := w.Stream.DeadLetter(ctx, dead); err != nil {
		// Left pending: the reclaimer will try again.
		w.Logger.Error("dead-letter failed", "err", err)
		return
	}
	metrics.DeadLettered.Add(float64(len(dead)))
}

// maintain publishes backlog metrics and trims processed entries from the stream.
func (w *Worker) maintain(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if b, err := w.Stream.Backlog(ctx); err == nil {
			metrics.StreamLag.Set(float64(b.Lag))
			metrics.StreamPending.Set(float64(b.Pending))
			metrics.StreamLength.Set(float64(b.Length))
		}
		if n, err := w.Stream.Trim(ctx); err != nil {
			w.Logger.Warn("trim failed", "err", err)
		} else {
			metrics.Trimmed.Add(float64(n))
		}
	}
}

type backoff struct{ cur, min, max time.Duration }

func newBackoff(min, max time.Duration) *backoff { return &backoff{cur: min, min: min, max: max} }

func (b *backoff) next() time.Duration {
	d := b.cur
	b.cur = min(b.cur*2, b.max)
	return d
}

func (b *backoff) reset() { b.cur = b.min }

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
