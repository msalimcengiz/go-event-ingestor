// Package batch groups items into batches that are flushed by size or by time.
package batch

import (
	"context"
	"time"
)

// FlushFunc persists one batch. A returned error is reported to OnError;
// the batch is not retried by the batcher (the Redis pending list owns retries).
type FlushFunc[T any] func(ctx context.Context, items []T) error

type Batcher[T any] struct {
	Size     int
	Interval time.Duration
	Flush    FlushFunc[T]
	OnError  func(error)
}

// Run consumes in until it is closed or ctx is cancelled. A batch is flushed when it
// reaches Size items or when Interval has passed since its first item arrived,
// whichever comes first. Remaining items are flushed before Run returns.
func (b *Batcher[T]) Run(ctx context.Context, in <-chan T) {
	buf := make([]T, 0, b.Size)
	timer := time.NewTimer(b.Interval)
	timer.Stop()
	timerRunning := false

	flush := func(fctx context.Context) {
		if timerRunning {
			timer.Stop()
			timerRunning = false
		}
		if len(buf) == 0 {
			return
		}
		if err := b.Flush(fctx, buf); err != nil && b.OnError != nil {
			b.OnError(err)
		}
		buf = make([]T, 0, b.Size)
	}

	for {
		select {
		case <-ctx.Done():
			// Use a fresh context so the final flush is not cancelled immediately.
			drainCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			flush(drainCtx)
			cancel()
			return
		case item, ok := <-in:
			if !ok {
				flush(ctx)
				return
			}
			buf = append(buf, item)
			if len(buf) == 1 {
				timer.Reset(b.Interval)
				timerRunning = true
			}
			if len(buf) >= b.Size {
				flush(ctx)
			}
		case <-timer.C:
			timerRunning = false
			flush(ctx)
		}
	}
}
