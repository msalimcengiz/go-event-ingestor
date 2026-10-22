package batch

import (
	"context"
	"sync"
	"testing"
	"time"
)

type recorder struct {
	mu      sync.Mutex
	batches [][]int
}

func (r *recorder) flush(_ context.Context, items []int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.batches = append(r.batches, append([]int(nil), items...))
	return nil
}

func (r *recorder) snapshot() [][]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]int(nil), r.batches...)
}

func TestFlushesBySize(t *testing.T) {
	rec := &recorder{}
	b := &Batcher[int]{Size: 3, Interval: time.Hour, Flush: rec.flush}
	in := make(chan int)
	done := make(chan struct{})
	go func() { b.Run(context.Background(), in); close(done) }()

	for i := range 7 {
		in <- i
	}
	close(in)
	<-done

	got := rec.snapshot()
	want := [][]int{{0, 1, 2}, {3, 4, 5}, {6}}
	if len(got) != len(want) {
		t.Fatalf("got %d batches %v, want %v", len(got), got, want)
	}
	for i := range want {
		if len(got[i]) != len(want[i]) {
			t.Fatalf("batch %d: got %v want %v", i, got[i], want[i])
		}
	}
}

func TestFlushesByInterval(t *testing.T) {
	rec := &recorder{}
	b := &Batcher[int]{Size: 1000, Interval: 20 * time.Millisecond, Flush: rec.flush}
	in := make(chan int)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx, in)

	in <- 1
	in <- 2
	deadline := time.After(time.Second)
	for {
		if got := rec.snapshot(); len(got) == 1 && len(got[0]) == 2 {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("interval flush did not happen, batches=%v", rec.snapshot())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestFlushesOnCancel(t *testing.T) {
	rec := &recorder{}
	b := &Batcher[int]{Size: 1000, Interval: time.Hour, Flush: rec.flush}
	in := make(chan int)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { b.Run(ctx, in); close(done) }()

	in <- 42
	cancel()
	<-done

	got := rec.snapshot()
	if len(got) != 1 || got[0][0] != 42 {
		t.Fatalf("pending item lost on shutdown: %v", got)
	}
}
