package pool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestProcessesEverythingThenDrains(t *testing.T) {
	var n atomic.Int64
	p := New(4, 8, func(_ int, _ int) { n.Add(1) })

	for i := range 1000 {
		if err := p.Submit(context.Background(), i); err != nil {
			t.Fatal(err)
		}
	}
	p.Close()
	p.Wait() // Close must DRAIN the queue, not discard it

	if n.Load() != 1000 {
		t.Fatalf("processed %d of 1000", n.Load())
	}
}

func TestSubmitAfterClose(t *testing.T) {
	p := New(1, 1, func(int, int) {})
	p.Close()
	p.Close() // idempotent
	if err := p.Submit(context.Background(), 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("want ErrClosed, got %v", err)
	}
	p.Wait()
}

func TestSubmitUnblocksOnContextCancel(t *testing.T) {
	block := make(chan struct{})
	p := New(1, 1, func(int, int) { <-block })
	defer func() { close(block); p.Close(); p.Wait() }()

	p.Submit(context.Background(), 1) // taken by the worker
	p.Submit(context.Background(), 2) // fills the queue

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := p.Submit(ctx, 3); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
}

func TestPanicDoesNotKillWorker(t *testing.T) {
	var ok atomic.Int64
	p := New(1, 4, func(_ int, v int) {
		if v == 0 {
			panic("boom")
		}
		ok.Add(1)
	})
	for _, v := range []int{0, 1, 2} {
		p.Submit(context.Background(), v)
	}
	p.Close()
	p.Wait()
	if ok.Load() != 2 {
		t.Fatalf("worker died after panic; processed %d", ok.Load())
	}
}
