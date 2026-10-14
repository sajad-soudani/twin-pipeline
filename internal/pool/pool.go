// Package pool is a small bounded worker pool. It knows nothing about MQTT,
// topics or the store: it just runs a handler over a queue.
package pool

import (
	"context"
	"errors"
	"log/slog"
	"sync"
)

var ErrClosed = errors.New("pool: closed")

type Pool[T any] struct {
	jobs   chan T
	handle func(workerID int, job T)

	mu     sync.RWMutex // guards closed + sends vs close(jobs)
	closed bool
	wg     sync.WaitGroup
}

// New starts `workers` goroutines reading from a queue of size `queue`.
func New[T any](workers, queue int, handle func(workerID int, job T)) *Pool[T] {
	p := &Pool[T]{jobs: make(chan T, queue), handle: handle}
	p.wg.Add(workers)
	for id := range workers {
		go p.run(id)
	}
	return p
}

// Submit enqueues a job, blocking while the queue is full (backpressure)
// until ctx is cancelled. Blocking is a deliberate policy: the README's
// milestone is "no dropped messages". Swap for a non-blocking send plus a
// drop counter if you ever prefer freshness over completeness.
func (p *Pool[T]) Submit(ctx context.Context, job T) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return ErrClosed
	}
	select {
	case p.jobs <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close stops accepting jobs; workers drain what is queued, then exit.
func (p *Pool[T]) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		p.closed = true
		close(p.jobs)
	}
}

// Wait blocks until every worker has exited (call after Close).
func (p *Pool[T]) Wait() { p.wg.Wait() }

func (p *Pool[T]) run(id int) {
	defer p.wg.Done()
	for job := range p.jobs {
		p.safely(id, job)
	}
}

// safely stops one bad message from killing a worker (and the process).
func (p *Pool[T]) safely(id int, job T) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("worker panic recovered", "worker", id, "panic", r)
		}
	}()
	p.handle(id, job)
}
