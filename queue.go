package jobs

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync/atomic"
	"time"
)

// ErrQueueFull is returned by Enqueue when the queue holds Capacity items.
var ErrQueueFull = errors.New("jobs: queue full")

// QueueOptions configures a queue. The zero value of each field means its
// default.
type QueueOptions[T any] struct {
	// Workers is how many items are handled at the same time, default 1.
	Workers int
	// Capacity is how many items can wait, default 1000. The store for them
	// is allocated up front, when the queue is defined.
	Capacity int
	// MaxAttempts is how many times an item is tried, default 1: no retry.
	// Retries wait 1s, 2s, 4s… up to a minute.
	MaxAttempts int
	// OnFailure, if set, is called with an item and its last error once
	// every attempt has failed.
	OnFailure func(ctx context.Context, item T, err error)
}

// Queue is a bounded queue of typed items, handled in the background while the
// application serves.
type Queue[T any] struct {
	p     *Plugin
	name  string
	fn    func(context.Context, T) error
	opts  QueueOptions[T]
	store store[T]
	// unfinished counts items handed to the queue and not yet handled:
	// waiting, running or waiting for a retry.
	unfinished atomic.Int64
	// popCtx is what the workers take items under. It ends once Shutdown has
	// begun and the store is empty, or when the runs' ctx ends.
	popCtx  context.Context
	stopPop context.CancelFunc
	// cutShort counts items dropped while waiting for a retry, because the
	// runs' ctx ended.
	cutShort atomic.Int64
}

// worker is a queue, seen by the plugin that starts and stops its workers.
type worker interface {
	// startWorkers starts the workers, under workCtx. p.mu is held.
	startWorkers(workCtx context.Context)
	// finish tells the workers that Shutdown has begun: they stop once the
	// store is empty.
	finish()
	// leftovers returns how many items were dropped, left in the store or
	// cut short in a retry's wait, and, if Shutdown gave up, how many were
	// abandoned while running.
	leftovers(gaveUp bool) (dropped, abandoned int64)
	queueName() string
}

// NewQueue defines a queue on p whose items fn handles. Items can be enqueued
// at once; they are handled once p starts. It panics if name is empty or
// taken by a job or queue, fn is nil, an option is negative, or p's Init has
// run.
func NewQueue[T any](p *Plugin, name string, fn func(context.Context, T) error, opts QueueOptions[T]) *Queue[T] {
	if p == nil {
		panic(fmt.Sprintf("jobs: queue %q: the plugin is nil", name))
	}
	if fn == nil {
		panic(fmt.Sprintf("jobs: queue %q: the function is nil", name))
	}
	for _, o := range []struct {
		field string
		v     *int
		def   int
	}{{"Workers", &opts.Workers, 1}, {"Capacity", &opts.Capacity, 1000}, {"MaxAttempts", &opts.MaxAttempts, 1}} {
		switch {
		case *o.v < 0:
			panic(fmt.Sprintf("jobs: queue %q: %s must not be negative, got %d", name, o.field, *o.v))
		case *o.v == 0:
			*o.v = o.def
		}
	}
	q := &Queue[T]{p: p, name: name, fn: fn, opts: opts}
	p.register(name, func() {
		q.store = newMemStore[T](opts.Capacity) // only once the name is taken: a misuse panic allocates nothing
		p.queues = append(p.queues, q)
	})
	return q
}

// Enqueue adds item to the queue without blocking. It returns ErrDraining once
// the plugin's Shutdown has begun, and ErrQueueFull when the queue is full.
// Before the plugin starts, items wait in the queue. During the drain items
// are still accepted, since the server still serves; Shutdown finishes them,
// or logs them as dropped.
func (q *Queue[T]) Enqueue(_ context.Context, item T) error {
	// The check and the push under one hold of p.mu, which Shutdown takes to
	// close the plugin: an item is refused, or in the store before Shutdown
	// counts what it drops.
	q.p.mu.Lock()
	defer q.p.mu.Unlock()
	if q.p.closed.Load() {
		return ErrDraining
	}
	q.unfinished.Add(1)
	if err := q.store.Push(item); err != nil {
		q.unfinished.Add(-1)
		return err
	}
	return nil
}

func (q *Queue[T]) startWorkers(workCtx context.Context) {
	q.popCtx, q.stopPop = context.WithCancel(workCtx)
	for range q.opts.Workers {
		q.p.active.Add(1)
		go q.work()
	}
}

func (q *Queue[T]) finish() { q.stopIfEmpty() }

func (q *Queue[T]) leftovers(gaveUp bool) (dropped, abandoned int64) {
	queued := int64(q.store.Len())
	if gaveUp {
		// unfinished counts the queued items and those still running; one
		// cut short has stopped counting.
		abandoned = max(q.unfinished.Load()-queued, 0)
	}
	return queued + q.cutShort.Load(), abandoned
}

func (q *Queue[T]) queueName() string { return q.name }

// stopIfEmpty ends popCtx if Shutdown has begun and the store is empty. A
// worker checks after each item and Shutdown once as it begins; closed is set
// before Shutdown's check, so whichever comes last sees both.
func (q *Queue[T]) stopIfEmpty() {
	if q.p.closed.Load() && q.store.Len() == 0 {
		q.stopPop()
	}
}

// work handles items until the store's Pop ends.
func (q *Queue[T]) work() {
	defer q.p.active.Done()
	// This worker's own ctx: its backoff waits derive from it, and it ends
	// when the worker returns.
	ctx, cancel := context.WithCancel(q.p.workCtx)
	defer cancel()
	for {
		item, err := q.store.Pop(q.popCtx)
		if err != nil {
			return
		}
		q.handle(ctx, item)
		q.unfinished.Add(-1)
		q.stopIfEmpty()
	}
}

// handle tries item up to MaxAttempts times, waiting a backoff between tries.
// The same worker retries, so its items keep their order.
func (q *Queue[T]) handle(ctx context.Context, item T) {
	log := q.p.log
	for attempt := 1; ; attempt++ {
		began := time.Now()
		err := callItem(q.p.workCtx, q.fn, item)
		took := time.Since(began)
		if err == nil {
			log.Debug("jobs: ran", "queue", q.name, "attempt", attempt, "duration", took)
			return
		}
		if attempt < q.opts.MaxAttempts {
			if ctx.Err() == nil {
				wait := backoff(attempt)
				log.Warn("jobs: failed, will retry", "queue", q.name, "attempt", attempt, "retry_in", wait, "duration", took, "error", err)
				if q.wait(ctx, wait) {
					continue
				}
			}
			// Shutdown ended the runs' ctx before the retry. The item is
			// dropped, as a queued one is, not failed: Shutdown counts it, and
			// OnFailure is not called. With no attempt left, as always with
			// MaxAttempts 1, a failure is final even when the cancel caused
			// it: it is logged at Error and handed to OnFailure, below.
			log.Debug("jobs: dropped at shutdown, not retried", "queue", q.name, "attempt", attempt, "duration", took, "error", err)
			q.cutShort.Add(1)
			return
		}
		log.Error("jobs: failed", "queue", q.name, "attempts", attempt, "duration", took, "error", err)
		q.failed(item, err)
		return
	}
}

// wait waits d on the plugin's clock, and reports false if ctx ended first.
// Each wait has a ctx of its own, ended before wait returns: the fake clock
// counts a waiter as done once its ctx ends, so the retry that follows runs
// after Advance has moved on, and a worker that goes back to its store never
// holds Advance.
func (q *Queue[T]) wait(ctx context.Context, d time.Duration) bool {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	select {
	case <-q.p.clock.After(ctx, d):
		return true
	case <-ctx.Done():
		return false
	}
}

// failed calls OnFailure, if set, containing a panic in it.
func (q *Queue[T]) failed(item T, err error) {
	if q.opts.OnFailure == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			q.p.log.Error("jobs: OnFailure panicked", "queue", q.name, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
		}
	}()
	q.opts.OnFailure(q.p.workCtx, item, err)
}

// callItem runs fn on item, turning a panic into an error that carries the
// stack.
func callItem[T any](ctx context.Context, fn func(context.Context, T) error, item T) error {
	return call(ctx, func(ctx context.Context) error { return fn(ctx, item) })
}

// backoff returns the wait before retry n, n ≥ 1: 1s, 2s, 4s… capped at a
// minute. Large n cannot overflow the shift.
func backoff(n int) time.Duration {
	if n > 6 { // 1s<<6 is already past a minute
		return time.Minute
	}
	return min(time.Second<<(n-1), time.Minute)
}
