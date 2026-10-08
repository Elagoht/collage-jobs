package jobs

import "time"

// FakeClock is the fake clock, for the tests in package jobs_test.
type FakeClock = fakeClock

// UseFakeClock makes p schedule on a fake clock reading start, and returns it.
// Call it before the plugin starts.
func UseFakeClock(p *Plugin, start time.Time) *FakeClock {
	c := newFakeClock(start)
	p.clock = c
	return c
}

// WaitIdle waits up to d for every scheduler loop and run of p to end, and
// reports whether they did.
func WaitIdle(p *Plugin, d time.Duration) bool {
	select {
	case <-p.active.Idle():
		return true
	case <-time.After(d):
		return false
	}
}

// Draining reports whether the drain has begun, or Shutdown has.
func Draining(p *Plugin) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed.Load() || p.running != nil && p.running.Err() != nil
}

// Store is the queue's store interface, for the tests in package jobs_test.
type Store[T any] = store[T]

// Busy reports whether any scheduled job of p has a run in progress. A run's
// outcome is logged before it stops counting as in progress.
func Busy(p *Plugin) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, j := range p.scheduled {
		if j.busy.Load() {
			return true
		}
	}
	return false
}

// QueueLen returns how many items wait in q's store.
func QueueLen[T any](q *Queue[T]) int { return q.store.Len() }

// Unfinished returns how many items handed to q are not yet handled: queued,
// running or waiting for a retry.
func Unfinished[T any](q *Queue[T]) int64 { return q.unfinished.Load() }

// Backoff is backoff.
var Backoff = backoff

// NextDue returns when the earliest timer whose waiter's ctx is live falls
// due, or the zero time if there is none.
func (c *fakeClock) NextDue() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	var due time.Time
	for _, t := range c.timers {
		if t.ctx.Err() == nil && (due.IsZero() || t.at.Before(due)) {
			due = t.at
		}
	}
	return due
}

// NewMemStore returns the in-memory store, holding up to capacity items.
func NewMemStore[T any](capacity int) Store[T] { return newMemStore[T](capacity) }

// WrapStore replaces q's store with wrap's result. Call it before the plugin
// starts.
func WrapStore[T any](q *Queue[T], wrap func(Store[T]) Store[T]) { q.store = wrap(q.store) }

// Closed reports whether Shutdown has begun, without taking p.mu.
func Closed(p *Plugin) bool { return p.closed.Load() }

// Active returns how many loops, runs and workers of p are counted.
func Active(p *Plugin) int { return p.active.Count() }
