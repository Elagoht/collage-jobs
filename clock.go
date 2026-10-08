package jobs

import (
	"context"
	"slices"
	"sync"
	"time"
)

// clock is the time the scheduler reads and waits on, so that tests can move
// it by hand.
type clock interface {
	Now() time.Time
	// After returns a channel that receives once d has passed. ctx is the
	// waiter's lifetime: the waiter passes the same ctx each time it waits,
	// and cancels it when it stops waiting for good. A waiter may instead use
	// a ctx per wait, cancelled once that wait is over, as a queue's backoff
	// does; see fakeClock for what that means in tests.
	After(ctx context.Context, d time.Duration) <-chan time.Time
}

// realClock is the system clock. It has no use for ctx.
type realClock struct{}

func (realClock) Now() time.Time                                            { return time.Now() }
func (realClock) After(_ context.Context, d time.Duration) <-chan time.Time { return time.After(d) }

// fakeClock moves only when Advance moves it, and never waits on real time.
//
// Advance hands each due timer to its waiter over an unbuffered channel, or
// gives up on it when the waiter's ctx ends. Then it waits until that waiter
// arms again with the same ctx, or its ctx ends. So a waiter that re-arms after
// each trigger sees every trigger one Advance passes, in time order.
//
// A waiter with a ctx per wait (a queue's backoff) is done once its ctx ends,
// which is before it arms its next wait under a new ctx. Whether one Advance
// also fires that next wait is a race, so step such a waiter one backoff per
// Advance, waiting for its next timer to be pending in between.
type fakeClock struct {
	mu     sync.Mutex
	cond   *sync.Cond // broadcast when a waiter arms or a waiter's ctx ends
	now    time.Time
	timers []fakeTimer
	armed  map[context.Context]uint64 // After calls per waiter ctx
}

type fakeTimer struct {
	at  time.Time
	ch  chan time.Time
	ctx context.Context
}

func newFakeClock(start time.Time) *fakeClock {
	c := &fakeClock{now: start, armed: map[context.Context]uint64{}}
	c.cond = sync.NewCond(&c.mu)
	return c
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) After(ctx context.Context, d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, seen := c.armed[ctx]; !seen {
		context.AfterFunc(ctx, func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			delete(c.armed, ctx)
			c.cond.Broadcast()
		})
	}
	c.armed[ctx]++
	c.cond.Broadcast()
	if d <= 0 {
		ch := make(chan time.Time, 1)
		ch <- c.now
		return ch
	}
	ch := make(chan time.Time)
	c.timers = append(c.timers, fakeTimer{at: c.now.Add(d), ch: ch, ctx: ctx})
	return ch
}

// Pending returns how many timers are waiting to fire, not counting those
// whose waiter's ctx has ended.
func (c *fakeClock) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, t := range c.timers {
		if t.ctx.Err() == nil {
			n++
		}
	}
	return n
}

// Advance moves the clock forward by d, firing each timer that falls due on
// the way, earliest first, with the clock set to its time.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	target := c.now.Add(d)
	for {
		i := -1
		for k, t := range c.timers {
			if !t.at.After(target) && (i < 0 || t.at.Before(c.timers[i].at)) {
				i = k
			}
		}
		if i < 0 {
			c.now = target
			return
		}
		t := c.timers[i]
		c.timers = slices.Delete(c.timers, i, i+1)
		c.now = t.at
		before := c.armed[t.ctx]
		c.mu.Unlock()
		select {
		case t.ch <- t.at:
		case <-t.ctx.Done():
		}
		c.mu.Lock()
		for c.armed[t.ctx] == before && t.ctx.Err() == nil {
			c.cond.Wait()
		}
	}
}
