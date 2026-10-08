package jobs

import (
	"slices"
	"sync"
	"time"
)

// clock is the time the scheduler reads and waits on, so that tests can move
// it by hand.
type clock interface {
	Now() time.Time
	// After returns a channel that receives once d has passed.
	After(d time.Duration) <-chan time.Time
}

// realClock is the system clock.
type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// fakeClock is a clock that moves only when Advance moves it.
//
// Its channels are unbuffered, so Advance knows whether somebody took a
// trigger. When somebody did, Advance waits for them to arm their next timer
// before it moves on, so that a loop that re-arms after each trigger sees every
// trigger an Advance passes, in time order.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []fakeTimer
	armed  uint64 // After calls so far
}

type fakeTimer struct {
	at time.Time
	ch chan time.Time
}

// The fake clock waits this long, in real time, for a trigger to be taken,
// and then for the taker to arm its next timer.
const (
	fakeDeliver = 500 * time.Millisecond
	fakeRearm   = 2 * time.Second
)

func newFakeClock(start time.Time) *fakeClock { return &fakeClock{now: start} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.armed++
	if d <= 0 {
		ch := make(chan time.Time, 1)
		ch <- c.now
		return ch
	}
	ch := make(chan time.Time)
	c.timers = append(c.timers, fakeTimer{at: c.now.Add(d), ch: ch})
	return ch
}

// Pending returns how many timers are waiting to fire.
func (c *fakeClock) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers)
}

// Advance moves the clock forward by d, firing each timer that falls due on
// the way, earliest first, with the clock set to its time. A timer nobody is
// waiting on any more (its loop has stopped) is dropped.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	target := c.now.Add(d)
	c.mu.Unlock()
	for {
		c.mu.Lock()
		i := -1
		for k, t := range c.timers {
			if !t.at.After(target) && (i < 0 || t.at.Before(c.timers[i].at)) {
				i = k
			}
		}
		if i < 0 {
			c.now = target
			c.mu.Unlock()
			return
		}
		t := c.timers[i]
		c.timers = slices.Delete(c.timers, i, i+1)
		c.now = t.at
		armed := c.armed
		c.mu.Unlock()

		select {
		case t.ch <- t.at:
			c.waitArmed(armed)
		case <-time.After(fakeDeliver):
		}
	}
}

// waitArmed waits until After has been called since armed was read.
func (c *fakeClock) waitArmed(armed uint64) {
	deadline := time.Now().Add(fakeRearm)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		done := c.armed > armed
		c.mu.Unlock()
		if done {
			return
		}
		time.Sleep(100 * time.Microsecond)
	}
}
