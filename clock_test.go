package jobs

import (
	"context"
	"strings"
	"testing"
	"time"
)

var clockStart = time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)

// A waiter that re-arms after each trigger sees every trigger one Advance
// passes, in order, at its own time.
func TestFakeClock_AdvanceWaitsForRearm(t *testing.T) {
	c := newFakeClock(clockStart)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seen := make(chan time.Time, 3)
	armed := make(chan struct{})
	go func() {
		for i := 0; i < 3; i++ {
			ch := c.After(ctx, time.Minute)
			if i == 0 {
				close(armed)
			}
			seen <- <-ch
		}
		<-c.After(ctx, time.Minute) // armed again, never fired
	}()
	<-armed
	c.Advance(3 * time.Minute)
	for i := 1; i <= 3; i++ {
		if got, want := <-seen, clockStart.Add(time.Duration(i)*time.Minute); !got.Equal(want) {
			t.Fatalf("trigger %d at %v, want %v", i, got, want)
		}
	}
}

// A timer whose waiter's ctx has ended is not waited for: Advance returns
// without the waiter, on no real-time grace period.
func TestFakeClock_DoneWaiter(t *testing.T) {
	c := newFakeClock(clockStart)
	ctx, cancel := context.WithCancel(context.Background())
	c.After(ctx, time.Minute)
	cancel()
	done := make(chan struct{})
	go func() { c.Advance(time.Hour); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Advance waited on a waiter whose ctx ended")
	}
	if got := c.Now(); !got.Equal(clockStart.Add(time.Hour)) {
		t.Fatalf("Now %v", got)
	}
}

// A waiter whose ctx ends after it took a trigger, without arming again,
// releases Advance.
func TestFakeClock_WaiterStopsAfterTrigger(t *testing.T) {
	c := newFakeClock(clockStart)
	ctx, cancel := context.WithCancel(context.Background())
	ch := c.After(ctx, time.Minute)
	go func() { <-ch; cancel() }()
	done := make(chan struct{})
	go func() { c.Advance(time.Hour); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Advance hung after its waiter stopped")
	}
}

// Every keeps the monotonic reading of now, so a wall clock set back does not
// stretch the wait.
func TestNextTrigger_EveryKeepsMonotonic(t *testing.T) {
	loc, err := time.LoadLocation("Europe/Istanbul")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	next := nextTrigger(every{time.Minute}, loc, now, now.Add(-time.Minute))
	if !strings.Contains(next.String(), "m=") {
		t.Fatalf("next %v has no monotonic reading", next)
	}
	if d := next.Sub(now); d != time.Minute {
		t.Fatalf("next is %v after now, want 1m", d)
	}
}

// Cron counts from the last trigger, in the configured location; from now if
// that has fallen behind.
func TestNextTrigger_Cron(t *testing.T) {
	loc, err := time.LoadLocation("Europe/Istanbul")
	if err != nil {
		t.Fatal(err)
	}
	spec, err := parseCron("0 3 * * *")
	if err != nil {
		t.Fatal(err)
	}
	last := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC) // 03:00 in Istanbul
	want := time.Date(2026, 10, 10, 3, 0, 0, 0, loc)
	if got := nextTrigger(spec, loc, last.Add(time.Second), last); !got.Equal(want) || got.Location() != loc {
		t.Fatalf("from last: %v, want %v", got, want)
	}
	// Three days late: the missed days are not replayed.
	now := last.Add(72*time.Hour + time.Minute)
	want = time.Date(2026, 10, 13, 3, 0, 0, 0, loc)
	if got := nextTrigger(spec, loc, now, last); !got.Equal(want) {
		t.Fatalf("late: %v, want %v", got, want)
	}
}
