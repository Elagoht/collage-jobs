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
	done := make(chan struct{})
	go func() { p.wg.Wait(); close(done) }()
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

// Draining reports p.draining().
func Draining(p *Plugin) bool { return p.draining() }
