package jobs

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"
)

// start runs for the first of OnServe and Start, unless Shutdown came first.
// It keeps ctx as the serve ctx, makes the ctx every run derives from, and
// starts one loop per enabled scheduled job, unless ctx is already done, and
// every queue's workers in any case. It does all of that under p.mu, so a
// Shutdown sees the plugin either not started or started in full.
func (p *Plugin) start(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.inited {
		panic("jobs: Start before Init: start the application (app.Start or app.Handler) first")
	}
	if p.running != nil || p.closed.Load() {
		return
	}
	p.running = ctx
	p.loopCtx, p.stopLoops = context.WithCancel(ctx)
	p.workCtx, p.cancelWork = context.WithCancel(context.Background())

	// A drain that began before the server got to serve starts no scheduled
	// job. The workers still start, so that Shutdown can finish what is
	// already queued.
	if ctx.Err() == nil {
		for _, j := range p.scheduled {
			if p.disabled[j.name] {
				continue
			}
			p.active.Add(1)
			go p.loop(p.loopCtx, j)
		}
	}
	for _, q := range p.queues {
		q.startWorkers(p.workCtx)
	}
}

// loop triggers j on its schedule until ctx ends. A trigger that comes while
// the previous run is still in progress is skipped.
func (p *Plugin) loop(ctx context.Context, j *Job) {
	defer p.active.Done()
	// This loop's own ctx, ended when the loop returns for any reason: the
	// clock knows this waiter by it.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if j.runOnStart && ctx.Err() == nil {
		p.trigger(j)
	}
	var last time.Time // the last trigger's time
	for {
		now := p.clock.Now()
		next := nextTrigger(j.sched, p.loc, now, last)
		if next.IsZero() {
			p.log.Warn("jobs: the schedule has no next time; the job stops", "job", j.name)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-p.clock.After(ctx, next.Sub(now)):
		}
		if ctx.Err() != nil {
			return // the drain began as the trigger fired
		}
		last = next
		p.trigger(j)
	}
}

// nextTrigger returns when s triggers next, given the time now and the last
// trigger (zero before the first), or the zero time if it never does.
//
// An interval counts from now as the clock gave it, keeping its monotonic
// reading, so the wait is real elapsed time: a wall clock set back or forward
// does not stretch or shrink it. In(loc) would strip that reading.
//
// A cron schedule counts from the last trigger, read in loc; from now when that
// has fallen behind, so a long stall does not replay what it missed.
func nextTrigger(s schedule, loc *time.Location, now, last time.Time) time.Time {
	if _, isEvery := s.(every); isEvery {
		return s.next(now)
	}
	var next time.Time
	if !last.IsZero() {
		next = s.next(last.In(loc))
	}
	if !next.After(now) {
		next = s.next(now.In(loc))
	}
	return next
}

// trigger starts a run of j, or skips it if one is in progress.
func (p *Plugin) trigger(j *Job) {
	if p.closed.Load() {
		return // Shutdown has begun, and stops the loops next
	}
	if !j.busy.CompareAndSwap(false, true) {
		p.log.Debug("jobs: skipped, the previous run is still in progress", "job", j.name)
		return
	}
	p.active.Add(1)
	go func() {
		defer p.active.Done()
		defer j.busy.Store(false)
		p.run(j)
	}()
}

// run runs j once and logs the outcome. Scheduled jobs are not retried.
func (p *Plugin) run(j *Job) {
	ctx := p.workCtx
	if j.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, j.timeout)
		defer cancel()
	}
	began := time.Now()
	err := call(ctx, j.fn)
	took := time.Since(began)
	if err != nil {
		p.log.Error("jobs: failed", "job", j.name, "duration", took, "error", err)
		return
	}
	p.log.Debug("jobs: ran", "job", j.name, "duration", took)
}

// call runs fn, turning a panic into an error that carries the stack.
func call(ctx context.Context, fn func(context.Context) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
		}
	}()
	return fn(ctx)
}
