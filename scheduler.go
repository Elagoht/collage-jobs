package jobs

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"
)

// start runs once, for the first of OnServe and Start. It keeps ctx as the
// serve ctx, makes the ctx every run derives from, and starts one loop per
// enabled scheduled job, unless ctx is already done.
func (p *Plugin) start(ctx context.Context) {
	p.mu.Lock()
	inited := p.inited
	p.mu.Unlock()
	if !inited {
		panic("elagoht/jobs: Start before Init: start the application (app.Start or app.Handler) first")
	}
	p.startOnce.Do(func() {
		p.mu.Lock()
		p.running = ctx
		p.loopCtx, p.stopLoops = context.WithCancel(ctx)
		p.workCtx, p.cancelWork = context.WithCancel(context.Background())
		scheduled := p.scheduled
		p.mu.Unlock()

		if ctx.Err() != nil {
			return // the drain began before the server got to serve
		}
		for _, j := range scheduled {
			if p.disabled[j.name] {
				continue
			}
			p.wg.Add(1)
			go p.loop(p.loopCtx, j)
		}
	})
}

// loop triggers j on its schedule until ctx ends. A trigger that comes while
// the previous run is still in progress is skipped.
func (p *Plugin) loop(ctx context.Context, j *Job) {
	defer p.wg.Done()
	if j.runOnStart && ctx.Err() == nil {
		p.trigger(j)
	}
	var last time.Time // the last trigger's time
	for {
		now := p.clock.Now()
		// Count from the last trigger, so that a late wake-up does not shift
		// an interval; from now if that falls behind.
		var next time.Time
		if !last.IsZero() {
			next = j.sched.next(last.In(p.loc))
		}
		if !next.After(now) {
			next = j.sched.next(now.In(p.loc))
		}
		if next.IsZero() {
			p.log.Warn("jobs: the schedule has no next time; the job stops", "job", j.name)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-p.clock.After(next.Sub(now)):
		}
		if ctx.Err() != nil {
			return // the drain began as the trigger fired
		}
		last = next
		p.trigger(j)
	}
}

// trigger starts a run of j, or skips it if one is in progress.
func (p *Plugin) trigger(j *Job) {
	if !j.busy.CompareAndSwap(false, true) {
		p.log.Debug("jobs: skipped, the previous run is still in progress", "job", j.name)
		return
	}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
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
