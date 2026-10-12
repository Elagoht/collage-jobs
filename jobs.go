// Package jobs runs scheduled jobs and typed background queues, only while the
// application serves, and finishes queued work on shutdown.
package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Elagoht/collage/pkg/collage"
)

// Name is the plugin's name.
const Name = "elagoht/jobs"

const version = "0.2.0"

// ErrDraining is returned by Enqueue once the plugin's Shutdown has begun. The
// drain alone does not refuse work: the server still serves then, on purpose.
var ErrDraining = errors.New("jobs: draining")

// Options configures the plugin. Its section of the application's plugin
// configuration is decoded over it.
type Options struct {
	Timezone string   `json:"timezone"` // IANA time zone cron expressions are read in, default "UTC"
	Disabled []string `json:"disabled"` // names of scheduled jobs that do not run; an unknown name fails startup
}

// Plugin is the jobs plugin.
type Plugin struct {
	cfg   Options
	clock clock

	// mu guards names, scheduled, queues, inited and running, and the write
	// of closed. Enqueue checks closed and pushes under it, and start
	// launches everything under it, so Shutdown, which sets closed under it,
	// sees each of them either done in full or not begun.
	mu        sync.Mutex
	names     map[string]bool // every job and queue name taken
	scheduled []*Job
	queues    []worker
	inited    bool // Init succeeded: no more definitions

	// Set by Init, read-only afterwards.
	loc      *time.Location
	log      *slog.Logger
	disabled map[string]bool

	// running is the serve ctx: OnServe's or Start's. It is nil before the
	// plugin starts, and done once the drain has begun.
	running context.Context
	// closed is set, under mu, when Shutdown begins. From then on Enqueue is
	// refused, nothing starts, and an emptied queue's workers stop. The drain
	// alone refuses no Enqueue.
	closed atomic.Bool
	// loopCtx ends the scheduler loops: it is done when running is, or when
	// stopLoops is called.
	loopCtx   context.Context
	stopLoops context.CancelFunc
	// workCtx is the parent of every job run's ctx. cancelWork cancels it at
	// the end of Shutdown.
	workCtx    context.Context
	cancelWork context.CancelFunc
	// active counts every scheduler loop, run and queue worker.
	active group
}

var (
	_ collage.Plugin    = (*Plugin)(nil)
	_ collage.ServeHook = (*Plugin)(nil)
)

// New returns the plugin with opts as the starting point configuration is
// decoded over. Keep it in a variable: jobs and queues are defined on it, with
// Every, Cron and NewQueue, before the application starts.
//
//	jobsPlugin := jobs.New(jobs.Options{})
//	jobsPlugin.Every("cleanup", time.Hour, cleanup)
func New(opts Options) *Plugin {
	return &Plugin{cfg: opts, clock: realClock{}, names: map[string]bool{}}
}

// Name returns Name.
func (p *Plugin) Name() string { return Name }

// Version returns the plugin's version.
func (p *Plugin) Version() string { return version }

// Job is a scheduled job, as Every and Cron define it.
type Job struct {
	p          *Plugin
	name       string
	sched      schedule
	fn         func(context.Context) error
	runOnStart bool
	timeout    time.Duration
	busy       atomic.Bool // a run is in progress
}

// Every defines a job that runs fn every d, the first time d after the plugin
// starts. It panics if name is empty or taken, fn is nil, d is not positive,
// or Init has run.
func (p *Plugin) Every(name string, d time.Duration, fn func(context.Context) error) *Job {
	if d <= 0 {
		panic(fmt.Sprintf("jobs: job %q: the interval must be positive, got %v", name, d))
	}
	return p.define(name, every{d}, fn)
}

// Cron defines a job that runs fn on a five-field cron expression (minute,
// hour, day of month, month, day of week), read in the configured time zone.
// It panics if expr does not parse, naming the field, if it can never match,
// as February 30 cannot, and on the same misuse as Every.
func (p *Plugin) Cron(name, expr string, fn func(context.Context) error) *Job {
	spec, err := parseCron(expr)
	if err != nil {
		panic(fmt.Sprintf("jobs: job %q: %v", name, err))
	}
	if !spec.possible() {
		panic(fmt.Sprintf("jobs: job %q: cron %q never matches: no month it names has a day it names", name, expr))
	}
	return p.define(name, spec, fn)
}

func (p *Plugin) define(name string, s schedule, fn func(context.Context) error) *Job {
	if fn == nil {
		panic(fmt.Sprintf("jobs: job %q: the function is nil", name))
	}
	j := &Job{p: p, name: name, sched: s, fn: fn}
	p.register(name, func() { p.scheduled = append(p.scheduled, j) })
	return j
}

// register takes name for a job or a queue and calls add, both under one hold
// of p.mu, so that Init sees a name only together with what it names. It
// panics if name is empty or already taken, or if Init has run.
func (p *Plugin) register(name string, add func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch {
	case name == "":
		panic("jobs: a job or queue needs a name")
	case p.inited:
		panic(fmt.Sprintf("jobs: %q defined after Init: define jobs and queues before the application starts", name))
	case p.names[name]:
		panic(fmt.Sprintf("jobs: the name %q is taken by another job or queue", name))
	}
	p.names[name] = true
	add()
}

// RunOnStart makes the job also run as soon as the plugin starts. It panics
// after Init.
func (j *Job) RunOnStart() *Job {
	j.p.mu.Lock()
	defer j.p.mu.Unlock()
	j.checkOpen("RunOnStart")
	j.runOnStart = true
	return j
}

// Timeout bounds each run's ctx to d; zero means no bound. It panics if d is
// negative, or after Init.
func (j *Job) Timeout(d time.Duration) *Job {
	if d < 0 {
		panic(fmt.Sprintf("jobs: job %q: the timeout must not be negative, got %v", j.name, d))
	}
	j.p.mu.Lock()
	defer j.p.mu.Unlock()
	j.checkOpen("Timeout")
	j.timeout = d
	return j
}

// checkOpen panics if Init has run. j.p.mu is held.
func (j *Job) checkOpen(method string) {
	if j.p.inited {
		panic(fmt.Sprintf("jobs: job %q: %s after Init: set it before the application starts", j.name, method))
	}
}

// Init reads the configuration, loads the time zone and checks every disabled
// name against the scheduled jobs. From then on no job or queue can be defined.
// A Plugin value serves one App: a second Init fails.
func (p *Plugin) Init(_ context.Context, host collage.Host) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.inited {
		return errors.New("jobs: a Plugin value serves one App; create another with New")
	}
	cfg, err := collage.PluginConfig(host, p.cfg)
	if err != nil {
		return err
	}
	zone := cfg.Timezone
	if zone == "" {
		zone = "UTC"
	}
	loc, err := time.LoadLocation(zone)
	if err != nil {
		// A minimal image without zoneinfo fails the same way as a typo.
		return fmt.Errorf(`jobs: timezone %q: %w (if the name is right, the image may lack time zone data: import _ "time/tzdata" in main)`, zone, err)
	}
	disabled := map[string]bool{}
	for _, name := range cfg.Disabled {
		if !slices.ContainsFunc(p.scheduled, func(j *Job) bool { return j.name == name }) {
			return fmt.Errorf("jobs: disabled: no scheduled job is named %q", name)
		}
		disabled[name] = true
	}
	p.cfg, p.loc, p.disabled = cfg, loc, disabled
	p.log = host.Logger()
	p.inited = true
	return nil
}

// OnServe starts the scheduled jobs and the queues' workers. collage calls it
// once the server listens; ctx ends when the drain begins, and from then on no
// job starts. Only the first of OnServe and Start does anything, and neither
// does anything after Shutdown.
func (p *Plugin) OnServe(ctx context.Context) { p.start(ctx) }

// Start does what OnServe does, for an application that runs its own server
// over app.Handler and so never gets OnServe. Cancel ctx when that server
// starts to drain, and call Shutdown, or App.Shutdown, after it. Only the first
// of OnServe and Start does anything, and neither does anything after
// Shutdown. It panics before Init: start the application first.
func (p *Plugin) Start(ctx context.Context) { p.start(ctx) }

// giveUp is how long Shutdown still waits, once it has cancelled the runs'
// ctx, for the runs to return.
const giveUp = time.Second

// Shutdown stops the plugin. It is meant to come after the drain, as
// App.Shutdown calls it, but does the drain's part too if that has not
// happened. From its start Enqueue returns ErrDraining, no job is triggered,
// and Start or OnServe after it starts nothing.
//
// Runs in progress finish, and each queue's workers keep taking queued items
// until the queue is empty or ctx ends. Then the runs' ctx is cancelled, and
// Shutdown waits a second more for them to return. A run that ignores its ctx
// past that is left running: Shutdown logs at Warn that it gave up, and
// returns. Such a run's goroutine lives on until the function returns, but the
// plugin keeps no goroutine of its own waiting for it.
//
// Last, each queue that still has items logs at Warn "jobs: dropped queued
// items". Its count is the items dropped: those left in the store, and those
// waiting for a retry when the runs' ctx ended, which are dropped rather than
// failed, so OnFailure is not called. Its abandoned is, after a give-up, the
// items whose runs were still going: they may yet finish, but nothing waits
// for them. The store is in memory, so dropped items are lost.
//
// Shutdown always returns nil. Before the plugin has started it returns at
// once, logging the items that were queued and never handled. Only the first
// call does anything: a second, even one made while the first is still
// waiting, returns nil at once without waiting for it.
func (p *Plugin) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	if p.closed.Load() {
		p.mu.Unlock()
		return nil
	}
	// Under mu, as Enqueue checks and pushes: an Enqueue racing this has either
	// pushed already, and is counted below, or will see closed.
	p.closed.Store(true)
	started := p.running != nil
	queues := p.queues
	p.mu.Unlock()

	gaveUp := false
	if started {
		p.stopLoops()
		for _, q := range queues {
			q.finish()
		}
		gaveUp = p.wait(ctx)
	}
	if p.log == nil {
		return nil // Init never ran, so there is no logger
	}
	for _, q := range queues {
		dropped, abandoned := q.leftovers(gaveUp)
		if dropped > 0 || abandoned > 0 {
			p.log.Warn("jobs: dropped queued items", "queue", q.queueName(), "count", dropped, "abandoned", abandoned)
		}
	}
	return nil
}

// wait waits for every loop, run and worker to end, or for ctx to end. Then it
// cancels the runs' ctx and waits up to giveUp more. It reports whether it
// gave up.
func (p *Plugin) wait(ctx context.Context) bool {
	defer p.cancelWork()
	select {
	case <-p.active.Idle():
		return false
	case <-ctx.Done():
	}
	p.cancelWork()
	timer := time.NewTimer(giveUp)
	defer timer.Stop()
	select {
	case <-p.active.Idle():
		return false
	case <-timer.C:
		// goroutines counts what is still going: the stuck runs, and for a
		// queue's item, the worker running it.
		p.log.Warn("jobs: gave up waiting for runs that ignore their ctx", "goroutines", p.active.Count(), "waited", giveUp)
		return true
	}
}

// group counts goroutines, like a sync.WaitGroup that can be waited on in a
// select and that leaves no goroutine behind when the wait is abandoned.
type group struct {
	mu   sync.Mutex
	n    int
	idle chan struct{} // closed when n reaches zero; nil until Idle asks
}

// Add adds d to the count. It panics if the count goes negative.
func (g *group) Add(d int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.n += d
	switch {
	case g.n < 0:
		panic("jobs: negative group count")
	case g.n == 0 && g.idle != nil:
		close(g.idle)
		g.idle = nil
	}
}

// Done subtracts one from the count.
func (g *group) Done() { g.Add(-1) }

// Count returns the count.
func (g *group) Count() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.n
}

// Idle returns a channel closed once the count is zero.
func (g *group) Idle() <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.n == 0 {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	if g.idle == nil {
		g.idle = make(chan struct{})
	}
	return g.idle
}
