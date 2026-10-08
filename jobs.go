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

const version = "0.1.0"

// ErrDraining is returned for work handed to the plugin after the drain has
// begun.
var ErrDraining = errors.New("jobs: draining")

// Config configures the plugin.
type Config struct {
	// Timezone is the IANA time zone cron expressions are read in, default
	// "UTC".
	Timezone string `json:"timezone"`
	// Disabled names scheduled jobs that do not run.
	Disabled []string `json:"disabled"`
}

// Plugin is the jobs plugin.
type Plugin struct {
	cfg   Config
	clock clock

	// mu guards names, scheduled, inited and running.
	mu        sync.Mutex
	names     map[string]bool // every job and queue name taken
	scheduled []*Job
	inited    bool // Init succeeded: no more definitions

	// Set by Init, read-only afterwards.
	loc      *time.Location
	log      *slog.Logger
	disabled map[string]bool

	startOnce sync.Once
	// running is the serve ctx: OnServe's or Start's. It is nil before the
	// plugin starts, and done once the drain has begun.
	running context.Context
	// loopCtx ends the scheduler loops: it is done when running is, or when
	// stopLoops is called.
	loopCtx   context.Context
	stopLoops context.CancelFunc
	// workCtx is the parent of every job run's ctx. cancelWork cancels it at
	// the end of Shutdown.
	workCtx    context.Context
	cancelWork context.CancelFunc
	// wg counts every scheduler loop, run and worker.
	wg sync.WaitGroup
}

var (
	_ collage.Plugin    = (*Plugin)(nil)
	_ collage.ServeHook = (*Plugin)(nil)
)

// New returns the plugin with its defaults, which configuration may change.
func New() *Plugin { return NewWith(Config{}) }

// NewWith returns the plugin with cfg as the defaults configuration starts from.
func NewWith(cfg Config) *Plugin {
	return &Plugin{cfg: cfg, clock: realClock{}, names: map[string]bool{}}
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
		panic(fmt.Sprintf("elagoht/jobs: job %q: the interval must be positive, got %v", name, d))
	}
	return p.define(name, every{d}, fn)
}

// Cron defines a job that runs fn on a five-field cron expression (minute,
// hour, day of month, month, day of week), read in the configured time zone.
// It panics if expr does not parse, naming the field, and on the same misuse as
// Every.
func (p *Plugin) Cron(name, expr string, fn func(context.Context) error) *Job {
	spec, err := parseCron(expr)
	if err != nil {
		panic(fmt.Sprintf("elagoht/jobs: job %q: %v", name, err))
	}
	return p.define(name, spec, fn)
}

func (p *Plugin) define(name string, s schedule, fn func(context.Context) error) *Job {
	if fn == nil {
		panic(fmt.Sprintf("elagoht/jobs: job %q: the function is nil", name))
	}
	p.register(name)
	j := &Job{p: p, name: name, sched: s, fn: fn}
	p.mu.Lock()
	p.scheduled = append(p.scheduled, j)
	p.mu.Unlock()
	return j
}

// register takes name for a job or a queue. It panics if name is empty or
// already taken, or if Init has run.
func (p *Plugin) register(name string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch {
	case name == "":
		panic("elagoht/jobs: a job or queue needs a name")
	case p.inited:
		panic(fmt.Sprintf("elagoht/jobs: %q defined after Init: define jobs and queues before the application starts", name))
	case p.names[name]:
		panic(fmt.Sprintf("elagoht/jobs: the name %q is taken by another job or queue", name))
	}
	p.names[name] = true
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
		panic(fmt.Sprintf("elagoht/jobs: job %q: the timeout must not be negative, got %v", j.name, d))
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
		panic(fmt.Sprintf("elagoht/jobs: job %q: %s after Init: set it before the application starts", j.name, method))
	}
}

// Init reads the configuration, loads the time zone and checks every disabled
// name against the scheduled jobs. From then on no job or queue can be defined.
// A Plugin value serves one App: a second Init fails.
func (p *Plugin) Init(_ context.Context, host collage.Host) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.inited {
		return errors.New("elagoht/jobs: a Plugin value serves one App; create another with New")
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
		return fmt.Errorf("elagoht/jobs: timezone %q: %w", zone, err)
	}
	disabled := map[string]bool{}
	for _, name := range cfg.Disabled {
		if !slices.ContainsFunc(p.scheduled, func(j *Job) bool { return j.name == name }) {
			return fmt.Errorf("elagoht/jobs: disabled: no scheduled job is named %q", name)
		}
		disabled[name] = true
	}
	p.cfg, p.loc, p.disabled = cfg, loc, disabled
	p.log = host.Logger()
	p.inited = true
	return nil
}

// OnServe starts the scheduled jobs. collage calls it once the server listens;
// ctx ends when the drain begins, and from then on no job starts. Only the
// first of OnServe and Start does anything.
func (p *Plugin) OnServe(ctx context.Context) { p.start(ctx) }

// Start does what OnServe does, for an application that runs its own server
// over app.Handler and so never gets OnServe. Cancel ctx when that server
// starts to drain. Only the first of OnServe and Start does anything. It
// panics before Init: start the application first.
func (p *Plugin) Start(ctx context.Context) { p.start(ctx) }

// draining reports whether the serve ctx is done: the drain has begun.
func (p *Plugin) draining() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.running != nil && p.running.Err() != nil
}

// Shutdown stops the scheduler, waits for running jobs until ctx ends, and
// then cancels their ctx.
func (p *Plugin) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	started := p.running != nil
	p.mu.Unlock()
	if !started {
		return nil
	}
	p.stopLoops()
	done := make(chan struct{})
	go func() { p.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
	p.cancelWork()
	return nil
}
