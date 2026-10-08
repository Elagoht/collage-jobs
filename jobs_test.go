package jobs_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	jobs "github.com/Elagoht/collage-jobs"
	"github.com/Elagoht/collage/pkg/collage"
)

// t0 is where every fake clock starts: 2026-10-08T00:00:00Z.
var t0 = time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)

// logs is a slog handler that keeps every record.
type logs struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (l *logs) Enabled(context.Context, slog.Level) bool { return true }
func (l *logs) WithAttrs([]slog.Attr) slog.Handler       { return l }
func (l *logs) WithGroup(string) slog.Handler            { return l }
func (l *logs) Handle(_ context.Context, r slog.Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.recs = append(l.recs, r.Clone())
	return nil
}

// count returns how many records have level and msg and, if job is not empty,
// a "job" attribute equal to it.
func (l *logs) count(level slog.Level, msg, job string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, r := range l.recs {
		if r.Level != level || r.Message != msg {
			continue
		}
		if job == "" {
			n++
			continue
		}
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "job" && a.Value.String() == job {
				n++
				return false
			}
			return true
		})
	}
	return n
}

// newApp builds an application with p, logging to l, and starts it, which runs
// Init. It returns Start's error.
func newApp(t *testing.T, p *jobs.Plugin, config string, l *logs) error {
	t.Helper()
	var pc map[string]json.RawMessage
	if config != "" {
		pc = map[string]json.RawMessage{jobs.Name: json.RawMessage(config)}
	}
	a, err := collage.New(&collage.Config{
		Server: collage.ServerConfig{Host: "localhost", Port: 3000},
		Logger: slog.New(l),
		Template: collage.TemplateConfig{FS: fstest.MapFS{
			"t/p.html": {Data: []byte(`<main>home</main>`)},
		}, Root: "t"},
		Plugins:      []collage.Plugin{p},
		PluginConfig: pc,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.RegisterPage(collage.NewPage("home").WithContent(collage.NewFragment("home", "p.html").Build()).WithPath("en", "/").Build()); err != nil {
		t.Fatal(err)
	}
	return a.Start()
}

// setup puts p on a fake clock at t0 and runs its Init through an application
// that must start.
func setup(t *testing.T, p *jobs.Plugin, config string) (*logs, *jobs.FakeClock) {
	t.Helper()
	clk := jobs.UseFakeClock(p, t0)
	l := &logs{}
	if err := newApp(t, p, config, l); err != nil {
		t.Fatal(err)
	}
	return l, clk
}

// serve starts p with Start and waits until armed timers are pending. At the end
// of the test it cancels the serve ctx, shuts p down and fails if any loop or
// run is left. It returns the serve ctx's cancel.
func serve(t *testing.T, p *jobs.Plugin, clk *jobs.FakeClock, armed int) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)
	t.Cleanup(func() { stop(t, p, cancel) })
	waitPending(t, clk, armed)
	return cancel
}

func stop(t *testing.T, p *jobs.Plugin, cancel context.CancelFunc) {
	t.Helper()
	cancel()
	ctx, done := context.WithTimeout(context.Background(), 2*time.Second)
	defer done()
	if err := p.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
	if !jobs.WaitIdle(p, 2*time.Second) {
		t.Error("goroutines left after Shutdown")
	}
}

func waitPending(t *testing.T, clk *jobs.FakeClock, n int) {
	t.Helper()
	eventually(t, func() bool { return clk.Pending() >= n }, "timers pending")
}

// eventually fails the test unless cond holds within two seconds.
func eventually(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// runs counts a job's runs.
type runs struct{ n atomic.Int32 }

func (r *runs) fn(context.Context) error { r.n.Add(1); return nil }

// expectRuns waits for exactly want runs: it waits until the count reaches want,
// then checks that no more come for a moment.
func expectRuns(t *testing.T, r *runs, want int32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for r.n.Load() < want && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	if got := r.n.Load(); got != want {
		t.Fatalf("%d runs, want %d", got, want)
	}
}

func TestDisabled(t *testing.T) {
	p := jobs.New()
	var off, on runs
	p.Every("off", time.Minute, off.fn).RunOnStart()
	p.Every("on", time.Minute, on.fn)
	_, clk := setup(t, p, `{"disabled": ["off"]}`)
	serve(t, p, clk, 1)
	clk.Advance(time.Hour)
	expectRuns(t, &on, 60)
	expectRuns(t, &off, 0)
}

func TestDisabled_UnknownName(t *testing.T) {
	p := jobs.New()
	p.Every("on", time.Minute, func(context.Context) error { return nil })
	err := newApp(t, p, `{"disabled": ["nope"]}`, &logs{})
	if err == nil || !strings.Contains(err.Error(), "elagoht/jobs: ") || !strings.Contains(err.Error(), `"nope"`) {
		t.Fatalf("Init error %v, want one naming \"nope\"", err)
	}
}

func TestDisabled_FromNewWith(t *testing.T) {
	p := jobs.NewWith(jobs.Config{Disabled: []string{"off"}})
	var off runs
	p.Every("off", time.Minute, off.fn)
	p.Every("on", time.Minute, func(context.Context) error { return nil })
	_, clk := setup(t, p, "")
	serve(t, p, clk, 1)
	clk.Advance(5 * time.Minute)
	expectRuns(t, &off, 0)
}

func TestInit_BadTimezone(t *testing.T) {
	p := jobs.New()
	err := newApp(t, p, `{"timezone": "Mars/Olympus_Mons"}`, &logs{})
	if err == nil || !strings.Contains(err.Error(), "elagoht/jobs: timezone") {
		t.Fatalf("Init error %v, want an elagoht/jobs timezone error", err)
	}
	if !strings.Contains(err.Error(), "Mars/Olympus_Mons") {
		t.Fatalf("Init error %v does not name the zone", err)
	}
}

func TestInit_Twice(t *testing.T) {
	p := jobs.New()
	if err := newApp(t, p, "", &logs{}); err != nil {
		t.Fatal(err)
	}
	if err := newApp(t, p, "", &logs{}); err == nil {
		t.Fatal("a second Init succeeded")
	}
}

func TestMisuse(t *testing.T) {
	nop := func(context.Context) error { return nil }
	cases := []struct {
		name string
		want string // in the panic message
		do   func()
	}{
		{"duplicate name", `"a"`, func() {
			p := jobs.New()
			p.Every("a", time.Minute, nop)
			p.Cron("a", "* * * * *", nop)
		}},
		{"empty name", "name", func() { jobs.New().Every("", time.Minute, nop) }},
		{"nil Every fn", "nil", func() { jobs.New().Every("a", time.Minute, nil) }},
		{"nil Cron fn", "nil", func() { jobs.New().Cron("a", "* * * * *", nil) }},
		{"zero interval", "interval", func() { jobs.New().Every("a", 0, nop) }},
		{"negative timeout", "timeout", func() { jobs.New().Every("a", time.Minute, nop).Timeout(-time.Second) }},
		{"bad cron", "day of month", func() { jobs.New().Cron("a", "0 3 32 * *", nop) }},
		{"Every after Init", "after Init", func() {
			p := jobs.New()
			if err := newApp(t, p, "", &logs{}); err != nil {
				t.Fatal(err)
			}
			p.Every("a", time.Minute, nop)
		}},
		{"Cron after Init", "after Init", func() {
			p := jobs.New()
			if err := newApp(t, p, "", &logs{}); err != nil {
				t.Fatal(err)
			}
			p.Cron("a", "* * * * *", nop)
		}},
		{"RunOnStart after Init", "after Init", func() {
			p := jobs.New()
			j := p.Every("a", time.Minute, nop)
			if err := newApp(t, p, "", &logs{}); err != nil {
				t.Fatal(err)
			}
			j.RunOnStart()
		}},
		{"Start before Init", "Init", func() { jobs.New().Start(context.Background()) }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("no panic")
				}
				msg := ""
				switch v := r.(type) {
				case string:
					msg = v
				case error:
					msg = v.Error()
				}
				if !strings.HasPrefix(msg, "elagoht/jobs: ") || !strings.Contains(msg, c.want) {
					t.Fatalf("panic %q, want one prefixed elagoht/jobs: and naming %q", msg, c.want)
				}
			}()
			c.do()
		})
	}
}

func TestErrDraining(t *testing.T) {
	if !errors.Is(jobs.ErrDraining, jobs.ErrDraining) || jobs.ErrDraining.Error() != "jobs: draining" {
		t.Fatalf("ErrDraining = %v", jobs.ErrDraining)
	}
}

func TestNameAndVersion(t *testing.T) {
	p := jobs.New()
	if p.Name() != "elagoht/jobs" || p.Version() != "0.1.0" {
		t.Fatalf("Name %q Version %q", p.Name(), p.Version())
	}
}
