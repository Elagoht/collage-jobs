package jobs_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	jobs "github.com/Elagoht/collage-jobs"
	"github.com/Elagoht/collage/pkg/collage"
)

// newServeApp builds an application with p, logging to l, without starting it.
func newServeApp(t *testing.T, p *jobs.Plugin, server collage.ServerConfig, l *logs) *collage.App {
	t.Helper()
	a, err := collage.New(&collage.Config{
		Server: server,
		Logger: slog.New(l),
		Template: collage.TemplateConfig{FS: fstest.MapFS{
			"t/p.html": {Data: []byte(`<main>home</main>`)},
		}, Root: "t"},
		Plugins: []collage.Plugin{p},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.RegisterPage(collage.NewPage("home").WithContent(collage.NewFragment("home", "p.html").Build()).WithPath("en", "/").Build()); err != nil {
		t.Fatal(err)
	}
	return a
}

// freePort returns a TCP port on 127.0.0.1 that was free a moment ago.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

// Review Focus 5: a static build of an application using jobs runs no job and
// handles no queued item, though the build initialises the plugin.
func TestBuild_RunsNothing(t *testing.T) {
	p := jobs.New(jobs.Options{})
	var r runs
	p.Every("tick", time.Millisecond, r.fn).RunOnStart()
	var handled atomic.Int32
	q := jobs.NewQueue(p, "work", func(context.Context, int) error { handled.Add(1); return nil }, jobs.QueueOptions[int]{})
	if err := q.Enqueue(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	a := newServeApp(t, p, collage.ServerConfig{Host: "localhost", Port: 3000}, &logs{})

	if _, err := collage.Build(context.Background(), a, collage.BuildOptions{OutDir: t.TempDir()}); err != nil {
		t.Fatalf("Build: %v", err)
	}
	// A job or worker started by mistake would run on a goroutine that may not
	// have been scheduled yet; waiting on real time can only miss that bug,
	// never fail a correct plugin.
	time.Sleep(50 * time.Millisecond)
	if n, h := r.n.Load(), handled.Load(); n != 0 || h != 0 {
		t.Fatalf("the build ran the job %d times and handled %d items", n, h)
	}
	if n := jobs.Active(p); n != 0 {
		t.Fatalf("%d goroutines started by the build, want 0", n)
	}

	// The build did initialise the plugin, so the check above is not vacuous.
	defer func() {
		if rec := recover(); !strings.Contains(fmt.Sprint(rec), "after Init") {
			t.Fatalf("defining a job after the build: %v, want the after-Init panic", rec)
		}
	}()
	p.Every("late", time.Minute, r.fn)
}

// End to end with a real server and the real clock: Shutdown begins the drain,
// which stops the scheduler but still accepts items, and the queued items,
// those enqueued during the drain too, finish before ListenAndServe returns,
// without Shutdown waiting out its ctx. Once the plugin's Shutdown has begun,
// Enqueue is refused.
func TestE2E_ServeAndDrain(t *testing.T) {
	p := jobs.New(jobs.Options{})
	var r runs
	p.Every("tick", 20*time.Millisecond, r.fn)
	var handled atomic.Int32
	q := jobs.NewQueue(p, "work", func(context.Context, int) error {
		time.Sleep(100 * time.Millisecond)
		handled.Add(1)
		return nil
	}, jobs.QueueOptions[int]{})
	l := &logs{}
	port := freePort(t)
	a := newServeApp(t, p, collage.ServerConfig{
		Host:            "127.0.0.1",
		Port:            port,
		DrainDelay:      300 * time.Millisecond,
		ShutdownTimeout: 3 * time.Second,
	}, l)

	served := make(chan error, 1)
	go func() { served <- a.ListenAndServe() }()
	eventually(t, func() bool { return r.n.Load() >= 2 }, "two scheduled runs")

	// Five items of 100ms on one worker outlast the 300ms drain, so the last of
	// them are finished by the plugin's Shutdown, not by the drain.
	for i := range 5 {
		if err := q.Enqueue(context.Background(), i); err != nil {
			t.Fatal(err)
		}
	}
	shut := make(chan error, 1)
	var shutLive atomic.Bool
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		err := a.Shutdown(ctx)
		shutLive.Store(ctx.Err() == nil)
		shut <- err
	}()
	eventually(t, func() bool { return jobs.Draining(p) }, "the drain to begin")

	if err := q.Enqueue(context.Background(), 99); err != nil {
		t.Fatalf("Enqueue during the drain: %v, want nil", err)
	}
	// A run triggered just before the drain may still finish within 50ms;
	// after that the count must not move. Waiting on real time can only miss a
	// scheduler that keeps triggering, never fail a correct plugin.
	time.Sleep(50 * time.Millisecond)
	settledRuns := r.n.Load()
	time.Sleep(100 * time.Millisecond)
	if n := r.n.Load(); n != settledRuns {
		t.Fatalf("%d runs 50ms into the drain, %d 100ms later: the scheduler kept triggering", settledRuns, n)
	}

	if err := <-shut; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if !shutLive.Load() {
		t.Fatal("Shutdown waited for its ctx to end: the workers did not stop when the queue emptied")
	}
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("ListenAndServe: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ListenAndServe did not return after Shutdown")
	}
	if n := handled.Load(); n != 6 {
		t.Fatalf("%d queued items finished, want 6: five before the drain and one during it", n)
	}
	if err := q.Enqueue(context.Background(), 100); !errors.Is(err, jobs.ErrDraining) {
		t.Fatalf("Enqueue after Shutdown: %v, want ErrDraining", err)
	}
	if _, ok := l.dropped("work"); ok {
		t.Fatal("a drop was logged though every item finished")
	}
	expectIdle(t, p)
}
