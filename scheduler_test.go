package jobs_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	jobs "github.com/Elagoht/collage-jobs"
)

func TestEvery_TriggersOnSchedule(t *testing.T) {
	p := jobs.New()
	var r runs
	p.Every("tick", time.Minute, r.fn)
	_, clk := setup(t, p, "")
	serve(t, p, clk, 1)

	clk.Advance(59 * time.Second)
	expectRuns(t, &r, 0)
	clk.Advance(time.Second)
	expectRuns(t, &r, 1)
	clk.Advance(2 * time.Minute)
	expectRuns(t, &r, 3)
}

func TestCron_UsesTimezone(t *testing.T) {
	p := jobs.New()
	var r runs
	p.Cron("nightly", "0 3 * * *", r.fn)
	clk := jobs.UseFakeClock(p, time.Date(2026, 10, 8, 23, 30, 0, 0, time.UTC)) // 02:30 in Istanbul
	if err := newApp(t, p, `{"timezone": "Europe/Istanbul"}`, &logs{}); err != nil {
		t.Fatal(err)
	}
	serve(t, p, clk, 1)

	clk.Advance(29 * time.Minute)
	expectRuns(t, &r, 0)
	clk.Advance(time.Minute) // 03:00 in Istanbul, 00:00 UTC
	expectRuns(t, &r, 1)
	clk.Advance(23 * time.Hour)
	expectRuns(t, &r, 1)
	clk.Advance(time.Hour)
	expectRuns(t, &r, 2)
}

func TestCron_UTCByDefault(t *testing.T) {
	p := jobs.New()
	var r runs
	p.Cron("nightly", "0 3 * * *", r.fn)
	_, clk := setup(t, p, "")
	serve(t, p, clk, 1)

	clk.Advance(3*time.Hour - time.Minute)
	expectRuns(t, &r, 0)
	clk.Advance(time.Minute)
	expectRuns(t, &r, 1)
}

// Review Focus 1: a trigger that fires while the previous run is still in
// progress is skipped, not queued.
func TestEvery_SkipsWhileRunning(t *testing.T) {
	p := jobs.New()
	release := make(chan struct{})
	var entered runs
	p.Every("slow", time.Minute, func(ctx context.Context) error {
		entered.n.Add(1)
		<-release
		return nil
	})
	l, clk := setup(t, p, "")
	serve(t, p, clk, 1)
	released := false
	t.Cleanup(func() {
		if !released {
			close(release)
		}
	})

	clk.Advance(time.Minute)
	expectRuns(t, &entered, 1)
	clk.Advance(time.Minute)
	clk.Advance(time.Minute)
	expectRuns(t, &entered, 1)
	if n := l.count(slog.LevelDebug, "jobs: skipped, the previous run is still in progress", "slow"); n != 2 {
		t.Fatalf("%d skips logged, want 2", n)
	}

	close(release)
	released = true
	eventually(t, func() bool { return l.count(slog.LevelDebug, "jobs: ran", "slow") == 1 }, "the run to finish")
	clk.Advance(time.Minute)
	expectRuns(t, &entered, 2)
}

func TestRunOnStart(t *testing.T) {
	p := jobs.New()
	var r runs
	p.Every("warm", time.Hour, r.fn).RunOnStart()
	_, clk := setup(t, p, "")
	serve(t, p, clk, 1)
	expectRuns(t, &r, 1)
	clk.Advance(time.Hour)
	expectRuns(t, &r, 2)
}

func TestTimeout(t *testing.T) {
	p := jobs.New()
	got := make(chan error, 1)
	p.Every("bounded", time.Minute, func(ctx context.Context) error {
		<-ctx.Done()
		got <- ctx.Err()
		return ctx.Err()
	}).Timeout(20 * time.Millisecond).RunOnStart()
	l, clk := setup(t, p, "")
	serve(t, p, clk, 1)
	select {
	case err := <-got:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("ctx error %v, want DeadlineExceeded", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the job's ctx never ended")
	}
	eventually(t, func() bool { return l.count(slog.LevelError, "jobs: failed", "bounded") == 1 }, "the failure log")
}

func TestFailureAndPanic(t *testing.T) {
	p := jobs.New()
	var calls runs
	p.Every("flaky", time.Minute, func(context.Context) error {
		switch calls.n.Add(1) {
		case 1:
			return errors.New("boom")
		case 2:
			panic("kaboom")
		}
		return nil
	})
	l, clk := setup(t, p, "")
	serve(t, p, clk, 1)

	clk.Advance(time.Minute)
	eventually(t, func() bool { return l.count(slog.LevelError, "jobs: failed", "flaky") == 1 }, "the error log")
	clk.Advance(time.Minute)
	eventually(t, func() bool { return l.count(slog.LevelError, "jobs: failed", "flaky") == 2 }, "the panic log")
	clk.Advance(time.Minute)
	eventually(t, func() bool { return l.count(slog.LevelDebug, "jobs: ran", "flaky") == 1 }, "a run after the panic")
	expectRuns(t, &calls, 3) // not retried
}

func TestCron_NeverAgain(t *testing.T) {
	p := jobs.New()
	var r runs
	p.Cron("feb30", "0 0 30 2 *", r.fn)
	l, clk := setup(t, p, "")
	serve(t, p, clk, 0)
	eventually(t, func() bool {
		return l.count(slog.LevelWarn, "jobs: the schedule has no next time; the job stops", "feb30") == 1
	}, "the warning")
	clk.Advance(24 * 365 * time.Hour)
	expectRuns(t, &r, 0)
	if n := clk.Pending(); n != 0 {
		t.Fatalf("%d timers pending, want 0", n)
	}
	if n := l.count(slog.LevelWarn, "jobs: the schedule has no next time; the job stops", "feb30"); n != 1 {
		t.Fatalf("%d warnings, want 1", n)
	}
}

func TestNothingBeforeServe(t *testing.T) {
	p := jobs.New()
	var r runs
	p.Every("tick", time.Minute, r.fn).RunOnStart()
	_, clk := setup(t, p, "")
	clk.Advance(time.Hour)
	expectRuns(t, &r, 0)
	if n := clk.Pending(); n != 0 {
		t.Fatalf("%d timers pending before serving, want 0", n)
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestDrainStopsScheduling(t *testing.T) {
	p := jobs.New()
	var r runs
	p.Every("tick", time.Minute, r.fn)
	_, clk := setup(t, p, "")
	cancel := serve(t, p, clk, 1)

	clk.Advance(time.Minute)
	expectRuns(t, &r, 1)
	cancel()
	clk.Advance(time.Hour)
	expectRuns(t, &r, 1)
}

func TestOnServe_CtxAlreadyDone(t *testing.T) {
	p := jobs.New()
	var r runs
	p.Every("tick", time.Minute, r.fn).RunOnStart()
	_, clk := setup(t, p, "")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p.OnServe(ctx)
	t.Cleanup(func() { stop(t, p, cancel) })
	clk.Advance(time.Hour)
	expectRuns(t, &r, 0)
}

func TestStartTwice(t *testing.T) {
	p := jobs.New()
	var r runs
	p.Every("tick", time.Minute, r.fn).RunOnStart()
	_, clk := setup(t, p, "")
	ctx, cancel := context.WithCancel(context.Background())
	p.OnServe(ctx)
	p.Start(ctx)
	p.Start(context.Background())
	t.Cleanup(func() { stop(t, p, cancel) })
	waitPending(t, clk, 1)
	expectRuns(t, &r, 1)
	clk.Advance(time.Minute)
	expectRuns(t, &r, 2)
	if n := clk.Pending(); n != 1 {
		t.Fatalf("%d timers pending, want 1", n)
	}
}

func TestDraining(t *testing.T) {
	p := jobs.New()
	_, clk := setup(t, p, "")
	if jobs.Draining(p) {
		t.Fatal("draining before start")
	}
	cancel := serve(t, p, clk, 0)
	if jobs.Draining(p) {
		t.Fatal("draining while serving")
	}
	cancel()
	if !jobs.Draining(p) {
		t.Fatal("not draining once the serve ctx is done")
	}
}
