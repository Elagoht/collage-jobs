package jobs_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	jobs "github.com/Elagoht/collage-jobs"
)

// serveQueues starts p with Start. At the end of the test it cancels the serve
// ctx and shuts p down with a short ctx: workers wait for items until the jobs'
// ctx ends, so Shutdown always waits for its ctx while a queue exists. No
// assertion depends on that wait.
func serveQueues(t *testing.T, p *jobs.Plugin) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)
	t.Cleanup(func() {
		cancel()
		sctx, done := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer done()
		if err := p.Shutdown(sctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
		if !jobs.WaitIdle(p, 2*time.Second) {
			t.Error("goroutines left after Shutdown")
		}
	})
	return cancel
}

// settled waits until every item enqueued on q has been handled.
func settled[T any](t *testing.T, q *jobs.Queue[T]) {
	t.Helper()
	eventually(t, func() bool { return jobs.Unfinished(q) == 0 }, "the queue to settle")
}

// recorder records the items a queue's function was called with, and fails
// the first fail calls.
type recorder struct {
	mu    sync.Mutex
	items []int
	fail  int
}

func (r *recorder) fn(_ context.Context, item int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.items = append(r.items, item)
	if len(r.items) <= r.fail {
		return fmt.Errorf("attempt %d failed", len(r.items))
	}
	return nil
}

func (r *recorder) got() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int(nil), r.items...)
}

func expectItems(t *testing.T, r *recorder, want ...int) {
	t.Helper()
	got := r.got()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("called with %v, want %v", got, want)
	}
}

func TestQueue_RunsItems(t *testing.T) {
	p := jobs.New()
	var mu sync.Mutex
	seen := map[int]int{}
	// Items 0 and 1 each wait for the other: they finish only if two workers
	// run them at the same time.
	arrived := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	q := jobs.NewQueue(p, "work", func(_ context.Context, item int) error {
		if item < 2 {
			close(arrived[item])
			select {
			case <-arrived[1-item]:
			case <-time.After(10 * time.Second):
				return errors.New("the other worker never came")
			}
		}
		mu.Lock()
		seen[item]++
		mu.Unlock()
		return nil
	}, jobs.QueueOptions[int]{Workers: 2})
	l, _ := setup(t, p, "")
	for i := range 10 {
		if err := q.Enqueue(context.Background(), i); err != nil {
			t.Fatal(err)
		}
	}
	serveQueues(t, p)
	settled(t, q)

	mu.Lock()
	defer mu.Unlock()
	for i := range 10 {
		if seen[i] != 1 {
			t.Fatalf("item %d handled %d times, want 1 (all: %v)", i, seen[i], seen)
		}
	}
	if n := l.count(slog.LevelDebug, "jobs: ran", "work"); n != 10 {
		t.Fatalf("%d runs logged, want 10", n)
	}
	if n := l.count(slog.LevelError, "jobs: failed", "work"); n != 0 {
		t.Fatalf("%d failures logged, want 0", n)
	}
}

func TestQueue_Full(t *testing.T) {
	p := jobs.New()
	var r recorder
	q := jobs.NewQueue(p, "small", r.fn, jobs.QueueOptions[int]{Capacity: 2})
	for i := range 2 {
		if err := q.Enqueue(context.Background(), i); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}
	if err := q.Enqueue(context.Background(), 2); !errors.Is(err, jobs.ErrQueueFull) {
		t.Fatalf("third Enqueue: %v, want ErrQueueFull", err)
	}
	if n := jobs.QueueLen(q); n != 2 {
		t.Fatalf("%d queued, want 2", n)
	}
	if jobs.ErrQueueFull.Error() != "jobs: queue full" {
		t.Fatalf("ErrQueueFull = %v", jobs.ErrQueueFull)
	}
}

func TestQueue_DefaultCapacity(t *testing.T) {
	p := jobs.New()
	var r recorder
	q := jobs.NewQueue(p, "big", r.fn, jobs.QueueOptions[int]{})
	for i := range 1000 {
		if err := q.Enqueue(context.Background(), i); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}
	if err := q.Enqueue(context.Background(), 1000); !errors.Is(err, jobs.ErrQueueFull) {
		t.Fatalf("Enqueue 1001: %v, want ErrQueueFull", err)
	}
}

// Review Focus 3: Enqueue after the drain has begun returns ErrDraining at
// once, never blocks, and never hands the item to the handler.
func TestQueue_EnqueueAfterDrain(t *testing.T) {
	p := jobs.New()
	entered := make(chan int, 4)
	release := make(chan struct{})
	var calls atomic.Int32
	q := jobs.NewQueue(p, "mail", func(_ context.Context, item int) error {
		calls.Add(1)
		entered <- item
		if item == 1 {
			<-release
		}
		return nil
	}, jobs.QueueOptions[int]{Capacity: 1})
	setup(t, p, "")
	cancel := serveQueues(t, p)
	released := false
	t.Cleanup(func() {
		if !released {
			close(release)
		}
	})

	if err := q.Enqueue(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if got := <-entered; got != 1 {
		t.Fatalf("handled %d first, want 1", got)
	}
	// The worker is held on item 1, and item 2 fills the store: a blocking
	// send would now block, and a Push before the drain check would return
	// ErrQueueFull.
	if err := q.Enqueue(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	cancel()
	res := make(chan error, 1)
	go func() { res <- q.Enqueue(context.Background(), 3) }()
	select {
	case err := <-res:
		if !errors.Is(err, jobs.ErrDraining) {
			t.Fatalf("Enqueue after the drain: %v, want ErrDraining", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Enqueue after the drain blocked")
	}

	close(release)
	released = true
	settled(t, q)
	if got := <-entered; got != 2 {
		t.Fatalf("handled %d second, want 2", got)
	}
	if n := len(entered); n != 0 || calls.Load() != 2 || jobs.QueueLen(q) != 0 {
		t.Fatalf("%d more handled, %d calls, %d queued: item 3 got through", n, calls.Load(), jobs.QueueLen(q))
	}
}

func TestQueue_RetryWithBackoff(t *testing.T) {
	p := jobs.New()
	r := recorder{fail: 2}
	q := jobs.NewQueue(p, "flaky", r.fn, jobs.QueueOptions[int]{MaxAttempts: 3})
	l, clk := setup(t, p, "")
	serveQueues(t, p)
	if err := q.Enqueue(context.Background(), 7); err != nil {
		t.Fatal(err)
	}

	waitPending(t, clk, 1) // the first backoff
	expectItems(t, &r, 7)
	if due := clk.NextDue(); !due.Equal(t0.Add(time.Second)) {
		t.Fatalf("first retry due at %v, want %v", due, t0.Add(time.Second))
	}
	clk.Advance(time.Second - time.Millisecond)
	expectItems(t, &r, 7)
	clk.Advance(time.Millisecond)
	// Advance returns once the worker has taken the tick; the second attempt
	// fails and arms the second backoff.
	waitPending(t, clk, 1)
	expectItems(t, &r, 7, 7)
	if due := clk.NextDue(); !due.Equal(t0.Add(3 * time.Second)) {
		t.Fatalf("second retry due at %v, want %v", due, t0.Add(3*time.Second))
	}
	clk.Advance(2*time.Second - time.Millisecond)
	expectItems(t, &r, 7, 7)
	clk.Advance(time.Millisecond)
	settled(t, q)
	expectItems(t, &r, 7, 7, 7)
	if n := clk.Pending(); n != 0 {
		t.Fatalf("%d timers pending after the success, want 0", n)
	}
	if n := l.count(slog.LevelWarn, "jobs: failed, will retry", "flaky"); n != 2 {
		t.Fatalf("%d retries logged, want 2", n)
	}
	if n := l.count(slog.LevelError, "jobs: failed", "flaky"); n != 0 {
		t.Fatalf("%d failures logged, want 0", n)
	}
	if n := l.count(slog.LevelDebug, "jobs: ran", "flaky"); n != 1 {
		t.Fatalf("%d runs logged, want 1", n)
	}
}

func TestQueue_OnFailure(t *testing.T) {
	p := jobs.New()
	r := recorder{fail: 100}
	type failure struct {
		item int
		err  error
	}
	failed := make(chan failure, 4)
	q := jobs.NewQueue(p, "doomed", r.fn, jobs.QueueOptions[int]{
		MaxAttempts: 2,
		OnFailure: func(_ context.Context, item int, err error) {
			failed <- failure{item, err}
		},
	})
	l, clk := setup(t, p, "")
	serveQueues(t, p)
	if err := q.Enqueue(context.Background(), 9); err != nil {
		t.Fatal(err)
	}
	waitPending(t, clk, 1)
	clk.Advance(time.Second)
	settled(t, q)

	if n := len(failed); n != 1 {
		t.Fatalf("OnFailure called %d times, want 1", n)
	}
	f := <-failed
	if f.item != 9 || f.err == nil || f.err.Error() != "attempt 2 failed" {
		t.Fatalf("OnFailure(%d, %v), want (9, attempt 2 failed)", f.item, f.err)
	}
	expectItems(t, &r, 9, 9)
	if n := l.count(slog.LevelError, "jobs: failed", "doomed"); n != 1 {
		t.Fatalf("%d failures logged, want 1", n)
	}
	if n := l.count(slog.LevelWarn, "jobs: failed, will retry", "doomed"); n != 1 {
		t.Fatalf("%d retries logged, want 1", n)
	}
	if n := clk.Pending(); n != 0 {
		t.Fatalf("%d timers pending, want 0", n)
	}
}

func TestQueue_OneAttemptByDefault(t *testing.T) {
	p := jobs.New()
	r := recorder{fail: 100}
	var onFailure atomic.Int32
	q := jobs.NewQueue(p, "once", r.fn, jobs.QueueOptions[int]{
		OnFailure: func(context.Context, int, error) { onFailure.Add(1) },
	})
	l, clk := setup(t, p, "")
	serveQueues(t, p)
	if err := q.Enqueue(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	settled(t, q)
	expectItems(t, &r, 1)
	if onFailure.Load() != 1 || clk.Pending() != 0 {
		t.Fatalf("OnFailure %d times, %d timers pending; want 1 and 0", onFailure.Load(), clk.Pending())
	}
	if n := l.count(slog.LevelError, "jobs: failed", "once"); n != 1 {
		t.Fatalf("%d failures logged, want 1", n)
	}
}

func TestQueue_PanicRetried(t *testing.T) {
	p := jobs.New()
	var calls atomic.Int32
	q := jobs.NewQueue(p, "panicky", func(context.Context, int) error {
		if calls.Add(1) == 1 {
			panic("kaboom")
		}
		return nil
	}, jobs.QueueOptions[int]{MaxAttempts: 2})
	l, clk := setup(t, p, "")
	serveQueues(t, p)
	if err := q.Enqueue(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	waitPending(t, clk, 1)
	if n := l.count(slog.LevelWarn, "jobs: failed, will retry", "panicky"); n != 1 {
		t.Fatalf("%d retries logged after the panic, want 1", n)
	}
	clk.Advance(time.Second)
	settled(t, q)
	if n := calls.Load(); n != 2 {
		t.Fatalf("%d calls, want 2", n)
	}
	if n := l.count(slog.LevelError, "jobs: failed", "panicky"); n != 0 {
		t.Fatalf("%d failures logged, want 0", n)
	}
}

func TestQueue_OnFailurePanicContained(t *testing.T) {
	p := jobs.New()
	r := recorder{fail: 100}
	q := jobs.NewQueue(p, "bad-hook", r.fn, jobs.QueueOptions[int]{
		OnFailure: func(context.Context, int, error) { panic("hook") },
	})
	l, _ := setup(t, p, "")
	serveQueues(t, p)
	for i := range 2 {
		if err := q.Enqueue(context.Background(), i); err != nil {
			t.Fatal(err)
		}
	}
	settled(t, q)
	expectItems(t, &r, 0, 1) // the worker survived the first panic
	if n := l.count(slog.LevelError, "jobs: OnFailure panicked", "bad-hook"); n != 2 {
		t.Fatalf("%d OnFailure panics logged, want 2", n)
	}
}

func TestQueue_BeforeServe(t *testing.T) {
	p := jobs.New()
	var r recorder
	q := jobs.NewQueue(p, "early", r.fn, jobs.QueueOptions[int]{})
	if err := q.Enqueue(context.Background(), 1); err != nil { // before Init
		t.Fatal(err)
	}
	setup(t, p, "")
	if err := q.Enqueue(context.Background(), 2); err != nil { // after Init
		t.Fatal(err)
	}
	// A worker started by mistake before serving would run on a goroutine
	// that may not have been scheduled yet; waiting on real time can only miss
	// that bug, never fail a correct plugin.
	time.Sleep(50 * time.Millisecond)
	expectItems(t, &r)
	if n := jobs.QueueLen(q); n != 2 {
		t.Fatalf("%d queued before serving, want 2", n)
	}

	serveQueues(t, p)
	settled(t, q)
	expectItems(t, &r, 1, 2)
}

// Workers start even when the serve ctx is already done, so that Shutdown can
// finish what was queued before.
func TestQueue_WorkersStartAfterEarlyDrain(t *testing.T) {
	p := jobs.New()
	var r recorder
	q := jobs.NewQueue(p, "late", r.fn, jobs.QueueOptions[int]{})
	setup(t, p, "")
	if err := q.Enqueue(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p.Start(ctx)
	t.Cleanup(func() {
		sctx, done := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer done()
		if err := p.Shutdown(sctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
		if !jobs.WaitIdle(p, 2*time.Second) {
			t.Error("goroutines left after Shutdown")
		}
	})
	settled(t, q)
	expectItems(t, &r, 1)
	if err := q.Enqueue(context.Background(), 2); !errors.Is(err, jobs.ErrDraining) {
		t.Fatalf("Enqueue: %v, want ErrDraining", err)
	}
}

func TestBackoff(t *testing.T) {
	cases := []struct {
		n    int
		want time.Duration
	}{
		{1, time.Second}, {2, 2 * time.Second}, {3, 4 * time.Second},
		{4, 8 * time.Second}, {5, 16 * time.Second}, {6, 32 * time.Second},
		{7, time.Minute}, {8, time.Minute}, {35, time.Minute}, {64, time.Minute},
		{65, time.Minute}, {100, time.Minute},
	}
	for _, c := range cases {
		if got := jobs.Backoff(c.n); got != c.want {
			t.Errorf("backoff(%d) = %v, want %v", c.n, got, c.want)
		}
	}
}

func TestQueue_Misuse(t *testing.T) {
	nop := func(context.Context) error { return nil }
	nopItem := func(context.Context, int) error { return nil }
	cases := []struct {
		name string
		want string // in the panic message
		do   func()
	}{
		{"job then queue", `"a"`, func() {
			p := jobs.New()
			p.Every("a", time.Minute, nop)
			jobs.NewQueue(p, "a", nopItem, jobs.QueueOptions[int]{})
		}},
		{"queue then job", `"a"`, func() {
			p := jobs.New()
			jobs.NewQueue(p, "a", nopItem, jobs.QueueOptions[int]{})
			p.Cron("a", "* * * * *", nop)
		}},
		{"queue then queue", `"a"`, func() {
			p := jobs.New()
			jobs.NewQueue(p, "a", nopItem, jobs.QueueOptions[int]{})
			jobs.NewQueue(p, "a", func(context.Context, string) error { return nil }, jobs.QueueOptions[string]{})
		}},
		{"empty name", "name", func() { jobs.NewQueue(jobs.New(), "", nopItem, jobs.QueueOptions[int]{}) }},
		{"nil fn", "nil", func() { jobs.NewQueue[int](jobs.New(), "a", nil, jobs.QueueOptions[int]{}) }},
		{"negative Workers", "Workers", func() { jobs.NewQueue(jobs.New(), "a", nopItem, jobs.QueueOptions[int]{Workers: -1}) }},
		{"negative Capacity", "Capacity", func() { jobs.NewQueue(jobs.New(), "a", nopItem, jobs.QueueOptions[int]{Capacity: -1}) }},
		{"negative MaxAttempts", "MaxAttempts", func() { jobs.NewQueue(jobs.New(), "a", nopItem, jobs.QueueOptions[int]{MaxAttempts: -1}) }},
		{"nil plugin", "plugin", func() { jobs.NewQueue(nil, "a", nopItem, jobs.QueueOptions[int]{}) }},
		{"after Init", "after Init", func() {
			p := jobs.New()
			if err := newApp(t, p, "", &logs{}); err != nil {
				t.Fatal(err)
			}
			jobs.NewQueue(p, "a", nopItem, jobs.QueueOptions[int]{})
		}},
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
