package jobs_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	jobs "github.com/Elagoht/collage-jobs"
)

// The messages Shutdown logs.
const (
	droppedMsg = "jobs: dropped queued items"
	gaveUpMsg  = "jobs: gave up waiting for runs that ignore their ctx"
)

// dropped returns the count logged by the dropped Warn for queue, and whether
// one was logged at all.
func (l *logs) dropped(queue string) (int64, bool) { return l.dropAttr(queue, "count") }

// abandoned returns the abandoned count logged by the dropped Warn for queue,
// and whether one was logged at all.
func (l *logs) abandoned(queue string) (int64, bool) { return l.dropAttr(queue, "abandoned") }

// dropAttr returns the integer attribute key of the dropped Warn for queue,
// and whether one was logged at all.
func (l *logs) dropAttr(queue, key string) (int64, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, r := range l.recs {
		if r.Level != slog.LevelWarn || r.Message != droppedMsg {
			continue
		}
		var name string
		var v int64
		r.Attrs(func(a slog.Attr) bool {
			switch a.Key {
			case "queue":
				name = a.Value.String()
			case key:
				v = a.Value.Int64()
			}
			return true
		})
		if name == queue {
			return v, true
		}
	}
	return 0, false
}

// shutdown runs p.Shutdown with a ctx of d and returns how long it took and
// whether its ctx was still live when it returned.
func shutdown(t *testing.T, p *jobs.Plugin, d time.Duration) (took time.Duration, live bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	began := time.Now()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	return time.Since(began), ctx.Err() == nil
}

// expectIdle fails the test if any loop, run or worker of p is left.
func expectIdle(t *testing.T, p *jobs.Plugin) {
	t.Helper()
	if !jobs.WaitIdle(p, 2*time.Second) {
		t.Fatal("goroutines left after Shutdown")
	}
}

// Shutdown lets the workers finish what is queued and returns as soon as the
// queue is empty, long before its ctx ends.
func TestShutdown_FinishesQueued(t *testing.T) {
	p := jobs.New()
	gate := make(chan struct{})
	var handled atomic.Int32
	q := jobs.NewQueue(p, "work", func(_ context.Context, item int) error {
		if item == 0 {
			<-gate // held until Shutdown has begun
		}
		time.Sleep(10 * time.Millisecond)
		handled.Add(1)
		return nil
	}, jobs.QueueOptions[int]{})
	l, _ := setup(t, p, "")
	for i := range 5 {
		if err := q.Enqueue(context.Background(), i); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	type result struct {
		took time.Duration
		live bool
	}
	res := make(chan result, 1)
	go func() {
		sctx, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		began := time.Now()
		if err := p.Shutdown(sctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
		res <- result{time.Since(began), sctx.Err() == nil}
	}()
	eventually(t, func() bool { return jobs.Draining(p) }, "Shutdown to begin")
	close(gate)
	r := <-res

	if n := handled.Load(); n != 5 {
		t.Fatalf("%d items handled, want 5", n)
	}
	if !r.live {
		t.Fatalf("Shutdown waited for its ctx to end (%v): the workers did not stop when the queue emptied", r.took)
	}
	if _, ok := l.dropped("work"); ok {
		t.Fatal("a drop was logged for a queue that was emptied")
	}
	expectIdle(t, p)
}

// Workers idle on an empty queue stop at once when Shutdown begins.
func TestShutdown_IdleWorkersStopAtOnce(t *testing.T) {
	p := jobs.New()
	jobs.NewQueue(p, "idle", func(context.Context, int) error { return nil }, jobs.QueueOptions[int]{Workers: 3})
	setup(t, p, "")
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)
	cancel()
	took, live := shutdown(t, p, 5*time.Second)
	if !live {
		t.Fatalf("Shutdown waited for its ctx to end (%v) with nothing queued", took)
	}
	expectIdle(t, p)
}

// Review Focus 4: with a full queue and a short ctx, Shutdown returns once its
// ctx ends and the cancelled runs return, cancels the runs' ctx, and logs how
// many items were dropped.
func TestShutdown_DropsAndLogs(t *testing.T) {
	p := jobs.New()
	entered := make(chan struct{}, 1)
	var sawCancel atomic.Bool
	q := jobs.NewQueue(p, "full", func(ctx context.Context, _ int) error {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-ctx.Done()
		sawCancel.Store(true)
		return ctx.Err()
	}, jobs.QueueOptions[int]{Capacity: 100})
	l, _ := setup(t, p, "")
	for i := range 50 {
		if err := q.Enqueue(context.Background(), i); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)
	<-entered
	cancel() // the drain

	took, _ := shutdown(t, p, 100*time.Millisecond)
	if took >= time.Second {
		t.Fatalf("Shutdown took %v, as long as the give-up: it did not return once the cancelled run did", took)
	}
	if !sawCancel.Load() {
		t.Fatal("the running handler's ctx was not cancelled")
	}
	n, ok := l.dropped("full")
	if !ok || n < 48 {
		t.Fatalf("dropped Warn logged %v with count %d, want a count of at least 48", ok, n)
	}
	if n := l.count(slog.LevelWarn, gaveUpMsg, ""); n != 0 {
		t.Fatalf("the give-up was logged %d times, but every run returned when cancelled", n)
	}
	expectIdle(t, p)
}

// A run that ignores its ctx cannot hang Shutdown: it gives up a second after
// cancelling, and says so. The stuck run's item is logged as abandoned, beside
// the queued one dropped behind it.
func TestShutdown_IgnoringJobDoesNotHang(t *testing.T) {
	p := jobs.New()
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	q := jobs.NewQueue(p, "stubborn", func(context.Context, int) error {
		entered <- struct{}{}
		<-release
		return nil
	}, jobs.QueueOptions[int]{})
	l, _ := setup(t, p, "")
	for i := range 2 {
		if err := q.Enqueue(context.Background(), i); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)
	<-entered
	cancel()
	defer func() {
		close(release)
		expectIdle(t, p)
	}()

	type result struct{ took time.Duration }
	res := make(chan result, 1)
	go func() {
		sctx, done := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer done()
		began := time.Now()
		if err := p.Shutdown(sctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
		res <- result{time.Since(began)}
	}()
	var took time.Duration
	select {
	case r := <-res:
		took = r.took
	case <-time.After(10 * time.Second):
		t.Fatal("Shutdown hung on a run that ignores its ctx")
	}
	if took > 3*time.Second {
		t.Fatalf("Shutdown took %v", took)
	}
	if took < time.Second {
		t.Fatalf("Shutdown returned after %v, before the second it gives a cancelled run", took)
	}
	if n := l.count(slog.LevelWarn, gaveUpMsg, ""); n != 1 {
		t.Fatalf("the give-up was logged %d times, want 1", n)
	}
	if n, ok := l.abandoned("stubborn"); !ok || n != 1 {
		t.Fatalf("dropped Warn logged %v with abandoned %d, want 1: the stuck run's item", ok, n)
	}
	if n, _ := l.dropped("stubborn"); n != 1 {
		t.Fatalf("dropped count %d, want 1: the item queued behind the stuck run", n)
	}
}

// Shutdown before the plugin starts returns at once, and closes it: a Start
// after it launches nothing, and Enqueue is refused.
func TestShutdown_BeforeStart(t *testing.T) {
	p := jobs.New()
	var r runs
	p.Every("tick", time.Millisecond, r.fn).RunOnStart()
	var handled atomic.Int32
	q := jobs.NewQueue(p, "early", func(context.Context, int) error { handled.Add(1); return nil }, jobs.QueueOptions[int]{})
	l, _ := setup(t, p, "")
	if err := q.Enqueue(context.Background(), 1); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- p.Shutdown(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Shutdown before Start did not return")
	}

	if n, ok := l.dropped("early"); !ok || n != 1 {
		t.Fatalf("dropped Warn logged %v with count %d, want 1: the item queued before Shutdown", ok, n)
	}

	p.Start(context.Background())
	if n := jobs.Active(p); n != 0 {
		t.Fatalf("%d goroutines started by Start after Shutdown, want 0", n)
	}
	// A job or worker started by mistake would run on a goroutine that may not
	// have been scheduled yet; waiting on real time can only miss that bug,
	// never fail a correct plugin.
	time.Sleep(50 * time.Millisecond)
	if n, h := r.n.Load(), handled.Load(); n != 0 || h != 0 {
		t.Fatalf("after Shutdown, Start ran the job %d times and handled %d items", n, h)
	}
	if err := q.Enqueue(context.Background(), 2); !errors.Is(err, jobs.ErrDraining) {
		t.Fatalf("Enqueue after Shutdown: %v, want ErrDraining", err)
	}
	expectIdle(t, p)
}

// Shutdown without a drain first, its serve ctx still live, stops the
// scheduler and refuses new items.
func TestShutdown_WithoutDrain(t *testing.T) {
	p := jobs.New()
	var r runs
	p.Every("tick", time.Minute, r.fn)
	q := jobs.NewQueue(p, "q", func(context.Context, int) error { return nil }, jobs.QueueOptions[int]{})
	_, clk := setup(t, p, "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)
	waitPending(t, clk, 1)

	if _, live := shutdown(t, p, 5*time.Second); !live {
		t.Fatal("Shutdown waited for its ctx to end")
	}
	if !jobs.Draining(p) {
		t.Fatal("not draining after Shutdown")
	}
	if err := q.Enqueue(context.Background(), 1); !errors.Is(err, jobs.ErrDraining) {
		t.Fatalf("Enqueue after Shutdown: %v, want ErrDraining", err)
	}
	expectIdle(t, p)
	clk.Advance(time.Hour)
	expectRuns(t, p, &r, 0)
}

// An item waiting for a retry when the runs' ctx ends is dropped: counted in
// the dropped Warn, neither a final failure nor handed to OnFailure.
func TestShutdown_BackoffDropped(t *testing.T) {
	p := jobs.New()
	var onFailure atomic.Int32
	r := recorder{fail: 100}
	q := jobs.NewQueue(p, "retrying", r.fn, jobs.QueueOptions[int]{
		MaxAttempts: 3,
		OnFailure:   func(context.Context, int, error) { onFailure.Add(1) },
	})
	l, clk := setup(t, p, "")
	if err := q.Enqueue(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)
	waitPending(t, clk, 1) // the first attempt failed; the retry waits
	cancel()

	shutdown(t, p, 20*time.Millisecond)
	expectIdle(t, p)
	if n := onFailure.Load(); n != 0 {
		t.Fatalf("OnFailure called %d times for an item dropped at shutdown", n)
	}
	if n := l.count(slog.LevelError, "jobs: failed", "retrying"); n != 0 {
		t.Fatalf("%d final failures logged for an item dropped at shutdown", n)
	}
	if n, ok := l.dropped("retrying"); !ok || n != 1 {
		t.Fatalf("dropped Warn logged %v with count %d, want 1", ok, n)
	}
	expectItems(t, &r, 1)
}

// A store's Pop with a ctx already done takes nothing, even with an item
// waiting.
func TestStore_PopCancelledTakesNothing(t *testing.T) {
	s := jobs.NewMemStore[int](1)
	if err := s.Push(1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for range 200 {
		if _, err := s.Pop(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Pop with a cancelled ctx: %v, want context.Canceled", err)
		}
	}
	if n := s.Len(); n != 1 {
		t.Fatalf("%d left, want 1: a cancelled Pop took the item", n)
	}
}

// Every Enqueue racing Shutdown either lands in time to be handled or counted
// as dropped, or is refused: no item is accepted and then lost unlogged.
func TestShutdown_EnqueueRace(t *testing.T) {
	for round := range 20 {
		p := jobs.New()
		var handled atomic.Int64
		q := jobs.NewQueue(p, "race", func(context.Context, int) error { handled.Add(1); return nil },
			jobs.QueueOptions[int]{Workers: 2, Capacity: 100000})
		l, _ := setup(t, p, "")
		ctx, cancel := context.WithCancel(context.Background())
		p.Start(ctx)

		var accepted atomic.Int64
		var wg sync.WaitGroup
		started := make(chan struct{})
		for range 8 {
			wg.Go(func() {
				<-started
				for {
					err := q.Enqueue(context.Background(), 1)
					if errors.Is(err, jobs.ErrDraining) {
						return
					}
					if err == nil {
						accepted.Add(1)
					}
				}
			})
		}
		close(started)
		time.Sleep(time.Millisecond)
		shutdown(t, p, 5*time.Second)
		wg.Wait()
		cancel()
		expectIdle(t, p)

		dropped, _ := l.dropped("race")
		if a, h := accepted.Load(), handled.Load(); a != h+dropped {
			t.Fatalf("round %d: %d accepted, %d handled, %d logged dropped: %d lost unlogged", round, a, h, dropped, a-h-dropped)
		}
	}
}

// heldStore holds the first Push until release closes.
type heldStore struct {
	jobs.Store[int]
	inPush, release chan struct{}
	once            atomic.Bool
}

func (s *heldStore) Push(item int) error {
	if s.once.CompareAndSwap(false, true) {
		close(s.inPush)
		<-s.release
	}
	return s.Store.Push(item)
}

// An Enqueue held inside its push holds Shutdown off: the item is handled or
// counted dropped, never accepted and lost. Deterministic where
// TestShutdown_EnqueueRace is a soak.
func TestShutdown_EnqueueHeldInPush(t *testing.T) {
	p := jobs.New()
	var handled atomic.Int64
	q := jobs.NewQueue(p, "held", func(context.Context, int) error { handled.Add(1); return nil }, jobs.QueueOptions[int]{})
	hs := &heldStore{inPush: make(chan struct{}), release: make(chan struct{})}
	jobs.WrapStore(q, func(s jobs.Store[int]) jobs.Store[int] { hs.Store = s; return hs })
	l, _ := setup(t, p, "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	enq := make(chan error, 1)
	go func() { enq <- q.Enqueue(context.Background(), 1) }()
	<-hs.inPush
	shut := make(chan struct{})
	go func() { _ = p.Shutdown(context.Background()); close(shut) }()
	// Correct code: Shutdown waits on the lock the push holds, so it cannot
	// close the plugin. If it does, let it finish before the push lands. The
	// bounded wait can only miss the bug, never fail a correct plugin.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) && !jobs.Closed(p) {
		time.Sleep(time.Millisecond)
	}
	if jobs.Closed(p) {
		<-shut
	}
	close(hs.release)
	err := <-enq
	<-shut
	expectIdle(t, p)
	dropped, _ := l.dropped("held")
	if err == nil && handled.Load()+dropped != 1 {
		t.Fatalf("Enqueue accepted the item, but %d handled and %d logged dropped", handled.Load(), dropped)
	}
}
