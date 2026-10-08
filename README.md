# elagoht/jobs

A collage plugin that runs scheduled jobs, on an interval or a cron expression,
and typed background queues, only while the application serves. Nothing runs
in `collage build`, in CLI commands, or in tests that only call
`app.Handler()` or `app.Start()`: jobs and queue workers start when
`ListenAndServe` has bound its port, and jobs stop being triggered when the
drain begins.

> **Every instance runs its own scheduled jobs.** With N replicas behind a load
> balancer, a job runs N times per trigger. There is no leader election. If a
> job must run once across the fleet, make it safe to run concurrently, or run
> it on one instance only. Replicas usually share one configuration, so decide
> that per instance from the environment rather than with `disabled` in
> `plugins-config.json`:
>
> ```go
> var off []string
> if os.Getenv("RUN_JOBS") != "1" {
> 	off = []string{"cleanup"}
> }
> j := jobs.NewWith(jobs.Config{Disabled: off})
> ```

```sh
go get github.com/Elagoht/collage-jobs
```

Requires collage v0.55.0 or later, for `ServeHook`.

```json
{
  "elagoht/jobs": {
    "timezone": "Europe/Istanbul",
    "disabled": ["cleanup"]
  }
}
```

```go
j := jobs.New()
j.Every("refresh-feed", 15*time.Minute, refreshFeed).RunOnStart()
j.Cron("cleanup", "0 3 * * *", cleanup).Timeout(10 * time.Minute)

app, err := collage.New(&collage.Config{
	Plugins: []collage.Plugin{j},
})
```

## Scheduled jobs

A job is a `func(ctx context.Context) error`. Define jobs before the
application starts; defining one after `Init` panics.

- `Every(name, d, fn)` runs `fn` every `d`, the first time `d` after the
  plugin starts.
- `Cron(name, expr, fn)` runs `fn` on a five-field cron expression: minute,
  hour, day of month, month, day of week. Each field takes `*`, a value, a
  range `a-b`, a step `*/n` or `a-b/n`, and comma lists of those. Day of week
  runs 0-7, where both 0 and 7 are Sunday. Names, `@hourly`-style macros and
  six-field expressions are not supported. A bad expression panics at `Cron`,
  naming the field.
- When day of month and day of week are both restricted, a day matches if
  either one does, as in Vixie cron. `0 0 1 * 1` runs on the 1st and on every
  Monday.

Each returns a handle:

| Method | |
| --- | --- |
| `RunOnStart()` | Also run as soon as the plugin starts, not only at the first trigger |
| `Timeout(d)` | Bound each run's ctx to `d`. The job must watch its ctx: the plugin cannot stop a function that ignores it |

How runs behave:

- **No overlap.** While a run is in progress, a trigger is skipped and logged
  at Debug. A slow job never piles up runs.
- **No retries.** A run that returns an error, or panics, is logged at Error.
  The next trigger runs it again as usual.
- **Logging.** A successful run is logged at Debug with its name and duration.
- **A schedule that never matches** (`0 0 30 2 *`, February 30) is logged once
  at Warn, and the job stops.

Name, function, interval and timeout misuse panics: an empty or taken name, a
nil function, a non-positive `Every` interval, or a negative timeout.

### Time zones and DST

Cron expressions are read in the configured `timezone`, UTC by default. Across
a DST change:

- a time inside the skipped hour runs once, at the first instant after the gap;
- a time inside the repeated hour runs once, on its first occurrence, for a
  schedule with a fixed hour (`30 1 * * *`);
- a schedule whose hour field starts with `*` (`*/15 * * * *`, `0 * * * *`)
  fires through both copies of the repeated hour, as Vixie cron does.

A `*/15 * * * *` job follows the wall clock, so its real-time spacing is not
15 minutes at every DST change. When exact real-time spacing matters, use
`Every(name, 15*time.Minute, fn)`. An interval is measured in elapsed time,
from the moment the previous trigger fired. Neither DST, the time zone nor a
wall clock set back or forward changes it.

## Background queues

A queue hands typed items to a function in the background. The payload type is
the queue's type parameter, so there is no `any` and no type assertion:

```go
type Email struct{ To, Subject string }

mail := jobs.NewQueue(j, "email", func(ctx context.Context, m Email) error {
	return send(ctx, m)
}, jobs.QueueOptions[Email]{Workers: 2, MaxAttempts: 3})

// In a handler:
if err := mail.Enqueue(r.Context(), Email{To: to}); err != nil {
	// jobs.ErrQueueFull or jobs.ErrDraining: tell the user to try again
}
```

| Option | Default | |
| --- | --- | --- |
| `Workers` | `1` | How many items are handled at the same time |
| `Capacity` | `1000` | How many items can wait. The store for them is allocated up front, when the queue is defined. `Enqueue` on a full queue returns `ErrQueueFull` |
| `MaxAttempts` | `1` | How many times an item is tried. `1` means no retry |
| `OnFailure` | none | `func(ctx, item, err)`, called once with the last error when every attempt has failed |

How queues behave:

- **Enqueue never blocks.** It returns `ErrQueueFull` when the queue holds
  `Capacity` items, and `ErrDraining` once the drain has begun. An item is
  either queued or refused with an error, never silently dropped.
- **Before serving**, items wait in the queue. The workers start in `OnServe`
  or `Start`, so nothing is handled in `collage build`, CLI commands or tests
  that never serve.
- **Retries.** An error or a panic is a failed attempt. A failed attempt with
  attempts left is logged at Warn, and the same worker retries the item after
  1s, 2s, 4s… capped at a minute. A worker keeps its items in order, and
  handles nothing else while it waits. The last failure is logged at Error and
  `OnFailure` is called; a panic in `OnFailure` is logged and contained.
- **Logging.** A handled item is logged at Debug with the queue's name, the
  attempt and the duration.
- **Storage.** Items live in memory, behind the `Store[T]` interface
  (`Push`, `Pop`, `Len`). They are lost if the process ends.

Queue misuse panics: an empty name, a name taken by a job or another queue, a
nil function, a negative option, or a queue defined after `Init`.

## Configuration

| JSON | Default | |
| --- | --- | --- |
| `timezone` | `"UTC"` | IANA time zone cron expressions are read in. An invalid zone fails startup |
| `disabled` | `[]` | Names of scheduled jobs that do not run. An unknown name fails startup |

From Go, `jobs.NewWith(jobs.Config{Timezone: "Europe/Istanbul"})` sets the
defaults that configuration starts from.

## Lifecycle

With `app.ListenAndServe()`, the plugin needs nothing more:

1. **Serving.** Once the port is bound, collage calls the plugin's `OnServe`.
   The scheduled jobs start (and `RunOnStart` jobs run), and the queues'
   workers start on whatever was enqueued before.
2. **The drain.** On `SIGINT`, `SIGTERM` or `app.Shutdown(ctx)`, the drain
   begins, and the ctx `OnServe` got ends. From then on no job is triggered,
   and `Enqueue` returns `ErrDraining`. Runs in progress go on, and the workers
   keep handling what is queued, while the server keeps serving for
   `Server.DrainDelay`.
3. **Shutdown.** After the server has stopped, collage calls the plugin's
   `Shutdown(ctx)`, with `Server.ShutdownTimeout` on a signal, or your ctx when
   you call `app.Shutdown(ctx)`:
   - It waits for runs in progress, and the workers keep taking queued items
     until every queue is empty. Then it returns, without waiting for ctx to
     end.
   - If ctx ends first, the runs' ctx is cancelled, and `Shutdown` waits one
     more second for them to return. A job that ignores its ctx is left
     running: `Shutdown` logs at Warn that it gave up, and returns. Its
     goroutine lives on until the function returns, but the plugin keeps none
     of its own waiting for it.
   - Each queue with items left logs at Warn `jobs: dropped queued items`,
     with the queue's name and the count. An item waiting for a retry when
     the runs' ctx ends counts as dropped too; it is not a failure, so
     `OnFailure` is not called.

So a stop can take `DrainDelay` plus the time the queues need; give
`ShutdownTimeout`, or your ctx, enough for both. See collage's
[deployment guide](https://github.com/Elagoht/collage/blob/main/docs/deployment.md)
for the stop sequence.

**Queued work is in memory.** Items still queued when the process ends, or
when `Shutdown`'s ctx ends, are lost. The dropped count is logged so that
the loss is visible, but the items are not saved anywhere. Work that must
survive a restart belongs in your database, with a job that picks it up.

`Shutdown` closes the plugin for good: after it, `Enqueue` returns
`ErrDraining`, even if no drain came first, and `Start` or `OnServe` starts
nothing. A `Shutdown` before the plugin started returns at once.

### Serving with your own server

collage calls `OnServe` only from `ListenAndServe`. An application that runs
its own `http.Server` over `app.Handler()` starts the plugin with `Start(ctx)`,
cancels that ctx when its server starts to drain, and calls `app.Shutdown` after
the server has stopped, so that the plugin's `Shutdown` finishes the queues.
The plugin's `Init` must have run first, which `app.Handler()` or `app.Start()`
does; `Start` panics otherwise.

```go
if err := app.Start(); err != nil { // runs Init
	return err
}
srv := &http.Server{Addr: ":8080", Handler: app.Handler()}
ctx, stopJobs := context.WithCancel(context.Background())
j.Start(ctx)
go srv.ListenAndServe()

// On shutdown:
stopJobs()                // the drain: no more triggers, Enqueue refused
srv.Shutdown(shutdownCtx) // requests in flight finish
app.Shutdown(shutdownCtx) // the plugin finishes its queues
```

Only the first of `OnServe` and `Start` does anything.

## Out of scope

- A persistent queue. `Store[T]` is an interface, but the in-memory store is
  the only implementation.
- Leader election or locks across instances: every instance runs its own
  scheduled jobs.
- Triggering a job by hand, from the CLI or over HTTP.
- Metrics. Runs and failures are logged.
- Priorities.
- Delayed enqueue (`EnqueueAt`).
