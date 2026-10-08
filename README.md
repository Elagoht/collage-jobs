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
  naming the field, and so does one that can never match, such as
  `0 0 30 2 *` (February 30). `0 0 29 2 *` is accepted: it runs in leap
  years.
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
- **A schedule with no time in the next five years** is logged once at Warn,
  and the job stops. Only a valid but very rare schedule gets there, such as
  `0 0 29 2 */7` (February 29 on a Sunday: 2032, then 2060). See
  [Known limits](#known-limits).

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

// In an action:
if err := mail.Enqueue(r.Context(), Email{To: to}); err != nil {
	// jobs.ErrQueueFull, or jobs.ErrDraining once Shutdown has begun:
	// tell the user to try again
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
  `Capacity` items, and `ErrDraining` once the plugin's `Shutdown` has begun.
  The drain refuses nothing: the server still serves then, so an item
  enqueued during the drain is accepted, and `Shutdown` finishes it or logs it
  as dropped. Under `ListenAndServe` a request that finishes within
  `ShutdownTimeout` never sees `ErrDraining`, since the plugin's `Shutdown`
  runs only after the server has stopped; one still running when that timeout
  ends can. While the
  application serves, an item is either queued or refused with an error,
  never silently dropped. A build or a command is different: see
  [Builds, captures and commands](#builds-captures-and-commands).
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
- **Storage.** Items live in memory. They are lost if the process ends.

Queue misuse panics: an empty name, a name taken by a job or another queue, a
nil function, a negative option, or a queue defined after `Init`.

## Configuration

| JSON | Default | |
| --- | --- | --- |
| `timezone` | `"UTC"` | IANA time zone cron expressions are read in. An invalid zone fails startup |
| `disabled` | `[]` | Names of scheduled jobs that do not run. An unknown name fails startup |

From Go, `jobs.NewWith(jobs.Config{Timezone: "Europe/Istanbul"})` sets the
defaults that configuration starts from.

The time zone is read from the system's zoneinfo. A minimal image without it,
such as `FROM scratch`, fails startup with `unknown time zone`, the same error
a misspelt name gives. Embed the zone data in the binary, about 450 KB, with
one import in `main`:

```go
import _ "time/tzdata"
```

The scaffold's distroless image ships zoneinfo, so it needs no import.

## Builds, captures and commands

A static build (`collage export`), the header capture that follows it, and a
plugin command (`go run . <command>`) start the application but never serve
it, so the plugin never starts its workers. Data handlers still run there: once
per path in the build, and twice more per path in the capture
(`collage.IsCapture(ctx)` is true then). **Do not enqueue from them.** An item
enqueued in a build or a command is accepted and never handled. A project
scaffolded by collage v0.55.0 or later shuts the application down at the end
of a build or a command, so such items are at least logged as
`elagoht/jobs: dropped queued items`; an older project's `main.go` needs the
same `app.Shutdown` call added to log them. Enqueue from actions, which a build
never runs, or skip the enqueue when `collage.IsCapture(ctx)` is true. With
more than `Capacity` paths a build also fills the queue, and `Enqueue` returns
`ErrQueueFull`.

## Logging

Every log line and every error the plugin returns or panics with starts with
`elagoht/jobs:`, so one grep finds them all. Runs and handled items are logged
at Debug, retries and drops at Warn, and final failures at Error, with the
job's or queue's name as `job` or `queue`.

## Lifecycle

With `app.ListenAndServe()`, the plugin needs nothing more:

1. **Serving.** Once the port is bound, collage calls the plugin's `OnServe`.
   The scheduled jobs start (and `RunOnStart` jobs run), and the queues'
   workers start on whatever was enqueued before.
2. **The drain.** On `SIGINT`, `SIGTERM` or `app.Shutdown(ctx)`, the drain
   begins, and the ctx `OnServe` got ends. From then on no job is triggered.
   `Enqueue` still accepts items, because the server keeps serving for
   `Server.DrainDelay` on purpose, and requests in flight finish after that.
   Runs in progress go on, and the workers keep handling what is queued.
3. **Shutdown.** After the server has stopped, collage calls the plugin's
   `Shutdown(ctx)`, with `Server.ShutdownTimeout` on a signal, or your ctx when
   you call `app.Shutdown(ctx)`. From its start `Enqueue` returns
   `ErrDraining`.
   - It waits for runs in progress, and the workers keep taking queued items
     until every queue is empty. Then it returns, without waiting for ctx to
     end.
   - If ctx ends first, the runs' ctx is cancelled, and `Shutdown` waits one
     more second for them to return. A job that ignores its ctx is left
     running: `Shutdown` logs at Warn that it gave up, and returns. Its
     goroutine lives on until the function returns, but the plugin keeps none
     of its own waiting for it.
   - Each queue with items left logs at Warn `elagoht/jobs: dropped queued items`,
     with the queue's name and the count. An item waiting for a retry when
     the runs' ctx ends counts as dropped too; it is not a failure, so
     `OnFailure` is not called. After a give-up, `abandoned` counts the items
     whose runs were still going: they may yet finish, but nothing waits for
     them.

On a signal, `ShutdownTimeout` starts when the drain ends, so it has to cover
the requests in flight and the queues. A ctx you pass to `app.Shutdown(ctx)`
covers the drain too, so give it at least `DrainDelay` plus that. When a job
ignores its ctx, `Shutdown` waits up to one second past its ctx before it gives
up, so a stop can take `DrainDelay + ShutdownTimeout + 1s`: whatever stops the
process (systemd's `TimeoutStopSec`, Kubernetes'
`terminationGracePeriodSeconds`) must wait longer than that. See collage's
[deployment guide](https://github.com/Elagoht/collage/blob/main/docs/deployment.md)
for the stop sequence.

### Registration order and the shared deadline

collage shuts plugins down in reverse registration order, all on one ctx, one
deadline. Two things follow:

- **Register jobs after the plugins its work uses.** Then it stops first,
  while they still work. Registered before, say, elagoht/cdnpurge, a queue
  that purges would finish its items against a plugin that has already
  stopped and refuses them.
- **Leave time for the plugins after it.** With busy queues, or a job that
  ignores its ctx, jobs' `Shutdown` can use the whole ctx plus its one-second
  give-up, and every plugin shut down after it then gets a ctx that is already
  done. elagoht/errortrack, for one, drops its unsent events then. Size
  `ShutdownTimeout`, or your `app.Shutdown` ctx, for the queued work plus what
  those plugins need, or keep the queues short. A job can also cap its own
  work with a shorter ctx, through `Timeout(d)` or `context.WithTimeout`
  inside the function.

### In development

`collage dev` restarts the program on every rebuild, and each restart is a full
stop and start. `RunOnStart` jobs run again on every save, and queued work is
finished or dropped each time, which can delay the restart by up to
`ShutdownTimeout`. A run that ignores its ctx is killed: `collage dev` waits
10 seconds for the old process, less than the 10-second `ShutdownTimeout` plus
the one-second give-up, so the `gave up` and `dropped` Warns are never printed.
Keep development runs short, or put such jobs in `disabled` in a development
configuration.

**Queued work is in memory.** Items still queued when the process ends, or
when `Shutdown`'s ctx ends, are lost. The dropped count is logged so that
the loss is visible, but the items are not saved anywhere. Work that must
survive a restart belongs in your database, with a job that picks it up.

`Shutdown` closes the plugin for good: after it, `Enqueue` returns
`ErrDraining`, and `Start` or `OnServe` starts nothing. A `Shutdown` before the
plugin started returns at once, logging the items that were queued and never
handled.

### Serving with your own server

collage calls `OnServe` only from `ListenAndServe`. An application that runs
its own `http.Server` over `app.Handler()` starts the plugin with `Start(ctx)`
once the server is listening, cancels that ctx when its server starts to drain, and calls `app.Shutdown` after
the server has stopped, so that the plugin's `Shutdown` finishes the queues.
The plugin's `Init` must have run first, which `app.Handler()` or `app.Start()`
does; `Start` panics otherwise.

```go
if err := app.Start(); err != nil { // runs Init
	return err
}
ln, err := net.Listen("tcp", ":8080")
if err != nil {
	return err
}
srv := &http.Server{Handler: app.Handler()}
go srv.Serve(ln)
ctx, stopJobs := context.WithCancel(context.Background())
j.Start(ctx) // the port is bound: start the jobs

// On shutdown:
stopJobs()                // the drain: no more triggers; Enqueue still accepts
srv.Shutdown(shutdownCtx) // requests in flight finish
app.Shutdown(shutdownCtx) // the plugin finishes its queues; Enqueue refused from here
```

Only the first of `OnServe` and `Start` does anything.

## Known limits

- **A cron wait is one long timer.** A `0 3 * * *` job waits for up to a day
  on the monotonic clock. If the wall clock steps in the meantime (an NTP
  correction after boot, a VM resumed, a laptop woken in development), that
  trigger fires off its wall time; the one after it is on time again. A job
  never runs twice for one trigger.
- **Five years ahead.** The next time of a cron schedule is searched five
  years ahead. A valid schedule with no time in that window, such as February
  29 on a Sunday, is logged at Warn when the plugin starts, and the job stops.

## Out of scope

- A persistent queue. Items are kept in memory only.
- Leader election or locks across instances: every instance runs its own
  scheduled jobs.
- Triggering a job by hand, from the CLI or over HTTP.
- Metrics. Runs and failures are logged.
- Priorities.
- Delayed enqueue (`EnqueueAt`).
