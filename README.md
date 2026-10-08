# elagoht/jobs

A collage plugin that runs scheduled jobs, on an interval or a cron expression,
only while the application serves. Nothing runs in `collage build`, in CLI
commands, or in tests that only call `app.Handler()` or `app.Start()`: jobs start when
`ListenAndServe` has bound its port, and stop being triggered when the drain
begins.

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

## Configuration

| JSON | Default | |
| --- | --- | --- |
| `timezone` | `"UTC"` | IANA time zone cron expressions are read in. An invalid zone fails startup |
| `disabled` | `[]` | Names of scheduled jobs that do not run. An unknown name fails startup |

From Go, `jobs.NewWith(jobs.Config{Timezone: "Europe/Istanbul"})` sets the
defaults that configuration starts from.

## Starting without ListenAndServe

collage calls the plugin's `OnServe` from `ListenAndServe`. An application that
runs its own `http.Server` over `app.Handler()` starts the jobs itself, and
cancels the ctx when its server starts to drain. The plugin's `Init` must have
run first, which `app.Handler()` or `app.Start()` does; `Start` panics
otherwise.

```go
if err := app.Start(); err != nil { // runs Init
	return err
}
srv := &http.Server{Addr: ":8080", Handler: app.Handler()}
ctx, stopJobs := context.WithCancel(context.Background())
j.Start(ctx)
go srv.ListenAndServe()
// ... on shutdown, before srv.Shutdown:
stopJobs()
```

Only the first of `OnServe` and `Start` does anything.
