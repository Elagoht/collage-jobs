package jobs

import (
	"strings"
	"testing"
	"time"
)

func TestParseCron_Errors(t *testing.T) {
	cases := []struct {
		expr string
		want string // substring that names the field or the problem
	}{
		{"* * * *", "5 fields"},
		{"60 * * * *", "minute: "},
		{"* 24 * * *", "hour: "},
		{"* * 0 * *", "day of month: "},
		{"* * * 13 *", "month: "},
		{"* * * * 8", "day of week: "},
		{"*/0 * * * *", "minute: "},
		{"5-1 * * * *", "minute: "},
		{"MON * * * *", "minute: "},
		{"@hourly", "macros"},
		{"* * * * * *", "5 fields"},
		{"+5 * * * *", "minute: "},
		{"1,,2 * * * *", "minute: "},
		{"5/2 * * * *", "minute: "},
		{"* * * JAN *", "month: "},
		{"* * * * MON", "day of week: "},
	}
	for _, c := range cases {
		_, err := parseCron(c.expr)
		if err == nil {
			t.Errorf("parseCron(%q): want error, got nil", c.expr)
			continue
		}
		prefix := `cron "` + c.expr + `": `
		if !strings.HasPrefix(err.Error(), prefix) {
			t.Errorf("parseCron(%q) = %q, want prefix %q", c.expr, err, prefix)
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Errorf("parseCron(%q) = %q, want it to contain %q", c.expr, err, c.want)
		}
	}
}

func TestParseCron_Valid(t *testing.T) {
	for _, expr := range []string{
		"* * * * *",
		"0,15,30,45 0-23/2 1-31 1-12 0-7",
		"  5   4 * * 1-5 ",
		"*/7 * * * 5-7",
	} {
		if _, err := parseCron(expr); err != nil {
			t.Errorf("parseCron(%q): %v", expr, err)
		}
	}
}

func TestNext_UTC(t *testing.T) {
	from := time.Date(2026, 10, 8, 10, 17, 30, 0, time.UTC) // a Thursday
	utc := func(mo time.Month, d, h, mi int) time.Time {
		return time.Date(2026, mo, d, h, mi, 0, 0, time.UTC)
	}
	cases := []struct {
		expr string
		want time.Time
	}{
		{"* * * * *", utc(10, 8, 10, 18)},
		{"0 * * * *", utc(10, 8, 11, 0)},
		{"*/15 * * * *", utc(10, 8, 10, 30)},
		{"0 3 * * *", utc(10, 9, 3, 0)},
		{"0 0 1 * *", utc(11, 1, 0, 0)},
		{"0 0 * * 0", utc(10, 11, 0, 0)},   // Sunday
		{"0 0 13 * 5", utc(10, 9, 0, 0)},   // Vixie OR: Friday the 9th comes before the 13th
		{"0 0 31 2 *", time.Time{}},        // Feb 31 never happens
		{"30 2 * * 7", utc(10, 11, 2, 30)}, // 7 is Sunday
		// As in Vixie cron, a day field starting with "*" counts as
		// unrestricted, "*/10" included. So both day fields must match: a
		// Monday that is the 1st, 11th, 21st or 31st, first 2026-12-21.
		{"0 0 */10 * 1", utc(12, 21, 0, 0)},
		{"0 0 * * 5-7", utc(10, 9, 0, 0)}, // 5-7 is Fri, Sat, Sun
		{"0 12 * 1 *", time.Date(2027, 1, 1, 12, 0, 0, 0, time.UTC)},
		{"0 0 29 2 *", time.Date(2028, 2, 29, 0, 0, 0, 0, time.UTC)}, // next leap day
	}
	for _, c := range cases {
		spec, err := parseCron(c.expr)
		if err != nil {
			t.Fatalf("parseCron(%q): %v", c.expr, err)
		}
		if got := spec.next(from); !got.Equal(c.want) {
			t.Errorf("%q.next(%v) = %v, want %v", c.expr, from, got, c.want)
		}
	}
}

func TestNext_KeepsLocation(t *testing.T) {
	ist, err := time.LoadLocation("Europe/Istanbul")
	if err != nil {
		t.Fatal(err)
	}
	spec := mustParse(t, "0 3 * * *")
	got := spec.next(time.Date(2026, 10, 8, 10, 0, 0, 0, ist))
	want := time.Date(2026, 10, 9, 3, 0, 0, 0, ist)
	if !got.Equal(want) || got.Location() != ist {
		t.Errorf("next = %v (%v), want %v in Europe/Istanbul", got, got.Location(), want)
	}
}

func TestNext_DST(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("gap", func(t *testing.T) {
		// 2026-03-08 02:00 EST jumps to 03:00 EDT; 02:30 does not exist.
		spec := mustParse(t, "30 2 * * *")
		first := spec.next(time.Date(2026, 3, 8, 0, 0, 0, 0, ny))
		if want := time.Date(2026, 3, 8, 3, 0, 0, 0, ny); !first.Equal(want) {
			t.Fatalf("first = %v, want %v", first, want)
		}
		second := spec.next(first)
		if want := time.Date(2026, 3, 9, 2, 30, 0, 0, ny); !second.Equal(want) {
			t.Fatalf("second = %v, want %v", second, want)
		}
	})

	t.Run("gap, every minute", func(t *testing.T) {
		spec := mustParse(t, "* * * * *")
		got := spec.next(time.Date(2026, 3, 8, 1, 59, 0, 0, ny))
		if want := time.Date(2026, 3, 8, 3, 0, 0, 0, ny); !got.Equal(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
		if again := spec.next(got); !again.Equal(got.Add(time.Minute)) {
			t.Fatalf("after the gap got %v, want %v", again, got.Add(time.Minute))
		}
	})

	t.Run("overlap", func(t *testing.T) {
		// 2026-11-01 02:00 EDT falls back to 01:00 EST; 01:30 happens twice.
		spec := mustParse(t, "30 1 * * *")
		first := spec.next(time.Date(2026, 11, 1, 0, 0, 0, 0, ny))
		want := time.Date(2026, 11, 1, 5, 30, 0, 0, time.UTC) // 01:30 EDT
		if !first.Equal(want) {
			t.Fatalf("first = %v, want %v", first, want.In(ny))
		}
		if _, off := first.Zone(); off != -4*60*60 {
			t.Fatalf("first offset = %d, want -4h (EDT)", off)
		}
		second := spec.next(first)
		if want := time.Date(2026, 11, 2, 1, 30, 0, 0, ny); !second.Equal(want) {
			t.Fatalf("second = %v, want %v (not the 01:30 EST repeat)", second, want)
		}
	})

	t.Run("overlap, from inside the repeat", func(t *testing.T) {
		// 01:10 EST is in the repeated hour; 01:30 already ran at EDT.
		spec := mustParse(t, "30 1 * * *")
		from := time.Date(2026, 11, 1, 6, 10, 0, 0, time.UTC).In(ny) // 01:10 EST
		got := spec.next(from)
		if want := time.Date(2026, 11, 2, 1, 30, 0, 0, ny); !got.Equal(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})
}

func TestEvery_Next(t *testing.T) {
	from := time.Date(2026, 10, 8, 10, 17, 30, 0, time.UTC)
	if got := (every{15 * time.Minute}).next(from); !got.Equal(from.Add(15 * time.Minute)) {
		t.Errorf("every{15m}.next = %v, want %v", got, from.Add(15*time.Minute))
	}
}

func mustParse(t *testing.T, expr string) cronSpec {
	t.Helper()
	spec, err := parseCron(expr)
	if err != nil {
		t.Fatalf("parseCron(%q): %v", expr, err)
	}
	return spec
}

func mustLoad(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Fatal(err)
	}
	return loc
}

// assertSeq calls next repeatedly from `from` and compares each result, as an
// instant and as a UTC offset, with want.
func assertSeq(t *testing.T, expr string, from time.Time, want []time.Time) {
	t.Helper()
	spec := mustParse(t, expr)
	cur := from
	for i, w := range want {
		got := spec.next(cur)
		_, gotOff := got.Zone()
		_, wantOff := w.In(from.Location()).Zone()
		if !got.Equal(w) || gotOff != wantOff {
			t.Fatalf("%q step %d from %v: got %v, want %v", expr, i, cur, got, w.In(from.Location()))
		}
		cur = got
	}
}

// utcMinutes returns base plus each offset in minutes.
func utcMinutes(base time.Time, mins ...int) []time.Time {
	out := make([]time.Time, len(mins))
	for i, m := range mins {
		out[i] = base.Add(time.Duration(m) * time.Minute)
	}
	return out
}

func TestNext_OverlapEastOfUTC(t *testing.T) {
	// Europe/London falls back on 2026-10-25 at 02:00 BST (01:00 UTC) to
	// 01:00 GMT, so 01:xx happens at 00:xx UTC and again at 01:xx UTC. Go's
	// time.Date picks the second occurrence here; a fixed-hour job must still
	// run on the first.
	lon := mustLoad(t, "Europe/London")
	assertSeq(t, "30 1 * * *", time.Date(2026, 10, 25, 0, 0, 0, 0, lon), []time.Time{
		time.Date(2026, 10, 25, 0, 30, 0, 0, time.UTC), // 01:30 BST, +01:00
		time.Date(2026, 10, 26, 1, 30, 0, 0, time.UTC), // 01:30 GMT
	})
}

func TestNext_HourStarRunsThroughBothCopies(t *testing.T) {
	ny := mustLoad(t, "America/New_York")
	lon := mustLoad(t, "Europe/London")

	t.Run("New York */15", func(t *testing.T) {
		// 01:00 EDT is 05:00 UTC; 01:00 EST is 06:00 UTC; 02:00 EST is 07:00 UTC.
		from := time.Date(2026, 11, 1, 0, 50, 0, 0, ny)
		assertSeq(t, "*/15 * * * *", from, utcMinutes(time.Date(2026, 11, 1, 5, 0, 0, 0, time.UTC),
			0, 15, 30, 45, // EDT
			60, 75, 90, 105, // EST, the repeat
			120, // 02:00 EST
		))
	})

	t.Run("London */15", func(t *testing.T) {
		// 01:00 BST is 00:00 UTC; 01:00 GMT is 01:00 UTC; 02:00 GMT is 02:00 UTC.
		from := time.Date(2026, 10, 25, 0, 50, 0, 0, lon)
		assertSeq(t, "*/15 * * * *", from, utcMinutes(time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC),
			0, 15, 30, 45, 60, 75, 90, 105, 120))
	})

	t.Run("New York hourly", func(t *testing.T) {
		from := time.Date(2026, 11, 1, 0, 30, 0, 0, ny)
		assertSeq(t, "0 * * * *", from, utcMinutes(time.Date(2026, 11, 1, 5, 0, 0, 0, time.UTC),
			0,   // 01:00 EDT
			60,  // 01:00 EST
			120, // 02:00 EST
		))
	})

	t.Run("New York */15 from inside the repeat", func(t *testing.T) {
		from := time.Date(2026, 11, 1, 6, 20, 0, 0, time.UTC).In(ny) // 01:20 EST
		assertSeq(t, "*/15 * * * *", from, utcMinutes(time.Date(2026, 11, 1, 6, 30, 0, 0, time.UTC),
			0, 15, 30))
	})

	t.Run("spring-forward gap is unchanged", func(t *testing.T) {
		from := time.Date(2026, 3, 8, 1, 40, 0, 0, ny)
		// 01:45 EST, then 03:00 EDT once for the whole skipped hour, then 03:15.
		assertSeq(t, "*/15 * * * *", from, []time.Time{
			time.Date(2026, 3, 8, 6, 45, 0, 0, time.UTC),
			time.Date(2026, 3, 8, 7, 0, 0, 0, time.UTC),
			time.Date(2026, 3, 8, 7, 15, 0, 0, time.UTC),
		})
	})
}

func TestNext_ZeroWhenNothingWithinFiveYears(t *testing.T) {
	// 2100 is not a leap year, so after 2096 the next Feb 29 is in 2104.
	spec := mustParse(t, "0 0 29 2 *")
	if got := spec.next(time.Date(2096, 3, 1, 0, 0, 0, 0, time.UTC)); !got.IsZero() {
		t.Fatalf("got %v, want the zero time", got)
	}
}

func TestParseCron_Messages(t *testing.T) {
	cases := []struct {
		expr string
		want string
	}{
		{"99999 * * * *", `minute: 99999 is out of range 0-59`},
		{"*/10000 * * * *", `minute: step 10000 is out of range 1-9999`},
		{"*/0 * * * *", `minute: step 0 is out of range 1-9999`},
		{"99999999999999999999 * * * *", `minute: 99999999999999999999 is out of range 0-59`},
		{"-1 * * * *", `minute: "-1" is not a value or a range`},
		{"1- * * * *", `minute: "1-" is not a value or a range`},
		{"x * * * *", `minute: "x" is not a number`},
		{"* * * * 4-7/10", ""}, // a step past the span is allowed: just 4
	}
	for _, c := range cases {
		_, err := parseCron(c.expr)
		if c.want == "" {
			if err != nil {
				t.Errorf("parseCron(%q): %v", c.expr, err)
			}
			continue
		}
		if err == nil || !strings.HasSuffix(err.Error(), c.want) {
			t.Errorf("parseCron(%q) = %v, want it to end with %q", c.expr, err, c.want)
		}
	}
	// Leading zeros are plain decimal: 00005 is 5.
	spec := mustParse(t, "00005 * * * *")
	if spec.minute != 1<<5 {
		t.Errorf("00005: minute bits = %b, want only 5", spec.minute)
	}
}
