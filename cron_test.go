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
		// Only an exact "*" leaves a day field unrestricted. "*/10" restricts
		// it, so this is OR: the 11th (a Sunday) comes before Monday the 12th.
		{"0 0 */10 * 1", utc(10, 11, 0, 0)},
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
	spec, _ := parseCron("0 3 * * *")
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
		spec, _ := parseCron("30 2 * * *")
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
		spec, _ := parseCron("* * * * *")
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
		spec, _ := parseCron("30 1 * * *")
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
		spec, _ := parseCron("30 1 * * *")
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
