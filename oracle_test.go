package jobs

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"
)

// This file checks cronSpec.next against an independent oracle. The oracle
// knows nothing about wall-clock walking: it steps real instants a minute at a
// time and applies the spec's rules directly.
//
//   - A matching wall time that exists fires at its instant. If its hour is
//     fixed (the hour field does not start with "*") and the same wall time
//     already happened within the last three hours, it is a repeat after a
//     fall-back and is skipped. With a "*" hour, every occurrence fires.
//   - A matching wall time skipped by a gap fires at the first instant after
//     the gap.
//
// Both tests use fixed seeds and fixed counts, so they are deterministic and
// bounded.

// oracleZones have DST, half-hour DST (Lord Howe), midnight transitions
// (Beirut, Havana, Sao Paulo), and a zone that crossed the date line (Apia).
var oracleZones = []string{
	"America/New_York", "Europe/Istanbul", "Europe/London",
	"Australia/Lord_Howe", "America/Santiago", "Asia/Beirut",
	"America/Havana", "Pacific/Apia", "America/Sao_Paulo",
}

type oracleField struct{ min, max int }

var oracleFields = [5]oracleField{{0, 59}, {0, 23}, {1, 31}, {1, 12}, {0, 7}}

// oracleExpr is an expression with the set of values each field matches,
// worked out independently of parseCron.
type oracleExpr struct {
	text string
	sets [5][64]bool
	// domStar and dowStar: the day field starts with "*" (Vixie).
	domStar, dowStar, hourStar bool
}

func newOracleExpr(t *testing.T, text string) oracleExpr {
	t.Helper()
	o := oracleExpr{text: text}
	fields := strings.Fields(text)
	if len(fields) != 5 {
		t.Fatalf("oracle: %q has %d fields", text, len(fields))
	}
	for i, p := range fields {
		o.sets[i] = oracleSet(t, p, i)
	}
	o.hourStar = strings.HasPrefix(fields[1], "*")
	o.domStar = strings.HasPrefix(fields[2], "*")
	o.dowStar = strings.HasPrefix(fields[4], "*")
	return o
}

// oracleSet expands one field. Day of week 7 is Sunday; "*" there means 0-6.
func oracleSet(t *testing.T, field string, i int) [64]bool {
	t.Helper()
	atoi := func(s string) int {
		n, err := strconv.Atoi(s)
		if err != nil {
			t.Fatalf("oracle: field %q: %v", field, err)
		}
		return n
	}
	var set [64]bool
	for _, item := range strings.Split(field, ",") {
		rng, st, hasStep := strings.Cut(item, "/")
		lo, hi, step := oracleFields[i].min, oracleFields[i].max, 1
		if i == 4 && rng == "*" {
			hi = 6
		}
		if rng != "*" {
			if a, b, ok := strings.Cut(rng, "-"); ok {
				lo, hi = atoi(a), atoi(b)
			} else {
				lo = atoi(rng)
				hi = lo
			}
		}
		if hasStep {
			step = atoi(st)
		}
		for v := lo; v <= hi; v += step {
			if i == 4 && v == 7 {
				set[0] = true
			} else {
				set[v] = true
			}
		}
	}
	return set
}

func (o oracleExpr) matches(w time.Time) bool {
	if !o.sets[0][w.Minute()] || !o.sets[1][w.Hour()] || !o.sets[3][int(w.Month())] {
		return false
	}
	return o.dayMatches(w)
}

func (o oracleExpr) dayMatches(w time.Time) bool {
	dom := o.sets[2][w.Day()]
	dow := o.sets[4][int(w.Weekday())]
	if o.domStar || o.dowStar {
		return dom && dow
	}
	return dom || dow
}

// wallClock gives an instant's wall clock in loc, to the minute, held in UTC.
// It caches the zone offset for the current zone period, which keeps the
// minute-by-minute oracle fast under -race.
type wallClock struct {
	loc        *time.Location
	start, end time.Time
	off        int64
	valid      bool
}

func (c *wallClock) at(u time.Time) time.Time {
	if !c.valid || u.Before(c.start) || (!c.end.IsZero() && !u.Before(c.end)) {
		_, off := u.In(c.loc).Zone()
		c.off = int64(off)
		c.start, c.end = u.In(c.loc).ZoneBounds()
		c.valid = true
	}
	return time.Unix(u.Unix()+c.off, 0).UTC().Truncate(time.Minute)
}

type oracleStats struct{ compared, gapFires, repeatSkips, repeatFires int }

// oracleNext is the first trigger strictly after t, searching up to horizon
// ahead; it returns the zero time if there is none.
func oracleNext(o oracleExpr, t time.Time, loc *time.Location, horizon time.Duration, st *oracleStats) time.Time {
	limit := t.Add(horizon)
	clock := wallClock{loc: loc}
	u := t.Truncate(time.Minute).Add(time.Minute)
	prev := clock.at(u.Add(-time.Minute))
	for u.Before(limit) {
		wk := clock.at(u)
		if wk.Sub(prev) > time.Minute { // a gap just ended
			for s := prev.Add(time.Minute); s.Before(wk); s = s.Add(time.Minute) {
				if o.matches(s) {
					st.gapFires++
					return u
				}
			}
		}
		if o.matches(wk) {
			repeat := false
			for d := 1; d <= 180; d++ {
				if clock.at(u.Add(-time.Duration(d) * time.Minute)).Equal(wk) {
					repeat = true
					break
				}
			}
			switch {
			case !repeat:
				return u
			case o.hourStar:
				st.repeatFires++
				return u
			default:
				st.repeatSkips++
			}
		}
		// Advance a minute, or, when the whole wall day or hour cannot match
		// and no transition falls before its end, straight to its end: inside
		// one zone period the wall clock moves with real time.
		step := time.Minute
		switch {
		case !o.sets[3][int(wk.Month())] || !o.dayMatches(wk):
			step = time.Date(wk.Year(), wk.Month(), wk.Day()+1, 0, 0, 0, 0, time.UTC).Sub(wk)
		case !o.sets[1][wk.Hour()]:
			step = wk.Truncate(time.Hour).Add(time.Hour).Sub(wk)
		}
		clock.at(u) // the repeat check may have moved the cached period
		if step > time.Minute && !clock.end.IsZero() && !u.Add(step).Before(clock.end) {
			step = time.Minute
		}
		u, prev = u.Add(step), wk.Add(step-time.Minute)
	}
	return time.Time{}
}

// transitions lists the zone's offset changes from 2010 to 2028.
func transitions(loc *time.Location) []time.Time {
	var out []time.Time
	for u := time.Date(2010, 1, 1, 0, 0, 0, 0, loc); u.Year() < 2028; {
		_, end := u.ZoneBounds()
		if end.IsZero() {
			break
		}
		out = append(out, end)
		u = end
	}
	return out
}

// TestNext_OracleDST chains next() from random points around every zone's
// real transitions, with hour and minute fields chosen to hit gaps, repeats
// and both "*" and fixed hours.
func TestNext_OracleDST(t *testing.T) {
	r := rand.New(rand.NewSource(99))
	hours := []string{"*", "0", "1", "2", "3", "0-3", "1-2", "23", "*/2", "0,2"}
	mins := []string{"*", "0", "30", "*/15", "*/7", "59", "0-5", "45-59"}
	var st oracleStats
	for _, zn := range oracleZones {
		loc := mustLoad(t, zn)
		trans := transitions(loc)
		if len(trans) == 0 {
			t.Fatalf("%s: no transitions 2010-2028", zn)
		}
		for k := 0; k < 80; k++ {
			text := fmt.Sprintf("%s %s * * *", mins[r.Intn(len(mins))], hours[r.Intn(len(hours))])
			if r.Intn(3) == 0 {
				text = fmt.Sprintf("%s %s %d * %d", mins[r.Intn(len(mins))], hours[r.Intn(len(hours))], 1+r.Intn(31), r.Intn(7))
			}
			o := newOracleExpr(t, text)
			spec := mustParse(t, text)
			tr := trans[r.Intn(len(trans))]
			cur := tr.Add(time.Duration(r.Intn(8*60)-6*60) * time.Minute).
				Add(time.Duration(r.Intn(60)) * time.Second).In(loc)
			for step := 0; step < 12; step++ {
				st.compared++
				got := spec.next(cur)
				want := oracleNext(o, cur, loc, 40*24*time.Hour, &st)
				if !got.Equal(want) {
					t.Errorf("%s %q from %v: got %v, want %v", zn, text, cur, got, want.In(loc))
					break
				}
				if got.IsZero() {
					break
				}
				cur = got
			}
		}
	}
	t.Logf("DST oracle: %d compared, %d gap fires, %d repeats skipped (fixed hour), %d repeats fired (* hour)",
		st.compared, st.gapFires, st.repeatSkips, st.repeatFires)
	if st.gapFires == 0 || st.repeatSkips == 0 || st.repeatFires == 0 {
		t.Errorf("the oracle run did not cover gaps and both kinds of repeat: %+v", st)
	}
}

// genOracleField returns random field text using every syntax form.
func genOracleField(r *rand.Rand, i int) string {
	f := oracleFields[i]
	n := 1
	if r.Intn(4) == 0 {
		n = 2 + r.Intn(2)
	}
	items := make([]string, 0, n)
	for k := 0; k < n; k++ {
		switch r.Intn(6) {
		case 0:
			if n == 1 {
				items = append(items, "*")
				continue
			}
			fallthrough
		case 1:
			items = append(items, fmt.Sprintf("*/%d", 1+r.Intn(f.max-f.min+1)))
		case 2, 3:
			items = append(items, strconv.Itoa(f.min+r.Intn(f.max-f.min+1)))
		case 4:
			a := f.min + r.Intn(f.max-f.min+1)
			items = append(items, fmt.Sprintf("%d-%d", a, a+r.Intn(f.max-a+1)))
		case 5:
			a := f.min + r.Intn(f.max-f.min+1)
			items = append(items, fmt.Sprintf("%d-%d/%d", a, a+r.Intn(f.max-a+1), 1+r.Intn(10)))
		}
	}
	return strings.Join(items, ",")
}

// TestNext_OracleRandom compares random expressions over all five fields,
// from random starts in 2026-2027 and near transitions.
func TestNext_OracleRandom(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	const horizon = 400 * 24 * time.Hour
	var st oracleStats
	for _, zn := range append([]string{"UTC"}, oracleZones...) {
		loc := mustLoad(t, zn)
		trans := transitions(loc)
		for k := 0; k < 40; k++ {
			var parts [5]string
			for i := range parts {
				parts[i] = genOracleField(r, i)
			}
			text := strings.Join(parts[:], " ")
			o := newOracleExpr(t, text)
			spec := mustParse(t, text)
			var cur time.Time
			if k%3 == 0 && len(trans) > 0 {
				tr := trans[r.Intn(len(trans))]
				cur = tr.Add(time.Duration(r.Intn(6*60)-3*60) * time.Minute).
					Add(time.Duration(r.Intn(60)) * time.Second).In(loc)
			} else {
				cur = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).
					Add(time.Duration(r.Int63n(int64(2 * 365 * 24 * time.Hour)))).In(loc)
			}
			for step := 0; step < 5; step++ {
				st.compared++
				got := spec.next(cur)
				want := oracleNext(o, cur, loc, horizon, &st)
				if want.IsZero() {
					// Beyond the oracle's horizon; next may still find one.
					if !got.IsZero() && got.Sub(cur) <= horizon {
						t.Errorf("%s %q from %v: got %v, oracle found none within %v", zn, text, cur, got, horizon)
					}
					break
				}
				if !got.Equal(want) {
					t.Errorf("%s %q from %v: got %v, want %v", zn, text, cur, got, want.In(loc))
					break
				}
				cur = got
			}
		}
	}
	t.Logf("random oracle: %d compared, %d gap fires, %d repeats skipped, %d repeats fired",
		st.compared, st.gapFires, st.repeatSkips, st.repeatFires)
}
