package jobs

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// schedule says when a job triggers next.
type schedule interface {
	// next returns the first trigger strictly after t, in t's location. It
	// returns the zero time when nothing matches within five years: February
	// 30, or "0 0 29 2 *" searched from 2096, since 2100 is not a leap year.
	next(t time.Time) time.Time
}

// every triggers at a fixed interval.
type every struct{ d time.Duration }

func (e every) next(t time.Time) time.Time { return t.Add(e.d) }

// cronSpec is a parsed five-field cron expression. Each field is a bitset:
// bit n is set when value n matches. Day of week uses bits 0-6, 0 = Sunday.
type cronSpec struct {
	minute, hour, dom, month, dow uint64
	// domStar and dowStar are true when the field starts with "*" ("*" or
	// "*/n"), as in Vixie cron. They decide how the two day fields combine;
	// see dayMatches.
	domStar, dowStar bool
	// hourStar is true when the hour field starts with "*". Such a schedule
	// fires through both copies of a repeated hour; see next.
	hourStar bool
}

var (
	_ schedule = every{}
	_ schedule = cronSpec{}
)

// cronField describes one of the five fields.
type cronField struct {
	name     string
	min, max int
}

var cronFields = [5]cronField{
	{"minute", 0, 59},
	{"hour", 0, 23},
	{"day of month", 1, 31},
	{"month", 1, 12},
	{"day of week", 0, 7}, // 7 is folded into 0 (Sunday)
}

// parseCron parses a standard five-field expression: minute, hour, day of
// month, month and day of week. Each field accepts "*", "a", "a-b", "*/n",
// "a-b/n" and comma lists of those. Names, macros such as "@hourly" and
// six-field expressions are rejected. An error names the field.
func parseCron(expr string) (cronSpec, error) {
	parts := strings.Fields(expr)
	if len(parts) == 1 && strings.HasPrefix(parts[0], "@") {
		return cronSpec{}, fmt.Errorf("cron %q: macros such as @hourly are not supported", expr)
	}
	if len(parts) != 5 {
		return cronSpec{}, fmt.Errorf(
			"cron %q: want 5 fields (minute hour day-of-month month day-of-week), got %d",
			expr, len(parts))
	}
	var bits [5]uint64
	for i, part := range parts {
		b, err := parseField(part, cronFields[i])
		if err != nil {
			return cronSpec{}, fmt.Errorf("cron %q: %s: %w", expr, cronFields[i].name, err)
		}
		bits[i] = b
	}
	// Day of week 7 is Sunday, the same as 0.
	if bits[4]&(1<<7) != 0 {
		bits[4] = bits[4]&^(1<<7) | 1
	}
	return cronSpec{
		minute: bits[0], hour: bits[1], dom: bits[2], month: bits[3], dow: bits[4],
		domStar:  strings.HasPrefix(parts[2], "*"),
		dowStar:  strings.HasPrefix(parts[4], "*"),
		hourStar: strings.HasPrefix(parts[1], "*"),
	}, nil
}

// parseField parses one field into a bitset of the values it matches.
func parseField(field string, f cronField) (uint64, error) {
	var bits uint64
	for _, item := range strings.Split(field, ",") {
		if item == "" {
			return 0, errors.New("empty list item")
		}
		rng, stepStr, hasStep := strings.Cut(item, "/")
		lo, hi := f.min, f.max
		if f.name == "day of week" && rng == "*" {
			hi = 6 // "*" and "*/n" walk 0-6, so Sunday is not counted twice
		}
		switch {
		case rng == "*":
		case strings.Contains(rng, "-"):
			a, b, _ := strings.Cut(rng, "-")
			if a == "" || b == "" {
				return 0, fmt.Errorf("%q is not a value or a range", rng)
			}
			var err error
			if lo, err = parseValue(a, f); err != nil {
				return 0, err
			}
			if hi, err = parseValue(b, f); err != nil {
				return 0, err
			}
			if lo > hi {
				return 0, fmt.Errorf("range %q runs backwards", rng)
			}
		default:
			if hasStep {
				return 0, fmt.Errorf("step %q needs \"*\" or a range before the slash", item)
			}
			v, err := parseValue(rng, f)
			if err != nil {
				return 0, err
			}
			lo, hi = v, v
		}
		step := 1
		if hasStep {
			n, err := parseNumber(stepStr)
			if err != nil {
				return 0, fmt.Errorf("step: %w", err)
			}
			// A step larger than the field's span is allowed, as in Vixie
			// cron ("4-7/10" is just 4); only zero and absurd sizes fail.
			if n < 1 || n > maxStep {
				return 0, fmt.Errorf("step %s is out of range 1-%d", stepStr, maxStep)
			}
			step = n
		}
		for v := lo; v <= hi; v += step {
			bits |= 1 << uint(v)
		}
	}
	return bits, nil
}

// parseValue parses a single in-range number for the field.
func parseValue(s string, f cronField) (int, error) {
	v, err := parseNumber(s)
	if err != nil {
		return 0, err
	}
	if v < f.min || v > f.max {
		return 0, fmt.Errorf("%s is out of range %d-%d", s, f.min, f.max)
	}
	return v, nil
}

// maxStep is the largest step accepted.
const maxStep = 9999

// tooBig stands in for any number with more digits than a field can use, so
// that it is reported as out of range rather than as an overflow.
const tooBig = 1 << 20

// parseNumber accepts plain decimal digits only: no sign, no names. Leading
// zeros are allowed ("05" is 5).
func parseNumber(s string) (int, error) {
	if s == "" {
		return 0, errors.New("missing number")
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("%q is not a number", s)
		}
	}
	if digits := strings.TrimLeft(s, "0"); len(digits) > 4 {
		return tooBig, nil
	}
	return strconv.Atoi(s)
}

// dayMatches applies the Vixie cron rule. When both day of month and day of
// week are restricted, a day matches if either one does. When either starts
// with "*" ("*" or "*/n"), both must match.
func (c cronSpec) dayMatches(wall time.Time) bool {
	dom := c.dom&(1<<uint(wall.Day())) != 0
	dow := c.dow&(1<<uint(wall.Weekday())) != 0
	if c.domStar || c.dowStar {
		return dom && dow
	}
	return dom || dow
}

// monthDays is the most days each month can have, February's in a leap year.
var monthDays = [13]int{0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}

// possible reports whether c ever matches, in any year. Minute, hour, month
// and day of week are never empty once parsed, so only the days can rule a
// schedule out. When both day fields are restricted, a day matches on either,
// and every month has every weekday. Otherwise both must match: over the
// 400-year Gregorian cycle every date, February 29 included, falls on every
// weekday, so c matches when one month it names has a day it names.
func (c cronSpec) possible() bool {
	if !c.domStar && !c.dowStar {
		return true
	}
	for m := 1; m <= 12; m++ {
		if c.month&(1<<uint(m)) == 0 {
			continue
		}
		for d := 1; d <= monthDays[m]; d++ {
			if c.dom&(1<<uint(d)) != 0 {
				return true
			}
		}
	}
	return false
}

// matchesWall reports whether a wall-clock time (held in UTC) matches all
// five fields.
func (c cronSpec) matchesWall(wall time.Time) bool {
	return c.month&(1<<uint(wall.Month())) != 0 &&
		c.dayMatches(wall) &&
		c.hour&(1<<uint(wall.Hour())) != 0 &&
		c.minute&(1<<uint(wall.Minute())) != 0
}

// next returns the first trigger strictly after t, in t's location, or the
// zero time if none comes within five years.
//
// The search walks wall-clock fields (month, day, hour, minute) on a plain
// calendar kept in UTC, which has no DST, and only then resolves a matching
// wall time to an instant in t's location. That gives DST its meaning:
//
//   - Gap: a wall time skipped by spring-forward (02:30 on a day that jumps
//     from 02:00 to 03:00) has no instant. Go's time.Date does not refuse it;
//     it normalises it, in practice backward (02:30 becomes 01:30 standard
//     time). The resolved wall clock then differs from the one asked for, and
//     the trigger moves to the transition itself, the first instant after the
//     gap (03:00). Every skipped wall time on that day lands on that same
//     instant, so a job runs there once.
//   - Overlap: a wall time repeated by fall-back occurs twice, and time.Date
//     does not promise which one it returns (in practice the earlier one west
//     of UTC and the later one in Europe/London). resolve always moves to the
//     first occurrence. The walk produces each wall time once, so the second
//     occurrence is never a candidate from it; when t is inside the repeated
//     hour, candidates whose first occurrence is not after t are skipped as
//     already run. That is the whole story for a fixed hour ("30 1 * * *"
//     runs once).
//   - Overlap with a "*" hour ("*/15 * * * *", "0 * * * *"): such a schedule
//     fires on every real occurrence, as Vixie cron does, or a 15-minute job
//     would leave a 75-minute hole. repeatNext finds the earliest match among
//     the second occurrences, and the earlier of the two results wins.
func (c cronSpec) next(t time.Time) time.Time {
	loc := t.Location()
	wall := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute()+1, 0, 0, time.UTC)
	limit := wall.AddDate(5, 0, 0)

	found := time.Time{}
	for wall.Before(limit) {
		if c.month&(1<<uint(wall.Month())) == 0 {
			wall = time.Date(wall.Year(), wall.Month()+1, 1, 0, 0, 0, 0, time.UTC)
			continue
		}
		if !c.dayMatches(wall) {
			wall = time.Date(wall.Year(), wall.Month(), wall.Day()+1, 0, 0, 0, 0, time.UTC)
			continue
		}
		if c.hour&(1<<uint(wall.Hour())) == 0 {
			wall = time.Date(wall.Year(), wall.Month(), wall.Day(), wall.Hour()+1, 0, 0, 0, time.UTC)
			continue
		}
		if c.minute&(1<<uint(wall.Minute())) == 0 {
			wall = wall.Add(time.Minute)
			continue
		}
		if at := resolve(wall, loc); at.After(t) {
			found = at
			break
		}
		// The wall time's instant is not after t: the first occurrence of a
		// repeated time, or a gap that maps back to the transition t is at
		// or past. It already ran; keep walking.
		wall = wall.Add(time.Minute)
	}
	if c.hourStar {
		if rep := c.repeatNext(t, found, limit); !rep.IsZero() && (found.IsZero() || rep.Before(found)) {
			return rep
		}
	}
	return found
}

// repeatNext returns the earliest second occurrence of a matching wall time
// that is after t and before until (or, with until zero, before the wall-clock
// limit), or the zero time. Second occurrences only exist in the window just
// after a fall-back transition, [transition, transition+shift), so it visits
// the transitions from the one that began t's zone period onward and scans
// each window a minute at a time; a window is at most a few hours.
func (c cronSpec) repeatNext(t, until, limit time.Time) time.Time {
	loc := t.Location()
	tr, _ := t.ZoneBounds()
	for !tr.IsZero() {
		if !until.IsZero() && !tr.Before(until) {
			return time.Time{}
		}
		if wallOf(tr, loc).After(limit) {
			return time.Time{}
		}
		_, after := tr.Zone()
		_, before := tr.Add(-time.Second).Zone()
		if shift := time.Duration(before-after) * time.Second; shift > 0 {
			for u := tr; u.Before(tr.Add(shift)); u = u.Add(time.Minute) {
				if u.After(t) && c.matchesWall(wallOf(u, loc)) {
					return u.In(loc)
				}
			}
		}
		_, tr = tr.ZoneBounds()
	}
	return time.Time{}
}

// wallOf returns u's wall clock in loc, to the minute, held in UTC.
func wallOf(u time.Time, loc *time.Location) time.Time {
	w := u.In(loc)
	return time.Date(w.Year(), w.Month(), w.Day(), w.Hour(), w.Minute(), 0, 0, time.UTC)
}

// resolve turns a wall-clock time (held in UTC) into an instant in loc. A
// wall time inside a DST gap resolves to the first instant after the gap; a
// wall time inside a fall-back overlap resolves to its first occurrence.
func resolve(wall time.Time, loc *time.Location) time.Time {
	at := time.Date(wall.Year(), wall.Month(), wall.Day(), wall.Hour(), wall.Minute(), 0, 0, loc)
	got := time.Date(at.Year(), at.Month(), at.Day(), at.Hour(), at.Minute(), 0, 0, time.UTC)
	if got.Equal(wall) {
		// In an overlap time.Date may return either occurrence. If the zone
		// period before at's had a larger offset, the same wall time may also
		// exist earlier, in that period: step back to it if so.
		if start, _ := at.ZoneBounds(); !start.IsZero() {
			_, off := at.Zone()
			_, prevOff := start.Add(-time.Second).Zone()
			if prevOff > off {
				if earlier := at.Add(-time.Duration(prevOff-off) * time.Second); earlier.Before(start) {
					return earlier
				}
			}
		}
		return at
	}
	// The wall time does not exist. at sits in the zone period on one side
	// of the transition; the transition is the boundary between them.
	start, end := at.ZoneBounds()
	if got.Before(wall) {
		return end
	}
	return start
}
