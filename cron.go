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
	// next returns the first trigger strictly after t, in t's location.
	next(t time.Time) time.Time
}

// every triggers at a fixed interval.
type every struct{ d time.Duration }

func (e every) next(t time.Time) time.Time { return t.Add(e.d) }

// cronSpec is a parsed five-field cron expression. Each field is a bitset:
// bit n is set when value n matches. Day of week uses bits 0-6, 0 = Sunday.
type cronSpec struct {
	minute, hour, dom, month, dow uint64
	// domStar and dowStar are true when the field is exactly "*". They decide
	// how the two day fields combine; see dayMatches.
	domStar, dowStar bool
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
		domStar: parts[2] == "*",
		dowStar: parts[4] == "*",
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
				return 0, fmt.Errorf("step %q: %w", stepStr, err)
			}
			if n == 0 {
				return 0, errors.New("step must be at least 1")
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
		return 0, fmt.Errorf("%d is out of range %d-%d", v, f.min, f.max)
	}
	return v, nil
}

// parseNumber accepts plain decimal digits only: no sign, no names.
func parseNumber(s string) (int, error) {
	if s == "" || len(s) > 4 {
		return 0, fmt.Errorf("%q is not a number", s)
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("%q is not a number", s)
		}
	}
	return strconv.Atoi(s)
}

// dayMatches applies the Vixie cron rule. When both day of month and day of
// week are restricted, a day matches if either one does. When either is "*",
// both must match, which leaves the other one deciding.
func (c cronSpec) dayMatches(wall time.Time) bool {
	dom := c.dom&(1<<uint(wall.Day())) != 0
	dow := c.dow&(1<<uint(wall.Weekday())) != 0
	if c.domStar || c.dowStar {
		return dom && dow
	}
	return dom || dow
}

// next returns the first trigger strictly after t, in t's location, or the
// zero time if none comes within five years (such as February 30).
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
//   - Overlap: a wall time repeated by fall-back occurs twice. time.Date picks
//     the first occurrence (daylight time). Because the walk is in wall-clock
//     time, each wall time is produced once, so the second occurrence is never
//     a candidate. When t itself is inside the repeated hour, candidates whose
//     first occurrence is not after t are skipped as already run.
func (c cronSpec) next(t time.Time) time.Time {
	loc := t.Location()
	lt := t.In(loc)
	wall := time.Date(lt.Year(), lt.Month(), lt.Day(), lt.Hour(), lt.Minute()+1, 0, 0, time.UTC)
	limit := wall.AddDate(5, 0, 0)

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
			return at
		}
		// The wall time's instant is not after t: the first occurrence of a
		// repeated time, or a gap that maps back to the transition t is at
		// or past. It already ran; keep walking.
		wall = wall.Add(time.Minute)
	}
	return time.Time{}
}

// resolve turns a wall-clock time (held in UTC) into an instant in loc. A
// wall time inside a DST gap resolves to the first instant after the gap.
func resolve(wall time.Time, loc *time.Location) time.Time {
	at := time.Date(wall.Year(), wall.Month(), wall.Day(), wall.Hour(), wall.Minute(), 0, 0, loc)
	got := time.Date(at.Year(), at.Month(), at.Day(), at.Hour(), at.Minute(), 0, 0, time.UTC)
	if got.Equal(wall) {
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
