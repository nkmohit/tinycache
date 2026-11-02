package cache

import (
	"math"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// ParseTTL parses an EX-style TTL. A bare integer is a number of seconds.
// A number with a unit uses the same suffixes as time.ParseDuration
// (ns, us, µs, μs, ms, s, m, h). Negative values and products that do not
// fit in time.Duration are rejected instead of wrapping.
func ParseTTL(raw string) (time.Duration, error) {
	s := strings.TrimSpace(raw)
	if s == "" || len(s) > maxTTLString {
		return 0, ErrMalformedTTL
	}
	if s[0] == '-' || strings.ContainsRune(s, '-') {
		return 0, ErrInvalidTTL
	}
	if s[0] == '+' {
		s = strings.TrimSpace(s[1:])
		if s == "" || len(s) > maxTTLString {
			return 0, ErrMalformedTTL
		}
	}
	if strings.IndexFunc(s, unicode.IsSpace) >= 0 {
		return 0, ErrMalformedTTL
	}

	unitName := ""
	num := s
	for _, unit := range []string{"ns", "us", "µs", "μs", "ms", "s", "m", "h"} {
		if strings.HasSuffix(s, unit) {
			unitName = unit
			num = strings.TrimSuffix(s, unit)
			break
		}
	}
	if num == "" {
		return 0, ErrMalformedTTL
	}
	unit, err := ttlUnit(unitName)
	if err != nil {
		return 0, err
	}
	whole, frac, digits, roundUp, err := splitDecimal(num)
	if err != nil {
		return 0, err
	}
	return durationFromParts(whole, frac, digits, roundUp, unit)
}

// DurationFromSeconds converts an EX seconds count into a duration.
// Values that would overflow time.Duration are rejected.
func DurationFromSeconds(seconds int64) (time.Duration, error) {
	if seconds < 0 {
		return 0, ErrInvalidTTL
	}
	return durationFromParts(seconds, 0, 0, false, int64(time.Second))
}

func ttlUnit(name string) (int64, error) {
	switch name {
	case "", "s":
		return int64(time.Second), nil
	case "ms":
		return int64(time.Millisecond), nil
	case "us", "µs", "μs":
		return int64(time.Microsecond), nil
	case "ns":
		return int64(1), nil
	case "m":
		return int64(time.Minute), nil
	case "h":
		return int64(time.Hour), nil
	default:
		return 0, ErrMalformedTTL
	}
}

func splitDecimal(num string) (whole, frac int64, digits int, roundUp bool, err error) {
	if strings.ContainsAny(num, "eE+") {
		return 0, 0, 0, false, ErrMalformedTTL
	}
	wholePart, fracPart, hasFrac := strings.Cut(num, ".")
	if strings.ContainsRune(fracPart, '.') || wholePart == "" || (hasFrac && fracPart == "") {
		return 0, 0, 0, false, ErrMalformedTTL
	}
	if !digitsOnly(wholePart) || (hasFrac && !digitsOnly(fracPart)) {
		return 0, 0, 0, false, ErrMalformedTTL
	}
	whole, err = strconv.ParseInt(wholePart, 10, 64)
	if err != nil {
		return 0, 0, 0, false, ErrTTLOverflow
	}
	if !hasFrac {
		return whole, 0, 0, false, nil
	}
	if len(fracPart) > 18 {
		for _, c := range fracPart[18:] {
			if c != '0' {
				roundUp = true
				break
			}
		}
		fracPart = fracPart[:18]
	}
	frac, err = strconv.ParseInt(fracPart, 10, 64)
	if err != nil {
		return 0, 0, 0, false, ErrTTLOverflow
	}
	return whole, frac, len(fracPart), roundUp, nil
}

func digitsOnly(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func durationFromParts(whole, frac int64, digits int, roundUp bool, unit int64) (time.Duration, error) {
	if whole < 0 || frac < 0 || unit <= 0 {
		return 0, ErrInvalidTTL
	}
	base, err := mulInt64(whole, unit)
	if err != nil {
		return 0, ErrTTLOverflow
	}
	extra := int64(0)
	rem := false
	if frac != 0 {
		pow := int64(1)
		for i := 0; i < digits; i++ {
			next, mulErr := mulInt64(pow, 10)
			if mulErr != nil {
				return 0, ErrTTLOverflow
			}
			pow = next
		}
		var divErr error
		extra, rem, divErr = mulDiv(frac, unit, pow)
		if divErr != nil {
			return 0, ErrTTLOverflow
		}
	}
	if roundUp {
		rem = true
	}
	sum, err := addInt64(base, extra)
	if err != nil {
		return 0, ErrTTLOverflow
	}
	if rem {
		if sum == math.MaxInt64 {
			return 0, ErrTTLOverflow
		}
		sum++
	}
	if whole == 0 && frac == 0 && !roundUp {
		return 0, nil
	}
	if sum <= 0 {
		// A positive TTL that truncates to zero would be stored as no expiry.
		return 0, ErrTTLOverflow
	}
	return time.Duration(sum), nil
}

func mulInt64(a, b int64) (int64, error) {
	if a < 0 || b < 0 {
		return 0, ErrInvalidTTL
	}
	if a == 0 || b == 0 {
		return 0, nil
	}
	if a > math.MaxInt64/b {
		return 0, ErrTTLOverflow
	}
	return a * b, nil
}

func addInt64(a, b int64) (int64, error) {
	if a < 0 || b < 0 {
		return 0, ErrInvalidTTL
	}
	if a > math.MaxInt64-b {
		return 0, ErrTTLOverflow
	}
	return a + b, nil
}

// mulDiv returns (a*b)/den and whether the division had a remainder.
// a, b, and den are positive.
func mulDiv(a, b, den int64) (int64, bool, error) {
	if a <= 0 || b <= 0 || den <= 0 {
		return 0, false, ErrInvalidTTL
	}
	g := gcd(b, den)
	b /= g
	den /= g
	g = gcd(a, den)
	a /= g
	den /= g
	prod, err := mulInt64(a, b)
	if err != nil {
		return 0, false, err
	}
	return prod / den, prod%den != 0, nil
}

func gcd(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	if a < 0 {
		return -a
	}
	return a
}

// replayTTL splits a duration into the seconds field stored for humans and
// the exact nanosecond count. Sub-second TTLs used to become ttlSeconds 0,
// which replay treated as no expiry. Seconds are rounded away from zero so
// the legacy field stays positive whenever the key expires.
func replayTTL(ttl time.Duration) (seconds, nanos int64) {
	if ttl <= 0 {
		return 0, 0
	}
	nanos = int64(ttl)
	seconds = int64(ttl / time.Second)
	if ttl%time.Second != 0 {
		seconds++
	}
	return seconds, nanos
}

func (r ReplayRecord) ttlDuration() (time.Duration, error) {
	if r.TTLNanos < 0 || r.TTLSeconds < 0 {
		return 0, ErrInvalidTTL
	}
	if r.TTLNanos > 0 {
		return time.Duration(r.TTLNanos), nil
	}
	return DurationFromSeconds(r.TTLSeconds)
}
