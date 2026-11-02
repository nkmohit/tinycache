package cache

import (
	"math"
	"math/big"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode"
)

func FuzzParseTTL(f *testing.F) {
	f.Add("0")
	f.Add("60")
	f.Add("1s")
	f.Add("500ms")
	f.Add("1ms")
	f.Add("-1")
	f.Add("-1s")
	f.Add("9223372036")
	f.Add("9223372036854ms")
	f.Fuzz(func(t *testing.T, raw string) {
		if len(raw) > 64 {
			return
		}
		d, err := ParseTTL(raw)
		if err != nil {
			if d != 0 {
				t.Fatalf("ParseTTL(%q) err=%v duration=%d", raw, err, d)
			}
			return
		}
		if d < 0 {
			t.Fatalf("ParseTTL(%q)=%d, want non-negative", raw, int64(d))
		}
		want, overflow, ok := plainTTLNanos(raw)
		if !ok {
			return
		}
		if overflow {
			t.Fatalf("ParseTTL accepted overflowing %q as %s", raw, d)
		}
		if int64(d) != want {
			t.Fatalf("ParseTTL(%q)=%d want %d", raw, int64(d), want)
		}
	})
}

// plainTTLNanos reports the exact nanosecond value of a simple "<int><unit>"
// TTL. overflow is true when the product does not fit in int64.
func plainTTLNanos(raw string) (nanos int64, overflow bool, ok bool) {
	s := strings.TrimSpace(raw)
	s = strings.TrimPrefix(s, "+")
	if s == "" || strings.IndexFunc(s, unicode.IsSpace) >= 0 {
		return 0, false, false
	}
	unit := int64(time.Second)
	switch {
	case strings.HasSuffix(s, "ms"):
		unit = int64(time.Millisecond)
		s = strings.TrimSuffix(s, "ms")
	case strings.HasSuffix(s, "s"):
		unit = int64(time.Second)
		s = strings.TrimSuffix(s, "s")
	}
	if s == "" || strings.ContainsAny(s, ".eE+-") {
		return 0, false, false
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return 0, false, false
	}
	if n == 0 {
		return 0, false, true
	}
	prod := new(big.Int).Mul(big.NewInt(n), big.NewInt(unit))
	if !prod.IsInt64() {
		return 0, true, true
	}
	if prod.Cmp(big.NewInt(math.MaxInt64)) > 0 {
		return 0, true, true
	}
	return prod.Int64(), false, true
}
