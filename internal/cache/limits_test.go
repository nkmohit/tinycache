package cache

import (
	"bytes"
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestParseTTLAndDurationOverflow(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in      string
		want    time.Duration
		wantErr error
	}{
		{in: "0", want: 0},
		{in: "60", want: 60 * time.Second},
		{in: " 60s ", want: 60 * time.Second},
		{in: "+60", want: 60 * time.Second},
		{in: "1500ms", want: 1500 * time.Millisecond},
		{in: "1.5s", want: 1500 * time.Millisecond},
		{in: "1h", want: time.Hour},
		{in: "1m", want: time.Minute},
		{in: "1us", want: time.Microsecond},
		{in: "1µs", want: time.Microsecond},
		{in: "1μs", want: time.Microsecond},
		{in: "0.1ns", want: time.Nanosecond},
		{in: "-1", wantErr: ErrInvalidTTL},
		{in: "-1s", wantErr: ErrInvalidTTL},
		{in: "", wantErr: ErrMalformedTTL},
		{in: "60s60s", wantErr: ErrMalformedTTL},
		{in: "1e3", wantErr: ErrMalformedTTL},
		{in: "9223372036", want: 9223372036 * time.Second},
		{in: "9223372037", wantErr: ErrTTLOverflow},
		{in: "9227000000", wantErr: ErrTTLOverflow},
		{in: "4611686018427387904", wantErr: ErrTTLOverflow},
		{in: "9223372036854775807ms", wantErr: ErrTTLOverflow},
		// 1e13 ms is 1e19 ns, past MaxInt64. 1e12 ms still fits.
		{in: "10000000000000ms", wantErr: ErrTTLOverflow},
	}
	for _, tt := range cases {
		t.Run(tt.in, func(t *testing.T) {
			got, err := ParseTTL(tt.in)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("ParseTTL(%q) err=%v, want %v", tt.in, err, tt.wantErr)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("ParseTTL(%q)=%s err=%v, want %s", tt.in, got, err, tt.want)
			}
		})
	}

	if _, err := DurationFromSeconds(math.MaxInt64); !errors.Is(err, ErrTTLOverflow) {
		t.Fatalf("DurationFromSeconds(MaxInt64) err=%v", err)
	}
	got, err := DurationFromSeconds(0)
	if err != nil || got != 0 {
		t.Fatalf("DurationFromSeconds(0)=%s err=%v", got, err)
	}
}

func TestExpiryRejectsWrappedDuration(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	expires, err := expiryFromTTL(now, time.Second)
	if err != nil || !expires.Equal(now.Add(time.Second)) {
		t.Fatalf("expiry=%s err=%v", expires, err)
	}
	if _, err := expiryFromTTL(now, -time.Second); !errors.Is(err, ErrInvalidTTL) {
		t.Fatalf("negative ttl err=%v", err)
	}
}

func TestSubSecondTTLSurvivesReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tinycache.aof")
	c := newTestCache(t, Options{MaxKeys: 64, AppendLogPath: path, AppendFsync: FsyncAlways})
	if err := c.Set(context.Background(), "short", "v", 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	c.Close()

	records, err := c.ReplayRecords()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].TTLNanos != int64(20*time.Millisecond) || records[0].ExpiresAt.IsZero() || records[0].TTLSeconds != 1 {
		t.Fatalf("record = %#v", records)
	}
	// An older line has ttlNanos and no expiresAt. Replay must keep the
	// exact duration instead of treating a zero ttlSeconds as immortal.
	// At is "now" so the assertion is about the duration, not about how
	// long this test took to reach replay.
	legacy := ReplayRecord{
		Command:  "set",
		Key:      "short",
		Value:    "v",
		TTLNanos: int64(20 * time.Millisecond),
		At:       time.Now().UTC(),
	}
	replayed := newTestCache(t, Options{MaxKeys: 64})
	replayed.lockAll()
	err = replayed.applyReplayLocked(legacy)
	replayed.unlockAll()
	if err != nil {
		t.Fatal(err)
	}
	result, err := replayed.Get(context.Background(), "short")
	if err != nil || !result.Hit {
		t.Fatalf("replayed get = %#v err=%v", result, err)
	}
	time.Sleep(80 * time.Millisecond)
	result, err = replayed.Get(context.Background(), "short")
	if err != nil || result.Hit {
		t.Fatalf("sub-second ttl lived too long: %#v err=%v", result, err)
	}
}

func TestApplyReplayRejectsOverflowingTTL(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: 64})
	c.lockAll()
	err := c.applyReplayLocked(ReplayRecord{
		Command:    "set",
		Key:        "a",
		Value:      "b",
		TTLSeconds: 1 << 62,
		At:         time.Now().UTC(),
	})
	c.unlockAll()
	if !errors.Is(err, ErrTTLOverflow) {
		t.Fatalf("err=%v", err)
	}
	result, getErr := c.Get(context.Background(), "a")
	if getErr != nil || result.Hit {
		t.Fatalf("overflowing ttl was stored: %#v err=%v", result, getErr)
	}
}

func TestDecodeAOFDropsTornTail(t *testing.T) {
	t.Parallel()
	good := []byte("{\"command\":\"set\",\"key\":\"a\",\"value\":\"b\"}\n{\"comma")
	records, _, truncated, err := parseAOF(good)
	if err != nil || !truncated {
		t.Fatalf("records=%#v truncated=%v err=%v", records, truncated, err)
	}
	if len(records) != 1 || records[0].Key != "a" {
		t.Fatalf("records=%#v", records)
	}
	if _, _, _, err := parseAOF([]byte("{not json}\n")); !errors.Is(err, ErrCorruptAOF) {
		t.Fatal("expected a complete bad line to fail")
	}
	base := []byte("{\"command\":\"base\",\"at\":\"2026-11-02T00:00:00Z\"}\n{\"command\":\"set\",\"key\":\"a\",\"value\":\"b\",\"ttlNanos\":20000000,\"at\":\"2026-11-02T00:00:00Z\"}\n")
	records, _, truncated, err = parseAOF(base)
	if err != nil || truncated || len(records) != 2 || records[0].Command != "base" || records[1].TTLNanos != 20000000 {
		t.Fatalf("base records=%#v truncated=%v err=%v", records, truncated, err)
	}
	tooLong := bytes.Repeat([]byte("a"), MaxAOFLineBytes+1)
	if _, _, _, err := parseAOF(tooLong); !errors.Is(err, ErrAOFTooLarge) {
		t.Fatalf("long line err=%v", err)
	}
}

func TestDescendingSortIsStrict(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: 64})
	mustSet(t, c, "a", "1")
	mustSet(t, c, "b", "1")
	keys, err := c.SnapshotKeys(KeyQuery{Sort: "access", Desc: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || keys[0].Key != "b" || keys[1].Key != "a" {
		t.Fatalf("keys=%#v", keys)
	}
	if _, err := c.SnapshotKeys(KeyQuery{Sort: "nope"}); !errors.Is(err, ErrInvalidKeySort) {
		t.Fatalf("invalid sort err=%v", err)
	}
}

func TestMemoryCounterDoesNotWrap(t *testing.T) {
	if got := saturatingAddNonNegative(math.MaxInt64-5, 10); got != math.MaxInt64 {
		t.Fatalf("memory=%d", got)
	}
	if got := saturatingAddNonNegative(math.MaxInt64, math.MinInt64); got != 0 {
		t.Fatalf("memory after negative delta=%d", got)
	}
}

func TestEntrySizeLimits(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: 2})
	ctx := context.Background()
	if err := c.Set(ctx, strings.Repeat("k", MaxKeyBytes+1), "v", 0); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("key err=%v", err)
	}
	if err := c.Set(ctx, "k", strings.Repeat("v", MaxValueBytes+1), 0); !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("value err=%v", err)
	}
	if _, err := c.Get(ctx, strings.Repeat("k", MaxKeyBytes+1)); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("get err=%v", err)
	}
}

func TestCorruptSnapshotFailsOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Options{SnapshotPath: path, EventLogSize: 4}); err == nil {
		t.Fatal("expected corrupt snapshot to fail")
	}
}

func TestInvalidEvictionPolicy(t *testing.T) {
	t.Parallel()
	if _, err := Open(Options{EvictionPolicy: "lruu"}); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("err=%v", err)
	}
}

func TestCloseIsIdempotentAndStopsGoroutines(t *testing.T) {
	defer goleak.VerifyNone(t)
	path := filepath.Join(t.TempDir(), "tinycache.aof")
	c, err := Open(Options{
		MaxKeys:         32,
		CleanupInterval: time.Millisecond,
		EventLogSize:    8,
		AppendLogPath:   path,
		AppendFsync:     FsyncEverySec,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for id := 0; id < 4; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := "k"
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = c.Set(ctx, key, "v", time.Millisecond)
				_, _ = c.Get(ctx, key)
				_, _ = c.Delete(ctx, key)
				_, _ = c.Expire(ctx, key, time.Second)
			}
		}(id)
	}
	time.Sleep(30 * time.Millisecond)
	c.Close()
	c.Close()
	close(stop)
	wg.Wait()
}

func TestConcurrentSoak(t *testing.T) {
	for _, policy := range []EvictionPolicy{EvictionLRU, EvictionLFU} {
		t.Run(string(policy), func(t *testing.T) {
			c := newTestCache(t, Options{
				MaxKeys:         32,
				EvictionPolicy:  policy,
				CleanupInterval: time.Millisecond,
			})
			ctx := context.Background()
			var wg sync.WaitGroup
			deadline := time.Now().Add(3 * time.Second)
			for g := 0; g < 8; g++ {
				wg.Add(1)
				go func(g int) {
					defer wg.Done()
					for i := 0; time.Now().Before(deadline); i++ {
						key := "k-" + itoa((g*17+i)%48)
						_ = c.Set(ctx, key, "v", time.Millisecond)
						_, _ = c.Get(ctx, key)
						_, _ = c.Delete(ctx, key)
						_, _ = c.Expire(ctx, key, time.Millisecond)
						if i%32 == 0 {
							if _, err := c.SnapshotKeys(KeyQuery{Sort: "access", Desc: true}); err != nil {
								t.Errorf("snapshot: %v", err)
								return
							}
						}
					}
				}(g)
			}
			wg.Wait()
			if got := c.Metrics().KeyCount; got < 0 || got > 32 {
				t.Fatalf("key count %d", got)
			}
			assertKeyCount(t, c)
			if policy == EvictionLFU {
				assertLFUBuckets(t, c)
			}
		})
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [16]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
