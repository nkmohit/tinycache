package cache

import (
	"context"
	"testing"
	"time"
)

func FuzzDecodeAOF(f *testing.F) {
	f.Add([]byte("{\"command\":\"set\",\"key\":\"a\",\"value\":\"b\",\"ttlSeconds\":60,\"at\":\"2026-11-02T00:00:00Z\"}\n"))
	f.Add([]byte("{\"command\":\"set\",\"key\":\"a\",\"value\":\"b\",\"ttlSeconds\":9227000000,\"at\":\"2026-11-02T00:00:00Z\"}\n"))
	f.Add([]byte("{\"command\":\"expire\",\"key\":\"a\",\"ttlSeconds\":1,\"at\":\"2026-11-02T00:00:00Z\"}\n{\"command\":\"del\",\"key\":\"a\",\"at\":\"2026-11-02T00:00:01Z\"}\n"))
	f.Add([]byte("{\"command\":\"base\",\"at\":\"2026-11-02T00:00:00Z\"}\n{\"command\":\"set\",\"key\":\"a\",\"value\":\"b\",\"expiresAt\":\"2026-11-02T01:00:00Z\",\"ttlSeconds\":3600,\"ttlNanos\":3600000000000,\"at\":\"2026-11-02T00:00:00Z\"}\n"))
	f.Add([]byte("{"))
	f.Add([]byte(""))
	f.Add([]byte("{\"command\":\"set\",\"key\":\"a\",\"value\":\"b\"}\n{\"comma"))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 32<<10 {
			return
		}
		records, _, _, err := parseAOF(data)
		if err != nil {
			return
		}
		c := New(Options{MaxKeys: 16, CleanupInterval: time.Hour, EventLogSize: 4})
		defer c.Close()
		c.lockAll()
		for _, rec := range records {
			if err := c.applyReplayLocked(rec); err != nil {
				c.unlockAll()
				return
			}
		}
		c.unlockAll()
		if got := c.Metrics().KeyCount; got < 0 {
			t.Fatalf("negative key count %d", got)
		}
		if len(records) != 1 || records[0].Command != "set" || records[0].Key == "" || !records[0].ExpiresAt.IsZero() || records[0].TTLNanos != 0 || records[0].TTLSeconds <= 0 {
			return
		}
		exp, err := recordExpiry(records[0])
		if err != nil {
			t.Fatal(err)
		}
		if exp.IsZero() || !exp.After(time.Now()) {
			return
		}
		ttl, err := c.TTL(context.Background(), records[0].Key)
		if err != nil {
			t.Fatal(err)
		}
		if ttl < 0 {
			t.Fatalf("ttlSeconds %d stored as %d", records[0].TTLSeconds, ttl)
		}
	})
}
