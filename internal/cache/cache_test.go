package cache

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestSetGetHit(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: 10})

	if err := c.Set(context.Background(), "name", "tiny", 0); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	result, err := c.Get(context.Background(), "name")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if !result.Hit || result.Value != "tiny" {
		t.Fatalf("expected hit with value tiny, got %#v", result)
	}
}

func TestGetMissingIncrementsMiss(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: 10})

	result, err := c.Get(context.Background(), "missing")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if result.Hit {
		t.Fatal("expected miss")
	}
	if got := c.Metrics().Misses; got != 1 {
		t.Fatalf("expected 1 miss, got %d", got)
	}
}

func TestDeleteRemovesKey(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: 10})

	if err := c.Set(context.Background(), "name", "tiny", 0); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	deleted, err := c.Delete(context.Background(), "name")
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if !deleted {
		t.Fatal("expected key to be deleted")
	}
	result, err := c.Get(context.Background(), "name")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if result.Hit {
		t.Fatal("expected miss after delete")
	}
}

func TestExpireUpdatesTTL(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: 10})

	if err := c.Set(context.Background(), "name", "tiny", 0); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	updated, err := c.Expire(context.Background(), "name", 2*time.Second)
	if err != nil {
		t.Fatalf("expire failed: %v", err)
	}
	if !updated {
		t.Fatal("expected expire to update key")
	}
	ttl, err := c.TTL(context.Background(), "name")
	if err != nil {
		t.Fatalf("ttl failed: %v", err)
	}
	if ttl < 1 || ttl > 2 {
		t.Fatalf("expected ttl between 1 and 2, got %d", ttl)
	}
}

func TestTTLStates(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: 10})

	if ttl, err := c.TTL(context.Background(), "missing"); err != nil || ttl != TTLMissing {
		t.Fatalf("expected missing ttl -2, got ttl=%d err=%v", ttl, err)
	}
	if err := c.Set(context.Background(), "forever", "value", 0); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	if ttl, err := c.TTL(context.Background(), "forever"); err != nil || ttl != TTLNoExpiry {
		t.Fatalf("expected no expiry ttl -1, got ttl=%d err=%v", ttl, err)
	}
	if err := c.Set(context.Background(), "short", "value", time.Second); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	if ttl, err := c.TTL(context.Background(), "short"); err != nil || ttl < 1 {
		t.Fatalf("expected positive ttl, got ttl=%d err=%v", ttl, err)
	}
}

func TestLazyExpiration(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: 10, CleanupInterval: time.Hour})

	if err := c.Set(context.Background(), "short", "value", 20*time.Millisecond); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	time.Sleep(40 * time.Millisecond)

	result, err := c.Get(context.Background(), "short")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if result.Hit {
		t.Fatal("expected expired key to miss")
	}
	if got := c.Metrics().KeyCount; got != 0 {
		t.Fatalf("expected expired key to be removed, got %d keys", got)
	}
}

func TestBackgroundCleanup(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: 10, CleanupInterval: 10 * time.Millisecond})

	if err := c.Set(context.Background(), "short", "value", 10*time.Millisecond); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	time.Sleep(60 * time.Millisecond)

	if got := c.Metrics().KeyCount; got != 0 {
		t.Fatalf("expected cleanup to remove expired key, got %d keys", got)
	}
}

func TestLRUEvictsLeastRecentlyUsed(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: 2, EventLogSize: 10})

	mustSet(t, c, "a", "1")
	mustSet(t, c, "b", "2")
	if _, err := c.Get(context.Background(), "a"); err != nil {
		t.Fatalf("get failed: %v", err)
	}
	mustSet(t, c, "c", "3")

	if result, _ := c.Get(context.Background(), "b"); result.Hit {
		t.Fatal("expected b to be evicted")
	}
	if result, _ := c.Get(context.Background(), "a"); !result.Hit {
		t.Fatal("expected a to remain")
	}
	if result, _ := c.Get(context.Background(), "c"); !result.Hit {
		t.Fatal("expected c to remain")
	}
	if got := c.Metrics().Evictions; got != 1 {
		t.Fatalf("expected 1 eviction, got %d", got)
	}
}

func TestLFUEvictsLeastFrequentlyUsed(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: 2, EvictionPolicy: EvictionLFU})

	mustSet(t, c, "a", "1")
	mustSet(t, c, "b", "2")
	for i := 0; i < 4; i++ {
		if _, err := c.Get(context.Background(), "a"); err != nil {
			t.Fatalf("get a failed: %v", err)
		}
	}
	if _, err := c.Get(context.Background(), "b"); err != nil {
		t.Fatalf("get b failed: %v", err)
	}
	mustSet(t, c, "c", "3")

	if result, _ := c.Get(context.Background(), "b"); result.Hit {
		t.Fatal("expected b to be evicted by LFU")
	}
	if result, _ := c.Get(context.Background(), "a"); !result.Hit {
		t.Fatal("expected frequently used a to remain")
	}
}

func TestUpdateExistingKeyDoesNotIncreaseKeyCount(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: 1})

	mustSet(t, c, "a", "1")
	mustSet(t, c, "a", "2")

	if got := c.Metrics().KeyCount; got != 1 {
		t.Fatalf("expected 1 key, got %d", got)
	}
	result, err := c.Get(context.Background(), "a")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if !result.Hit || result.Value != "2" {
		t.Fatalf("expected updated value, got %#v", result)
	}
}

func TestConcurrentAccess(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: 100})

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i%10)
			_ = c.Set(context.Background(), key, "value", time.Second)
			_, _ = c.Get(context.Background(), key)
			_, _ = c.Expire(context.Background(), key, time.Second)
			_, _ = c.Delete(context.Background(), key)
		}(i)
	}
	wg.Wait()
}

func TestSnapshotKeysFiltersSortsAndLimits(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: 10})

	mustSet(t, c, "session:1", "value")
	mustSet(t, c, "session:2", "larger-value")
	mustSet(t, c, "user:1", "value")
	if _, err := c.Get(context.Background(), "session:1"); err != nil {
		t.Fatalf("get failed: %v", err)
	}

	keys := c.SnapshotKeys(KeyQuery{
		Filter: "session",
		Sort:   "access",
		Desc:   true,
		Limit:  1,
	})
	if len(keys) != 1 || keys[0].Key != "session:1" {
		t.Fatalf("unexpected queried keys: %#v", keys)
	}
}

func TestSaveAndLoadSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tinycache.snapshot.json")
	c := newTestCache(t, Options{MaxKeys: 10, SnapshotPath: path})
	mustSet(t, c, "name", "tiny")

	if err := c.SaveSnapshot(""); err != nil {
		t.Fatalf("save snapshot failed: %v", err)
	}
	loaded := newTestCache(t, Options{MaxKeys: 10, SnapshotPath: path})
	result, err := loaded.Get(context.Background(), "name")
	if err != nil {
		t.Fatalf("get loaded key failed: %v", err)
	}
	if !result.Hit || result.Value != "tiny" {
		t.Fatalf("expected loaded snapshot value, got %#v", result)
	}
}

func TestAppendLogReplayRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tinycache.aof")
	c := newTestCache(t, Options{MaxKeys: 10, AppendLogPath: path})

	mustSet(t, c, "name", "tiny")
	if updated, err := c.Expire(context.Background(), "name", 30*time.Second); err != nil || !updated {
		t.Fatalf("expire failed updated=%v err=%v", updated, err)
	}
	if deleted, err := c.Delete(context.Background(), "name"); err != nil || !deleted {
		t.Fatalf("delete failed deleted=%v err=%v", deleted, err)
	}
	c.Close()

	records, err := c.ReplayRecords()
	if err != nil {
		t.Fatalf("replay records failed: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %#v", records)
	}
	if records[0].Command != "set" || records[1].Command != "expire" || records[2].Command != "del" {
		t.Fatalf("unexpected records: %#v", records)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected append log file: %v", err)
	}
}

func BenchmarkSet(b *testing.B) {
	c := New(Options{MaxKeys: b.N + 1})
	defer c.Close()
	for i := 0; i < b.N; i++ {
		_ = c.Set(context.Background(), fmt.Sprintf("key-%d", i), "value", 0)
	}
}

func BenchmarkGetHit(b *testing.B) {
	c := New(Options{MaxKeys: 10})
	defer c.Close()
	_ = c.Set(context.Background(), "key", "value", 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.Get(context.Background(), "key")
	}
}

func BenchmarkGetMiss(b *testing.B) {
	c := New(Options{MaxKeys: 10})
	defer c.Close()
	for i := 0; i < b.N; i++ {
		_, _ = c.Get(context.Background(), "missing")
	}
}

func BenchmarkEvictLRU(b *testing.B) {
	c := New(Options{MaxKeys: 100})
	defer c.Close()
	for i := 0; i < b.N; i++ {
		_ = c.Set(context.Background(), fmt.Sprintf("key-%d", i), "value", 0)
	}
}

func newTestCache(t *testing.T, opts Options) *Cache {
	t.Helper()
	if opts.EventLogSize == 0 {
		opts.EventLogSize = 100
	}
	c := New(opts)
	t.Cleanup(c.Close)
	return c
}

func mustSet(t *testing.T, c *Cache, key, value string) {
	t.Helper()
	if err := c.Set(context.Background(), key, value, 0); err != nil {
		t.Fatalf("set %s failed: %v", key, err)
	}
}
