package cache

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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

func TestExpiredKeysAreNeverReturned(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: 20, CleanupInterval: time.Hour})
	ctx := context.Background()
	for _, key := range []string{"get", "ttl", "expire", "snapshot", "lru"} {
		if err := c.Set(ctx, key, "secret-"+key, 20*time.Millisecond); err != nil {
			t.Fatalf("set %s: %v", key, err)
		}
	}
	if err := c.Set(ctx, "live", "ok", 0); err != nil {
		t.Fatalf("set live: %v", err)
	}
	time.Sleep(40 * time.Millisecond)

	result, err := c.Get(ctx, "get")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if result.Hit || result.Value != "" {
		t.Fatalf("get returned expired key: %#v", result)
	}

	ttl, err := c.TTL(ctx, "ttl")
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if ttl != TTLMissing {
		t.Fatalf("ttl of expired key = %d, want %d", ttl, TTLMissing)
	}

	updated, err := c.Expire(ctx, "expire", time.Minute)
	if err != nil {
		t.Fatalf("expire: %v", err)
	}
	if updated {
		t.Fatal("expire updated an already expired key")
	}

	for _, debug := range c.SnapshotKeys(KeyQuery{}) {
		if debug.Key != "live" {
			t.Fatalf("snapshot returned expired key %s", debug.Key)
		}
	}
	lru := c.LRUKeys()
	if len(lru) != 1 || lru[0] != "live" {
		t.Fatalf("lru = %#v, want [live]", lru)
	}
	result, err = c.Get(ctx, "live")
	if err != nil || !result.Hit || result.Value != "ok" {
		t.Fatalf("live key: %#v err=%v", result, err)
	}
	assertTTLIndex(t, c)
}

func TestSetDoesNotScanForExpiredKeys(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: 10, CleanupInterval: time.Hour})
	ctx := context.Background()
	if err := c.Set(ctx, "old", "gone", 15*time.Millisecond); err != nil {
		t.Fatalf("set old: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	if err := c.Set(ctx, "new", "value", 0); err != nil {
		t.Fatalf("set new: %v", err)
	}
	if got := c.Metrics().KeyCount; got != 2 {
		t.Fatalf("set should leave the expired key in place, got %d keys", got)
	}
	result, err := c.Get(ctx, "old")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if result.Hit || result.Value != "" {
		t.Fatalf("expired key returned: %#v", result)
	}
	if got := c.Metrics().KeyCount; got != 1 {
		t.Fatalf("lazy get should drop the expired key, got %d keys", got)
	}
	assertTTLIndex(t, c)
}

func TestSweeperCycleDrainsExpiredKeys(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: 500, CleanupInterval: time.Hour})
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		if err := c.Set(ctx, fmt.Sprintf("dead-%d", i), "v", 20*time.Millisecond); err != nil {
			t.Fatalf("set dead-%d: %v", i, err)
		}
	}
	for i := 0; i < 10; i++ {
		if _, err := c.Delete(ctx, fmt.Sprintf("dead-%d", i)); err != nil {
			t.Fatalf("delete dead-%d: %v", i, err)
		}
	}
	if err := c.Set(ctx, "keep", "v", 0); err != nil {
		t.Fatalf("set keep: %v", err)
	}
	if updated, err := c.Expire(ctx, "keep", time.Hour); err != nil || !updated {
		t.Fatalf("expire keep onto a ttl: updated=%v err=%v", updated, err)
	}
	if updated, err := c.Expire(ctx, "keep", 0); err != nil || !updated {
		t.Fatalf("expire keep clear ttl: updated=%v err=%v", updated, err)
	}
	if err := c.Set(ctx, "later", "v", time.Hour); err != nil {
		t.Fatalf("set later: %v", err)
	}
	assertTTLIndex(t, c)

	time.Sleep(40 * time.Millisecond)
	c.cleanupExpired()

	if got := c.Metrics().KeyCount; got != 2 {
		t.Fatalf("expected sweeper to leave keep and later, got %d keys", got)
	}
	for _, key := range []string{"keep", "later"} {
		result, err := c.Get(ctx, key)
		if err != nil || !result.Hit || result.Value != "v" {
			t.Fatalf("%s: %#v err=%v", key, result, err)
		}
	}
	result, err := c.Get(ctx, "dead-50")
	if err != nil || result.Hit {
		t.Fatalf("expired key returned: %#v err=%v", result, err)
	}
	assertTTLIndex(t, c)
}

func TestSweeperEventuallyReclaims(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: 500, CleanupInterval: 5 * time.Millisecond})
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		if err := c.Set(ctx, fmt.Sprintf("dead-%d", i), "v", 20*time.Millisecond); err != nil {
			t.Fatalf("set dead-%d: %v", i, err)
		}
	}
	if err := c.Set(ctx, "keep", "v", 0); err != nil {
		t.Fatalf("set keep: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.Metrics().KeyCount == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := c.Metrics().KeyCount; got != 1 {
		t.Fatalf("background sweeper left %d keys", got)
	}
	result, err := c.Get(ctx, "keep")
	if err != nil || !result.Hit || result.Value != "v" {
		t.Fatalf("keep: %#v err=%v", result, err)
	}
	for i := 0; i < 50; i++ {
		result, err := c.Get(ctx, fmt.Sprintf("dead-%d", i))
		if err != nil || result.Hit {
			t.Fatalf("dead-%d returned %#v err=%v", i, result, err)
		}
	}
	assertTTLIndex(t, c)
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
	assertLFUBuckets(t, c)
}

func TestLFUEvictsInFrequencyThenRecencyOrder(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: 3, EvictionPolicy: EvictionLFU})
	ctx := context.Background()

	mustSet(t, c, "a", "1")
	mustSet(t, c, "b", "2")
	mustSet(t, c, "c", "3")
	for i := 0; i < 3; i++ {
		if _, err := c.Get(ctx, "a"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.Get(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	// Frequencies: a=3, b=1, c=0. c is the least used.
	mustSet(t, c, "d", "4")
	assertMissing(t, c, "c")
	assertHit(t, c, "a", "1")
	assertHit(t, c, "b", "2")
	assertHit(t, c, "d", "4")
	assertLFUBuckets(t, c)

	// d is the only frequency-0 key.
	mustSet(t, c, "e", "5")
	assertMissing(t, c, "d")
	assertHit(t, c, "a", "1")
	assertHit(t, c, "b", "2")
	assertLFUBuckets(t, c)

	// Same frequency: the older access loses. Stamp the times so the result
	// does not depend on the clock resolution or which shard each key hits.
	tied := newTestCache(t, Options{MaxKeys: 2, EvictionPolicy: EvictionLFU})
	mustSet(t, tied, "older", "1")
	mustSet(t, tied, "newer", "2")
	if _, err := tied.Get(ctx, "older"); err != nil {
		t.Fatal(err)
	}
	if _, err := tied.Get(ctx, "newer"); err != nil {
		t.Fatal(err)
	}
	stampAccess(t, tied, "older", time.Unix(10, 0).UTC(), false)
	stampAccess(t, tied, "newer", time.Unix(20, 0).UTC(), true)
	mustSet(t, tied, "fresh", "3")
	assertMissing(t, tied, "older")
	assertHit(t, tied, "newer", "2")
	assertHit(t, tied, "fresh", "3")
	assertLFUBuckets(t, tied)
}

func TestLFUSnapshotRestoresFrequency(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tinycache.snapshot.json")
	c := newTestCache(t, Options{MaxKeys: 2, EvictionPolicy: EvictionLFU, SnapshotPath: path})
	mustSet(t, c, "hot", "1")
	mustSet(t, c, "cold", "2")
	for i := 0; i < 3; i++ {
		if _, err := c.Get(context.Background(), "hot"); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.SaveSnapshot(""); err != nil {
		t.Fatal(err)
	}
	c.Close()

	loaded := newTestCache(t, Options{MaxKeys: 2, EvictionPolicy: EvictionLFU, SnapshotPath: path})
	mustSet(t, loaded, "new", "3")
	assertMissing(t, loaded, "cold")
	assertHit(t, loaded, "hot", "1")
	assertLFUBuckets(t, loaded)
}

func TestConcurrentAccessUnderEviction(t *testing.T) {
	ctx := context.Background()
	for _, policy := range []EvictionPolicy{EvictionLRU, EvictionLFU} {
		t.Run(string(policy), func(t *testing.T) {
			c := newTestCache(t, Options{MaxKeys: 32, EvictionPolicy: policy, CleanupInterval: time.Hour})
			var wg sync.WaitGroup
			for g := 0; g < 8; g++ {
				wg.Add(1)
				go func(g int) {
					defer wg.Done()
					for i := 0; i < 100; i++ {
						key := fmt.Sprintf("k-%d", (g*17+i)%48)
						_ = c.Set(ctx, key, "v", time.Millisecond)
						_, _ = c.Get(ctx, key)
						_, _ = c.TTL(ctx, key)
						_, _ = c.Expire(ctx, key, time.Second)
						if i%5 == 0 {
							_, _ = c.Delete(ctx, key)
						}
					}
				}(g)
			}
			wg.Wait()
			if got := c.Metrics().KeyCount; got < 0 || got > 32 {
				t.Fatalf("key count %d out of range", got)
			}
			assertKeyCount(t, c)
			if policy == EvictionLFU {
				assertLFUBuckets(t, c)
			}
		})
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

func TestAOFWriteErrorSurfaces(t *testing.T) {
	c := newTestCache(t, Options{
		MaxKeys:       4,
		AppendLogPath: filepath.Join(t.TempDir(), "tinycache.aof"),
		AppendFsync:   FsyncNo,
	})
	c.aof.mu.Lock()
	real := c.aof.file
	c.aof.file = errSyncWriter{err: errors.New("disk full")}
	c.aof.mu.Unlock()
	t.Cleanup(func() {
		if real != nil {
			_ = real.Close()
		}
	})

	err := c.Set(context.Background(), "name", "tiny", 0)
	if err == nil || !strings.Contains(err.Error(), "disk full") {
		t.Fatalf("expected aof write error, got %v", err)
	}
}

func TestAOFDeviceWriteErrorSurfaces(t *testing.T) {
	if _, err := os.Stat("/dev/full"); err != nil {
		t.Skip("/dev/full is not available")
	}
	c := newTestCache(t, Options{
		MaxKeys:       4,
		AppendLogPath: "/dev/full",
		AppendFsync:   FsyncNo,
	})
	err := c.Set(context.Background(), "name", "tiny", 0)
	if err == nil {
		t.Fatal("expected /dev/full write to surface an error")
	}
}

func TestAOFOpenErrorSurfaces(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: 4, AppendLogPath: t.TempDir(), AppendFsync: FsyncNo})
	err := c.Set(context.Background(), "name", "tiny", 0)
	if err == nil {
		t.Fatal("expected append log open error to surface on set")
	}
}

func TestAppendFsyncAlwaysSyncsEachWrite(t *testing.T) {
	c := newTestCache(t, Options{
		MaxKeys:       4,
		AppendLogPath: filepath.Join(t.TempDir(), "tinycache.aof"),
		AppendFsync:   FsyncAlways,
	})
	fake := &fakeAOF{}
	c.aof.mu.Lock()
	real := c.aof.file
	c.aof.file = fake
	c.aof.mu.Unlock()
	t.Cleanup(func() {
		if real != nil {
			_ = real.Close()
		}
	})

	for _, key := range []string{"a", "b", "c"} {
		if err := c.Set(context.Background(), key, "v", 0); err != nil {
			t.Fatalf("set %s: %v", key, err)
		}
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.writes != 3 || fake.syncs != 3 {
		t.Fatalf("writes=%d syncs=%d, want 3 writes and 3 syncs", fake.writes, fake.syncs)
	}
}

func TestAppendFsyncNoSkipsSync(t *testing.T) {
	fake := &fakeAOF{}
	logger := &appendLogger{file: fake, policy: FsyncNo, interval: time.Second}
	if err := logger.write(ReplayRecord{Command: "set", Key: "k", Value: "v"}); err != nil {
		t.Fatal(err)
	}
	if err := logger.close(); err != nil {
		t.Fatal(err)
	}
	if fake.writes != 1 || fake.syncs != 0 {
		t.Fatalf("writes=%d syncs=%d, want 1 write and 0 syncs", fake.writes, fake.syncs)
	}
}

func TestAppendFsyncEverySecTicksAndShutsDown(t *testing.T) {
	fake := &fakeAOF{}
	logger := &appendLogger{file: fake, policy: FsyncEverySec, interval: 15 * time.Millisecond}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		logger.everySecLoop(stop)
	}()

	if err := logger.write(ReplayRecord{Command: "set", Key: "k", Value: "v"}); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	syncedOnWrite := fake.syncs
	fake.mu.Unlock()
	if syncedOnWrite != 0 {
		t.Fatalf("everysec synced inside write, syncs=%d", syncedOnWrite)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		fake.mu.Lock()
		synced := fake.syncs
		fake.mu.Unlock()
		if synced >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	fake.mu.Lock()
	synced := fake.syncs
	fake.mu.Unlock()
	if synced < 1 {
		t.Fatal("background ticker did not fsync")
	}

	close(stop)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("everysec loop did not return after stop")
	}
}

func TestAOFReplayAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tinycache.aof")
	c := newTestCache(t, Options{MaxKeys: 10, AppendLogPath: path, AppendFsync: FsyncAlways})
	mustSet(t, c, "name", "tiny")
	if updated, err := c.Expire(context.Background(), "name", 30*time.Second); err != nil || !updated {
		t.Fatalf("expire updated=%v err=%v", updated, err)
	}
	if deleted, err := c.Delete(context.Background(), "name"); err != nil || !deleted {
		t.Fatalf("delete deleted=%v err=%v", deleted, err)
	}
	c.Close()

	restarted := newTestCache(t, Options{MaxKeys: 10, AppendLogPath: path, AppendFsync: FsyncEverySec})
	records, err := restarted.ReplayRecords()
	if err != nil {
		t.Fatalf("replay records: %v", err)
	}
	if len(records) != 3 || records[0].Command != "set" || records[1].Command != "expire" || records[2].Command != "del" {
		t.Fatalf("unexpected records: %#v", records)
	}

	replayed := newTestCache(t, Options{MaxKeys: 10})
	for _, record := range records {
		switch record.Command {
		case "set":
			if err := replayed.Set(context.Background(), record.Key, record.Value, time.Duration(record.TTLSeconds)*time.Second); err != nil {
				t.Fatalf("replay set: %v", err)
			}
		case "expire":
			if _, err := replayed.Expire(context.Background(), record.Key, time.Duration(record.TTLSeconds)*time.Second); err != nil {
				t.Fatalf("replay expire: %v", err)
			}
		case "del":
			if _, err := replayed.Delete(context.Background(), record.Key); err != nil {
				t.Fatalf("replay del: %v", err)
			}
		default:
			t.Fatalf("unknown command %s", record.Command)
		}
	}
	result, err := replayed.Get(context.Background(), "name")
	if err != nil {
		t.Fatal(err)
	}
	if result.Hit {
		t.Fatal("replayed delete should leave name missing")
	}
}

func TestParseFsyncPolicy(t *testing.T) {
	policy, err := ParseFsyncPolicy("")
	if err != nil || policy != FsyncEverySec {
		t.Fatalf("empty policy = %s err=%v", policy, err)
	}
	if _, err := ParseFsyncPolicy("sometimes"); err == nil {
		t.Fatal("expected invalid policy error")
	}
}

type fakeAOF struct {
	mu     sync.Mutex
	writes int
	syncs  int
	err    error
}

func (f *fakeAOF) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return 0, f.err
	}
	f.writes++
	return len(p), nil
}

func (f *fakeAOF) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.syncs++
	return nil
}

func (f *fakeAOF) Close() error { return nil }

type errSyncWriter struct {
	err error
}

func (w errSyncWriter) Write([]byte) (int, error) { return 0, w.err }
func (w errSyncWriter) Sync() error               { return nil }
func (w errSyncWriter) Close() error              { return nil }

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

func BenchmarkEvictLFU(b *testing.B) {
	c := New(Options{MaxKeys: 100, EvictionPolicy: EvictionLFU})
	defer c.Close()
	for i := 0; i < b.N; i++ {
		_ = c.Set(context.Background(), fmt.Sprintf("key-%d", i), "value", 0)
	}
}

func BenchmarkGetHitParallel(b *testing.B) {
	c := New(Options{MaxKeys: 1024, CleanupInterval: time.Hour})
	defer c.Close()
	ctx := context.Background()
	keys := make([]string, 256)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
		if err := c.Set(ctx, keys[i], "value", 0); err != nil {
			b.Fatal(err)
		}
	}
	var seq atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(seq.Add(1) * 17)
		for pb.Next() {
			_, _ = c.Get(ctx, keys[i&255])
			i++
		}
	})
}

func BenchmarkMixedParallel(b *testing.B) {
	c := New(Options{MaxKeys: 1024, CleanupInterval: time.Hour})
	defer c.Close()
	ctx := context.Background()
	keys := make([]string, 256)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
		_ = c.Set(ctx, keys[i], "value", 0)
	}
	var seq atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(seq.Add(1) * 17)
		for pb.Next() {
			if i&7 == 0 {
				_ = c.Set(ctx, keys[i&255], "value", 0)
			} else {
				_, _ = c.Get(ctx, keys[i&255])
			}
			i++
		}
	})
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

func assertTTLIndex(t *testing.T, c *Cache) {
	t.Helper()
	c.lockAll()
	defer c.unlockAll()
	for id := range c.shards {
		s := &c.shards[id]
		if len(s.ttlKeys) != len(s.ttlIndex) {
			t.Fatalf("shard %d ttl set len keys=%d index=%d", id, len(s.ttlKeys), len(s.ttlIndex))
		}
		for i, key := range s.ttlKeys {
			idx, ok := s.ttlIndex[key]
			if !ok || idx != i {
				t.Fatalf("shard %d ttl index mismatch for %s at %d: idx=%d ok=%v", id, key, i, idx, ok)
			}
			item, ok := s.items[key]
			if !ok || item.expiresAt.IsZero() {
				t.Fatalf("ttl key %s missing a live expiry", key)
			}
		}
		for key, item := range s.items {
			_, tracked := s.ttlIndex[key]
			if item.expiresAt.IsZero() == tracked {
				t.Fatalf("ttl tracking mismatch for %s tracked=%v expires=%s", key, tracked, item.expiresAt)
			}
		}
	}
}

func stampAccess(t *testing.T, c *Cache, key string, at time.Time, front bool) {
	t.Helper()
	s := c.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.items[key]
	if !ok {
		t.Fatalf("missing %s", key)
	}
	item.lastAccessedAt = at
	if front {
		s.lru.MoveToFront(item.lruElement)
	} else {
		s.lru.MoveToBack(item.lruElement)
	}
	if c.evictionPolicy == EvictionLFU && item.lfuElement != nil {
		bucket := s.freqBuckets[item.accessCount]
		if front {
			bucket.MoveToFront(item.lfuElement)
		} else {
			bucket.MoveToBack(item.lfuElement)
		}
	}
}

func assertHit(t *testing.T, c *Cache, key, value string) {
	t.Helper()
	result, err := c.Get(context.Background(), key)
	if err != nil || !result.Hit || result.Value != value {
		t.Fatalf("%s: %#v err=%v", key, result, err)
	}
}

func assertMissing(t *testing.T, c *Cache, key string) {
	t.Helper()
	result, err := c.Get(context.Background(), key)
	if err != nil || result.Hit {
		t.Fatalf("%s still present: %#v err=%v", key, result, err)
	}
}

func assertKeyCount(t *testing.T, c *Cache) {
	t.Helper()
	c.lockAll()
	defer c.unlockAll()
	n := 0
	for i := range c.shards {
		n += len(c.shards[i].items)
	}
	if got := int(c.keyCount.Load()); got != n {
		t.Fatalf("keyCount=%d shard lens=%d", got, n)
	}
}

func assertLFUBuckets(t *testing.T, c *Cache) {
	t.Helper()
	c.lockAll()
	defer c.unlockAll()
	for id := range c.shards {
		s := &c.shards[id]
		seen := 0
		var min int64
		first := true
		for freq, bucket := range s.freqBuckets {
			if bucket.Len() == 0 {
				t.Fatalf("shard %d has an empty frequency bucket %d", id, freq)
			}
			if first || freq < min {
				min = freq
				first = false
			}
			for el := bucket.Front(); el != nil; el = el.Next() {
				item := el.Value.(*entry)
				if item.accessCount != freq || item.lfuElement != el {
					t.Fatalf("shard %d key %s freq bucket mismatch", id, item.key)
				}
				seen++
			}
		}
		if seen != len(s.items) {
			t.Fatalf("shard %d lfu nodes=%d items=%d", id, seen, len(s.items))
		}
		if seen > 0 && s.minFreq != min {
			t.Fatalf("shard %d minFreq=%d want %d", id, s.minFreq, min)
		}
	}
}
