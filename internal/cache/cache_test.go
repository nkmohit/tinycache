package cache

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSetGetHit(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: 64})

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
	c := newTestCache(t, Options{MaxKeys: 64})

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
	c := newTestCache(t, Options{MaxKeys: 64})

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
	c := newTestCache(t, Options{MaxKeys: 64})

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
	c := newTestCache(t, Options{MaxKeys: 64})

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
	c := newTestCache(t, Options{MaxKeys: 64, CleanupInterval: time.Hour})

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
	c := newTestCache(t, Options{MaxKeys: 64, CleanupInterval: 10 * time.Millisecond})

	if err := c.Set(context.Background(), "short", "value", 10*time.Millisecond); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	time.Sleep(60 * time.Millisecond)

	if got := c.Metrics().KeyCount; got != 0 {
		t.Fatalf("expected cleanup to remove expired key, got %d keys", got)
	}
}

func TestExpiredKeysAreNeverReturned(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: 64, CleanupInterval: time.Hour})
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
	c := newTestCache(t, Options{MaxKeys: 64, CleanupInterval: time.Hour})
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
	c := newTestCache(t, Options{MaxKeys: evenCap(2), EventLogSize: 10})
	keys := keysOnShard(t, 0, 3)
	a, b, fresh := keys[0], keys[1], keys[2]

	mustSet(t, c, a, "1")
	mustSet(t, c, b, "2")
	if _, err := c.Get(context.Background(), a); err != nil {
		t.Fatalf("get failed: %v", err)
	}
	mustSet(t, c, fresh, "3")

	assertMissing(t, c, b)
	assertHit(t, c, a, "1")
	assertHit(t, c, fresh, "3")
	if got := c.Metrics().Evictions; got != 1 {
		t.Fatalf("expected 1 eviction, got %d", got)
	}
	assertKeyIndex(t, c)
}

func TestLFUEvictsLeastFrequentlyUsed(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: evenCap(2), EvictionPolicy: EvictionLFU})
	keys := keysOnShard(t, 0, 3)
	a, b, fresh := keys[0], keys[1], keys[2]

	mustSet(t, c, a, "1")
	mustSet(t, c, b, "2")
	for i := 0; i < 4; i++ {
		if _, err := c.Get(context.Background(), a); err != nil {
			t.Fatalf("get a failed: %v", err)
		}
	}
	if _, err := c.Get(context.Background(), b); err != nil {
		t.Fatalf("get b failed: %v", err)
	}
	mustSet(t, c, fresh, "3")

	assertMissing(t, c, b)
	assertHit(t, c, a, "1")
	assertLFUBuckets(t, c)
}

func TestLFUEvictsInFrequencyThenRecencyOrder(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: evenCap(3), EvictionPolicy: EvictionLFU})
	ctx := context.Background()
	keys := keysOnShard(t, 0, 5)
	a, b, cold, d, e := keys[0], keys[1], keys[2], keys[3], keys[4]

	mustSet(t, c, a, "1")
	mustSet(t, c, b, "2")
	mustSet(t, c, cold, "3")
	for i := 0; i < 3; i++ {
		if _, err := c.Get(ctx, a); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.Get(ctx, b); err != nil {
		t.Fatal(err)
	}
	// Frequencies: a=3, b=1, cold=0. cold is the least used.
	mustSet(t, c, d, "4")
	assertMissing(t, c, cold)
	assertHit(t, c, a, "1")
	assertHit(t, c, b, "2")
	assertHit(t, c, d, "4")
	assertLFUBuckets(t, c)

	// d is the only frequency-0 key.
	mustSet(t, c, e, "5")
	assertMissing(t, c, d)
	assertHit(t, c, a, "1")
	assertHit(t, c, b, "2")
	assertLFUBuckets(t, c)

	// Same frequency: the older access loses. Stamp the times so the result
	// does not depend on the clock resolution.
	tied := newTestCache(t, Options{MaxKeys: evenCap(2), EvictionPolicy: EvictionLFU})
	pair := keysOnShard(t, 0, 3)
	older, newer, fresh := pair[0], pair[1], pair[2]
	mustSet(t, tied, older, "1")
	mustSet(t, tied, newer, "2")
	if _, err := tied.Get(ctx, older); err != nil {
		t.Fatal(err)
	}
	if _, err := tied.Get(ctx, newer); err != nil {
		t.Fatal(err)
	}
	stampAccess(t, tied, older, time.Unix(10, 0).UTC(), false)
	stampAccess(t, tied, newer, time.Unix(20, 0).UTC(), true)
	mustSet(t, tied, fresh, "3")
	assertMissing(t, tied, older)
	assertHit(t, tied, newer, "2")
	assertHit(t, tied, fresh, "3")
	assertLFUBuckets(t, tied)
}

func TestLFUSnapshotRestoresFrequency(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tinycache.snapshot.json")
	keys := keysOnShard(t, 0, 3)
	hot, cold, fresh := keys[0], keys[1], keys[2]
	c := newTestCache(t, Options{MaxKeys: evenCap(2), EvictionPolicy: EvictionLFU, SnapshotPath: path})
	mustSet(t, c, hot, "1")
	mustSet(t, c, cold, "2")
	for i := 0; i < 3; i++ {
		if _, err := c.Get(context.Background(), hot); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.SaveSnapshot(""); err != nil {
		t.Fatal(err)
	}
	c.Close()

	loaded := newTestCache(t, Options{MaxKeys: evenCap(2), EvictionPolicy: EvictionLFU, SnapshotPath: path})
	mustSet(t, loaded, fresh, "3")
	assertMissing(t, loaded, cold)
	assertHit(t, loaded, hot, "1")
	assertLFUBuckets(t, loaded)
}

func TestConcurrentAccessUnderEviction(t *testing.T) {
	ctx := context.Background()
	for _, policy := range []EvictionPolicy{
		EvictionLRU,
		EvictionLFU,
		EvictionAllKeysLRU,
		EvictionAllKeysLFU,
		EvictionAllKeysRandom,
		EvictionVolatileTTL,
		EvictionNoEviction,
	} {
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
			assertKeyIndex(t, c)
			assertMemory(t, c)
			if policy == EvictionLFU {
				assertLFUBuckets(t, c)
			}
		})
	}
}

func TestUpdateExistingKeyDoesNotIncreaseKeyCount(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: shardCount})

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
	c := newTestCache(t, Options{MaxKeys: 64})

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
	c := newTestCache(t, Options{MaxKeys: 64, SnapshotPath: path})
	mustSet(t, c, "name", "tiny")

	if err := c.SaveSnapshot(""); err != nil {
		t.Fatalf("save snapshot failed: %v", err)
	}
	loaded := newTestCache(t, Options{MaxKeys: 64, SnapshotPath: path})
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
	c := newTestCache(t, Options{MaxKeys: 64, AppendLogPath: path})

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
		MaxKeys:       64,
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
		MaxKeys:       64,
		AppendLogPath: "/dev/full",
		AppendFsync:   FsyncNo,
	})
	err := c.Set(context.Background(), "name", "tiny", 0)
	if err == nil {
		t.Fatal("expected /dev/full write to surface an error")
	}
}

func TestAOFOpenErrorSurfaces(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: 64, AppendLogPath: t.TempDir(), AppendFsync: FsyncNo})
	err := c.Set(context.Background(), "name", "tiny", 0)
	if err == nil {
		t.Fatal("expected append log open error to surface on set")
	}
}

func TestAppendFsyncAlwaysSyncsEachWrite(t *testing.T) {
	c := newTestCache(t, Options{
		MaxKeys:       64,
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
	c := newTestCache(t, Options{MaxKeys: 64, AppendLogPath: path, AppendFsync: FsyncAlways})
	mustSet(t, c, "name", "tiny")
	if updated, err := c.Expire(context.Background(), "name", 30*time.Second); err != nil || !updated {
		t.Fatalf("expire updated=%v err=%v", updated, err)
	}
	if deleted, err := c.Delete(context.Background(), "name"); err != nil || !deleted {
		t.Fatalf("delete deleted=%v err=%v", deleted, err)
	}
	c.Close()

	restarted := newTestCache(t, Options{MaxKeys: 64, AppendLogPath: path, AppendFsync: FsyncEverySec})
	records, err := restarted.ReplayRecords()
	if err != nil {
		t.Fatalf("replay records: %v", err)
	}
	if len(records) != 3 || records[0].Command != "set" || records[1].Command != "expire" || records[2].Command != "del" {
		t.Fatalf("unexpected records: %#v", records)
	}

	replayed := newTestCache(t, Options{MaxKeys: 64})
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

func TestShardCapacityRemainderSpread(t *testing.T) {
	const keyTotal = shardCount + 3
	const memTotal = int64(shardCount)*10 + 4
	c := newTestCache(t, Options{MaxKeys: keyTotal, MaxMemory: memTotal})
	var keys int
	var mem int64
	for i := range c.shards {
		keys += c.shards[i].maxKeys
		mem += c.shards[i].maxMemory
		wantKeys := 1
		if i < 3 {
			wantKeys = 2
		}
		if c.shards[i].maxKeys != wantKeys {
			t.Fatalf("shard %d maxKeys=%d want %d", i, c.shards[i].maxKeys, wantKeys)
		}
		wantMem := int64(10)
		if i < 4 {
			wantMem = 11
		}
		if c.shards[i].maxMemory != wantMem {
			t.Fatalf("shard %d maxMemory=%d want %d", i, c.shards[i].maxMemory, wantMem)
		}
	}
	if keys != keyTotal || mem != memTotal {
		t.Fatalf("spread keys=%d mem=%d", keys, mem)
	}
}

func TestPerShardEvictionLeavesOtherShards(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: evenCap(1)})
	hot := keysOnShard(t, 0, 2)
	cold := keysOnShard(t, 1, 1)
	mustSet(t, c, hot[0], "a")
	mustSet(t, c, cold[0], "b")
	mustSet(t, c, hot[1], "a2")
	assertMissing(t, c, hot[0])
	assertHit(t, c, hot[1], "a2")
	assertHit(t, c, cold[0], "b")
	if got := c.Metrics().KeyCount; got != 2 {
		t.Fatalf("key count=%d want 2", got)
	}
}

func TestExactLRUIgnoresClock(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: evenCap(2), EvictionPolicy: EvictionLRU, MaxMemorySamples: 20})
	keys := keysOnShard(t, 0, 3)
	a, b, fresh := keys[0], keys[1], keys[2]
	mustSet(t, c, a, "1")
	mustSet(t, c, b, "2")
	if _, err := c.Get(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	// a is the list head. Its clock is ancient; exact LRU still keeps it.
	setClock(t, c, a, 1)
	setClock(t, c, b, 100)
	mustSet(t, c, fresh, "3")
	assertMissing(t, c, b)
	assertHit(t, c, a, "1")
	assertHit(t, c, fresh, "3")
}

func TestAllKeysLRUEvictsOldestClock(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: evenCap(2), EvictionPolicy: EvictionAllKeysLRU, MaxMemorySamples: 20})
	keys := keysOnShard(t, 0, 3)
	a, b, fresh := keys[0], keys[1], keys[2]
	mustSet(t, c, a, "1")
	mustSet(t, c, b, "2")
	if _, err := c.Get(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	setClock(t, c, a, 1)
	setClock(t, c, b, 100)
	mustSet(t, c, fresh, "3")
	assertMissing(t, c, a)
	assertHit(t, c, b, "2")
	assertHit(t, c, fresh, "3")
}

func TestAllKeysLFUEvictsLowestCounter(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: evenCap(2), EvictionPolicy: EvictionAllKeysLFU, MaxMemorySamples: 20})
	keys := keysOnShard(t, 0, 3)
	cold, hot, fresh := keys[0], keys[1], keys[2]
	mustSet(t, c, cold, "1")
	mustSet(t, c, hot, "2")
	nowMin := time.Now().UTC().Unix() / 60
	setLFU(t, c, cold, 1, nowMin)
	setLFU(t, c, hot, 100, nowMin)
	mustSet(t, c, fresh, "3")
	assertMissing(t, c, cold)
	assertHit(t, c, hot, "2")
	assertHit(t, c, fresh, "3")
}

func TestAllKeysLFUCounterDecays(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: evenCap(2), EvictionPolicy: EvictionAllKeysLFU, MaxMemorySamples: 20})
	keys := keysOnShard(t, 0, 3)
	idle, freshHit, fresh := keys[0], keys[1], keys[2]
	mustSet(t, c, idle, "1")
	mustSet(t, c, freshHit, "2")
	nowMin := time.Now().UTC().Unix() / 60
	// 30 idle minutes drops a counter of 20 to 0. The fresh counter stays at 5.
	setLFU(t, c, idle, 20, nowMin-30)
	setLFU(t, c, freshHit, 5, nowMin)
	mustSet(t, c, fresh, "3")
	assertMissing(t, c, idle)
	assertHit(t, c, freshHit, "2")
}

func TestLFULogIncrementAndDecay(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	counter := uint8(0)
	for counter <= lfuInitVal {
		next := lfuLogIncr(counter, rng)
		if next != counter+1 {
			t.Fatalf("counter %d incremented to %d, want %d", counter, next, counter+1)
		}
		counter = next
	}
	stayed := 0
	for i := 0; i < 200; i++ {
		if lfuLogIncr(counter, rng) == counter {
			stayed++
		}
	}
	if stayed == 0 {
		t.Fatal("log counter incremented on every hit above the init value")
	}

	now := time.Now().UTC()
	st := lfuState{log: 20, decay: now.Unix()/60 - 30}
	if got := st.decayed(now); got != 0 {
		t.Fatalf("decayed counter=%d want 0", got)
	}
	st = lfuState{log: 40, decay: now.Unix()/60 - 5}
	if got := st.decayed(now); got != 35 {
		t.Fatalf("decayed counter=%d want 35", got)
	}
}

func TestAllKeysRandomEvictsAnyResident(t *testing.T) {
	keys := keysOnShard(t, 0, 3)
	evicted := map[string]int{}
	for trial := 0; trial < 40; trial++ {
		c := newTestCache(t, Options{MaxKeys: evenCap(2), EvictionPolicy: EvictionAllKeysRandom})
		mustSet(t, c, keys[0], "1")
		mustSet(t, c, keys[1], "2")
		mustSet(t, c, keys[2], "3")
		missing := 0
		for _, key := range keys[:2] {
			result, err := c.Get(context.Background(), key)
			if err != nil {
				t.Fatal(err)
			}
			if !result.Hit {
				missing++
				evicted[key]++
			}
		}
		if missing != 1 {
			t.Fatalf("trial %d missing=%d", trial, missing)
		}
		assertHit(t, c, keys[2], "3")
		if got := c.Metrics().KeyCount; got != 2 {
			t.Fatalf("key count=%d", got)
		}
	}
	if len(evicted) < 2 {
		t.Fatalf("random policy evicted only %v", evicted)
	}
}

func TestVolatileTTLEvictsSoonestAndSkipsPersistent(t *testing.T) {
	c := newTestCache(t, Options{
		MaxKeys:          evenCap(3),
		EvictionPolicy:   EvictionVolatileTTL,
		MaxMemorySamples: 20,
		CleanupInterval:  time.Hour,
	})
	keys := keysOnShard(t, 0, 4)
	immortal, soon, later, fresh := keys[0], keys[1], keys[2], keys[3]
	mustSet(t, c, immortal, "keep")
	mustSetTTL(t, c, soon, "s", time.Second)
	mustSetTTL(t, c, later, "l", time.Hour)
	mustSetTTL(t, c, fresh, "f", time.Hour)
	assertMissing(t, c, soon)
	assertHit(t, c, immortal, "keep")
	assertHit(t, c, later, "l")
	assertHit(t, c, fresh, "f")
}

func TestVolatileTTLReturnsOOMWhenNothingExpires(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: evenCap(1), EvictionPolicy: EvictionVolatileTTL})
	keys := keysOnShard(t, 0, 2)
	mustSet(t, c, keys[0], "keep")
	err := c.Set(context.Background(), keys[1], "nope", 0)
	if !errors.Is(err, ErrOOM) {
		t.Fatalf("expected OOM, got %v", err)
	}
	assertHit(t, c, keys[0], "keep")
	if got := c.Metrics().Evictions; got != 0 {
		t.Fatalf("evictions=%d", got)
	}
}

func TestNoEvictionReturnsOOM(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: evenCap(2), EvictionPolicy: EvictionNoEviction})
	keys := keysOnShard(t, 0, 3)
	mustSet(t, c, keys[0], "1")
	mustSet(t, c, keys[1], "2")
	err := c.Set(context.Background(), keys[2], "3", 0)
	if !errors.Is(err, ErrOOM) {
		t.Fatalf("expected OOM, got %v", err)
	}
	assertHit(t, c, keys[0], "1")
	assertHit(t, c, keys[1], "2")
	assertMissing(t, c, keys[2])
	if got := c.Metrics().Evictions; got != 0 {
		t.Fatalf("evictions=%d", got)
	}
	mustSet(t, c, keys[0], "updated")
	assertHit(t, c, keys[0], "updated")
	if got := c.Metrics().KeyCount; got != 2 {
		t.Fatalf("key count=%d", got)
	}
}

func TestMaxMemoryEvictsWithinShard(t *testing.T) {
	keys := keysOnShard(t, 0, 2)
	other := keysOnShard(t, 1, 1)
	one := estimateSize(keys[0], "v")
	c := newTestCache(t, Options{
		MaxKeys:        evenCap(8),
		MaxMemory:      one * shardCount,
		EvictionPolicy: EvictionLRU,
	})
	mustSet(t, c, keys[0], "v")
	mustSet(t, c, keys[1], "v")
	assertMissing(t, c, keys[0])
	assertHit(t, c, keys[1], "v")
	mustSet(t, c, other[0], "v")
	assertHit(t, c, other[0], "v")
	if got := c.Metrics().MemoryBytes; got != one*2 {
		t.Fatalf("memory=%d want %d", got, one*2)
	}
	assertMemory(t, c)
}

func TestMaxMemoryRejectsOversizedValue(t *testing.T) {
	keys := keysOnShard(t, 0, 1)
	one := estimateSize(keys[0], "v")
	c := newTestCache(t, Options{
		MaxKeys:        evenCap(4),
		MaxMemory:      one * shardCount,
		EvictionPolicy: EvictionLRU,
	})
	mustSet(t, c, keys[0], "v")
	big := strings.Repeat("x", int(one))
	err := c.Set(context.Background(), keys[0], big, 0)
	if !errors.Is(err, ErrOOM) {
		t.Fatalf("expected OOM, got %v", err)
	}
	assertHit(t, c, keys[0], "v")
	if got := c.Metrics().Evictions; got != 0 {
		t.Fatalf("oversized write evicted %d keys", got)
	}
}

func TestMaxMemoryUpdateEvictsNeighbor(t *testing.T) {
	keys := keysOnShard(t, 0, 2)
	small := estimateSize(keys[0], "v")
	budget := small * 2
	c := newTestCache(t, Options{
		MaxKeys:        evenCap(8),
		MaxMemory:      budget * shardCount,
		EvictionPolicy: EvictionLRU,
	})
	mustSet(t, c, keys[0], "v")
	mustSet(t, c, keys[1], "v")
	need := int(budget - int64(len(keys[0])) - perEntryOverhead)
	if need <= 0 {
		t.Fatalf("value length %d", need)
	}
	mustSet(t, c, keys[0], strings.Repeat("y", need))
	assertMissing(t, c, keys[1])
	assertHit(t, c, keys[0], strings.Repeat("y", need))
	assertMemory(t, c)
}

func TestNoEvictionMaxMemoryRejectsGrowth(t *testing.T) {
	keys := keysOnShard(t, 0, 2)
	small := estimateSize(keys[0], "v")
	c := newTestCache(t, Options{
		MaxKeys:        evenCap(8),
		MaxMemory:      small * 2 * shardCount,
		EvictionPolicy: EvictionNoEviction,
	})
	mustSet(t, c, keys[0], "v")
	mustSet(t, c, keys[1], "v")
	big := strings.Repeat("z", int(small))
	err := c.Set(context.Background(), keys[0], big, 0)
	if !errors.Is(err, ErrOOM) {
		t.Fatalf("expected OOM, got %v", err)
	}
	assertHit(t, c, keys[0], "v")
	assertHit(t, c, keys[1], "v")
	if got := c.Metrics().Evictions; got != 0 {
		t.Fatalf("evictions=%d", got)
	}
}

func TestConcurrentMaxMemory(t *testing.T) {
	one := estimateSize("key-00", "v")
	c := newTestCache(t, Options{
		MaxKeys:         evenCap(32),
		MaxMemory:       one * 4 * shardCount,
		EvictionPolicy:  EvictionAllKeysLRU,
		CleanupInterval: time.Hour,
	})
	ctx := context.Background()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 80; i++ {
				key := fmt.Sprintf("key-%02d", (g*3+i)%40)
				_ = c.Set(ctx, key, "v", 0)
				_, _ = c.Get(ctx, key)
				if i%4 == 0 {
					_, _ = c.Delete(ctx, key)
				}
			}
		}(g)
	}
	wg.Wait()
	assertKeyCount(t, c)
	assertKeyIndex(t, c)
	assertMemory(t, c)
	if got := c.Metrics().MemoryBytes; got < 0 || got > one*4*shardCount {
		t.Fatalf("memory=%d over budget %d", got, one*4*shardCount)
	}
}

func TestShardSkewEvictsBelowGlobalCap(t *testing.T) {
	const n = 20000
	c := newTestCache(t, Options{MaxKeys: n + 1, CleanupInterval: time.Hour})
	for i := 0; i < n; i++ {
		if err := c.Set(context.Background(), fmt.Sprintf("key-%d", i), "value", 0); err != nil {
			t.Fatal(err)
		}
	}
	metrics := c.Metrics()
	if metrics.Evictions == 0 {
		t.Fatal("expected hash skew to fill at least one shard before the global cap")
	}
	if metrics.KeyCount >= n {
		t.Fatalf("key count %d, want fewer than %d after local eviction", metrics.KeyCount, n)
	}
	if int(metrics.Evictions)+metrics.KeyCount != n {
		t.Fatalf("evictions %d + keys %d != %d", metrics.Evictions, metrics.KeyCount, n)
	}
	assertMemory(t, c)
	assertKeyIndex(t, c)
}

func TestMaxMemoryUpdateEvictsLFUNeighbor(t *testing.T) {
	keys := keysOnShard(t, 0, 2)
	small := estimateSize(keys[0], "v")
	budget := small * 2
	c := newTestCache(t, Options{
		MaxKeys:        evenCap(8),
		MaxMemory:      budget * shardCount,
		EvictionPolicy: EvictionLFU,
	})
	mustSet(t, c, keys[0], "v")
	mustSet(t, c, keys[1], "v")
	for i := 0; i < 3; i++ {
		if _, err := c.Get(context.Background(), keys[0]); err != nil {
			t.Fatal(err)
		}
	}
	need := int(budget - int64(len(keys[0])) - perEntryOverhead)
	mustSet(t, c, keys[0], strings.Repeat("y", need))
	assertMissing(t, c, keys[1])
	assertHit(t, c, keys[0], strings.Repeat("y", need))
	assertLFUBuckets(t, c)
	assertMemory(t, c)
}

func TestParseEvictionPolicy(t *testing.T) {
	cases := []string{"", "lru", "LFU", "allkeys-lru", "allkeys-lfu", "allkeys-random", "volatile-ttl", "noeviction"}
	for _, name := range cases {
		if _, err := ParseEvictionPolicy(name); err != nil {
			t.Fatalf("parse %q: %v", name, err)
		}
	}
	if _, err := ParseEvictionPolicy("volatile-lru"); err == nil {
		t.Fatal("expected invalid policy")
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
	c := New(Options{MaxKeys: 64})
	defer c.Close()
	_ = c.Set(context.Background(), "key", "value", 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.Get(context.Background(), "key")
	}
}

func BenchmarkGetMiss(b *testing.B) {
	c := New(Options{MaxKeys: 64})
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

func evenCap(perShard int) int {
	return perShard * shardCount
}

func keysOnShard(t *testing.T, shard, n int) []string {
	t.Helper()
	keys := make([]string, 0, n)
	for i := 0; len(keys) < n && i < 1_000_000; i++ {
		key := fmt.Sprintf("s%d-%06d", shard, i)
		if shardIndex(key) == shard {
			keys = append(keys, key)
		}
	}
	if len(keys) != n {
		t.Fatalf("found %d keys on shard %d, want %d", len(keys), shard, n)
	}
	return keys
}

func mustSetTTL(t *testing.T, c *Cache, key, value string, ttl time.Duration) {
	t.Helper()
	if err := c.Set(context.Background(), key, value, ttl); err != nil {
		t.Fatalf("set %s failed: %v", key, err)
	}
}

func setClock(t *testing.T, c *Cache, key string, clock uint32) {
	t.Helper()
	s := c.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.items[key]
	if !ok {
		t.Fatalf("missing %s", key)
	}
	item.lastAccessedAt = time.Unix(int64(clock), 0).UTC()
}

func setLFU(t *testing.T, c *Cache, key string, counter uint8, decayMin int64) {
	t.Helper()
	s := c.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.items[key]
	if !ok {
		t.Fatalf("missing %s", key)
	}
	if s.lfuMeta == nil {
		s.lfuMeta = map[*entry]lfuState{}
	}
	s.lfuMeta[item] = lfuState{log: counter, decay: decayMin}
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

func assertKeyIndex(t *testing.T, c *Cache) {
	t.Helper()
	c.lockAll()
	defer c.unlockAll()
	for id := range c.shards {
		s := &c.shards[id]
		if !c.samplesKeys() {
			if len(s.keys) != 0 || len(s.keyIndex) != 0 {
				t.Fatalf("shard %d tracked a key sample for policy %s", id, c.evictionPolicy)
			}
			continue
		}
		if len(s.keys) != len(s.items) || len(s.keyIndex) != len(s.items) {
			t.Fatalf("shard %d keys=%d index=%d items=%d", id, len(s.keys), len(s.keyIndex), len(s.items))
		}
		for i, item := range s.keys {
			if item == nil || s.keyIndex[item] != i || s.items[item.key] != item {
				t.Fatalf("shard %d index mismatch at %d", id, i)
			}
		}
	}
}

func assertMemory(t *testing.T, c *Cache) {
	t.Helper()
	c.lockAll()
	defer c.unlockAll()
	var sum int64
	for id := range c.shards {
		s := &c.shards[id]
		var bytes int64
		for _, item := range s.items {
			bytes += item.sizeBytes
		}
		if s.bytes != bytes {
			t.Fatalf("shard %d bytes=%d sum=%d", id, s.bytes, bytes)
		}
		if s.maxMemory > 0 && s.bytes > s.maxMemory {
			t.Fatalf("shard %d bytes=%d over %d", id, s.bytes, s.maxMemory)
		}
		if len(s.items) > s.maxKeys {
			t.Fatalf("shard %d keys=%d over %d", id, len(s.items), s.maxKeys)
		}
		sum += s.bytes
	}
	if got := c.memoryBytes.Load(); got != sum {
		t.Fatalf("memoryBytes=%d shard sum=%d", got, sum)
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
