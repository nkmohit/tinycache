package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAOFRewriteBuffersWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tinycache.aof")
	c := newTestCache(t, Options{
		MaxKeys:         1000,
		AppendLogPath:   path,
		AppendFsync:     FsyncAlways,
		CleanupInterval: time.Hour,
	})
	mustSet(t, c, "keep", "v1")
	mustSet(t, c, "old", "gone")
	if _, err := c.Expire(context.Background(), "keep", time.Hour); err != nil {
		t.Fatal(err)
	}
	beforeExp, ok := expiryOf(t, c, "keep")
	if !ok {
		t.Fatal("keep missing before rewrite")
	}

	sawBusy := false
	c.rewriteTestHook = func() {
		if err := c.RewriteAOF(); !errors.Is(err, ErrAOFRewriteInProgress) {
			t.Errorf("nested rewrite: %v", err)
		} else {
			sawBusy = true
		}
		mustSet(t, c, "keep", "v2")
		if _, err := c.Delete(context.Background(), "old"); err != nil {
			t.Errorf("delete: %v", err)
		}
		mustSet(t, c, "fresh", "new")
		if _, err := c.Expire(context.Background(), "keep", 2*time.Hour); err != nil {
			t.Errorf("expire: %v", err)
		}
	}
	if err := c.RewriteAOF(); err != nil {
		t.Fatal(err)
	}
	if !sawBusy {
		t.Fatal("rewrite did not report an in-progress rewrite")
	}
	want := keyValues(c)
	keepExp, ok := expiryOf(t, c, "keep")
	if !ok {
		t.Fatal("keep missing after rewrite")
	}
	if !keepExp.After(beforeExp) {
		t.Fatalf("expire during rewrite did not move expiry: before %s after %s", beforeExp, keepExp)
	}
	c.Close()

	loaded := newTestCache(t, Options{
		MaxKeys:         1000,
		AppendLogPath:   path,
		AppendFsync:     FsyncAlways,
		CleanupInterval: time.Hour,
	})
	equalKeyValues(t, keyValues(loaded), want)
	gotExp, ok := expiryOf(t, loaded, "keep")
	if !ok || !gotExp.Equal(keepExp) {
		t.Fatalf("expiry after restart = %s ok=%v, want %s", gotExp, ok, keepExp)
	}
	if _, ok := keyValues(loaded)["old"]; ok {
		t.Fatal("deleted key survived rewrite")
	}
	left, err := filepath.Glob(filepath.Join(dir, ".tinycache-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 0 {
		t.Fatalf("temp files left behind: %v", left)
	}
}

func TestRewriteKeepsEviction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tinycache.aof")
	// Two keys on every shard. Three keys that hash together fill that shard
	// and evict its LRU tail.
	names := keysOnShard(t, 0, 3)
	hot, victim, incoming := names[0], names[1], names[2]
	c := newTestCache(t, Options{
		MaxKeys:         evenCap(2),
		AppendLogPath:   path,
		AppendFsync:     FsyncAlways,
		CleanupInterval: time.Hour,
		EvictionPolicy:  EvictionLRU,
	})
	mustSet(t, c, hot, "1")
	mustSet(t, c, victim, "2")
	if _, err := c.Get(context.Background(), hot); err != nil {
		t.Fatal(err)
	}
	c.rewriteTestHook = func() {
		mustSet(t, c, incoming, "3")
	}
	if err := c.RewriteAOF(); err != nil {
		t.Fatal(err)
	}
	want := keyValues(c)
	if _, ok := want[victim]; ok {
		t.Fatalf("expected %s to be evicted, state %#v", victim, want)
	}
	if want[hot] != "1" || want[incoming] != "3" {
		t.Fatalf("state after eviction %#v", want)
	}
	c.Close()

	loaded := newTestCache(t, Options{
		MaxKeys:         evenCap(2),
		AppendLogPath:   path,
		AppendFsync:     FsyncNo,
		CleanupInterval: time.Hour,
		EvictionPolicy:  EvictionLRU,
	})
	equalKeyValues(t, keyValues(loaded), want)
}

func TestAOFRewriteConcurrentWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tinycache.aof")
	c := newTestCache(t, Options{
		MaxKeys:         1000,
		AppendLogPath:   path,
		AppendFsync:     FsyncNo,
		CleanupInterval: time.Hour,
	})
	ctx := context.Background()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 80; i++ {
				key := fmt.Sprintf("k-%d", (g*3+i)%24)
				if err := c.Set(ctx, key, fmt.Sprintf("%d-%d", g, i), time.Hour); err != nil {
					t.Errorf("set %s: %v", key, err)
					return
				}
				if i%4 == 0 {
					if _, err := c.Delete(ctx, fmt.Sprintf("k-%d", (g+i)%24)); err != nil {
						t.Errorf("del: %v", err)
						return
					}
				}
			}
		}(g)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			err := c.RewriteAOF()
			if err != nil && !errors.Is(err, ErrAOFRewriteInProgress) {
				t.Errorf("rewrite: %v", err)
				return
			}
		}
	}()
	wg.Wait()
	if t.Failed() {
		return
	}
	if err := c.RewriteAOF(); err != nil && !errors.Is(err, ErrAOFRewriteInProgress) {
		t.Fatal(err)
	}
	want := keyValues(c)
	c.Close()

	loaded := newTestCache(t, Options{
		MaxKeys:         1000,
		AppendLogPath:   path,
		AppendFsync:     FsyncNo,
		CleanupInterval: time.Hour,
	})
	equalKeyValues(t, keyValues(loaded), want)
}

func TestAutoAOFRewriteCompactsLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tinycache.aof")
	c := newTestCache(t, Options{
		MaxKeys:              1000,
		AppendLogPath:        path,
		AppendFsync:          FsyncNo,
		CleanupInterval:      time.Hour,
		AOFRewritePercentage: 100,
		AOFRewriteMinSize:    80,
	})
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		value := strings.Repeat("x", 60) + fmt.Sprintf("%d", i)
		if err := c.Set(ctx, "k", value, 0); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		data, err := os.ReadFile(path)
		if err == nil && bytes.Contains(data, []byte(`"command":"base"`)) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("auto rewrite did not publish a base record, file=%q err=%v", data, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := c.Set(ctx, "k", "final", 0); err != nil {
		t.Fatal(err)
	}
	c.Close()

	loaded := newTestCache(t, Options{
		MaxKeys:         1000,
		AppendLogPath:   path,
		AppendFsync:     FsyncNo,
		CleanupInterval: time.Hour,
	})
	assertHit(t, loaded, "k", "final")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte(`"command":"base"`)) {
		t.Fatalf("rewritten log lost its base record: %s", data)
	}
}

func TestAutoRewriteThreshold(t *testing.T) {
	logger := &appendLogger{file: &fakeAOF{}, percentage: 100, minSize: 1000, baseSize: 2000, size: 3999}
	if logger.needsRewriteLocked() {
		t.Fatal("3999 bytes has not doubled a 2000 byte base")
	}
	logger.size = 4000
	if !logger.needsRewriteLocked() {
		t.Fatal("4000 bytes is 100% growth and above the minimum")
	}
	logger.minSize = 4001
	if logger.needsRewriteLocked() {
		t.Fatal("size below the minimum must wait")
	}
	logger = &appendLogger{file: &fakeAOF{}, percentage: 100, minSize: 50, baseSize: 0, size: 49}
	if logger.needsRewriteLocked() {
		t.Fatal("under the minimum")
	}
	logger.size = 50
	if !logger.needsRewriteLocked() {
		t.Fatal("an empty base still rewrites once the file reaches the minimum")
	}
	logger.percentage = 0
	logger.size = 1 << 40
	if logger.needsRewriteLocked() {
		t.Fatal("percentage 0 disables auto rewrite")
	}
}

func TestStartupReplaysSnapshotThenAOF(t *testing.T) {
	dir := t.TempDir()
	snap := filepath.Join(dir, "snap")
	aofPath := filepath.Join(dir, "aof")
	c := newTestCache(t, Options{
		MaxKeys:         1000,
		SnapshotPath:    snap,
		AppendLogPath:   aofPath,
		AppendFsync:     FsyncAlways,
		CleanupInterval: time.Hour,
	})
	mustSet(t, c, "a", "1")
	if err := c.SaveSnapshot(""); err != nil {
		t.Fatal(err)
	}
	mustSet(t, c, "b", "2")
	if _, err := c.Delete(context.Background(), "a"); err != nil {
		t.Fatal(err)
	}
	c.Close()

	loaded := newTestCache(t, Options{
		MaxKeys:         1000,
		SnapshotPath:    snap,
		AppendLogPath:   aofPath,
		AppendFsync:     FsyncNo,
		CleanupInterval: time.Hour,
	})
	assertMissing(t, loaded, "a")
	assertHit(t, loaded, "b", "2")
}

func TestAOFRewriteSupersedesStaleSnapshot(t *testing.T) {
	dir := t.TempDir()
	snap := filepath.Join(dir, "snap")
	aofPath := filepath.Join(dir, "aof")
	c := newTestCache(t, Options{
		MaxKeys:         1000,
		SnapshotPath:    snap,
		AppendLogPath:   aofPath,
		AppendFsync:     FsyncAlways,
		CleanupInterval: time.Hour,
	})
	mustSet(t, c, "a", "1")
	mustSet(t, c, "b", "2")
	if err := c.SaveSnapshot(""); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Delete(context.Background(), "a"); err != nil {
		t.Fatal(err)
	}
	if err := c.RewriteAOF(); err != nil {
		t.Fatal(err)
	}
	c.Close()

	loaded := newTestCache(t, Options{
		MaxKeys:         1000,
		SnapshotPath:    snap,
		AppendLogPath:   aofPath,
		AppendFsync:     FsyncNo,
		CleanupInterval: time.Hour,
	})
	assertMissing(t, loaded, "a")
	assertHit(t, loaded, "b", "2")
	data, err := os.ReadFile(aofPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte(`"command":"base"`)) {
		t.Fatalf("rewrite did not write a base command: %s", data)
	}
}

func TestAOFTruncatedTailIsRepaired(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tinycache.aof")
	c := newTestCache(t, Options{
		MaxKeys:         1000,
		AppendLogPath:   path,
		AppendFsync:     FsyncAlways,
		CleanupInterval: time.Hour,
	})
	mustSet(t, c, "a", "1")
	mustSet(t, c, "b", "2")
	c.Close()

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"command":"set","key":"torn","value":"nope"`); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	var logs []string
	var mu sync.Mutex
	prev := aofInfof
	aofInfof = func(format string, args ...any) {
		mu.Lock()
		logs = append(logs, fmt.Sprintf(format, args...))
		mu.Unlock()
	}
	t.Cleanup(func() { aofInfof = prev })

	loaded := newTestCache(t, Options{
		MaxKeys:         1000,
		AppendLogPath:   path,
		AppendFsync:     FsyncNo,
		CleanupInterval: time.Hour,
	})
	assertHit(t, loaded, "a", "1")
	assertHit(t, loaded, "b", "2")
	assertMissing(t, loaded, "torn")
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(after, []byte("torn")) {
		t.Fatalf("torn tail still in the file: %s", after)
	}
	if !bytes.HasSuffix(after, []byte("\n")) {
		t.Fatalf("repaired file does not end on a record boundary: %q", after)
	}
	if len(after) >= len(before) {
		t.Fatalf("file was not truncated: before %d after %d", len(before), len(after))
	}
	mu.Lock()
	defer mu.Unlock()
	joined := strings.Join(logs, "\n")
	if !strings.Contains(joined, "truncated incomplete final record") {
		t.Fatalf("truncate was not logged: %q", joined)
	}
}

func TestAOFCorruptRecordAbortsStartup(t *testing.T) {
	dir := t.TempDir()
	good := commandRecord("set", "a", "1", time.Time{}, time.Now().UTC())
	line, err := marshalRecord(good)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("middle", func(t *testing.T) {
		path := filepath.Join(dir, "middle.aof")
		body := append(append([]byte{}, line...), []byte("{not-json}\n")...)
		body = append(body, line...)
		if err := os.WriteFile(path, body, 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := Open(Options{MaxKeys: 1000, AppendLogPath: path, AppendFsync: FsyncNo, CleanupInterval: time.Hour})
		if !errors.Is(err, ErrCorruptAOF) {
			t.Fatalf("expected corrupt aof, got %v", err)
		}
		got, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if !bytes.Equal(got, body) {
			t.Fatal("corrupt file was modified")
		}
	})

	t.Run("last complete line", func(t *testing.T) {
		path := filepath.Join(dir, "last.aof")
		body := append(append([]byte{}, line...), []byte("{not-json}\n")...)
		if err := os.WriteFile(path, body, 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := Open(Options{MaxKeys: 1000, AppendLogPath: path, AppendFsync: FsyncNo, CleanupInterval: time.Hour})
		if !errors.Is(err, ErrCorruptAOF) {
			t.Fatalf("expected corrupt aof, got %v", err)
		}
	})
}

func TestTTLAbsoluteAcrossRestart(t *testing.T) {
	t.Run("aof", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "tinycache.aof")
		c := newTestCache(t, Options{
			MaxKeys:         1000,
			AppendLogPath:   path,
			AppendFsync:     FsyncAlways,
			CleanupInterval: time.Hour,
		})
		if err := c.Set(context.Background(), "live", "v", 30*time.Second); err != nil {
			t.Fatal(err)
		}
		if err := c.Set(context.Background(), "dead", "v", 20*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		liveExp, ok := expiryOf(t, c, "live")
		if !ok {
			t.Fatal("live missing")
		}
		time.Sleep(40 * time.Millisecond)
		c.Close()

		loaded := newTestCache(t, Options{
			MaxKeys:         1000,
			AppendLogPath:   path,
			AppendFsync:     FsyncNo,
			CleanupInterval: time.Hour,
		})
		got, ok := expiryOf(t, loaded, "live")
		if !ok || !got.Equal(liveExp) {
			t.Fatalf("live expiry = %s ok=%v, want %s", got, ok, liveExp)
		}
		assertMissing(t, loaded, "dead")
	})

	t.Run("expire extends a past set", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "tinycache.aof")
		now := time.Now().UTC()
		records := []ReplayRecord{
			commandRecord("set", "extended", "v", now.Add(-time.Minute), now.Add(-2*time.Minute)),
			commandRecord("expire", "extended", "", now.Add(time.Hour), now.Add(-time.Second)),
			commandRecord("set", "dead", "v", now.Add(-time.Second), now.Add(-time.Hour)),
			commandRecord("set", "removed", "v", time.Time{}, now.Add(-time.Hour)),
			commandRecord("del", "removed", "", time.Time{}, now.Add(-time.Minute)),
		}
		writeAOF(t, path, records)
		loaded := newTestCache(t, Options{
			MaxKeys:         1000,
			AppendLogPath:   path,
			AppendFsync:     FsyncNo,
			CleanupInterval: time.Hour,
		})
		assertHit(t, loaded, "extended", "v")
		assertMissing(t, loaded, "dead")
		assertMissing(t, loaded, "removed")
		got, ok := expiryOf(t, loaded, "extended")
		want := now.Add(time.Hour).UTC()
		if !ok || !got.Equal(want) {
			t.Fatalf("extended expiry = %s ok=%v, want %s", got, ok, want)
		}
	})
}

func TestSnapshotRoundTripAndV1(t *testing.T) {
	t.Run("v2 preserves expiry", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "snap")
		c := newTestCache(t, Options{MaxKeys: 1000, SnapshotPath: path, CleanupInterval: time.Hour})
		if err := c.Set(context.Background(), "name", "tiny", time.Hour); err != nil {
			t.Fatal(err)
		}
		want, ok := expiryOf(t, c, "name")
		if !ok {
			t.Fatal("missing")
		}
		if err := c.SaveSnapshot(""); err != nil {
			t.Fatal(err)
		}
		left, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".tinycache-snapshot-*"))
		if err != nil {
			t.Fatal(err)
		}
		if len(left) != 0 {
			t.Fatalf("snapshot temp files left behind: %v", left)
		}
		c.Close()

		loaded := newTestCache(t, Options{MaxKeys: 1000, SnapshotPath: path, CleanupInterval: time.Hour})
		assertHit(t, loaded, "name", "tiny")
		got, ok := expiryOf(t, loaded, "name")
		if !ok || !got.Equal(want) {
			t.Fatalf("expiry = %s ok=%v, want %s", got, ok, want)
		}
	})

	t.Run("expired entry stays out", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "snap")
		now := time.Now().UTC()
		future := now.Add(15 * time.Second)
		past := now.Add(-time.Minute)
		data, err := encodeSnapshot(now, []snapshotEntry{
			{Key: "soon", Value: "v", ExpiresAt: future, CreatedAt: now, UpdatedAt: now, LastAccessedAt: now},
			{Key: "old", Value: "v", ExpiresAt: past, CreatedAt: past, UpdatedAt: past, LastAccessedAt: past},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		loaded := newTestCache(t, Options{MaxKeys: 1000, SnapshotPath: path, CleanupInterval: time.Hour})
		assertHit(t, loaded, "soon", "v")
		assertMissing(t, loaded, "old")
		got, ok := expiryOf(t, loaded, "soon")
		if !ok || !got.Equal(future.UTC()) {
			t.Fatalf("soon expiry = %s ok=%v, want %s", got, ok, future.UTC())
		}
	})

	t.Run("v1", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "snap")
		body := []byte(`{
  "version": 1,
  "savedAt": "2020-01-01T00:00:00Z",
  "entries": [{
    "key": "name",
    "value": "tiny",
    "createdAt": "2020-01-01T00:00:00Z",
    "updatedAt": "2020-01-01T00:00:00Z",
    "lastAccessedAt": "2020-01-01T00:00:00Z",
    "accessCount": 3
  }]
}`)
		if err := os.WriteFile(path, body, 0o600); err != nil {
			t.Fatal(err)
		}
		loaded := newTestCache(t, Options{MaxKeys: 1000, SnapshotPath: path, CleanupInterval: time.Hour})
		s := loaded.shard("name")
		s.mu.Lock()
		item := s.items["name"]
		value := ""
		var access int64
		if item != nil {
			value = item.value
			access = item.accessCount
		}
		s.mu.Unlock()
		if item == nil || value != "tiny" || access != 3 {
			t.Fatalf("v1 entry value=%q access=%d missing=%v", value, access, item == nil)
		}
	})
}

func TestCorruptSnapshotRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap")
	c := newTestCache(t, Options{MaxKeys: 1000, SnapshotPath: path, CleanupInterval: time.Hour})
	mustSet(t, c, "name", "tiny")
	if err := c.SaveSnapshot(""); err != nil {
		t.Fatal(err)
	}
	c.Close()
	good, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		file []byte
	}{
		{name: "payload byte", file: flipLast(good)},
		{name: "crc32", file: bumpHexField(t, good, "crc32")},
		{name: "crc64", file: bumpHexField(t, good, "crc64")},
		{name: "truncated", file: good[:12]},
		{name: "empty", file: []byte{}},
		{name: "bad magic", file: []byte("{\"magic\":\"NOPE\",\"version\":2,\"savedAt\":\"2020-01-01T00:00:00Z\",\"crc32\":\"00000000\",\"crc64\":\"0000000000000000\",\"length\":2}\n{}")},
		{name: "version", file: []byte("{\"magic\":\"TINYCACHE\",\"version\":99,\"savedAt\":\"2020-01-01T00:00:00Z\",\"crc32\":\"00000000\",\"crc64\":\"0000000000000000\",\"length\":2}\n{}")},
		{name: "not a snapshot", file: []byte(`{"hello":"world"}`)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(path, tc.file, 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := Open(Options{MaxKeys: 1000, SnapshotPath: path, CleanupInterval: time.Hour})
			if !errors.Is(err, ErrCorruptSnapshot) {
				t.Fatalf("expected corrupt snapshot, got %v", err)
			}
		})
	}
}

func TestRewriteAOFRequiresPath(t *testing.T) {
	c := newTestCache(t, Options{MaxKeys: 2})
	if err := c.RewriteAOF(); !errors.Is(err, ErrAppendLogPath) {
		t.Fatalf("expected missing path, got %v", err)
	}
}

func keyValues(c *Cache) map[string]string {
	now := time.Now().UTC()
	c.lockAll()
	defer c.unlockAll()
	out := make(map[string]string)
	for i := range c.shards {
		for key, item := range c.shards[i].items {
			if item.expired(now) {
				continue
			}
			out[key] = item.value
		}
	}
	return out
}

func equalKeyValues(t *testing.T, got, want map[string]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("key count got %d want %d\ngot %#v\nwant %#v", len(got), len(want), got, want)
	}
	for key, value := range want {
		if got[key] != value {
			t.Fatalf("key %s = %q, want %q\ngot %#v", key, got[key], value, got)
		}
	}
}

func expiryOf(t *testing.T, c *Cache, key string) (time.Time, bool) {
	t.Helper()
	s := c.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.items[key]
	if !ok {
		return time.Time{}, false
	}
	return item.expiresAt, true
}

func writeAOF(t *testing.T, path string, records []ReplayRecord) {
	t.Helper()
	var buf bytes.Buffer
	for _, rec := range records {
		line, err := marshalRecord(rec)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := buf.Write(line); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
}

func flipLast(in []byte) []byte {
	out := bytes.Clone(in)
	out[len(out)-1] ^= 0xff
	return out
}

func bumpHexField(t *testing.T, in []byte, field string) []byte {
	t.Helper()
	key := []byte(`"` + field + `":"`)
	i := bytes.Index(in, key)
	if i < 0 {
		t.Fatalf("field %s not in snapshot", field)
	}
	pos := i + len(key)
	out := bytes.Clone(in)
	switch out[pos] {
	case '9':
		out[pos] = 'a'
	case 'f', 'F':
		out[pos] = '0'
	default:
		out[pos]++
	}
	return out
}
