package cache

import (
	"bufio"
	"bytes"
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// rewriteEntry is one live key copied for an AOF rewrite.
type rewriteEntry struct {
	key       string
	value     string
	expiresAt time.Time
	updatedAt time.Time
}

// RewriteAOF compacts the append-only file to the keys currently in memory.
// Writers keep running while the temp file is built. Commands that land in
// that window are buffered and appended before the file is fsynced and renamed
// into place. The rewritten file starts with a base command so replay drops
// keys that exist only in an older snapshot.
func (c *Cache) RewriteAOF() error {
	if c == nil || c.aof == nil || c.appendLogPath == "" {
		return ErrAppendLogPath
	}
	if err := c.aof.beginRewrite(); err != nil {
		return err
	}
	defer c.aof.finishRewrite()
	return c.rewriteAOFNow()
}

func (c *Cache) maybeAutoRewrite() {
	if c == nil || c.aof == nil || c.loading.Load() {
		return
	}
	if !c.aof.needsRewrite() {
		return
	}
	if err := c.aof.beginRewrite(); err != nil {
		return
	}
	go func() {
		defer c.aof.finishRewrite()
		if err := c.rewriteAOFNow(); err != nil {
			aofInfof("tinycache aof: rewrite %s: %v", c.appendLogPath, err)
		}
	}()
}

func (c *Cache) appendRecord(rec ReplayRecord) error {
	if c.aof == nil || c.loading.Load() {
		return nil
	}
	err := c.aof.write(rec)
	c.maybeAutoRewrite()
	return err
}

func (c *Cache) rewriteAOFNow() error {
	dir := filepath.Dir(c.appendLogPath)
	tmp, err := os.CreateTemp(dir, ".tinycache-aof-*")
	if err != nil {
		return err
	}
	rf := &rewriteFile{f: tmp, name: tmp.Name()}
	defer rf.cleanup()
	if err := tmp.Chmod(0o644); err != nil {
		return err
	}

	now := time.Now().UTC()
	oldSize := c.aof.currentSize()
	entries := c.captureForRewrite(now)
	bw := bufio.NewWriterSize(tmp, 64*1024)
	if err := writeRecord(bw, ReplayRecord{Command: "base", At: now}); err != nil {
		return err
	}
	for _, entry := range entries {
		rec := commandRecord("set", entry.key, entry.value, entry.expiresAt, entry.updatedAt)
		if err := writeRecord(bw, rec); err != nil {
			return err
		}
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	if c.rewriteTestHook != nil {
		c.rewriteTestHook()
	}

	// Drain the buffer a few times without holding writers off. The last
	// drain, the fsync, and the rename happen under the AOF lock.
	for i := 0; i < 8; i++ {
		chunk := c.aof.takeRewriteBuf()
		if len(chunk) == 0 {
			break
		}
		if _, err := bw.Write(chunk); err != nil {
			return err
		}
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}

	newSize, err := c.publishRewrite(rf, bw)
	if err != nil {
		return err
	}
	aofInfof("tinycache aof: rewrite %s (%d bytes -> %d bytes)", c.appendLogPath, oldSize, newSize)
	return nil
}

// captureForRewrite copies live keys and starts the rewrite buffer.
// Shard locks are taken in index order, then the AOF mutex: writers take one
// shard and then the AOF mutex, so this order cannot deadlock with them.
// The temp file is written after the locks are released.
func (c *Cache) captureForRewrite(now time.Time) []rewriteEntry {
	c.lockAll()
	defer c.unlockAll()
	c.aof.mu.Lock()
	defer c.aof.mu.Unlock()

	entries := make([]rewriteEntry, 0)
	for i := range c.shards {
		s := &c.shards[i]
		for _, item := range s.items {
			if item.expired(now) {
				continue
			}
			entries = append(entries, rewriteEntry{
				key:       item.key,
				value:     item.value,
				expiresAt: item.expiresAt,
				updatedAt: item.updatedAt,
			})
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })
	c.aof.rewriting = true
	c.aof.rewriteBuf = nil
	return entries
}

type rewriteFile struct {
	f         *os.File
	name      string
	closed    bool
	published bool
}

func (r *rewriteFile) cleanup() {
	if r == nil {
		return
	}
	if !r.closed && r.f != nil {
		_ = r.f.Close()
		r.closed = true
	}
	if !r.published {
		_ = os.Remove(r.name)
	}
}

func (c *Cache) publishRewrite(rf *rewriteFile, bw *bufio.Writer) (int64, error) {
	c.aof.mu.Lock()
	defer c.aof.mu.Unlock()

	if len(c.aof.rewriteBuf) > 0 {
		if _, err := bw.Write(c.aof.rewriteBuf); err != nil {
			return 0, err
		}
		c.aof.rewriteBuf = nil
		if err := bw.Flush(); err != nil {
			return 0, err
		}
		if err := rf.f.Sync(); err != nil {
			return 0, err
		}
	}
	if err := rf.f.Close(); err != nil {
		return 0, err
	}
	rf.closed = true
	if err := os.Rename(rf.name, c.appendLogPath); err != nil {
		return 0, err
	}
	rf.published = true
	dirErr := fsyncDir(filepath.Dir(c.appendLogPath))

	file, err := os.OpenFile(c.appendLogPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		c.aof.err = err
		old := c.aof.file
		c.aof.file = nil
		c.aof.rewriting = false
		c.aof.rewriteBuf = nil
		if old != nil {
			_ = old.Close()
		}
		if dirErr != nil {
			return 0, dirErr
		}
		return 0, err
	}
	var newSize int64
	if fi, statErr := file.Stat(); statErr == nil {
		newSize = fi.Size()
	}
	old := c.aof.file
	c.aof.file = file
	c.aof.rewriting = false
	c.aof.rewriteBuf = nil
	c.aof.dirty = false
	c.aof.size = newSize
	c.aof.baseSize = newSize
	if old != nil {
		_ = old.Close()
	}
	return newSize, dirErr
}

func writeRecord(w io.Writer, rec ReplayRecord) error {
	data, err := marshalRecord(rec)
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

func (c *Cache) loadFromDisk() error {
	if c.snapshotPath != "" {
		if err := c.loadSnapshot(c.snapshotPath); err != nil {
			return err
		}
	}
	if c.appendLogPath != "" {
		if err := c.replayAOF(); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cache) replayAOF() error {
	if c.aof == nil || c.appendLogPath == "" {
		return nil
	}
	fi, err := os.Stat(c.appendLogPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if !fi.Mode().IsRegular() {
		return nil
	}
	data, err := os.ReadFile(c.appendLogPath)
	if err != nil {
		return err
	}
	records, end, truncated, err := parseAOF(data)
	if err != nil {
		return err
	}
	if truncated {
		aofInfof("tinycache aof: truncated incomplete final record in %s at byte %d", c.appendLogPath, end)
		if err := os.Truncate(c.appendLogPath, int64(end)); err != nil {
			return err
		}
		if err := c.aof.syncFile(); err != nil {
			aofInfof("tinycache aof: sync after truncate: %v", err)
		}
		c.aof.setSize(int64(end))
	} else if end > 0 && data[end-1] != '\n' {
		if err := c.aof.appendRaw([]byte{'\n'}); err != nil {
			return err
		}
		aofInfof("tinycache aof: appended missing newline in %s", c.appendLogPath)
	}

	now := time.Now().UTC()
	c.lockAll()
	defer c.unlockAll()
	for _, rec := range records {
		if err := c.applyReplayLocked(rec); err != nil {
			return err
		}
	}
	for i := range c.shards {
		s := &c.shards[i]
		s.removeExpired(now, c)
		// ensureRoom only evicts while this shard is over its own budget.
		_ = c.ensureRoom(s, 0, 0, nil)
	}
	return nil
}

func (a *appendLogger) appendRaw(p []byte) error {
	if a == nil {
		return ErrAppendLogPath
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.file == nil {
		return ErrAppendLogPath
	}
	n, err := a.file.Write(p)
	if err != nil {
		return err
	}
	a.size += int64(n)
	a.baseSize = a.size
	return a.file.Sync()
}

func (c *Cache) ReplayRecords() ([]ReplayRecord, error) {
	if c.appendLogPath == "" {
		return nil, ErrAppendLogPath
	}
	data, err := os.ReadFile(c.appendLogPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []ReplayRecord{}, nil
		}
		return nil, err
	}
	records, _, _, err := parseAOF(data)
	if err != nil {
		return nil, err
	}
	if records == nil {
		return []ReplayRecord{}, nil
	}
	return records, nil
}

// parseAOF splits a JSON-lines log. A final line with no newline that is not
// valid JSON is a torn write: truncated is set and validEnd is the byte to
// keep. A bad line that ends in a newline is corruption anywhere in the file.
func parseAOF(data []byte) (records []ReplayRecord, validEnd int, truncated bool, err error) {
	i := 0
	for i < len(data) {
		rel := bytes.IndexByte(data[i:], '\n')
		if rel < 0 {
			tail := data[i:]
			if len(bytes.TrimSpace(tail)) == 0 {
				if len(tail) == 0 {
					return records, i, false, nil
				}
				return records, i, true, nil
			}
			var rec ReplayRecord
			if uerr := json.Unmarshal(bytes.TrimSpace(tail), &rec); uerr != nil {
				return records, i, true, nil
			}
			if rec.Command == "" {
				return nil, 0, false, fmt.Errorf("%w: invalid record at byte %d", ErrCorruptAOF, i)
			}
			records = append(records, rec)
			return records, len(data), false, nil
		}
		line := data[i : i+rel]
		next := i + rel + 1
		if len(bytes.TrimSpace(line)) == 0 {
			i = next
			validEnd = next
			continue
		}
		var rec ReplayRecord
		if uerr := json.Unmarshal(line, &rec); uerr != nil || rec.Command == "" {
			return nil, 0, false, fmt.Errorf("%w: invalid record at byte %d", ErrCorruptAOF, i)
		}
		records = append(records, rec)
		validEnd = next
		i = next
	}
	return records, validEnd, false, nil
}

func (c *Cache) applyReplayLocked(rec ReplayRecord) error {
	switch rec.Command {
	case "base":
		c.resetLocked()
		return nil
	case "set":
		if rec.Key == "" {
			return fmt.Errorf("%w: set without key", ErrCorruptAOF)
		}
		c.applySetLocked(rec)
		return nil
	case "expire":
		if rec.Key == "" {
			return fmt.Errorf("%w: expire without key", ErrCorruptAOF)
		}
		c.applyExpireLocked(rec)
		return nil
	case "del":
		if rec.Key == "" {
			return fmt.Errorf("%w: del without key", ErrCorruptAOF)
		}
		s := c.shard(rec.Key)
		if item, ok := s.items[rec.Key]; ok {
			c.removeItem(s, item, "")
		}
		return nil
	default:
		return fmt.Errorf("%w: unknown command %q", ErrCorruptAOF, rec.Command)
	}
}

func (c *Cache) applySetLocked(rec ReplayRecord) {
	at := rec.At
	if at.IsZero() {
		at = time.Now().UTC()
	}
	exp := recordExpiry(rec)
	size := estimateSize(rec.Key, rec.Value)
	s := c.shard(rec.Key)
	if item, ok := s.items[rec.Key]; ok {
		c.updateItem(s, item, rec.Value, exp, size, at)
		return
	}
	c.insertItem(s, &entry{
		key:            rec.Key,
		value:          rec.Value,
		expiresAt:      exp,
		createdAt:      at,
		updatedAt:      at,
		lastAccessedAt: at,
		sizeBytes:      size,
	})
}

func (c *Cache) applyExpireLocked(rec ReplayRecord) {
	s := c.shard(rec.Key)
	item, ok := s.items[rec.Key]
	if !ok {
		return
	}
	at := rec.At
	if at.IsZero() {
		at = time.Now().UTC()
	}
	item.expiresAt = recordExpiry(rec)
	item.updatedAt = at
	s.trackTTL(item)
}

func recordExpiry(rec ReplayRecord) time.Time {
	if !rec.ExpiresAt.IsZero() {
		return rec.ExpiresAt
	}
	if rec.TTLSeconds > 0 && !rec.At.IsZero() {
		return rec.At.Add(time.Duration(rec.TTLSeconds) * time.Second)
	}
	return time.Time{}
}

func (c *Cache) resetLocked() {
	for i := range c.shards {
		s := &c.shards[i]
		s.items = map[string]*entry{}
		s.lru = list.New()
		s.bytes = 0
		s.keys = nil
		s.keyIndex = nil
		s.lfuMeta = nil
		s.ttlKeys = nil
		s.ttlIndex = map[string]int{}
		s.freqBuckets = map[int64]*list.List{}
		s.minFreq = 0
	}
	c.memoryBytes.Store(0)
	c.keyCount.Store(0)
}
