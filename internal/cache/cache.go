package cache

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nkmohit/tinycache/internal/metrics"
)

var (
	ErrEmptyKey       = errors.New("key cannot be empty")
	ErrInvalidTTL     = errors.New("ttl cannot be negative")
	ErrInvalidPolicy  = errors.New("eviction policy must be lru or lfu")
	ErrSnapshotPath   = errors.New("snapshot path is not configured")
	ErrAppendLogPath  = errors.New("append-only log path is not configured")
	ErrInvalidKeySort = errors.New("sort must be key, ttl, size, access, updated, or accessed")
)

const (
	TTLNoExpiry = -1
	TTLMissing  = -2
)

type EvictionPolicy string

const (
	EvictionLRU EvictionPolicy = "lru"
	EvictionLFU EvictionPolicy = "lfu"
)

type Options struct {
	MaxKeys         int
	CleanupInterval time.Duration
	EventLogSize    int
	EvictionPolicy  EvictionPolicy
	SnapshotPath    string
	AppendLogPath   string
}

type Cache struct {
	mu sync.RWMutex

	items map[string]*entry
	lru   *list.List

	maxKeys        int
	evictionPolicy EvictionPolicy
	memoryBytes    int64
	recorder       *metrics.Recorder

	snapshotPath  string
	appendLogPath string
	appendLog     *os.File

	stopCleanup chan struct{}
	closeOnce   sync.Once
}

type entry struct {
	key            string
	value          string
	expiresAt      time.Time
	createdAt      time.Time
	updatedAt      time.Time
	lastAccessedAt time.Time
	accessCount    int64
	sizeBytes      int64
	lruElement     *list.Element
}

type GetResult struct {
	Key   string
	Value string
	Hit   bool
}

type KeyDebug struct {
	Key            string     `json:"key"`
	SizeBytes      int64      `json:"sizeBytes"`
	TTL            int64      `json:"ttl"`
	ExpiresAt      *time.Time `json:"expiresAt,omitempty"`
	CreatedAt      time.Time  `json:"createdAt"`
	UpdatedAt      time.Time  `json:"updatedAt"`
	LastAccessedAt time.Time  `json:"lastAccessedAt"`
	AccessCount    int64      `json:"accessCount"`
}

type DebugSnapshot struct {
	Keys []KeyDebug `json:"keys"`
}

type KeyQuery struct {
	Filter string
	Sort   string
	Desc   bool
	Limit  int
}

type Summary struct {
	Metrics metrics.Snapshot `json:"metrics"`
	Keys    []KeyDebug       `json:"keys"`
	LRU     []string         `json:"lru"`
	Events  []metrics.Event  `json:"events"`
}

type ReplayRecord struct {
	Command    string    `json:"command"`
	Key        string    `json:"key,omitempty"`
	Value      string    `json:"value,omitempty"`
	TTLSeconds int64     `json:"ttlSeconds,omitempty"`
	At         time.Time `json:"at"`
}

type snapshotFile struct {
	Version int             `json:"version"`
	SavedAt time.Time       `json:"savedAt"`
	Entries []snapshotEntry `json:"entries"`
}

type snapshotEntry struct {
	Key            string    `json:"key"`
	Value          string    `json:"value"`
	ExpiresAt      time.Time `json:"expiresAt,omitempty"`
	CreatedAt      time.Time `json:"createdAt"`
	UpdatedAt      time.Time `json:"updatedAt"`
	LastAccessedAt time.Time `json:"lastAccessedAt"`
	AccessCount    int64     `json:"accessCount"`
}

func New(opts Options) *Cache {
	maxKeys := opts.MaxKeys
	if maxKeys <= 0 {
		maxKeys = 1000
	}
	cleanupInterval := opts.CleanupInterval
	if cleanupInterval <= 0 {
		cleanupInterval = time.Second
	}
	evictionPolicy := opts.EvictionPolicy
	if evictionPolicy == "" {
		evictionPolicy = EvictionLRU
	}
	if evictionPolicy != EvictionLRU && evictionPolicy != EvictionLFU {
		evictionPolicy = EvictionLRU
	}

	c := &Cache{
		items:          map[string]*entry{},
		lru:            list.New(),
		maxKeys:        maxKeys,
		evictionPolicy: evictionPolicy,
		recorder:       metrics.NewRecorder(opts.EventLogSize),
		snapshotPath:   opts.SnapshotPath,
		appendLogPath:  opts.AppendLogPath,
		stopCleanup:    make(chan struct{}),
	}
	if opts.SnapshotPath != "" {
		_ = c.loadSnapshot(opts.SnapshotPath)
	}
	if opts.AppendLogPath != "" {
		if file, err := os.OpenFile(opts.AppendLogPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644); err == nil {
			c.appendLog = file
		}
	}
	go c.cleanupLoop(cleanupInterval)
	return c
}

func (c *Cache) Close() {
	c.closeOnce.Do(func() {
		close(c.stopCleanup)
		if c.appendLog != nil {
			_ = c.appendLog.Close()
		}
	})
}

func (c *Cache) Set(_ context.Context, key, value string, ttl time.Duration) error {
	start := time.Now()
	defer func() {
		c.recorder.Record("set", time.Since(start), metrics.Event{
			Type:  metrics.EventSet,
			Key:   key,
			Value: value,
		})
	}()

	if key == "" {
		return ErrEmptyKey
	}
	if ttl < 0 {
		return ErrInvalidTTL
	}

	now := time.Now().UTC()
	c.mu.Lock()
	defer c.mu.Unlock()

	c.removeExpiredLocked(now)
	expiresAt := expiryFromTTL(now, ttl)
	size := estimateSize(key, value)

	if item, ok := c.items[key]; ok {
		c.memoryBytes += size - item.sizeBytes
		item.value = value
		item.expiresAt = expiresAt
		item.updatedAt = now
		item.lastAccessedAt = now
		item.sizeBytes = size
		c.lru.MoveToFront(item.lruElement)
		c.appendLogLocked(ReplayRecord{
			Command:    "set",
			Key:        key,
			Value:      value,
			TTLSeconds: int64(ttl.Seconds()),
			At:         now,
		})
		return nil
	}

	for len(c.items) >= c.maxKeys {
		victim := c.evictionVictimLocked()
		if victim == nil {
			break
		}
		c.removeItemLocked(victim, "max_keys")
	}

	item := &entry{
		key:            key,
		value:          value,
		expiresAt:      expiresAt,
		createdAt:      now,
		updatedAt:      now,
		lastAccessedAt: now,
		sizeBytes:      size,
	}
	item.lruElement = c.lru.PushFront(item)
	c.items[key] = item
	c.memoryBytes += size

	c.evictUntilWithinLimitLocked()
	c.appendLogLocked(ReplayRecord{
		Command:    "set",
		Key:        key,
		Value:      value,
		TTLSeconds: int64(ttl.Seconds()),
		At:         now,
	})
	return nil
}

func (c *Cache) Get(_ context.Context, key string) (GetResult, error) {
	start := time.Now()
	result := GetResult{Key: key}
	defer func() {
		hit := result.Hit
		c.recorder.Record("get", time.Since(start), metrics.Event{
			Type: metrics.EventGet,
			Key:  key,
			Hit:  &hit,
		})
	}()

	if key == "" {
		return result, ErrEmptyKey
	}

	now := time.Now().UTC()
	c.mu.Lock()
	defer c.mu.Unlock()

	item, ok := c.items[key]
	if !ok {
		return result, nil
	}
	if item.expired(now) {
		c.removeItemLocked(item, "expired")
		return result, nil
	}

	item.lastAccessedAt = now
	item.accessCount++
	c.lru.MoveToFront(item.lruElement)
	result.Value = item.value
	result.Hit = true
	return result, nil
}

func (c *Cache) Delete(_ context.Context, key string) (bool, error) {
	start := time.Now()
	defer func() {
		c.recorder.Record("delete", time.Since(start), metrics.Event{
			Type: metrics.EventDelete,
			Key:  key,
		})
	}()

	if key == "" {
		return false, ErrEmptyKey
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	item, ok := c.items[key]
	if !ok {
		return false, nil
	}
	c.removeItemLocked(item, "")
	c.appendLogLocked(ReplayRecord{
		Command: "del",
		Key:     key,
		At:      time.Now().UTC(),
	})
	return true, nil
}

func (c *Cache) Expire(_ context.Context, key string, ttl time.Duration) (bool, error) {
	start := time.Now()
	defer func() {
		c.recorder.Record("expire", time.Since(start), metrics.Event{
			Type: metrics.EventExpire,
			Key:  key,
		})
	}()

	if key == "" {
		return false, ErrEmptyKey
	}
	if ttl < 0 {
		return false, ErrInvalidTTL
	}

	now := time.Now().UTC()
	c.mu.Lock()
	defer c.mu.Unlock()

	item, ok := c.items[key]
	if !ok {
		return false, nil
	}
	if item.expired(now) {
		c.removeItemLocked(item, "expired")
		return false, nil
	}

	item.expiresAt = expiryFromTTL(now, ttl)
	item.updatedAt = now
	c.appendLogLocked(ReplayRecord{
		Command:    "expire",
		Key:        key,
		TTLSeconds: int64(ttl.Seconds()),
		At:         now,
	})
	return true, nil
}

func (c *Cache) TTL(_ context.Context, key string) (int64, error) {
	start := time.Now()
	defer func() {
		c.recorder.Record("ttl", time.Since(start), metrics.Event{
			Type: metrics.EventTTL,
			Key:  key,
		})
	}()

	if key == "" {
		return TTLMissing, ErrEmptyKey
	}

	now := time.Now().UTC()
	c.mu.Lock()
	defer c.mu.Unlock()

	item, ok := c.items[key]
	if !ok {
		return TTLMissing, nil
	}
	if item.expired(now) {
		c.removeItemLocked(item, "expired")
		return TTLMissing, nil
	}
	if item.expiresAt.IsZero() {
		return TTLNoExpiry, nil
	}

	remaining := int64(item.expiresAt.Sub(now).Seconds())
	if remaining < 0 {
		return TTLMissing, nil
	}
	if remaining == 0 {
		return 1, nil
	}
	return remaining, nil
}

func (c *Cache) SnapshotDebug() DebugSnapshot {
	return DebugSnapshot{Keys: c.SnapshotKeys(KeyQuery{})}
}

func (c *Cache) SnapshotKeys(query KeyQuery) []KeyDebug {
	now := time.Now().UTC()
	c.mu.Lock()
	defer c.mu.Unlock()

	c.removeExpiredLocked(now)

	keys := make([]KeyDebug, 0, len(c.items))
	for _, item := range c.items {
		debug := keyDebugFromEntry(item, now)
		if query.Filter == "" || strings.Contains(strings.ToLower(debug.Key), strings.ToLower(query.Filter)) {
			keys = append(keys, debug)
		}
	}
	sortKeys(keys, query)
	if query.Limit > 0 && len(keys) > query.Limit {
		keys = keys[:query.Limit]
	}
	return keys
}

func (c *Cache) LRUKeys() []string {
	now := time.Now().UTC()
	c.mu.Lock()
	defer c.mu.Unlock()

	c.removeExpiredLocked(now)

	keys := make([]string, 0, c.lru.Len())
	for el := c.lru.Front(); el != nil; el = el.Next() {
		keys = append(keys, el.Value.(*entry).key)
	}
	return keys
}

func (c *Cache) Metrics() metrics.Snapshot {
	c.mu.RLock()
	keyCount := len(c.items)
	memoryBytes := c.memoryBytes
	c.mu.RUnlock()

	return c.recorder.Snapshot(keyCount, memoryBytes)
}

func (c *Cache) Events() []metrics.Event {
	return c.recorder.Events()
}

func (c *Cache) Summary(query KeyQuery) Summary {
	keys := c.SnapshotKeys(query)
	return Summary{
		Metrics: c.Metrics(),
		Keys:    keys,
		LRU:     c.LRUKeys(),
		Events:  c.Events(),
	}
}

func (c *Cache) SaveSnapshot(path string) error {
	if path == "" {
		path = c.snapshotPath
	}
	if path == "" {
		return ErrSnapshotPath
	}

	now := time.Now().UTC()
	c.mu.Lock()
	defer c.mu.Unlock()

	c.removeExpiredLocked(now)
	file := snapshotFile{
		Version: 1,
		SavedAt: now,
		Entries: make([]snapshotEntry, 0, len(c.items)),
	}
	for _, item := range c.items {
		file.Entries = append(file.Entries, snapshotEntry{
			Key:            item.key,
			Value:          item.value,
			ExpiresAt:      item.expiresAt,
			CreatedAt:      item.createdAt,
			UpdatedAt:      item.updatedAt,
			LastAccessedAt: item.lastAccessedAt,
			AccessCount:    item.accessCount,
		})
	}

	data, err := json.MarshalIndent(file, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
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
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	records := make([]ReplayRecord, 0, len(lines))
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var record ReplayRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func (c *Cache) cleanupLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.cleanupExpired()
		case <-c.stopCleanup:
			return
		}
	}
}

func (c *Cache) cleanupExpired() {
	start := time.Now()
	now := start.UTC()

	c.mu.Lock()
	removed := c.removeExpiredLocked(now)
	c.mu.Unlock()

	if removed > 0 {
		c.recorder.Record("cleanup", time.Since(start), metrics.Event{
			Type:   metrics.EventCleanup,
			Reason: "expired",
		})
	}
}

func (c *Cache) removeExpiredLocked(now time.Time) int {
	removed := 0
	for _, item := range c.items {
		if item.expired(now) {
			c.removeItemLocked(item, "expired")
			removed++
		}
	}
	return removed
}

func (c *Cache) evictUntilWithinLimitLocked() {
	for len(c.items) > c.maxKeys {
		item := c.evictionVictimLocked()
		if item == nil {
			return
		}
		c.removeItemLocked(item, "max_keys")
	}
}

func (c *Cache) evictionVictimLocked() *entry {
	if c.evictionPolicy == EvictionLFU {
		var victim *entry
		for _, item := range c.items {
			if victim == nil ||
				item.accessCount < victim.accessCount ||
				(item.accessCount == victim.accessCount && item.lastAccessedAt.Before(victim.lastAccessedAt)) {
				victim = item
			}
		}
		return victim
	}
	el := c.lru.Back()
	if el == nil {
		return nil
	}
	return el.Value.(*entry)
}

func (c *Cache) removeItemLocked(item *entry, reason string) {
	delete(c.items, item.key)
	c.lru.Remove(item.lruElement)
	c.memoryBytes -= item.sizeBytes
	if c.memoryBytes < 0 {
		c.memoryBytes = 0
	}
	if reason == "max_keys" {
		c.recorder.RecordEviction(item.key, reason)
	}
}

func (c *Cache) appendLogLocked(record ReplayRecord) {
	if c.appendLog == nil {
		return
	}
	data, err := json.Marshal(record)
	if err != nil {
		return
	}
	_, _ = c.appendLog.Write(append(data, '\n'))
}

func (c *Cache) loadSnapshot(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	var file snapshotFile
	if err := json.Unmarshal(data, &file); err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, saved := range file.Entries {
		if !saved.ExpiresAt.IsZero() && !saved.ExpiresAt.After(now) {
			continue
		}
		size := estimateSize(saved.Key, saved.Value)
		item := &entry{
			key:            saved.Key,
			value:          saved.Value,
			expiresAt:      saved.ExpiresAt,
			createdAt:      saved.CreatedAt,
			updatedAt:      saved.UpdatedAt,
			lastAccessedAt: saved.LastAccessedAt,
			accessCount:    saved.AccessCount,
			sizeBytes:      size,
		}
		item.lruElement = c.lru.PushFront(item)
		c.items[item.key] = item
		c.memoryBytes += size
	}
	c.evictUntilWithinLimitLocked()
	return nil
}

func (e *entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && !e.expiresAt.After(now)
}

func expiryFromTTL(now time.Time, ttl time.Duration) time.Time {
	if ttl == 0 {
		return time.Time{}
	}
	return now.Add(ttl)
}

func estimateSize(key, value string) int64 {
	return int64(len(key) + len(value))
}

func keyDebugFromEntry(item *entry, now time.Time) KeyDebug {
	debug := KeyDebug{
		Key:            item.key,
		SizeBytes:      item.sizeBytes,
		TTL:            TTLNoExpiry,
		CreatedAt:      item.createdAt,
		UpdatedAt:      item.updatedAt,
		LastAccessedAt: item.lastAccessedAt,
		AccessCount:    item.accessCount,
	}
	if !item.expiresAt.IsZero() {
		expiresAt := item.expiresAt
		debug.ExpiresAt = &expiresAt
		ttl := int64(item.expiresAt.Sub(now).Seconds())
		if ttl <= 0 {
			ttl = 1
		}
		debug.TTL = ttl
	}
	return debug
}

func sortKeys(keys []KeyDebug, query KeyQuery) {
	sortBy := query.Sort
	if sortBy == "" {
		sortBy = "key"
	}
	sort.Slice(keys, func(i, j int) bool {
		var less bool
		switch sortBy {
		case "ttl":
			less = keys[i].TTL < keys[j].TTL
		case "size":
			less = keys[i].SizeBytes < keys[j].SizeBytes
		case "access":
			less = keys[i].AccessCount < keys[j].AccessCount
		case "updated":
			less = keys[i].UpdatedAt.Before(keys[j].UpdatedAt)
		case "accessed":
			less = keys[i].LastAccessedAt.Before(keys[j].LastAccessedAt)
		default:
			less = keys[i].Key < keys[j].Key
		}
		if query.Desc {
			return !less
		}
		return less
	})
}
