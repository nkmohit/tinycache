package cache

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/mohneeru/tinycache/internal/metrics"
)

var (
	ErrEmptyKey    = errors.New("key cannot be empty")
	ErrInvalidTTL = errors.New("ttl cannot be negative")
)

const (
	TTLNoExpiry = -1
	TTLMissing  = -2
)

type Options struct {
	MaxKeys         int
	CleanupInterval time.Duration
	EventLogSize    int
}

type Cache struct {
	mu sync.RWMutex

	items map[string]*entry
	lru   *list.List

	maxKeys     int
	memoryBytes int64
	recorder    *metrics.Recorder

	stopCleanup chan struct{}
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

func New(opts Options) *Cache {
	maxKeys := opts.MaxKeys
	if maxKeys <= 0 {
		maxKeys = 1000
	}
	cleanupInterval := opts.CleanupInterval
	if cleanupInterval <= 0 {
		cleanupInterval = time.Second
	}

	c := &Cache{
		items:       map[string]*entry{},
		lru:         list.New(),
		maxKeys:     maxKeys,
		recorder:    metrics.NewRecorder(opts.EventLogSize),
		stopCleanup: make(chan struct{}),
	}
	go c.cleanupLoop(cleanupInterval)
	return c
}

func (c *Cache) Close() {
	close(c.stopCleanup)
}

func (c *Cache) Set(_ context.Context, key, value string, ttl time.Duration) error {
	start := time.Now()
	defer func() {
		c.recorder.Record("set", time.Since(start), metrics.Event{
			Type: metrics.EventSet,
			Key:  key,
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
		return nil
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
	now := time.Now().UTC()
	c.mu.Lock()
	defer c.mu.Unlock()

	c.removeExpiredLocked(now)

	keys := make([]KeyDebug, 0, len(c.items))
	for _, item := range c.items {
		keys = append(keys, keyDebugFromEntry(item, now))
	}
	return DebugSnapshot{Keys: keys}
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
		el := c.lru.Back()
		if el == nil {
			return
		}
		c.removeItemLocked(el.Value.(*entry), "max_keys")
	}
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
