package cache

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nkmohit/tinycache/internal/metrics"
)

var (
	ErrEmptyKey       = errors.New("key cannot be empty")
	ErrInvalidTTL     = errors.New("ttl cannot be negative")
	ErrInvalidPolicy  = errors.New("eviction policy must be lru, lfu, allkeys-lru, allkeys-lfu, allkeys-random, volatile-ttl, or noeviction")
	ErrSnapshotPath   = errors.New("snapshot path is not configured")
	ErrAppendLogPath  = errors.New("append-only log path is not configured")
	ErrInvalidKeySort = errors.New("sort must be key, ttl, size, access, updated, or accessed")
	// ErrOOM is returned when a write needs room and the policy cannot free any.
	// noeviction always returns it at the limit. volatile-ttl returns it when the
	// shard has no expiring key to remove. A value larger than the shard's byte
	// budget returns it without deleting other keys.
	ErrOOM = errors.New("OOM command not allowed when cache exceeds maxkeys or maxmemory")
)

const (
	TTLNoExpiry = -1
	TTLMissing  = -2

	// Active expiry matches Redis: sample a handful of keys that actually
	// have a TTL, and keep sampling while more than a quarter of them are
	// already expired. The budget stops one cycle from running without bound.
	expireSampleSize  = 20
	expireStaleRatio  = 4 // repeat while removed/sampled > 1/expireStaleRatio (25%)
	expireCycleBudget = 25 * time.Millisecond

	// shardCount is the number of independent key locks. A power of two lets
	// the key hash pick a shard with a mask. GETs of different keys take
	// different locks; a GET still updates recency, so it takes that shard's
	// write lock rather than a read lock.
	shardCount = 16

	// defaultMaxMemorySamples matches Redis maxmemory-samples.
	defaultMaxMemorySamples = 5

	// perEntryOverhead is the fixed part of the byte estimate: the entry,
	// its map slot, and the LRU list element. Key and value bytes are added
	// on top. It is an approximation, not a read of the heap.
	perEntryOverhead = 128

	// Redis LFU: new keys start at lfuInitVal. A hit increments the 8-bit
	// counter with probability 1/(base*lfuLogFactor+1), where base is how far
	// the counter sits above the init value. The counter then drops by one
	// for each idle minute (Redis lfu-decay-time 1, lfu-log-factor 10).
	lfuInitVal   = 5
	lfuLogFactor = 10
)

type EvictionPolicy string

const (
	// EvictionLRU and EvictionLFU are exact inside the key's shard.
	EvictionLRU EvictionPolicy = "lru"
	EvictionLFU EvictionPolicy = "lfu"
	// EvictionAllKeysLRU samples MaxMemorySamples keys and evicts the oldest
	// LRU clock (Unix seconds of the last access).
	EvictionAllKeysLRU EvictionPolicy = "allkeys-lru"
	// EvictionAllKeysLFU samples keys and evicts the lowest logarithmic counter.
	EvictionAllKeysLFU EvictionPolicy = "allkeys-lfu"
	// EvictionAllKeysRandom evicts one random key in the shard.
	EvictionAllKeysRandom EvictionPolicy = "allkeys-random"
	// EvictionVolatileTTL samples keys that have a TTL and evicts the soonest.
	// Keys with no expiry are never victims. If none can be evicted, Set
	// returns ErrOOM.
	EvictionVolatileTTL EvictionPolicy = "volatile-ttl"
	// EvictionNoEviction refuses the write with ErrOOM when the shard is full.
	EvictionNoEviction EvictionPolicy = "noeviction"
)

// ParseEvictionPolicy accepts the policy names. An empty string is exact LRU.
func ParseEvictionPolicy(value string) (EvictionPolicy, error) {
	switch EvictionPolicy(strings.ToLower(strings.TrimSpace(value))) {
	case "":
		return EvictionLRU, nil
	case EvictionLRU, EvictionLFU, EvictionAllKeysLRU, EvictionAllKeysLFU, EvictionAllKeysRandom, EvictionVolatileTTL, EvictionNoEviction:
		return EvictionPolicy(strings.ToLower(strings.TrimSpace(value))), nil
	default:
		return "", ErrInvalidPolicy
	}
}

type Options struct {
	MaxKeys         int
	CleanupInterval time.Duration
	EventLogSize    int
	EvictionPolicy  EvictionPolicy
	SnapshotPath    string
	AppendLogPath   string
	// AppendFsync is always, everysec, or no. Empty means everysec.
	AppendFsync FsyncPolicy
	// MaxMemory is a byte budget split across shards. Zero means no byte limit.
	// The key-count limit still applies.
	MaxMemory int64
	// MaxMemorySamples is Redis maxmemory-samples: how many keys an approximate
	// policy looks at before it picks a victim. Zero means the default of 5.
	// Exact lru and lfu ignore it.
	MaxMemorySamples int
}

type Cache struct {
	shards [shardCount]shard

	samples        int
	evictionPolicy EvictionPolicy
	memoryBytes    atomic.Int64
	keyCount       atomic.Int64
	recorder       *metrics.Recorder

	snapshotPath  string
	appendLogPath string
	aof           *appendLogger
	aofDone       chan struct{}

	stopCleanup chan struct{}
	closeOnce   sync.Once
}

// shard owns the keys whose hash lands here, plus that subset's LRU list,
// TTL sample set, and LFU frequency buckets. maxKeys and maxMemory are this
// shard's share of the cache limits. Eviction looks only at this shard, and
// only while mu is held.
type shard struct {
	mu    sync.Mutex
	items map[string]*entry
	lru   *list.List

	maxKeys   int
	maxMemory int64
	bytes     int64

	// keys is every resident entry when the policy samples the keyspace.
	// keyIndex makes swap-remove O(1). Exact lru and lfu leave both empty
	// and evict from the list or the frequency buckets instead.
	keys     []*entry
	keyIndex map[*entry]int
	// lfuMeta holds the logarithmic counter for allkeys-lfu. Exact LFU uses
	// accessCount and freqBuckets, so it does not allocate this map.
	lfuMeta map[*entry]lfuState
	rng     *rand.Rand

	// ttlKeys is the unordered set of keys that have an expiry. The slice
	// makes a random sample O(1); ttlIndex maps a key to its slice slot so
	// insert and delete are O(1) as well. Keys with no TTL are not stored.
	ttlKeys  []string
	ttlIndex map[string]int

	// freqBuckets is one doubly linked list per access count. The list head
	// was touched most recently. minFreq is the smallest count that still
	// has a key, so exact LFU pops the tail of that list in O(1).
	freqBuckets map[int64]*list.List
	minFreq     int64
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
	lfuElement     *list.Element
}

// lfuState is the Redis logarithmic frequency counter and the minute it was
// last decayed. It lives beside the entry so exact LRU and LFU keys stay the
// same size they were before approximate policies existed.
type lfuState struct {
	log   uint8
	decay int64
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
	evictionPolicy, err := ParseEvictionPolicy(string(opts.EvictionPolicy))
	if err != nil {
		log.Printf("tinycache eviction: %v", err)
		evictionPolicy = EvictionLRU
	}
	samples := opts.MaxMemorySamples
	if samples <= 0 {
		samples = defaultMaxMemorySamples
	}
	fsyncPolicy, err := ParseFsyncPolicy(string(opts.AppendFsync))
	if err != nil {
		log.Printf("tinycache aof: %v", err)
		fsyncPolicy = FsyncEverySec
	}

	c := &Cache{
		samples:        samples,
		evictionPolicy: evictionPolicy,
		recorder:       metrics.NewRecorder(opts.EventLogSize),
		snapshotPath:   opts.SnapshotPath,
		appendLogPath:  opts.AppendLogPath,
		aof:            openAppendLogger(opts.AppendLogPath, fsyncPolicy),
		stopCleanup:    make(chan struct{}),
	}
	keyCaps := splitCapacity(int64(maxKeys))
	var memCaps [shardCount]int64
	if opts.MaxMemory > 0 {
		memCaps = splitCapacity(opts.MaxMemory)
	}
	seed := time.Now().UnixNano()
	for i := range c.shards {
		c.shards[i].items = map[string]*entry{}
		c.shards[i].lru = list.New()
		c.shards[i].ttlIndex = map[string]int{}
		c.shards[i].freqBuckets = map[int64]*list.List{}
		c.shards[i].maxKeys = int(keyCaps[i])
		c.shards[i].maxMemory = memCaps[i]
		c.shards[i].rng = rand.New(rand.NewSource(seed + int64(i)*1_000_003))
	}
	if opts.SnapshotPath != "" {
		_ = c.loadSnapshot(opts.SnapshotPath)
	}
	if c.aof.runsBackgroundSync() {
		c.aofDone = make(chan struct{})
		go func() {
			defer close(c.aofDone)
			c.aof.everySecLoop(c.stopCleanup)
		}()
	}
	go c.cleanupLoop(cleanupInterval)
	return c
}

func (c *Cache) Close() {
	c.closeOnce.Do(func() {
		close(c.stopCleanup)
		if c.aofDone != nil {
			<-c.aofDone
		}
		if err := c.aof.close(); err != nil {
			log.Printf("tinycache aof: close: %v", err)
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
	expiresAt := expiryFromTTL(now, ttl)
	size := estimateSize(key, value)
	record := ReplayRecord{
		Command:    "set",
		Key:        key,
		Value:      value,
		TTLSeconds: int64(ttl.Seconds()),
		At:         now,
	}

	s := c.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	if item, ok := s.items[key]; ok {
		if err := c.updateExisting(s, item, value, expiresAt, size, now); err != nil {
			return err
		}
		return c.aof.write(record)
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
	if err := c.ensureRoom(s, size, 1, nil); err != nil {
		return err
	}
	c.insertItem(s, item)
	return c.aof.write(record)
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
	s := c.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.items[key]
	if !ok {
		return result, nil
	}
	if item.expired(now) {
		c.removeItem(s, item, "expired")
		return result, nil
	}

	item.lastAccessedAt = now
	s.lru.MoveToFront(item.lruElement)
	if c.evictionPolicy == EvictionLFU {
		s.promoteLFU(item)
	} else {
		item.accessCount++
		if c.evictionPolicy == EvictionAllKeysLFU {
			st := s.lfuMeta[item]
			st.log = lfuLogIncr(st.decayed(now), s.rng)
			st.decay = now.Unix() / 60
			s.lfuMeta[item] = st
		}
	}
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

	s := c.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.items[key]
	if !ok {
		return false, nil
	}
	c.removeItem(s, item, "")
	if err := c.aof.write(ReplayRecord{
		Command: "del",
		Key:     key,
		At:      time.Now().UTC(),
	}); err != nil {
		return true, err
	}
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
	s := c.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.items[key]
	if !ok {
		return false, nil
	}
	if item.expired(now) {
		c.removeItem(s, item, "expired")
		return false, nil
	}

	item.expiresAt = expiryFromTTL(now, ttl)
	item.updatedAt = now
	s.trackTTL(item)
	if err := c.aof.write(ReplayRecord{
		Command:    "expire",
		Key:        key,
		TTLSeconds: int64(ttl.Seconds()),
		At:         now,
	}); err != nil {
		return true, err
	}
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
	s := c.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.items[key]
	if !ok {
		return TTLMissing, nil
	}
	if item.expired(now) {
		c.removeItem(s, item, "expired")
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
	c.lockAll()
	defer c.unlockAll()

	keys := make([]KeyDebug, 0)
	for i := range c.shards {
		s := &c.shards[i]
		s.removeExpired(now, c)
		for _, item := range s.items {
			debug := keyDebugFromEntry(item, now)
			if query.Filter == "" || strings.Contains(strings.ToLower(debug.Key), strings.ToLower(query.Filter)) {
				keys = append(keys, debug)
			}
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
	c.lockAll()

	type stamped struct {
		key string
		at  time.Time
	}
	stampedKeys := make([]stamped, 0)
	for i := range c.shards {
		s := &c.shards[i]
		s.removeExpired(now, c)
		for el := s.lru.Front(); el != nil; el = el.Next() {
			item := el.Value.(*entry)
			stampedKeys = append(stampedKeys, stamped{key: item.key, at: item.lastAccessedAt})
		}
	}
	c.unlockAll()

	sort.Slice(stampedKeys, func(i, j int) bool {
		if stampedKeys[i].at.Equal(stampedKeys[j].at) {
			return stampedKeys[i].key < stampedKeys[j].key
		}
		return stampedKeys[i].at.After(stampedKeys[j].at)
	})
	keys := make([]string, len(stampedKeys))
	for i, item := range stampedKeys {
		keys[i] = item.key
	}
	return keys
}

func (c *Cache) Metrics() metrics.Snapshot {
	return c.recorder.Snapshot(int(c.keyCount.Load()), c.memoryBytes.Load())
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
	c.lockAll()
	entries := make([]snapshotEntry, 0)
	for i := range c.shards {
		s := &c.shards[i]
		s.removeExpired(now, c)
		for _, item := range s.items {
			entries = append(entries, snapshotEntry{
				Key:            item.key,
				Value:          item.value,
				ExpiresAt:      item.expiresAt,
				CreatedAt:      item.createdAt,
				UpdatedAt:      item.updatedAt,
				LastAccessedAt: item.lastAccessedAt,
				AccessCount:    item.accessCount,
			})
		}
	}
	c.unlockAll()

	data, err := json.MarshalIndent(snapshotFile{
		Version: 1,
		SavedAt: now,
		Entries: entries,
	}, "", "  ")
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

// cleanupExpired samples keys that have a TTL instead of scanning the keyspace.
// Each shard is sampled under its own lock, then the lock is dropped so a long
// cycle cannot stall Get and Set. Sampling repeats while more than 25% of a
// shard's sample was expired, or until expireCycleBudget elapses.
func (c *Cache) cleanupExpired() {
	start := time.Now()
	deadline := start.Add(expireCycleBudget)
	removedTotal := 0
	for {
		stale := false
		for i := range c.shards {
			s := &c.shards[i]
			s.mu.Lock()
			removed, sampled := s.expireSample(time.Now().UTC(), c)
			s.mu.Unlock()
			removedTotal += removed
			if sampled > 0 && removed*expireStaleRatio > sampled {
				stale = true
			}
		}
		if !stale || !time.Now().Before(deadline) {
			break
		}
	}

	if removedTotal > 0 {
		c.recorder.Record("cleanup", time.Since(start), metrics.Event{
			Type:   metrics.EventCleanup,
			Reason: "expired",
		})
	}
}

func (c *Cache) shard(key string) *shard {
	return &c.shards[shardIndex(key)]
}

func shardIndex(key string) int {
	// FNV-1a, 32-bit. Inlined so a GET does not allocate a hasher.
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return int(h & (shardCount - 1))
}

func (c *Cache) lockAll() {
	for i := range c.shards {
		c.shards[i].mu.Lock()
	}
}

func (c *Cache) unlockAll() {
	for i := len(c.shards) - 1; i >= 0; i-- {
		c.shards[i].mu.Unlock()
	}
}

// splitCapacity divides total across the shards. The first remainder shards
// get one extra unit, so the shares sum to total. Zero stays zero on every
// shard: for bytes that means "no limit", and for keys New never passes zero
// because a non-positive MaxKeys becomes the default first.
func splitCapacity(total int64) [shardCount]int64 {
	var out [shardCount]int64
	if total <= 0 {
		return out
	}
	base := total / shardCount
	rem := total % shardCount
	for i := 0; i < shardCount; i++ {
		out[i] = base
		if int64(i) < rem {
			out[i]++
		}
	}
	return out
}

// ensureRoom frees this shard until a write of `adding` new keys and `extra`
// more bytes fits. keep is the key being updated and is not a victim. The
// caller holds s.mu and no other shard lock.
func (c *Cache) ensureRoom(s *shard, extra int64, adding int, keep *entry) error {
	if s.maxMemory > 0 {
		projected := extra
		if adding == 0 && keep != nil {
			projected = keep.sizeBytes + extra
		}
		if projected > s.maxMemory {
			return ErrOOM
		}
	}
	for s.exceedsKeys(adding) || s.exceedsMemory(extra) {
		if c.evictionPolicy == EvictionNoEviction {
			return ErrOOM
		}
		victim := s.pickVictim(c, keep)
		if victim == nil {
			return ErrOOM
		}
		reason := "maxmemory"
		if s.exceedsKeys(adding) {
			reason = "max_keys"
		}
		c.removeItem(s, victim, reason)
	}
	return nil
}

func (s *shard) exceedsKeys(adding int) bool {
	return len(s.items)+adding > s.maxKeys
}

func (s *shard) exceedsMemory(extra int64) bool {
	if s.maxMemory <= 0 {
		return false
	}
	return s.bytes+extra > s.maxMemory
}

func (c *Cache) updateExisting(s *shard, item *entry, value string, expiresAt time.Time, size int64, now time.Time) error {
	delta := size - item.sizeBytes
	if err := c.ensureRoom(s, delta, 0, item); err != nil {
		return err
	}
	c.updateItem(s, item, value, expiresAt, size, now)
	return nil
}

func (c *Cache) updateItem(s *shard, item *entry, value string, expiresAt time.Time, size int64, now time.Time) {
	c.addMemory(s, size-item.sizeBytes)
	item.value = value
	item.expiresAt = expiresAt
	item.updatedAt = now
	item.lastAccessedAt = now
	item.sizeBytes = size
	s.lru.MoveToFront(item.lruElement)
	s.trackTTL(item)
	if c.evictionPolicy == EvictionLFU {
		s.touchLFU(item)
	}
}

func (c *Cache) samplesKeys() bool {
	switch c.evictionPolicy {
	case EvictionAllKeysLRU, EvictionAllKeysLFU, EvictionAllKeysRandom:
		return true
	default:
		return false
	}
}

func (c *Cache) insertItem(s *shard, item *entry) {
	item.lruElement = s.lru.PushFront(item)
	if c.samplesKeys() {
		s.trackKey(item)
	}
	if c.evictionPolicy == EvictionAllKeysLFU {
		if s.lfuMeta == nil {
			s.lfuMeta = map[*entry]lfuState{}
		}
		decay := item.lastAccessedAt.Unix() / 60
		s.lfuMeta[item] = lfuState{log: lfuInitVal, decay: decay}
	}
	s.items[item.key] = item
	c.addMemory(s, item.sizeBytes)
	c.keyCount.Add(1)
	s.trackTTL(item)
	if c.evictionPolicy == EvictionLFU {
		s.linkLFU(item)
	}
}

func (c *Cache) addMemory(s *shard, delta int64) {
	if delta == 0 {
		return
	}
	s.bytes += delta
	if s.bytes < 0 {
		s.bytes = 0
	}
	for {
		cur := c.memoryBytes.Load()
		next := cur + delta
		if next < 0 {
			next = 0
		}
		if c.memoryBytes.CompareAndSwap(cur, next) {
			return
		}
	}
}

func (s *shard) expireSample(now time.Time, c *Cache) (removed, sampled int) {
	n := len(s.ttlKeys)
	if n == 0 {
		return 0, 0
	}
	sampled = expireSampleSize
	if sampled > n {
		sampled = n
	}

	var victims [expireSampleSize]*entry
	for i := 0; i < sampled; i++ {
		j := i + rand.Intn(n-i)
		if i != j {
			s.swapTTL(i, j)
		}
		item := s.items[s.ttlKeys[i]]
		if item != nil && item.expired(now) {
			victims[removed] = item
			removed++
		}
	}
	for i := 0; i < removed; i++ {
		c.removeItem(s, victims[i], "expired")
	}
	return removed, sampled
}

func (s *shard) trackTTL(item *entry) {
	hasTTL := !item.expiresAt.IsZero()
	_, tracked := s.ttlIndex[item.key]
	if hasTTL == tracked {
		return
	}
	if !hasTTL {
		s.untrackTTL(item.key)
		return
	}
	s.ttlIndex[item.key] = len(s.ttlKeys)
	s.ttlKeys = append(s.ttlKeys, item.key)
}

func (s *shard) untrackTTL(key string) {
	idx, ok := s.ttlIndex[key]
	if !ok {
		return
	}
	last := len(s.ttlKeys) - 1
	moved := s.ttlKeys[last]
	s.ttlKeys[idx] = moved
	s.ttlIndex[moved] = idx
	s.ttlKeys = s.ttlKeys[:last]
	delete(s.ttlIndex, key)
}

func (s *shard) swapTTL(i, j int) {
	s.ttlKeys[i], s.ttlKeys[j] = s.ttlKeys[j], s.ttlKeys[i]
	s.ttlIndex[s.ttlKeys[i]] = i
	s.ttlIndex[s.ttlKeys[j]] = j
}

func (s *shard) removeExpired(now time.Time, c *Cache) {
	for _, item := range s.items {
		if item.expired(now) {
			c.removeItem(s, item, "expired")
		}
	}
}

// pickVictim chooses one key in this shard. keep, when set, is the entry a
// size update is rewriting and must stay. Approximate policies sample; exact
// lru and lfu take the real tail.
func (s *shard) pickVictim(c *Cache, keep *entry) *entry {
	now := time.Now().UTC()
	var buf [8]*entry
	switch c.evictionPolicy {
	case EvictionNoEviction:
		return nil
	case EvictionLFU:
		return s.lfuVictim(keep)
	case EvictionAllKeysRandom:
		return s.randomVictim(keep)
	case EvictionAllKeysLRU:
		return oldestClock(s.sampleKeys(c.samples, keep, buf[:0]), keep)
	case EvictionAllKeysLFU:
		return s.coldestLFU(s.sampleKeys(c.samples, keep, buf[:0]), keep, now)
	case EvictionVolatileTTL:
		return soonestTTL(s.sampleTTL(c.samples, keep, buf[:0]), keep)
	default:
		return s.lruVictim(keep)
	}
}

func (s *shard) lruVictim(keep *entry) *entry {
	for el := s.lru.Back(); el != nil; el = el.Prev() {
		item := el.Value.(*entry)
		if item != keep {
			return item
		}
	}
	return nil
}

func (s *shard) lfuVictim(keep *entry) *entry {
	if keep != nil && keep.lfuElement != nil {
		s.unlinkLFU(keep)
		defer s.linkLFU(keep)
	}
	bucket := s.freqBuckets[s.minFreq]
	if bucket == nil || bucket.Len() == 0 {
		s.recomputeMinFreq()
		bucket = s.freqBuckets[s.minFreq]
		if bucket == nil || bucket.Len() == 0 {
			return nil
		}
	}
	return bucket.Back().Value.(*entry)
}

func (s *shard) randomVictim(keep *entry) *entry {
	n := len(s.keys)
	if n == 0 || (n == 1 && s.keys[0] == keep) {
		return nil
	}
	if keep == nil {
		return s.keys[s.rng.Intn(n)]
	}
	for tries := 0; tries < 8; tries++ {
		item := s.keys[s.rng.Intn(n)]
		if item != keep {
			return item
		}
	}
	for _, item := range s.keys {
		if item != keep {
			return item
		}
	}
	return nil
}

// sampleKeys returns k distinct resident entries. It partially shuffles keys
// so the sample is uniform and allocation-free for the default sample size.
func (s *shard) sampleKeys(k int, keep *entry, dst []*entry) []*entry {
	return s.sampleSlice(len(s.keys), k, keep, dst, func(i, j int) {
		if i == j {
			return
		}
		s.keys[i], s.keys[j] = s.keys[j], s.keys[i]
		s.keyIndex[s.keys[i]] = i
		s.keyIndex[s.keys[j]] = j
	}, func(i int) *entry {
		return s.keys[i]
	})
}

func (s *shard) sampleTTL(k int, keep *entry, dst []*entry) []*entry {
	return s.sampleSlice(len(s.ttlKeys), k, keep, dst, func(i, j int) {
		if i != j {
			s.swapTTL(i, j)
		}
	}, func(i int) *entry {
		return s.items[s.ttlKeys[i]]
	})
}

func (s *shard) sampleSlice(n, k int, keep *entry, dst []*entry, swap func(i, j int), at func(i int) *entry) []*entry {
	if n == 0 || k <= 0 {
		return nil
	}
	if keep != nil && k < 2 {
		k = 2
	}
	if k > n {
		k = n
	}
	if cap(dst) < k {
		dst = make([]*entry, 0, k)
	}
	dst = dst[:0]
	for i := 0; i < k; i++ {
		j := i
		if span := n - i; span > 1 {
			j = i + s.rng.Intn(span)
		}
		swap(i, j)
		if item := at(i); item != nil {
			dst = append(dst, item)
		}
	}
	return dst
}

func oldestClock(sample []*entry, keep *entry) *entry {
	var best *entry
	for _, item := range sample {
		if item == nil || item == keep {
			continue
		}
		if best == nil || olderClock(item, best) {
			best = item
		}
	}
	return best
}

// olderClock compares the Redis LRU clock: Unix seconds of the last access.
// A same-second tie uses the full timestamp, which is what exact LRU's list
// already ordered.
func olderClock(a, b *entry) bool {
	as := a.lastAccessedAt.Unix()
	bs := b.lastAccessedAt.Unix()
	if as != bs {
		return as < bs
	}
	return a.lastAccessedAt.Before(b.lastAccessedAt)
}

func (s *shard) coldestLFU(sample []*entry, keep *entry, now time.Time) *entry {
	var best *entry
	var bestCounter uint8
	for _, item := range sample {
		if item == nil || item == keep {
			continue
		}
		st := s.lfuMeta[item]
		counter := st.decayed(now)
		s.lfuMeta[item] = st
		if best == nil || counter < bestCounter || (counter == bestCounter && olderClock(item, best)) {
			best = item
			bestCounter = counter
		}
	}
	return best
}

func soonestTTL(sample []*entry, keep *entry) *entry {
	var best *entry
	for _, item := range sample {
		if item == nil || item == keep || item.expiresAt.IsZero() {
			continue
		}
		if best == nil || item.expiresAt.Before(best.expiresAt) {
			best = item
		}
	}
	return best
}

func (st *lfuState) decayed(now time.Time) uint8 {
	if st.decay == 0 {
		return st.log
	}
	nowMin := now.Unix() / 60
	if nowMin <= st.decay {
		return st.log
	}
	elapsed := nowMin - st.decay
	if elapsed >= int64(st.log) {
		st.log = 0
	} else {
		st.log -= uint8(elapsed)
	}
	st.decay = nowMin
	return st.log
}

func lfuLogIncr(counter uint8, rng *rand.Rand) uint8 {
	if counter == 255 {
		return 255
	}
	base := int(counter) - lfuInitVal
	if base < 0 {
		base = 0
	}
	if rng.Intn(base*lfuLogFactor+1) == 0 {
		return counter + 1
	}
	return counter
}

func (s *shard) trackKey(item *entry) {
	if s.keyIndex == nil {
		s.keyIndex = map[*entry]int{}
	}
	s.keyIndex[item] = len(s.keys)
	s.keys = append(s.keys, item)
}

func (s *shard) untrackKey(item *entry) {
	if s.keyIndex == nil {
		return
	}
	i, ok := s.keyIndex[item]
	if !ok {
		return
	}
	last := len(s.keys) - 1
	moved := s.keys[last]
	s.keys[i] = moved
	s.keyIndex[moved] = i
	s.keys = s.keys[:last]
	delete(s.keyIndex, item)
}

func (c *Cache) removeItem(s *shard, item *entry, reason string) {
	s.untrackKey(item)
	delete(s.lfuMeta, item)
	s.untrackTTL(item.key)
	if c.evictionPolicy == EvictionLFU {
		s.unlinkLFU(item)
	}
	delete(s.items, item.key)
	if item.lruElement != nil {
		s.lru.Remove(item.lruElement)
		item.lruElement = nil
	}
	c.addMemory(s, -item.sizeBytes)
	c.keyCount.Add(-1)
	if reason == "max_keys" || reason == "maxmemory" {
		c.recorder.RecordEviction(item.key, reason)
	}
}

func (s *shard) linkLFU(item *entry) {
	bucket := s.freqBuckets[item.accessCount]
	if bucket == nil {
		bucket = list.New()
		s.freqBuckets[item.accessCount] = bucket
	}
	item.lfuElement = bucket.PushFront(item)
	if len(s.freqBuckets) == 1 && bucket.Len() == 1 {
		s.minFreq = item.accessCount
		return
	}
	if item.accessCount < s.minFreq {
		s.minFreq = item.accessCount
	}
}

// touchLFU moves a key to the front of its current frequency list without
// counting another hit. SET uses that so a rewrite loses a same-frequency tie.
func (s *shard) touchLFU(item *entry) {
	s.unlinkLFU(item)
	s.linkLFU(item)
}

func (s *shard) promoteLFU(item *entry) {
	s.unlinkLFU(item)
	item.accessCount++
	s.linkLFU(item)
}

func (s *shard) unlinkLFU(item *entry) {
	if item.lfuElement == nil {
		return
	}
	freq := item.accessCount
	bucket := s.freqBuckets[freq]
	if bucket != nil {
		bucket.Remove(item.lfuElement)
	}
	item.lfuElement = nil
	if bucket == nil || bucket.Len() == 0 {
		delete(s.freqBuckets, freq)
		if freq == s.minFreq {
			s.minFreq++
			if _, ok := s.freqBuckets[s.minFreq]; !ok {
				s.recomputeMinFreq()
			}
		}
	}
}

func (s *shard) recomputeMinFreq() {
	if len(s.freqBuckets) == 0 {
		s.minFreq = 0
		return
	}
	first := true
	var min int64
	for freq := range s.freqBuckets {
		if first || freq < min {
			min = freq
			first = false
		}
	}
	s.minFreq = min
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
	c.lockAll()
	defer c.unlockAll()
	for _, saved := range file.Entries {
		if !saved.ExpiresAt.IsZero() && !saved.ExpiresAt.After(now) {
			continue
		}
		s := c.shard(saved.Key)
		if _, exists := s.items[saved.Key]; exists {
			continue
		}
		item := &entry{
			key:            saved.Key,
			value:          saved.Value,
			expiresAt:      saved.ExpiresAt,
			createdAt:      saved.CreatedAt,
			updatedAt:      saved.UpdatedAt,
			lastAccessedAt: saved.LastAccessedAt,
			accessCount:    saved.AccessCount,
			sizeBytes:      estimateSize(saved.Key, saved.Value),
		}
		if err := c.ensureRoom(s, item.sizeBytes, 1, nil); err != nil {
			continue
		}
		c.insertItem(s, item)
	}
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
	return int64(len(key)+len(value)) + perEntryOverhead
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
