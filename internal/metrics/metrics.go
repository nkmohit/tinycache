package metrics

import (
	"sync"
	"time"
)

type EventType string

const (
	EventSet     EventType = "set"
	EventGet     EventType = "get"
	EventDelete  EventType = "delete"
	EventExpire  EventType = "expire"
	EventTTL     EventType = "ttl"
	EventEvict   EventType = "evict"
	EventCleanup EventType = "cleanup"
)

type Event struct {
	Type      EventType `json:"type"`
	Key       string    `json:"key,omitempty"`
	Hit       *bool     `json:"hit,omitempty"`
	Reason    string    `json:"reason,omitempty"`
	LatencyUS int64     `json:"latencyUs,omitempty"`
	At        time.Time `json:"at"`
}

type LatencyStats struct {
	Count int64 `json:"count"`
	MinUS int64 `json:"minUs"`
	MaxUS int64 `json:"maxUs"`
	AvgUS int64 `json:"avgUs"`
}

type Snapshot struct {
	StartedAt       time.Time               `json:"startedAt"`
	UptimeSeconds   int64                   `json:"uptimeSeconds"`
	TotalRequests   int64                   `json:"totalRequests"`
	Hits            int64                   `json:"hits"`
	Misses          int64                   `json:"misses"`
	Sets            int64                   `json:"sets"`
	Deletes         int64                   `json:"deletes"`
	Expires         int64                   `json:"expires"`
	Evictions       int64                   `json:"evictions"`
	Cleanups        int64                   `json:"cleanups"`
	HitRatio        float64                 `json:"hitRatio"`
	KeyCount        int                     `json:"keyCount"`
	MemoryBytes     int64                   `json:"memoryBytes"`
	LatencyByCommand map[string]LatencyStats `json:"latencyByCommand"`
}

type Recorder struct {
	mu sync.RWMutex

	startedAt time.Time

	totalRequests int64
	hits          int64
	misses        int64
	sets          int64
	deletes       int64
	expires       int64
	evictions     int64
	cleanups      int64

	latencies map[string]*latencyAccumulator

	eventLimit int
	events     []Event
}

type latencyAccumulator struct {
	count   int64
	totalUS int64
	minUS   int64
	maxUS   int64
}

func NewRecorder(eventLimit int) *Recorder {
	if eventLimit < 0 {
		eventLimit = 0
	}
	return &Recorder{
		startedAt: time.Now().UTC(),
		latencies: map[string]*latencyAccumulator{},
		eventLimit: eventLimit,
		events:     make([]Event, 0, eventLimit),
	}
}

func (r *Recorder) Record(command string, latency time.Duration, event Event) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.totalRequests++
	switch event.Type {
	case EventSet:
		r.sets++
	case EventGet:
		if event.Hit != nil && *event.Hit {
			r.hits++
		} else {
			r.misses++
		}
	case EventDelete:
		r.deletes++
	case EventExpire:
		r.expires++
	case EventEvict:
		r.evictions++
	case EventCleanup:
		r.cleanups++
	}

	us := latency.Microseconds()
	acc := r.latencies[command]
	if acc == nil {
		acc = &latencyAccumulator{minUS: us}
		r.latencies[command] = acc
	}
	acc.count++
	acc.totalUS += us
	if us < acc.minUS {
		acc.minUS = us
	}
	if us > acc.maxUS {
		acc.maxUS = us
	}

	event.LatencyUS = us
	if event.At.IsZero() {
		event.At = time.Now().UTC()
	}
	r.appendEvent(event)
}

func (r *Recorder) RecordEviction(key, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.evictions++
	r.appendEvent(Event{
		Type:   EventEvict,
		Key:    key,
		Reason: reason,
		At:     time.Now().UTC(),
	})
}

func (r *Recorder) Snapshot(keyCount int, memoryBytes int64) Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()

	latencies := make(map[string]LatencyStats, len(r.latencies))
	for command, acc := range r.latencies {
		avg := int64(0)
		if acc.count > 0 {
			avg = acc.totalUS / acc.count
		}
		latencies[command] = LatencyStats{
			Count: acc.count,
			MinUS: acc.minUS,
			MaxUS: acc.maxUS,
			AvgUS: avg,
		}
	}

	lookups := r.hits + r.misses
	ratio := float64(0)
	if lookups > 0 {
		ratio = float64(r.hits) / float64(lookups)
	}

	return Snapshot{
		StartedAt:       r.startedAt,
		UptimeSeconds:   int64(time.Since(r.startedAt).Seconds()),
		TotalRequests:   r.totalRequests,
		Hits:            r.hits,
		Misses:          r.misses,
		Sets:            r.sets,
		Deletes:         r.deletes,
		Expires:         r.expires,
		Evictions:       r.evictions,
		Cleanups:        r.cleanups,
		HitRatio:        ratio,
		KeyCount:        keyCount,
		MemoryBytes:     memoryBytes,
		LatencyByCommand: latencies,
	}
}

func (r *Recorder) Events() []Event {
	r.mu.RLock()
	defer r.mu.RUnlock()

	events := make([]Event, len(r.events))
	copy(events, r.events)
	return events
}

func (r *Recorder) appendEvent(event Event) {
	if r.eventLimit == 0 {
		return
	}
	if len(r.events) == r.eventLimit {
		copy(r.events, r.events[1:])
		r.events[len(r.events)-1] = event
		return
	}
	r.events = append(r.events, event)
}
