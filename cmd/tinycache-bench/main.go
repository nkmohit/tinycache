package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/nkmohit/tinycache/internal/cache"
)

type result struct {
	Operations     int     `json:"operations"`
	MaxKeys        int     `json:"maxKeys"`
	EvictionPolicy string  `json:"evictionPolicy"`
	WriteRatio     float64 `json:"writeRatio"`
	DurationMS     int64   `json:"durationMs"`
	OpsPerSecond   float64 `json:"opsPerSecond"`
	KeyCount       int     `json:"keyCount"`
	HitRatio       float64 `json:"hitRatio"`
	Evictions      int64   `json:"evictions"`
}

func main() {
	operations := flag.Int("ops", 100000, "number of operations to run")
	maxKeys := flag.Int("max-keys", 10000, "maximum keys in the benchmark cache")
	writeRatio := flag.Float64("write-ratio", 0.30, "fraction of operations that are writes")
	policy := flag.String("eviction-policy", "lru", "eviction policy: lru or lfu")
	flag.Parse()

	if *operations <= 0 {
		log.Fatal("ops must be positive")
	}
	if *writeRatio < 0 || *writeRatio > 1 {
		log.Fatal("write-ratio must be between 0 and 1")
	}

	c := cache.New(cache.Options{
		MaxKeys:        *maxKeys,
		EventLogSize:   0,
		EvictionPolicy: cache.EvictionPolicy(*policy),
	})
	defer c.Close()

	ctx := context.Background()
	start := time.Now()
	writeThreshold := int(*writeRatio * 1000)
	keyspace := *maxKeys * 4
	writes := 0
	reads := 0
	for i := 0; i < *operations; i++ {
		if i%1000 < writeThreshold {
			key := fmt.Sprintf("bench:%d", writes%keyspace)
			writes++
			_ = c.Set(ctx, key, "value", 0)
			continue
		}
		key := fmt.Sprintf("bench:%d", reads%keyspace)
		reads++
		_, _ = c.Get(ctx, key)
	}
	elapsed := time.Since(start)
	metrics := c.Metrics()
	out := result{
		Operations:     *operations,
		MaxKeys:        *maxKeys,
		EvictionPolicy: *policy,
		WriteRatio:     *writeRatio,
		DurationMS:     elapsed.Milliseconds(),
		OpsPerSecond:   float64(*operations) / elapsed.Seconds(),
		KeyCount:       metrics.KeyCount,
		HitRatio:       metrics.HitRatio,
		Evictions:      metrics.Evictions,
	}

	data, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		log.Fatalf("marshal result: %v", err)
	}
	fmt.Println(string(data))
}
