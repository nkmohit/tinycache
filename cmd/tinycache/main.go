package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nkmohit/tinycache/internal/cache"
	"github.com/nkmohit/tinycache/internal/httpapi"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	maxKeys := flag.Int("max-keys", 1000, "maximum number of keys; split evenly across shards")
	maxMemory := flag.Int64("maxmemory", 0, "maximum approximate bytes (key+value+overhead); 0 means no byte limit")
	maxMemorySamples := flag.Int("maxmemory-samples", 5, "keys sampled by approximate eviction (allkeys-lru, allkeys-lfu, volatile-ttl)")
	cleanupInterval := flag.Duration("cleanup-interval", time.Second, "TTL cleanup interval")
	eventLogSize := flag.Int("event-log-size", 1000, "number of recent debug events to retain")
	evictionPolicy := flag.String("eviction-policy", "lru", "eviction policy: lru, lfu, allkeys-lru, allkeys-lfu, allkeys-random, volatile-ttl, or noeviction")
	snapshotPath := flag.String("snapshot-path", "", "optional JSON snapshot path to load on start and save through /admin/snapshot")
	appendLogPath := flag.String("aof-path", "", "optional append-only operation log path")
	appendFsync := flag.String("appendfsync", "everysec", "aof fsync policy: always, everysec, or no")
	autoRewritePct := flag.Int("auto-aof-rewrite-percentage", 100, "rewrite the AOF after it grows by this percent since the last rewrite; 0 disables")
	autoRewriteMin := flag.Int64("auto-aof-rewrite-min-size", 64<<20, "minimum AOF size in bytes before an automatic rewrite")
	uiDir := flag.String("ui-dir", "", "optional directory to serve CacheScope static UI")
	flag.Parse()

	policy, err := cache.ParseEvictionPolicy(*evictionPolicy)
	if err != nil {
		log.Fatal(err)
	}
	fsyncPolicy, err := cache.ParseFsyncPolicy(*appendFsync)
	if err != nil {
		log.Fatal(err)
	}

	c, err := cache.Open(cache.Options{
		MaxKeys:              *maxKeys,
		MaxMemory:            *maxMemory,
		MaxMemorySamples:     *maxMemorySamples,
		CleanupInterval:      *cleanupInterval,
		EventLogSize:         *eventLogSize,
		EvictionPolicy:       policy,
		SnapshotPath:         *snapshotPath,
		AppendLogPath:        *appendLogPath,
		AppendFsync:          fsyncPolicy,
		AOFRewritePercentage: *autoRewritePct,
		AOFRewriteMinSize:    *autoRewriteMin,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	server := &http.Server{
		Addr:              *addr,
		Handler:           httpapi.NewWithOptions(c, httpapi.Options{UIDir: *uiDir}),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("tinycache listening on %s", *addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server failed: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("server shutdown failed: %v", err)
	}
}
