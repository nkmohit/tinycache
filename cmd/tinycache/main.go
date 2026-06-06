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
	maxKeys := flag.Int("max-keys", 1000, "maximum number of keys before LRU eviction")
	cleanupInterval := flag.Duration("cleanup-interval", time.Second, "TTL cleanup interval")
	eventLogSize := flag.Int("event-log-size", 1000, "number of recent debug events to retain")
	uiDir := flag.String("ui-dir", "", "optional directory to serve CacheScope static UI")
	flag.Parse()

	c := cache.New(cache.Options{
		MaxKeys:         *maxKeys,
		CleanupInterval: *cleanupInterval,
		EventLogSize:    *eventLogSize,
	})
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
