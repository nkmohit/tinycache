.PHONY: build fmt test race bench bench-cli run demo screenshot smoke

# `make` builds the server. TMPDIR is set on macOS and often unset on Linux.
.DEFAULT_GOAL := build

TMPDIR ?= /tmp
ADDR ?= :8080
MAX_KEYS ?= 1000
CLEANUP_INTERVAL ?= 1s
EVENT_LOG_SIZE ?= 1000
UI_DIR ?= web/cachescope
GOCACHE ?= $(TMPDIR)/tinycache-gocache
BASE_URL ?= http://localhost:8080
SNAPSHOT_PATH ?= $(TMPDIR)/tinycache.snapshot.json
AOF_PATH ?= $(TMPDIR)/tinycache.aof
CACHESCOPE_SCREENSHOT ?= docs/assets/cachescope.png
NODE_BIN ?= node

build:
	GOCACHE=$(GOCACHE) go build -o bin/tinycache ./cmd/tinycache

fmt:
	gofmt -w .

test:
	GOCACHE=$(GOCACHE) go test ./...

race:
	GOCACHE=$(GOCACHE) go test -race ./...

bench:
	GOCACHE=$(GOCACHE) go test -bench=. ./internal/cache

bench-cli:
	GOCACHE=$(GOCACHE) go run ./cmd/tinycache-bench --ops 100000 --max-keys 10000 --eviction-policy lru

run:
	GOCACHE=$(GOCACHE) go run ./cmd/tinycache --addr $(ADDR) --max-keys $(MAX_KEYS) --cleanup-interval $(CLEANUP_INTERVAL) --event-log-size $(EVENT_LOG_SIZE) --snapshot-path $(SNAPSHOT_PATH) --aof-path $(AOF_PATH) --ui-dir $(UI_DIR)

demo:
	GOCACHE=$(GOCACHE) go run ./cmd/tinycache-demo --base-url $(BASE_URL)

screenshot:
	CACHESCOPE_URL=$(BASE_URL) CACHESCOPE_SCREENSHOT=$(CACHESCOPE_SCREENSHOT) $(NODE_BIN) scripts/capture-cachescope.js

smoke:
	curl -s -X POST http://localhost:8080/command/set -H 'Content-Type: application/json' -d '{"key":"name","value":"tiny","ttlSeconds":60}'
	curl -s 'http://localhost:8080/command/get?key=name'
	curl -s 'http://localhost:8080/metrics'
	curl -s 'http://localhost:8080/debug/lru'
