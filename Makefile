.PHONY: fmt test race bench run smoke

ADDR ?= :8080
MAX_KEYS ?= 1000
CLEANUP_INTERVAL ?= 1s
EVENT_LOG_SIZE ?= 1000
UI_DIR ?= web/cachescope
GOCACHE ?= /private/tmp/tinycache-gocache
BASE_URL ?= http://localhost:8080
SNAPSHOT_PATH ?= /private/tmp/tinycache.snapshot.json
AOF_PATH ?= /private/tmp/tinycache.aof
CACHESCOPE_SCREENSHOT ?= docs/assets/cachescope.png
NODE_BIN ?= /Users/mohneeru/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin/node
NODE_PATH ?= /Users/mohneeru/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules

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
	NODE_PATH=$(NODE_PATH) CACHESCOPE_URL=$(BASE_URL) CACHESCOPE_SCREENSHOT=$(CACHESCOPE_SCREENSHOT) $(NODE_BIN) scripts/capture-cachescope.js

smoke:
	curl -s -X POST http://localhost:8080/command/set -H 'Content-Type: application/json' -d '{"key":"name","value":"tiny","ttlSeconds":60}'
	curl -s 'http://localhost:8080/command/get?key=name'
	curl -s 'http://localhost:8080/metrics'
	curl -s 'http://localhost:8080/debug/lru'
