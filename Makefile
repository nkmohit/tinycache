.PHONY: fmt test race bench run smoke

ADDR ?= :8080
MAX_KEYS ?= 1000
CLEANUP_INTERVAL ?= 1s
EVENT_LOG_SIZE ?= 1000
UI_DIR ?= web/cachescope
GOCACHE ?= /private/tmp/tinycache-gocache

fmt:
	gofmt -w .

test:
	GOCACHE=$(GOCACHE) go test ./...

race:
	GOCACHE=$(GOCACHE) go test -race ./...

bench:
	GOCACHE=$(GOCACHE) go test -bench=. ./internal/cache

run:
	GOCACHE=$(GOCACHE) go run ./cmd/tinycache --addr $(ADDR) --max-keys $(MAX_KEYS) --cleanup-interval $(CLEANUP_INTERVAL) --event-log-size $(EVENT_LOG_SIZE) --ui-dir $(UI_DIR)

smoke:
	curl -s -X POST http://localhost:8080/command/set -H 'Content-Type: application/json' -d '{"key":"name","value":"tiny","ttlSeconds":60}'
	curl -s 'http://localhost:8080/command/get?key=name'
	curl -s 'http://localhost:8080/metrics'
	curl -s 'http://localhost:8080/debug/lru'
