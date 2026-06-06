# TinyCache

TinyCache is a Redis-inspired in-memory cache engine with HTTP commands and debugger-friendly observability endpoints. V1 focuses on the core cache internals that CacheScope will visualize later: TTL expiration, LRU eviction, memory/key accounting, command metrics, event logs, and debug snapshots.

## Run

```sh
go run ./cmd/tinycache --addr :8080
```

Optional flags:

```sh
go run ./cmd/tinycache \
  --addr :8080 \
  --max-keys 1000 \
  --cleanup-interval 1s \
  --event-log-size 1000 \
  --ui-dir web/cachescope
```

## Commands

```sh
curl -X POST http://localhost:8080/command/set \
  -H 'Content-Type: application/json' \
  -d '{"key":"name","value":"tiny","ttlSeconds":60}'

curl 'http://localhost:8080/command/get?key=name'
curl 'http://localhost:8080/command/ttl?key=name'

curl -X POST http://localhost:8080/command/expire \
  -H 'Content-Type: application/json' \
  -d '{"key":"name","ttlSeconds":120}'

curl -X DELETE 'http://localhost:8080/command/del?key=name'
```

## Debug Endpoints

- `GET /metrics` returns aggregate counters, hit ratio, key count, memory estimate, latency summaries, evictions, and uptime.
- `GET /debug/keys` returns key metadata only.
- `GET /debug/lru` returns keys from most-recently-used to least-recently-used.
- `GET /debug/events` returns the bounded recent operation and eviction log.
- `GET /debug/events/stream` streams live snapshots for CacheScope over server-sent events.
- `GET /healthz` returns server health.

## CacheScope

Run TinyCache with the static UI enabled:

```sh
make run
```

Then open `http://localhost:8080`.

Useful Make targets:

```sh
make fmt
make test
make race
make bench
make run
make smoke
```

Sample HTTP requests are available in `examples/tinycache.http`.

## Test

```sh
go test ./...
go test -race ./...
go test -bench=. ./internal/cache
```
