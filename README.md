# TinyCache

TinyCache is a Redis-inspired in-memory cache engine with HTTP commands and debugger-friendly observability endpoints. V1 focuses on the core cache internals that CacheScope will visualize later: TTL expiration, LRU eviction, memory/key accounting, command metrics, event logs, and debug snapshots.

![CacheScope dashboard](docs/assets/cachescope.png)

## Features

- HTTP cache commands: `SET`, `GET`, `DEL`, `EXPIRE`, `TTL`.
- TTL cleanup with lazy expiration on read and a Redis-style background sweeper.
- LRU and LFU eviction modes.
- Metrics with hit ratio, memory estimate, evictions, and latency percentiles.
- Debug endpoints for keys, LRU order, events, replay records, and one-call UI hydration.
- JSON snapshot persistence and append-only mutation logging.
- CacheScope static dashboard served by the Go server.
- Demo seeder and benchmark CLI.

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
  --eviction-policy lru \
  --snapshot-path /private/tmp/tinycache.snapshot.json \
  --aof-path /private/tmp/tinycache.aof \
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
- `GET /debug/keys` returns key metadata only. Supports `filter`, `sort`, `desc`, and `limit`.
- `GET /debug/summary` returns metrics, keys, LRU order, and recent events in one response.
- `GET /debug/lru` returns keys from most-recently-used to least-recently-used.
- `GET /debug/events` returns the bounded recent operation and eviction log.
- `GET /debug/events/stream` streams live snapshots for CacheScope over server-sent events.
- `GET /debug/replay` returns append-only log records when `--aof-path` is configured.
- `POST /admin/snapshot` writes a JSON snapshot when `--snapshot-path` is configured.
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
make bench-cli
make run
make demo
make smoke
```

Sample HTTP requests are available in `examples/tinycache.http`.

For a screenshot-ready demo:

```sh
make run MAX_KEYS=10
make demo
make screenshot
```

The screenshot is written to `docs/assets/cachescope.png`.

## Benchmarks

`SET` used to call `removeExpiredLocked` and walk every key. On the VM in [docs/benchmarks.md](docs/benchmarks.md), `BenchmarkSet` was 129,590 ns/op and `BenchmarkGetHit` was 301 ns/op (about 431×).

Reads still drop an expired key on touch. A background cycle samples up to 20 keys from the TTL set and repeats while more than 25% of the sample is expired. The same commands afterward:

| | before | after |
| --- | ---: | ---: |
| `BenchmarkSet` | 129,590 ns/op (7,717 ops/s) | 906 ns/op (1,103,387 ops/s) |
| `BenchmarkGetHit` | 301 ns/op | 298 ns/op |
| bench CLI, LRU, 100k ops, 10k keys, 30% writes | 32,509 ops/s | 1,970,416 ops/s |
| bench CLI, LFU, same workload | 13,434 ops/s | 27,895 ops/s |

`SET` is 3.0× `GET` hit (906 ns/op vs 298 ns/op). Allocs, the LFU eviction note, machine spec, and why the sweeper samples instead of using a min-heap are in [docs/benchmarks.md](docs/benchmarks.md).

## Test

```sh
go test ./...
go test -race ./...
go test -bench=. ./internal/cache
```
