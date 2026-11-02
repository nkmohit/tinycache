# TinyCache

[![CI](https://github.com/nkmohit/tinycache/actions/workflows/ci.yml/badge.svg)](https://github.com/nkmohit/tinycache/actions/workflows/ci.yml)

TinyCache is a Redis-inspired in-memory cache engine with HTTP commands and debugger-friendly observability endpoints. V1 focuses on the core cache internals that CacheScope will visualize later: TTL expiration, LRU eviction, memory/key accounting, command metrics, event logs, and debug snapshots.

![CacheScope dashboard](docs/assets/cachescope.png)

## Features

- HTTP cache commands: `SET`, `GET`, `DEL`, `EXPIRE`, `TTL`.
- TTL cleanup with lazy expiration on read and a Redis-style background sweeper.
- LRU and LFU eviction modes.
- Metrics with hit ratio, memory estimate, evictions, and latency percentiles.
- Debug endpoints for keys, LRU order, events, replay records, and one-call UI hydration.
- JSON snapshot persistence and an append-only log with `always`, `everysec`, or `no` fsync.
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
  --snapshot-path "${TMPDIR:-/tmp}/tinycache.snapshot.json" \
  --aof-path "${TMPDIR:-/tmp}/tinycache.aof" \
  --appendfsync everysec \
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

![CacheScope evictions](docs/assets/cachescope-evictions.gif)

The clip is CacheScope while `SET` keeps writing into an 8-key cache. The eviction counter climbs and the LRU list turns over. It was recorded with headless Chrome and ffmpeg (`scripts/record-cachescope-gif.js`).

Useful Make targets:

```sh
make
make build
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

Numbers below are the tables in [docs/benchmarks.md](docs/benchmarks.md), measured on the 4-core cloud VM (Intel Xeon, Linux 6.12, Go 1.22).

`SET` used to walk every key looking for expirations. Sampling the TTL set dropped that cost:

| | before | after |
| --- | ---: | ---: |
| `BenchmarkSet` | 129,590 ns/op (7,717 ops/s) | 906 ns/op (1,103,387 ops/s) |
| `BenchmarkGetHit` | 301 ns/op | 298 ns/op |
| bench CLI, LRU, 100k ops, 10k keys, 30% writes | 32,509 ops/s | 1,970,416 ops/s |
| bench CLI, LFU, same workload | 13,434 ops/s | 27,895 ops/s |

LFU was still scanning every resident key to pick a victim. Frequency buckets and 16 shards were remeasured on this VM just before and after that change (the baseline is a fresh run, so it sits a little off the 906 ns/op row above):

| | before | after |
| --- | ---: | ---: |
| bench CLI, LFU, 100k ops, 10k keys, 30% writes | 27,944 ops/s | 1,599,824 ops/s |
| bench CLI, LRU, same workload | 1,987,993 ops/s | 1,761,864 ops/s |
| `BenchmarkEvictLFU` | 2,421 ns/op | 1,018 ns/op |
| `BenchmarkGetHitParallel` | 1,007 ns/op | 768 ns/op |
| `BenchmarkMixedParallel` | 676 ns/op | 446 ns/op |
| `BenchmarkSet` (single goroutine) | 852 ns/op | 1,040 ns/op |
| `BenchmarkGetHit` (one hot key) | 295 ns/op | 289 ns/op |

## Design tradeoffs

### Locking on GET, and sharding

A GET updates recency (and, for LFU, the frequency bucket), so it takes the write lock of the shard that owns the key. There are 16 shards, and each key hashes to one of them. GETs of different keys take different locks. Operations on one key still run one at a time. A single hot key still takes one shard lock, which is why `BenchmarkGetHit` stayed near 290 ns/op while `BenchmarkGetHitParallel` went from 1,007 ns/op to 768 ns/op.

The key limit is global. An insert that has to evict takes an admission lock and then every shard lock, in index order, and compares the 16 local victims. GETs do not take the admission lock. The extra lock shows up when one goroutine is already the only writer: single-threaded `BenchmarkSet` went from 852 ns/op to 1,040 ns/op, and the LRU bench CLI from 1,987,993 ops/s to 1,761,864 ops/s.

### LFU buckets

Each shard keeps a doubly linked list per access count and the smallest count that still has a key. Eviction removes the tail of that list, which is O(1). Inside one count, the tail is the key touched least recently, the same tie-break as the old full scan (lower access count, then older last access).

The price is a second list element on every key, and finding the next minimum when the lowest bucket becomes empty (a walk of the live frequencies, not of the keys). GET promotion is an unlink and a link. The LRU list is still updated so `/debug/lru` can show recency while the policy is LFU. On a 10,000-key working set the bench CLI went from 27,944 ops/s to 1,599,824 ops/s.

### AOF fsync

`--appendfsync` follows Redis `appendfsync`. A failed append is logged and returned to the caller. The memory update has already been applied when that error comes back.

| Policy | What returns | What a crash can lose |
| --- | --- | --- |
| `always` | after `fsync` of that command | a command that has not returned success |
| `everysec` (default) | after the append | about one second of commands; `Close` syncs once more |
| `no` | after the append | whatever the kernel has not flushed yet |

## Test

```sh
go test ./...
go test -race ./...
go test -bench=. ./internal/cache
```
