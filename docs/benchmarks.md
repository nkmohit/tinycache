# Cache benchmarks

Measured on the cloud VM that produced this change, before and after removing the full expiry scan from `Cache.Set`.

## Machine

| | |
| --- | --- |
| CPU | Intel(R) Xeon(R) Processor (KVM, family 6, model 207) |
| Cores | 4 (1 thread per core, 1 socket) |
| OS | Linux 6.12.94+ x86_64 |
| Go | go1.22.2 linux/amd64 |

`go test` reports the same CPU string (`cpu: Intel(R) Xeon(R) Processor`).

## Commands

```sh
go test -bench . -benchmem ./internal/cache
go run ./cmd/tinycache-bench --ops 100000 --max-keys 10000 --write-ratio 0.30 --eviction-policy lru
go run ./cmd/tinycache-bench --ops 100000 --max-keys 10000 --write-ratio 0.30 --eviction-policy lfu
```

ops/s for the Go benchmarks is `1e9 / ns/op`, rounded to the nearest integer. The bench CLI prints its own `opsPerSecond`.

## Before

`Cache.Set` called `removeExpiredLocked`, which walked every key.

| Benchmark | ns/op | B/op | allocs/op | ops/s |
| --- | ---: | ---: | ---: | ---: |
| BenchmarkSet | 129590 | 320 | 4 | 7,717 |
| BenchmarkGetHit | 301.0 | 1 | 1 | 3,322,259 |
| BenchmarkGetMiss | 289.1 | 1 | 1 | 3,459,011 |
| BenchmarkEvictLRU | 1909 | 241 | 4 | 523,834 |

`BenchmarkSet` was 431× `BenchmarkGetHit`.

| Policy | Duration | ops/s | Keys | Evictions |
| --- | ---: | ---: | ---: | ---: |
| LRU | 3076 ms | 32,509 | 10000 | 20000 |
| LFU | 7443 ms | 13,434 | 10000 | 20000 |

## After

`SET` no longer scans. Expired keys are removed on `GET` / `TTL` / `EXPIRE`, and by the background sweeper.

| Benchmark | ns/op | B/op | allocs/op | ops/s |
| --- | ---: | ---: | ---: | ---: |
| BenchmarkSet | 906.3 | 329 | 4 | 1,103,387 |
| BenchmarkGetHit | 297.9 | 1 | 1 | 3,356,831 |
| BenchmarkGetMiss | 290.6 | 1 | 1 | 3,441,156 |
| BenchmarkEvictLRU | 640.3 | 241 | 4 | 1,561,768 |

`BenchmarkSet` is 3.04× `BenchmarkGetHit` (906.3 ns vs 297.9 ns), down from 431×. That is a 143× reduction in ns/op (129,590 → 906). Allocs stayed at 4/op; `BenchmarkSet` still formats a new key and inserts it into a growing map on every iteration, while `BenchmarkGetHit` reads one hot key.

| Policy | Duration | ops/s | Keys | Evictions |
| --- | ---: | ---: | ---: | ---: |
| LRU | 50 ms | 1,970,416 | 10000 | 20000 |
| LFU | 3584 ms | 27,895 | 10000 | 20000 |

The LRU workload went from 32,509 ops/s to 1,970,416 ops/s. LFU went from 13,434 to 27,895. LFU still scans every resident key when it picks an eviction victim, and this workload evicts 20,000 times, so that policy stays far below LRU once the expiry scan is gone.

## Why a Redis-style sampler

The sweeper samples instead of keeping a min-heap of expiry times.

A heap pops the soonest expiry first, but every `SET` or `EXPIRE` that changes a TTL pays `O(log n)` on the write path. The sampler keeps that bookkeeping `O(1)`: keys with an expiry live in a side set (`ttlKeys` slice plus `ttlIndex`), insert appends, and delete swap-removes. Random access is an index into the slice, which a Go map iteration cannot do in `O(1)`.

Each cycle:

1. Under the cache lock, sample up to 20 TTL keys (partial shuffle, so the sample has no duplicates).
2. Delete the expired ones from that sample.
3. Release the lock.
4. Repeat while more than 25% of the sample was expired and the cycle is still inside its 25ms budget.

Reads stay correct without the sweeper: `GET`, `TTL`, and `EXPIRE` treat an expired key as missing and delete it. Debug snapshots and `SaveSnapshot` still purge expired keys before they report or persist. The sweeper's job is to reclaim keys nobody touches. A fully stale TTL set is drained in batches of 20 for up to 25ms per cycle; a fresh set stops after one sample.
