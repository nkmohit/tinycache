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

1. Under that shard's lock, sample up to 20 TTL keys (partial shuffle, so the sample has no duplicates).
2. Delete the expired ones from that sample.
3. Release the lock. The sweeper does this once per shard, and repeats while any shard is still stale.
4. Repeat while more than 25% of the sample was expired and the cycle is still inside its 25ms budget.

Reads stay correct without the sweeper: `GET`, `TTL`, and `EXPIRE` treat an expired key as missing and delete it. Debug snapshots and `SaveSnapshot` still purge expired keys before they report or persist. The sweeper's job is to reclaim keys nobody touches. A fully stale TTL set is drained in batches of 20 for up to 25ms per cycle; a fresh set stops after one sample.

## After frequency buckets and sharding

Same machine as above. Remeasured immediately before this change (expiry sampling already in place), then again with LFU frequency buckets and 16 key-hash shards.

```sh
go test -bench . -benchmem -count=1 ./internal/cache
go run ./cmd/tinycache-bench --ops 100000 --max-keys 10000 --write-ratio 0.30 --eviction-policy lru
go run ./cmd/tinycache-bench --ops 100000 --max-keys 10000 --write-ratio 0.30 --eviction-policy lfu
```

ops/s for the Go benchmarks is `1e9 / ns/op`, rounded to the nearest integer.

### Before buckets and shards

LFU eviction walked every resident key. One mutex covered the whole cache.

| Benchmark | ns/op | B/op | allocs/op | ops/s |
| --- | ---: | ---: | ---: | ---: |
| BenchmarkSet | 851.9 | 322 | 4 | 1,173,847 |
| BenchmarkGetHit | 294.9 | 1 | 1 | 3,390,980 |
| BenchmarkGetMiss | 285.6 | 1 | 1 | 3,501,401 |
| BenchmarkEvictLRU | 651.1 | 241 | 4 | 1,535,862 |
| BenchmarkEvictLFU | 2421 | 241 | 4 | 413,052 |
| BenchmarkGetHitParallel | 1007 | 1 | 1 | 993,049 |
| BenchmarkMixedParallel | 676.4 | 0 | 0 | 1,478,415 |

| Policy | Duration | ops/s | Keys | Evictions |
| --- | ---: | ---: | ---: | ---: |
| LRU | 50 ms | 1,987,993 | 10000 | 20000 |
| LFU | 3578 ms | 27,944 | 10000 | 20000 |

`BenchmarkEvictLFU` keeps only 100 keys, so the scan is cheap next to the bench CLI, which evicts from 10,000 keys on 20,000 writes. `BenchmarkGetHitParallel` and `BenchmarkMixedParallel` spread GETs across 256 keys on every core (`b.RunParallel`).

### After

Each shard keeps a doubly linked list per access count and the minimum live frequency. A victim is the tail of that list. Keys hash to 16 shards; a GET locks only its shard.

| Benchmark | ns/op | B/op | allocs/op | ops/s |
| --- | ---: | ---: | ---: | ---: |
| BenchmarkSet | 1040 | 355 | 4 | 961,538 |
| BenchmarkGetHit | 288.6 | 1 | 1 | 3,465,003 |
| BenchmarkGetMiss | 284.2 | 1 | 1 | 3,518,649 |
| BenchmarkEvictLRU | 934.1 | 232 | 4 | 1,070,549 |
| BenchmarkEvictLFU | 1018 | 280 | 5 | 982,318 |
| BenchmarkGetHitParallel | 768.0 | 1 | 1 | 1,302,083 |
| BenchmarkMixedParallel | 445.8 | 0 | 0 | 2,243,158 |

| Policy | Duration | ops/s | Keys | Evictions |
| --- | ---: | ---: | ---: | ---: |
| LRU | 56 ms | 1,761,864 | 10000 | 20000 |
| LFU | 62 ms | 1,599,824 | 10000 | 20000 |

LFU on the bench CLI went from 27,944 ops/s to 1,599,824 ops/s (57×) and now sits next to LRU. `BenchmarkEvictLFU` went from 2,421 ns/op to 1,018 ns/op. The extra alloc is the second list element (the frequency bucket); the LRU list is still there for `GET /debug/lru`.

Parallel GETs went from 1,007 ns/op to 768 ns/op, and the mixed parallel bench from 676 ns/op to 446 ns/op. A single-threaded `BenchmarkGetHit` did not change in any meaningful way (295 ns/op to 289 ns/op): one hot key still takes one shard lock.

Single-threaded `BenchmarkSet` (852 ns/op to 1,040 ns/op) and `BenchmarkEvictLRU` (651 ns/op to 934 ns/op) got slower. Inserts take an admission lock, and an eviction at the global max locks every shard long enough to compare the 16 local victims. That cost shows up when one goroutine is already the only writer. The LRU bench CLI moved from 1,987,993 ops/s to 1,761,864 ops/s for the same reason.

## After per-shard budgets

Same machine as above. Remeasured immediately before this change (16 shards, one global victim, admission lock on insert), then again with each shard evicting under its own lock. Both sides are the median of three runs. ops/s for the Go benchmarks is `1e9 / ns/op`, rounded to the nearest integer.

```sh
go test -bench . -benchmem -count=3 ./internal/cache
go run ./cmd/tinycache-bench --ops 100000 --max-keys 10000 --write-ratio 0.30 --eviction-policy lru
go run ./cmd/tinycache-bench --ops 100000 --max-keys 10000 --write-ratio 0.30 --eviction-policy lfu
```

The bench CLI still passes `lru` and `lfu`, which are the exact policies. Approximate policies are not in this table. This VM's rerun of the previous code landed a little off the single run recorded above (Set 1,040 ns/op, LRU CLI 1,761,864 ops/s), so the before column is the paired median rather than that writeup.

### Before

One admission lock on every new key. At the global cap, an eviction locks all 16 shards and compares their local victims.

| Benchmark | ns/op | B/op | allocs/op | ops/s |
| --- | ---: | ---: | ---: | ---: |
| BenchmarkSet | 1068 | 355 | 4 | 936,330 |
| BenchmarkGetHit | 296.6 | 1 | 1 | 3,371,544 |
| BenchmarkGetMiss | 288.4 | 1 | 1 | 3,467,406 |
| BenchmarkEvictLRU | 1008 | 232 | 4 | 992,063 |
| BenchmarkEvictLFU | 1041 | 280 | 5 | 960,615 |
| BenchmarkGetHitParallel | 745.8 | 1 | 1 | 1,340,842 |
| BenchmarkMixedParallel | 503.3 | 0 | 0 | 1,986,886 |

| Policy | Duration | ops/s | Keys | Evictions |
| --- | ---: | ---: | ---: | ---: |
| LRU | 60 ms | 1,651,650 | 10000 | 20000 |
| LFU | 68 ms | 1,451,155 | 10000 | 20000 |

### After

`maxKeys` is split across the shards (`total/16`, remainder on the first shards). `maxmemory`, when set, is split the same way. A full shard evicts one of its own keys and does not lock the others. There is no admission lock.

| Benchmark | ns/op | B/op | allocs/op | ops/s |
| --- | ---: | ---: | ---: | ---: |
| BenchmarkSet | 898.0 | 332 | 4 | 1,113,586 |
| BenchmarkGetHit | 305.6 | 1 | 1 | 3,272,251 |
| BenchmarkGetMiss | 301.7 | 1 | 1 | 3,314,551 |
| BenchmarkEvictLRU | 745.8 | 232 | 4 | 1,340,842 |
| BenchmarkEvictLFU | 778.8 | 280 | 5 | 1,284,027 |
| BenchmarkGetHitParallel | 707.0 | 1 | 1 | 1,414,427 |
| BenchmarkMixedParallel | 473.5 | 0 | 0 | 2,111,932 |

| Policy | Duration | ops/s | Keys | Evictions |
| --- | ---: | ---: | ---: | ---: |
| LRU | 55 ms | 1,788,250 | 10000 | 20000 |
| LFU | 62 ms | 1,587,813 | 10000 | 20000 |

`BenchmarkEvictLRU` went from 1,008 ns/op to 746 ns/op, and `BenchmarkEvictLFU` from 1,041 ns/op to 779 ns/op. Allocations on those two are unchanged. `BenchmarkSet` went from 1,068 ns/op to 898 ns/op. The three Set runs were 1,183, 898, and 824 ns/op, so the median is the middle one; its B/op is 332 because that iteration's `b.N` grew the map a different number of times, not because the per-insert allocation count changed (still 4). A single hot `BenchmarkGetHit` stayed near 300 ns/op.

The LRU bench CLI went from 1,651,650 ops/s to 1,788,250 ops/s, and LFU from 1,451,155 to 1,587,813. At 100,000 ops the CLI duration sits in a band of about 10 ms, and the medians still move with the eviction benchmarks.

The cost of the split is skew. A shard can evict while another still has room, so a `MaxKeys` of `n+1` no longer guarantees that `n` inserts are eviction-free. Exact `lru` and `lfu` are unchanged as algorithms; they just run on the shard's own list or frequency buckets. `allkeys-lru` and `allkeys-lfu` instead sample 5 keys in that shard and drop the worst by LRU clock or logarithmic LFU counter.
