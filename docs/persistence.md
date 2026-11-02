# Persistence

TinyCache recovers by loading the snapshot, then replaying the append-only file (AOF). Expiry times in both files are absolute. A key whose expiry is in the past at startup is not inserted as a live key. Tradeoffs for fsync and rewrite are in the README section [Persistence](../README.md#persistence).

## Snapshot

`SaveSnapshot` writes a temporary file in the destination directory, `fsync`s it, renames it over the snapshot path, and `fsync`s the directory. A crash keeps either the previous snapshot or the new one.

Version 2 is two parts. The first line is a JSON header. The rest of the file is the payload, exactly `length` bytes.

The checksums in this example are illustrative.

```json
{"magic":"TINYCACHE","version":2,"savedAt":"2026-11-02T00:00:00Z","crc32":"0123abcd","crc64":"0123456789abcdef","length":128}
{"entries":[{"key":"name","value":"tiny","expiresAt":"2026-11-02T01:00:00Z","createdAt":"2026-11-02T00:00:00Z","updatedAt":"2026-11-02T00:00:00Z","lastAccessedAt":"2026-11-02T00:00:00Z","accessCount":1}]}
```

| Header field | Meaning |
| --- | --- |
| `magic` | `TINYCACHE` |
| `version` | `2` |
| `savedAt` | Wall time of the save, RFC3339 |
| `crc32` | CRC-32/IEEE of the payload, 8 lowercase hex digits |
| `crc64` | CRC-64/ECMA-182 of the payload, 16 lowercase hex digits |
| `length` | Payload size in bytes |

The payload is a JSON object with an `entries` array. Each entry has `key`, `value`, `createdAt`, `updatedAt`, `lastAccessedAt`, `accessCount`, and `expiresAt` when the key has a TTL. Checksums cover the payload bytes only. They detect torn and bit-flipped files. Load rejects a bad magic, a version other than 2, a length that does not match, either checksum mismatch, or a payload that is not JSON.

Version 1 is the older single JSON document (`version`, `savedAt`, `entries`) with no checksum. It still loads. New saves write version 2.

## Append-only file

The AOF is JSON lines. One `Write` call stores one record plus a newline. Times are RFC3339.

| `command` | Fields | Effect on replay |
| --- | --- | --- |
| `set` | `key`, `value`, `expiresAt`, `at`, optional `ttlSeconds`, optional `ttlNanos` | Insert or replace the key |
| `expire` | `key`, `expiresAt`, `at`, optional `ttlSeconds`, optional `ttlNanos` | Set the expiry when the key exists |
| `del` | `key`, `at` | Remove the key |
| `base` | `at` | Drop every key, including keys loaded from the snapshot |

`expiresAt` is the absolute expiry. It is omitted when the key does not expire. `ttlNanos` is the exact duration from `at` to `expiresAt`. `ttlSeconds` is that duration rounded up to a whole second so an older reader does not treat a sub-second TTL as no expiry. Replay uses `expiresAt` when it is present. A record with no `expiresAt` uses `ttlNanos` when that field is positive, and otherwise `at` plus `ttlSeconds`. A duration that does not fit in `time.Duration` is rejected instead of wrapping.

```json
{"command":"base","at":"2026-11-02T00:00:00.1Z"}
{"command":"set","key":"name","value":"tiny","expiresAt":"2026-11-02T01:00:00Z","ttlSeconds":3600,"at":"2026-11-02T00:00:00Z"}
{"command":"expire","key":"name","expiresAt":"2026-11-02T02:00:00Z","ttlSeconds":7199,"at":"2026-11-02T00:00:01Z"}
{"command":"del","key":"name","at":"2026-11-02T00:00:02Z"}
```

A `set` whose own `expiresAt` is already past is still applied, so a later `expire` in the same file can extend it. After the whole file is applied, keys that are still expired are dropped.

## Rewrite

`RewriteAOF` replaces the file with a `base` line, one `set` per live key, and every command that arrived while the temp file was built. The copy takes each shard lock in index order and then the AOF lock, the same order a write uses, and releases them before the temp file is written. The temp file is created in the same directory. It is `fsync`ed, renamed over the AOF, and the directory is `fsync`ed. The previous file remains the AOF until that rename.

Auto rewrite uses the same path. It runs when the file is at least `auto-aof-rewrite-min-size` bytes and has grown by `auto-aof-rewrite-percentage` since the size at startup or after the previous rewrite. The server defaults are 64MiB and 100. Percentage 0 disables it.

## Crash recovery

1. Load the snapshot. A missing file is an empty start. A corrupt version 2 file, or a version 1 file that is not JSON, returns `corrupt snapshot` and the process does not serve.
2. Replay the AOF. A missing file is skipped.
3. If the last line has no newline and is not valid JSON, truncate the file at the last good byte, `fsync`, and log `truncated incomplete final record`. Startup continues.
4. If a line ends in a newline and is not a JSON record with a command, return `corrupt append-only file` and do not serve.

Evictions caused by the per-shard key or memory limit are `del` records, so a rewrite image plus the following commands does not bring an evicted key back.
