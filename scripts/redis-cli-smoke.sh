#!/usr/bin/env bash
# Smoke-test the RESP server with redis-cli, then run a short redis-benchmark.
# AOF is on for the smoke commands and off for the benchmark, so the timing
# is the protocol and the cache, not fsync.
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v redis-cli >/dev/null 2>&1 || ! command -v redis-benchmark >/dev/null 2>&1; then
  echo "redis-cli and redis-benchmark are required (package redis-tools)" >&2
  exit 1
fi

ROOT="${TMPDIR:-/tmp}/tinycache-resp-smoke"
rm -rf "$ROOT"
mkdir -p "$ROOT"
BIN="$ROOT/tinycache"
AOF="$ROOT/cache.aof"
LOG="$ROOT/server.log"
PORT="${REDIS_PORT:-16379}"
HTTP_PORT="${HTTP_PORT:-18080}"

go build -o "$BIN" ./cmd/tinycache

start_server() {
  local extra=()
  if [[ "${1:-}" == "aof" ]]; then
    extra+=(--aof-path "$AOF" --appendfsync always)
  fi
  "$BIN" \
    --addr "127.0.0.1:${HTTP_PORT}" \
    --redis-addr "127.0.0.1:${PORT}" \
    --redis-idle-timeout 0 \
    --max-keys 1000000 \
    "${extra[@]}" >"$LOG" 2>&1 &
  echo $! >"$ROOT/pid"
}

stop_server() {
  local pid
  pid="$(cat "$ROOT/pid" 2>/dev/null || true)"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    kill -INT "$pid" 2>/dev/null || true
    local i
    for i in $(seq 1 50); do
      if ! kill -0 "$pid" 2>/dev/null; then
        return 0
      fi
      sleep 0.1
    done
    kill -KILL "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
}

wait_for_ping() {
  local i
  for i in $(seq 1 50); do
    if redis-cli -h 127.0.0.1 -p "$PORT" PING 2>/dev/null | grep -qx PONG; then
      return 0
    fi
    sleep 0.1
  done
  echo "tinycache redis did not answer PING" >&2
  cat "$LOG" >&2 || true
  exit 1
}

cleanup() {
  stop_server
}
trap cleanup EXIT

start_server aof
wait_for_ping

redis-cli -h 127.0.0.1 -p "$PORT" ECHO hello | grep -qx hello
redis-cli -h 127.0.0.1 -p "$PORT" SET smoke ok | grep -qx OK
[[ "$(redis-cli -h 127.0.0.1 -p "$PORT" GET smoke)" == "ok" ]]
redis-cli -h 127.0.0.1 -p "$PORT" SET nxk 1 NX | grep -qx OK
# Non-interactive redis-cli prints an empty line for a null bulk. A TTY prints (nil).
nx_again="$(redis-cli -h 127.0.0.1 -p "$PORT" SET nxk 2 NX || true)"
if [[ "$nx_again" != "" && "$nx_again" != "(nil)" ]]; then
  echo "expected nil from SET NX, got: ${nx_again}" >&2
  exit 1
fi
redis-cli -h 127.0.0.1 -p "$PORT" SET temp v EX 30 | grep -qx OK
redis-cli -h 127.0.0.1 -p "$PORT" INCR counter | grep -qx 1
redis-cli -h 127.0.0.1 -p "$PORT" MSET a 1 b 2 | grep -qx OK
redis-cli -h 127.0.0.1 -p "$PORT" DEL smoke | grep -qx 1
redis-cli -h 127.0.0.1 -p "$PORT" DBSIZE >/dev/null
redis-cli -h 127.0.0.1 -p "$PORT" INFO server | grep -q "# Server"
redis-cli -h 127.0.0.1 -p "$PORT" COMMAND | grep -qi ping
redis-cli -h 127.0.0.1 -p "$PORT" FLUSHALL | grep -qx OK

set +e
unknown="$(redis-cli -h 127.0.0.1 -p "$PORT" NOTACOMMAND 2>&1)"
status=$?
set -e
if [[ "$unknown" != *"unknown command"* ]]; then
  echo "expected unknown command error, got: $unknown (status $status)" >&2
  exit 1
fi

if ! grep -q '"command":"set"' "$AOF"; then
  echo "AOF is missing a set record" >&2
  cat "$AOF" >&2 || true
  exit 1
fi

stop_server
: >"$LOG"

start_server
wait_for_ping
redis-benchmark -h 127.0.0.1 -p "$PORT" -t set,get -n 100000 -P 16
stop_server
