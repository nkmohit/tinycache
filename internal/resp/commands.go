package resp

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/nkmohit/tinycache/internal/cache"
)

type commandFunc func(s *Server, ctx context.Context, args []string, w *Writer) (bool, error)

type commandSpec struct {
	name       string
	arity      int
	flags      []string
	firstKey   int
	lastKey    int
	keyStep    int
	categories []string
	fn         commandFunc
}

// commandTable is the COMMAND catalog. Names are lowercase.
// Arity follows Redis: a positive number is an exact argc (including the
// command name) and a negative number is a minimum argc.
// It is filled from init so the handler functions can refer back to the table
// without an initialization cycle.
var commandTable []commandSpec

var commandByName map[string]commandSpec

func init() {
	commandTable = []commandSpec{
		{name: "ping", arity: -1, flags: []string{"fast"}, categories: []string{"@fast", "@connection"}, fn: cmdPing},
		{name: "echo", arity: 2, flags: []string{"fast"}, categories: []string{"@fast", "@connection"}, fn: cmdEcho},
		{name: "quit", arity: 1, flags: []string{"fast"}, categories: []string{"@fast", "@connection"}, fn: cmdQuit},
		{name: "get", arity: 2, flags: []string{"readonly", "fast"}, firstKey: 1, lastKey: 1, keyStep: 1, categories: []string{"@read", "@string", "@fast"}, fn: cmdGet},
		{name: "set", arity: -3, flags: []string{"write", "denyoom"}, firstKey: 1, lastKey: 1, keyStep: 1, categories: []string{"@write", "@string", "@slow"}, fn: cmdSet},
		{name: "del", arity: -2, flags: []string{"write"}, firstKey: 1, lastKey: -1, keyStep: 1, categories: []string{"@write", "@keyspace", "@fast"}, fn: cmdDel},
		{name: "exists", arity: -2, flags: []string{"readonly", "fast"}, firstKey: 1, lastKey: -1, keyStep: 1, categories: []string{"@read", "@keyspace", "@fast"}, fn: cmdExists},
		{name: "expire", arity: 3, flags: []string{"write", "fast"}, firstKey: 1, lastKey: 1, keyStep: 1, categories: []string{"@write", "@keyspace", "@fast"}, fn: cmdExpire},
		{name: "pexpire", arity: 3, flags: []string{"write", "fast"}, firstKey: 1, lastKey: 1, keyStep: 1, categories: []string{"@write", "@keyspace", "@fast"}, fn: cmdPExpire},
		{name: "ttl", arity: 2, flags: []string{"readonly", "fast"}, firstKey: 1, lastKey: 1, keyStep: 1, categories: []string{"@read", "@keyspace", "@fast"}, fn: cmdTTL},
		{name: "pttl", arity: 2, flags: []string{"readonly", "fast"}, firstKey: 1, lastKey: 1, keyStep: 1, categories: []string{"@read", "@keyspace", "@fast"}, fn: cmdPTTL},
		{name: "persist", arity: 2, flags: []string{"write", "fast"}, firstKey: 1, lastKey: 1, keyStep: 1, categories: []string{"@write", "@keyspace", "@fast"}, fn: cmdPersist},
		{name: "incr", arity: 2, flags: []string{"write", "denyoom", "fast"}, firstKey: 1, lastKey: 1, keyStep: 1, categories: []string{"@write", "@string", "@fast"}, fn: cmdIncr},
		{name: "incrby", arity: 3, flags: []string{"write", "denyoom", "fast"}, firstKey: 1, lastKey: 1, keyStep: 1, categories: []string{"@write", "@string", "@fast"}, fn: cmdIncrBy},
		{name: "decr", arity: 2, flags: []string{"write", "denyoom", "fast"}, firstKey: 1, lastKey: 1, keyStep: 1, categories: []string{"@write", "@string", "@fast"}, fn: cmdDecr},
		{name: "mget", arity: -2, flags: []string{"readonly", "fast"}, firstKey: 1, lastKey: -1, keyStep: 1, categories: []string{"@read", "@string", "@fast"}, fn: cmdMGet},
		{name: "mset", arity: -3, flags: []string{"write", "denyoom"}, firstKey: 1, lastKey: -1, keyStep: 2, categories: []string{"@write", "@string", "@slow"}, fn: cmdMSet},
		{name: "keys", arity: 2, flags: []string{"readonly", "sort_for_script"}, firstKey: 0, lastKey: 0, keyStep: 0, categories: []string{"@read", "@keyspace", "@slow"}, fn: cmdKeys},
		{name: "dbsize", arity: 1, flags: []string{"readonly", "fast"}, categories: []string{"@read", "@keyspace", "@fast"}, fn: cmdDBSize},
		{name: "flushall", arity: -1, flags: []string{"write"}, categories: []string{"@write", "@keyspace", "@slow", "@dangerous"}, fn: cmdFlushAll},
		{name: "info", arity: -1, flags: []string{"readonly", "loading", "stale"}, categories: []string{"@read", "@slow"}, fn: cmdInfo},
		{name: "command", arity: -1, flags: []string{"readonly", "loading", "stale"}, categories: []string{"@read", "@slow", "@connection"}, fn: cmdCommand},
		{name: "config", arity: -2, flags: []string{"admin", "loading", "stale"}, categories: []string{"@admin", "@slow"}, fn: cmdConfig},
	}
	commandByName = make(map[string]commandSpec, len(commandTable))
	for _, spec := range commandTable {
		commandByName[spec.name] = spec
	}
}

func arityOK(arity, n int) bool {
	if arity > 0 {
		return n == arity
	}
	return n >= -arity
}

func (s *Server) execute(ctx context.Context, args []string, w *Writer) (bool, error) {
	if len(args) == 0 {
		return false, w.WriteError("ERR empty command")
	}
	s.commands.Add(1)
	name := strings.ToLower(args[0])
	spec, ok := commandByName[name]
	if !ok {
		return false, w.WriteError(unknownCommand(args))
	}
	if !arityOK(spec.arity, len(args)) {
		return false, w.WriteError(fmt.Sprintf("ERR wrong number of arguments for '%s' command", name))
	}
	return spec.fn(s, ctx, args, w)
}

func unknownCommand(args []string) string {
	var b strings.Builder
	b.WriteString("ERR unknown command '")
	b.WriteString(sanitize(args[0]))
	b.WriteString("', with args beginning with:")
	for i := 1; i < len(args) && i <= 8 && b.Len() < 256; i++ {
		arg := sanitize(args[i])
		if len(arg) > 64 {
			arg = arg[:64]
		}
		b.WriteString(" '")
		b.WriteString(arg)
		b.WriteByte('\'')
	}
	return b.String()
}

func sanitize(s string) string {
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	return s
}

func redisErr(err error) string {
	switch {
	case errors.Is(err, cache.ErrOOM):
		// Redis replies with this sentence, and no ERR prefix, when noeviction
		// refuses a write that would pass maxmemory.
		return "OOM command not allowed when used memory > 'maxmemory'"
	case errors.Is(err, cache.ErrTTLOverflow), errors.Is(err, cache.ErrInvalidTTL):
		return "ERR invalid expire time"
	case errors.Is(err, cache.ErrKeyTooLarge),
		errors.Is(err, cache.ErrValueTooLarge),
		errors.Is(err, cache.ErrFilterTooLarge):
		return "ERR " + err.Error()
	case errors.Is(err, cache.ErrEmptyKey):
		return "ERR key cannot be empty"
	default:
		msg := sanitize(err.Error())
		if strings.HasPrefix(msg, "ERR ") {
			return msg
		}
		return "ERR " + msg
	}
}

func expireTimeErr(err error, command string) string {
	if errors.Is(err, cache.ErrTTLOverflow) {
		return "ERR invalid expire time"
	}
	if command == "set" {
		return "ERR invalid expire time in set"
	}
	return fmt.Sprintf("ERR invalid expire time in '%s' command", command)
}

func cmdPing(_ *Server, _ context.Context, args []string, w *Writer) (bool, error) {
	if len(args) > 2 {
		return false, w.WriteError("ERR wrong number of arguments for 'ping' command")
	}
	if len(args) == 1 {
		return false, w.WriteSimple("PONG")
	}
	return false, w.WriteBulk(args[1])
}

func cmdEcho(_ *Server, _ context.Context, args []string, w *Writer) (bool, error) {
	return false, w.WriteBulk(args[1])
}

func cmdQuit(_ *Server, _ context.Context, _ []string, w *Writer) (bool, error) {
	if err := w.WriteSimple("OK"); err != nil {
		return true, err
	}
	return true, nil
}

func cmdGet(s *Server, ctx context.Context, args []string, w *Writer) (bool, error) {
	got, err := s.cache.Get(ctx, args[1])
	if err != nil {
		return false, w.WriteError(redisErr(err))
	}
	if !got.Hit {
		return false, w.WriteNull()
	}
	return false, w.WriteBulk(got.Value)
}

func cmdSet(s *Server, ctx context.Context, args []string, w *Writer) (bool, error) {
	ttl, nx, xx, errMsg := parseSetOptions(args)
	if errMsg != "" {
		return false, w.WriteError(errMsg)
	}
	// Check-then-set through the public API. One connection runs commands
	// one at a time; two connections can both pass NX or XX.
	if nx || xx {
		got, err := s.cache.Get(ctx, args[1])
		if err != nil {
			return false, w.WriteError(redisErr(err))
		}
		if (nx && got.Hit) || (xx && !got.Hit) {
			return false, w.WriteNull()
		}
	}
	if err := s.cache.Set(ctx, args[1], args[2], ttl); err != nil {
		return false, w.WriteError(redisErr(err))
	}
	return false, w.WriteSimple("OK")
}

func parseSetOptions(args []string) (ttl time.Duration, nx, xx bool, errMsg string) {
	hasTTL := false
	for i := 3; i < len(args); {
		opt := strings.ToUpper(args[i])
		switch opt {
		case "EX", "PX":
			if hasTTL || i+1 >= len(args) {
				return 0, false, false, "ERR syntax error"
			}
			n, err := parseI64(args[i+1])
			if err != nil {
				return 0, false, false, "ERR value is not an integer or out of range"
			}
			var convErr error
			if opt == "EX" {
				ttl, convErr = secondsToDuration(n)
			} else {
				ttl, convErr = millisToDuration(n)
			}
			if convErr != nil {
				return 0, false, false, expireTimeErr(convErr, "set")
			}
			hasTTL = true
			i += 2
		case "NX":
			if nx || xx {
				return 0, false, false, "ERR syntax error"
			}
			nx = true
			i++
		case "XX":
			if nx || xx {
				return 0, false, false, "ERR syntax error"
			}
			xx = true
			i++
		default:
			return 0, false, false, "ERR syntax error"
		}
	}
	return ttl, nx, xx, ""
}

func cmdDel(s *Server, ctx context.Context, args []string, w *Writer) (bool, error) {
	for _, key := range args[1:] {
		if key == "" {
			return false, w.WriteError("ERR key cannot be empty")
		}
	}
	var n int64
	for _, key := range args[1:] {
		deleted, err := s.cache.Delete(ctx, key)
		if err != nil {
			return false, w.WriteError(redisErr(err))
		}
		if deleted {
			n++
		}
	}
	return false, w.WriteInt(n)
}

func cmdExists(s *Server, ctx context.Context, args []string, w *Writer) (bool, error) {
	var n int64
	for _, key := range args[1:] {
		got, err := s.cache.Get(ctx, key)
		if err != nil {
			return false, w.WriteError(redisErr(err))
		}
		if got.Hit {
			n++
		}
	}
	return false, w.WriteInt(n)
}

func cmdExpire(s *Server, ctx context.Context, args []string, w *Writer) (bool, error) {
	return expireAfter(s, ctx, args, w, false)
}

func cmdPExpire(s *Server, ctx context.Context, args []string, w *Writer) (bool, error) {
	return expireAfter(s, ctx, args, w, true)
}

func expireAfter(s *Server, ctx context.Context, args []string, w *Writer, millis bool) (bool, error) {
	n, err := parseI64(args[2])
	if err != nil {
		return false, w.WriteError("ERR value is not an integer or out of range")
	}
	command := "expire"
	if millis {
		command = "pexpire"
	}
	if n < 0 {
		return false, w.WriteError(fmt.Sprintf("ERR invalid expire time in '%s' command", command))
	}
	if n == 0 {
		deleted, err := s.cache.Delete(ctx, args[1])
		if err != nil {
			return false, w.WriteError(redisErr(err))
		}
		if deleted {
			return false, w.WriteInt(1)
		}
		return false, w.WriteInt(0)
	}
	var ttl time.Duration
	if millis {
		ttl, err = millisToDuration(n)
	} else {
		ttl, err = secondsToDuration(n)
	}
	if err != nil {
		return false, w.WriteError(expireTimeErr(err, command))
	}
	ok, err := s.cache.Expire(ctx, args[1], ttl)
	if err != nil {
		return false, w.WriteError(redisErr(err))
	}
	if ok {
		return false, w.WriteInt(1)
	}
	return false, w.WriteInt(0)
}

func cmdTTL(s *Server, ctx context.Context, args []string, w *Writer) (bool, error) {
	ttl, err := s.cache.TTL(ctx, args[1])
	if err != nil {
		return false, w.WriteError(redisErr(err))
	}
	return false, w.WriteInt(ttl)
}

func cmdPTTL(s *Server, ctx context.Context, args []string, w *Writer) (bool, error) {
	key := args[1]
	// SnapshotKeys is the public view that still carries absolute expiry.
	// TTL() is whole seconds, which is too coarse for PTTL after PX.
	matches, err := s.cache.SnapshotKeys(cache.KeyQuery{Filter: key})
	if err != nil {
		return false, w.WriteError(redisErr(err))
	}
	var found cache.KeyDebug
	ok := false
	for _, item := range matches {
		if item.Key == key {
			found = item
			ok = true
			break
		}
	}
	if !ok {
		return false, w.WriteInt(cache.TTLMissing)
	}
	if found.ExpiresAt == nil || found.ExpiresAt.IsZero() {
		return false, w.WriteInt(cache.TTLNoExpiry)
	}
	ms := time.Until(*found.ExpiresAt).Milliseconds()
	if ms < 0 {
		return false, w.WriteInt(cache.TTLMissing)
	}
	return false, w.WriteInt(ms)
}

func cmdPersist(s *Server, ctx context.Context, args []string, w *Writer) (bool, error) {
	ttl, err := s.cache.TTL(ctx, args[1])
	if err != nil {
		return false, w.WriteError(redisErr(err))
	}
	if ttl < 0 {
		return false, w.WriteInt(0)
	}
	// A zero duration clears the expiry. That is the public-API form of PERSIST.
	ok, err := s.cache.Expire(ctx, args[1], 0)
	if err != nil {
		return false, w.WriteError(redisErr(err))
	}
	if !ok {
		return false, w.WriteInt(0)
	}
	return false, w.WriteInt(1)
}

func cmdIncr(s *Server, ctx context.Context, args []string, w *Writer) (bool, error) {
	return incrBy(s, ctx, args[1], 1, w)
}

func cmdDecr(s *Server, ctx context.Context, args []string, w *Writer) (bool, error) {
	return incrBy(s, ctx, args[1], -1, w)
}

func cmdIncrBy(s *Server, ctx context.Context, args []string, w *Writer) (bool, error) {
	delta, err := parseI64(args[2])
	if err != nil {
		return false, w.WriteError("ERR value is not an integer or out of range")
	}
	return incrBy(s, ctx, args[1], delta, w)
}

func incrBy(s *Server, ctx context.Context, key string, delta int64, w *Writer) (bool, error) {
	ttlSec, err := s.cache.TTL(ctx, key)
	if err != nil {
		return false, w.WriteError(redisErr(err))
	}
	var cur int64
	if ttlSec != cache.TTLMissing {
		got, err := s.cache.Get(ctx, key)
		if err != nil {
			return false, w.WriteError(redisErr(err))
		}
		if got.Hit {
			cur, err = strconv.ParseInt(got.Value, 10, 64)
			if err != nil {
				return false, w.WriteError("ERR value is not an integer or out of range")
			}
		}
	}
	next, ok := addInt64(cur, delta)
	if !ok {
		return false, w.WriteError("ERR increment or decrement would overflow")
	}
	// Re-apply the remaining whole seconds. Set has no keep-TTL flag.
	var ttl time.Duration
	if ttlSec > 0 {
		ttl = time.Duration(ttlSec) * time.Second
	}
	if err := s.cache.Set(ctx, key, strconv.FormatInt(next, 10), ttl); err != nil {
		return false, w.WriteError(redisErr(err))
	}
	return false, w.WriteInt(next)
}

func addInt64(a, b int64) (int64, bool) {
	if b > 0 {
		if a > math.MaxInt64-b {
			return 0, false
		}
	} else if b < 0 {
		if a < math.MinInt64-b {
			return 0, false
		}
	}
	return a + b, true
}

func cmdMGet(s *Server, ctx context.Context, args []string, w *Writer) (bool, error) {
	type slot struct {
		null bool
		val  string
	}
	out := make([]slot, len(args)-1)
	for i, key := range args[1:] {
		got, err := s.cache.Get(ctx, key)
		if err != nil {
			return false, w.WriteError(redisErr(err))
		}
		if !got.Hit {
			out[i].null = true
			continue
		}
		out[i].val = got.Value
	}
	if err := w.WriteArray(len(out)); err != nil {
		return false, err
	}
	for _, slot := range out {
		var err error
		if slot.null {
			err = w.WriteNull()
		} else {
			err = w.WriteBulk(slot.val)
		}
		if err != nil {
			return false, err
		}
	}
	return false, nil
}

func cmdMSet(s *Server, ctx context.Context, args []string, w *Writer) (bool, error) {
	if len(args)%2 == 0 {
		return false, w.WriteError("ERR wrong number of arguments for 'mset' command")
	}
	for i := 1; i < len(args); i += 2 {
		if args[i] == "" {
			return false, w.WriteError("ERR key cannot be empty")
		}
	}
	for i := 1; i < len(args); i += 2 {
		if err := s.cache.Set(ctx, args[i], args[i+1], 0); err != nil {
			return false, w.WriteError(redisErr(err))
		}
	}
	return false, w.WriteSimple("OK")
}

func cmdKeys(s *Server, _ context.Context, args []string, w *Writer) (bool, error) {
	pattern := args[1]
	if len(pattern) > maxGlobPattern {
		return false, w.WriteError("ERR pattern is too long")
	}
	all, err := s.cache.SnapshotKeys(cache.KeyQuery{})
	if err != nil {
		return false, w.WriteError(redisErr(err))
	}
	matched := make([]string, 0)
	for _, item := range all {
		if matchGlob(pattern, item.Key) {
			matched = append(matched, item.Key)
		}
	}
	return false, w.WriteBulkSlice(matched)
}

func cmdDBSize(s *Server, _ context.Context, _ []string, w *Writer) (bool, error) {
	return false, w.WriteInt(int64(s.cache.Metrics().KeyCount))
}

func cmdFlushAll(s *Server, ctx context.Context, args []string, w *Writer) (bool, error) {
	if len(args) > 2 {
		return false, w.WriteError("ERR syntax error")
	}
	if len(args) == 2 {
		switch strings.ToUpper(args[1]) {
		case "ASYNC", "SYNC":
		default:
			return false, w.WriteError("ERR syntax error")
		}
	}
	keys, err := s.cache.SnapshotKeys(cache.KeyQuery{})
	if err != nil {
		return false, w.WriteError(redisErr(err))
	}
	for _, item := range keys {
		if _, err := s.cache.Delete(ctx, item.Key); err != nil {
			return false, w.WriteError(redisErr(err))
		}
	}
	return false, w.WriteSimple("OK")
}

func cmdInfo(s *Server, _ context.Context, args []string, w *Writer) (bool, error) {
	server, stats, memory := selectedSections(args)
	snap := s.cache.Metrics()
	port, started, clients, accepted, commands, rejected := s.infoSnapshot()
	if port == "" {
		port = "0"
	}
	uptime := snap.UptimeSeconds
	if !started.IsZero() {
		uptime = int64(time.Since(started).Seconds())
	}

	var b strings.Builder
	first := true
	writeSection := func(body string) {
		if !first {
			b.WriteString("\r\n")
		}
		first = false
		b.WriteString(body)
	}
	if server {
		writeSection(fmt.Sprintf("# Server\r\nredis_version:tinycache\r\ntinycache_mode:resp2\r\nos:%s %s\r\narch_bits:%d\r\nprocess_id:%d\r\ntcp_port:%s\r\nuptime_in_seconds:%d\r\nuptime_in_days:%d\r\n",
			runtime.GOOS, runtime.GOARCH, strconv.IntSize, os.Getpid(), port, uptime, uptime/86400))
	}
	if stats {
		writeSection(fmt.Sprintf("# Stats\r\ntotal_connections_received:%d\r\ntotal_commands_processed:%d\r\nrejected_connections:%d\r\nconnected_clients:%d\r\nkeyspace_hits:%d\r\nkeyspace_misses:%d\r\nevicted_keys:%d\r\n",
			accepted, commands, rejected, clients, snap.Hits, snap.Misses, snap.Evictions))
	}
	if memory {
		policy := s.cache.EngineInfo().EvictionPolicy
		writeSection(fmt.Sprintf("# Memory\r\nused_memory:%d\r\nused_memory_human:%s\r\nmaxmemory:%d\r\nmaxmemory_policy:%s\r\nmem_allocator:go\r\n",
			snap.MemoryBytes, humanBytes(snap.MemoryBytes), s.maxMemory, policy))
	}
	return false, w.WriteBulk(b.String())
}

func selectedSections(args []string) (server, stats, memory bool) {
	if len(args) == 1 {
		return true, true, true
	}
	any := false
	for _, arg := range args[1:] {
		for _, part := range strings.FieldsFunc(arg, func(r rune) bool {
			return r == ',' || r == ' ' || r == '\t'
		}) {
			switch strings.ToLower(part) {
			case "all", "default", "everything":
				return true, true, true
			case "server":
				server = true
				any = true
			case "stats":
				stats = true
				any = true
			case "memory":
				memory = true
				any = true
			}
		}
	}
	if !any {
		return false, false, false
	}
	return server, stats, memory
}

func humanBytes(n int64) string {
	if n < 0 {
		n = 0
	}
	if n < 1024 {
		return strconv.FormatInt(n, 10) + "B"
	}
	units := []string{"K", "M", "G", "T"}
	v := float64(n)
	i := -1
	for v >= 1024 && i < len(units)-1 {
		v /= 1024
		i++
	}
	return strconv.FormatFloat(v, 'f', 2, 64) + units[i]
}

func cmdCommand(_ *Server, _ context.Context, args []string, w *Writer) (bool, error) {
	if len(args) == 1 {
		return false, writeCommandList(w, commandTable)
	}
	sub := strings.ToUpper(args[1])
	switch sub {
	case "COUNT":
		if len(args) != 2 {
			return false, w.WriteError("ERR unknown subcommand or wrong number of arguments for 'COUNT'. Try COMMAND HELP.")
		}
		return false, w.WriteInt(int64(len(commandTable)))
	case "INFO":
		if len(args) < 3 {
			return false, w.WriteError("ERR unknown subcommand or wrong number of arguments for 'INFO'. Try COMMAND HELP.")
		}
		if err := w.WriteArray(len(args) - 2); err != nil {
			return false, err
		}
		for _, name := range args[2:] {
			spec, ok := commandByName[strings.ToLower(name)]
			if !ok {
				if err := w.WriteNullArray(); err != nil {
					return false, err
				}
				continue
			}
			if err := writeCommandSpec(w, spec); err != nil {
				return false, err
			}
		}
		return false, nil
	case "DOCS":
		return false, w.WriteArray(0)
	case "HELP":
		return false, w.WriteBulkSlice([]string{
			"COMMAND",
			"    Return an array of details about every TinyCache command.",
			"COMMAND COUNT",
			"    Return the number of commands in the registry.",
			"COMMAND INFO command-name [command-name ...]",
			"    Return details for the listed commands.",
			"COMMAND DOCS",
			"    Returns an empty array. Command docs are not served.",
		})
	default:
		return false, w.WriteError(fmt.Sprintf("ERR unknown subcommand '%s'. Try COMMAND HELP.", sanitize(args[1])))
	}
}

// cmdConfig answers the CONFIG GET probes redis-benchmark sends before a run.
// Only save and appendonly are populated; anything else is an empty array.
func cmdConfig(_ *Server, _ context.Context, args []string, w *Writer) (bool, error) {
	if !strings.EqualFold(args[1], "GET") {
		return false, w.WriteError(fmt.Sprintf("ERR unknown subcommand '%s'. Try CONFIG HELP.", sanitize(args[1])))
	}
	if len(args) != 3 {
		return false, w.WriteError("ERR wrong number of arguments for 'config|get' command")
	}
	key := strings.ToLower(args[2])
	var value string
	switch key {
	case "appendonly":
		value = "no"
	case "save":
		value = ""
	default:
		return false, w.WriteArray(0)
	}
	if err := w.WriteArray(2); err != nil {
		return false, err
	}
	if err := w.WriteBulk(key); err != nil {
		return false, err
	}
	return false, w.WriteBulk(value)
}

func writeCommandList(w *Writer, specs []commandSpec) error {
	if err := w.WriteArray(len(specs)); err != nil {
		return err
	}
	for _, spec := range specs {
		if err := writeCommandSpec(w, spec); err != nil {
			return err
		}
	}
	return nil
}

func writeCommandSpec(w *Writer, spec commandSpec) error {
	if err := w.WriteArray(7); err != nil {
		return err
	}
	if err := w.WriteBulk(spec.name); err != nil {
		return err
	}
	if err := w.WriteInt(int64(spec.arity)); err != nil {
		return err
	}
	if err := w.WriteBulkSlice(spec.flags); err != nil {
		return err
	}
	if err := w.WriteInt(int64(spec.firstKey)); err != nil {
		return err
	}
	if err := w.WriteInt(int64(spec.lastKey)); err != nil {
		return err
	}
	if err := w.WriteInt(int64(spec.keyStep)); err != nil {
		return err
	}
	return w.WriteBulkSlice(spec.categories)
}

func secondsToDuration(sec int64) (time.Duration, error) {
	if sec <= 0 {
		return 0, errBadExpire
	}
	// DurationFromSeconds reports ErrTTLOverflow when seconds*1e9 does not
	// fit in int64. That is the cache's representable-range check.
	return cache.DurationFromSeconds(sec)
}

func millisToDuration(ms int64) (time.Duration, error) {
	if ms <= 0 {
		return 0, errBadExpire
	}
	if ms > math.MaxInt64/int64(time.Millisecond) {
		return 0, cache.ErrTTLOverflow
	}
	return time.Duration(ms) * time.Millisecond, nil
}

var errBadExpire = errors.New("invalid expire time")

func parseI64(s string) (int64, error) {
	if s == "" {
		return 0, strconv.ErrSyntax
	}
	return strconv.ParseInt(s, 10, 64)
}
