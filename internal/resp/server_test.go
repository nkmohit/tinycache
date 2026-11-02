package resp

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nkmohit/tinycache/internal/cache"
)

func startServer(t *testing.T, c *cache.Cache, opts Options) (*Server, string) {
	t.Helper()
	if c == nil {
		c = cache.New(cache.Options{MaxKeys: 10000})
	}
	t.Cleanup(func() { c.Close() })
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := NewServer(c, opts)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		select {
		case err := <-errCh:
			if err != nil && !errors.Is(err, ErrClosed) {
				t.Errorf("serve: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Errorf("server did not return")
		}
	})
	return srv, ln.Addr().String()
}

type client struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
	bw   *bufio.Writer
	w    *Writer
}

func dialClient(t *testing.T, addr string) *client {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { conn.Close() })
	bw := bufio.NewWriter(conn)
	return &client{t: t, conn: conn, r: bufio.NewReader(conn), bw: bw, w: NewWriter(bw)}
}

func (c *client) Send(args ...string) {
	c.t.Helper()
	if err := c.w.WriteCommand(args...); err != nil {
		c.t.Fatalf("send: %v", err)
	}
}

func (c *client) Flush() {
	c.t.Helper()
	if err := c.bw.Flush(); err != nil {
		c.t.Fatalf("flush: %v", err)
	}
}

func (c *client) Receive() Value {
	c.t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	v, err := ReadValue(c.r, DefaultLimits())
	if err != nil {
		c.t.Fatalf("receive: %v", err)
	}
	return v
}

func (c *client) Do(args ...string) Value {
	c.t.Helper()
	c.Send(args...)
	c.Flush()
	return c.Receive()
}

func wantSimple(t *testing.T, v Value, s string) {
	t.Helper()
	if v.Type != TypeSimple || v.Str != s {
		t.Fatalf("got %+v, want simple %q", v, s)
	}
}

func wantBulk(t *testing.T, v Value, s string) {
	t.Helper()
	if v.Type != TypeBulk || v.Null || v.Str != s {
		t.Fatalf("got %+v, want bulk %q", v, s)
	}
}

func wantNull(t *testing.T, v Value) {
	t.Helper()
	if v.Type != TypeBulk || !v.Null {
		t.Fatalf("got %+v, want null bulk", v)
	}
}

func wantInt(t *testing.T, v Value, n int64) {
	t.Helper()
	if v.Type != TypeInt || v.Int != n {
		t.Fatalf("got %+v, want int %d", v, n)
	}
}

func wantErr(t *testing.T, v Value, substr string) {
	t.Helper()
	if v.Type != TypeError || !strings.Contains(v.Str, substr) {
		t.Fatalf("got %+v, want error containing %q", v, substr)
	}
}

func TestServerCommands(t *testing.T) {
	_, addr := startServer(t, nil, Options{})
	c := dialClient(t, addr)

	wantSimple(t, c.Do("PING"), "PONG")
	wantBulk(t, c.Do("ping", "hi"), "hi")
	wantErr(t, c.Do("PING", "a", "b"), "wrong number")
	wantBulk(t, c.Do("ECHO", "a\r\nb"), "a\r\nb")
	wantErr(t, c.Do("GET"), "wrong number")
	wantNull(t, c.Do("GET", "missing"))

	wantSimple(t, c.Do("SET", "name", "tiny"), "OK")
	wantBulk(t, c.Do("GET", "name"), "tiny")
	wantSimple(t, c.Do("SET", "name", "other", "XX"), "OK")
	wantNull(t, c.Do("SET", "brand-new", "x", "XX"))
	wantSimple(t, c.Do("SET", "once", "1", "NX"), "OK")
	wantNull(t, c.Do("SET", "once", "2", "NX"))
	wantBulk(t, c.Do("GET", "once"), "1")
	wantErr(t, c.Do("SET", "name", "v", "EX", "0"), "invalid expire time")
	wantErr(t, c.Do("SET", "name", "v", "PX", "nope"), "not an integer")
	wantErr(t, c.Do("SET", "name", "v", "NX", "XX"), "syntax error")
	wantSimple(t, c.Do("SET", "temp", "v", "EX", "30"), "OK")
	ttl := c.Do("TTL", "temp")
	if ttl.Type != TypeInt || ttl.Int < 1 || ttl.Int > 30 {
		t.Fatalf("ttl: %+v", ttl)
	}
	wantInt(t, c.Do("PERSIST", "temp"), 1)
	wantInt(t, c.Do("TTL", "temp"), -1)
	wantInt(t, c.Do("PERSIST", "temp"), 0)
	wantInt(t, c.Do("PERSIST", "missing"), 0)

	wantSimple(t, c.Do("SET", "ms", "v", "PX", "1800"), "OK")
	pttl := c.Do("PTTL", "ms")
	// Whole-second TTL()*1000 would be 1000 here. The snapshot expiry is ~1800ms.
	if pttl.Type != TypeInt || pttl.Int <= 1000 || pttl.Int > 1850 {
		t.Fatalf("pttl: %+v", pttl)
	}
	wantInt(t, c.Do("EXPIRE", "ms", "0"), 1)
	wantNull(t, c.Do("GET", "ms"))
	wantInt(t, c.Do("EXPIRE", "missing", "10"), 0)
	wantErr(t, c.Do("EXPIRE", "name", "-1"), "invalid expire time")
	wantErr(t, c.Do("PEXPIRE", "name", "abc"), "not an integer")
	wantInt(t, c.Do("PEXPIRE", "name", "5000"), 1)

	wantInt(t, c.Do("EXISTS", "name", "missing", "once"), 2)
	wantInt(t, c.Do("DEL", "once", "missing"), 1)
	wantInt(t, c.Do("EXISTS", "once"), 0)

	wantSimple(t, c.Do("SET", "n", "10"), "OK")
	wantInt(t, c.Do("INCR", "n"), 11)
	wantInt(t, c.Do("INCRBY", "n", "4"), 15)
	wantInt(t, c.Do("DECR", "n"), 14)
	wantInt(t, c.Do("INCR", "fresh"), 1)
	wantInt(t, c.Do("DECR", "fresh-dec"), -1)
	wantSimple(t, c.Do("SET", "big", "9223372036854775807"), "OK")
	wantErr(t, c.Do("INCR", "big"), "overflow")
	wantBulk(t, c.Do("GET", "big"), "9223372036854775807")
	wantSimple(t, c.Do("SET", "small", "-9223372036854775808"), "OK")
	wantErr(t, c.Do("DECR", "small"), "overflow")
	wantErr(t, c.Do("INCRBY", "small", "-1"), "overflow")
	wantSimple(t, c.Do("SET", "word", "hello"), "OK")
	wantErr(t, c.Do("INCR", "word"), "not an integer")
	wantBulk(t, c.Do("GET", "word"), "hello")

	wantSimple(t, c.Do("MSET", "a", "1", "b", "2"), "OK")
	wantErr(t, c.Do("MSET", "a", "1", "b"), "wrong number")
	mget := c.Do("MGET", "a", "missing", "b")
	if mget.Type != TypeArray || len(mget.Array) != 3 || mget.Array[0].Str != "1" || !mget.Array[1].Null || mget.Array[1].Type != TypeBulk || mget.Array[2].Str != "2" {
		t.Fatalf("mget: %+v", mget)
	}

	wantSimple(t, c.Do("SET", "user:1", "a"), "OK")
	wantSimple(t, c.Do("SET", "user:2", "b"), "OK")
	wantSimple(t, c.Do("SET", "other", "c"), "OK")
	keys := c.Do("KEYS", "user:*")
	if keys.Type != TypeArray || len(keys.Array) != 2 || keys.Array[0].Str != "user:1" || keys.Array[1].Str != "user:2" {
		t.Fatalf("keys: %+v", keys)
	}
	wantSimple(t, c.Do("SET", "hallo", "y"), "OK")
	wantSimple(t, c.Do("SET", "hello", "x"), "OK")
	wantSimple(t, c.Do("SET", "hxllo", "z"), "OK")
	class := c.Do("KEYS", "h[ae]llo")
	if class.Type != TypeArray || len(class.Array) != 2 || class.Array[0].Str != "hallo" || class.Array[1].Str != "hello" {
		t.Fatalf("class keys: %+v", class)
	}

	db := c.Do("DBSIZE")
	if db.Type != TypeInt || db.Int < 2 {
		t.Fatalf("dbsize: %+v", db)
	}
	info := c.Do("INFO")
	wantBulkContains(t, info, "# Server")
	wantBulkContains(t, info, "# Stats")
	wantBulkContains(t, info, "# Memory")
	wantBulkContains(t, info, "maxmemory:0")
	wantBulkContains(t, info, "maxmemory_policy:lru")
	serverOnly := c.Do("INFO", "server")
	wantBulkContains(t, serverOnly, "# Server")
	if strings.Contains(serverOnly.Str, "# Memory") {
		t.Fatalf("INFO server included memory: %s", serverOnly.Str)
	}
	cfg := c.Do("CONFIG", "GET", "appendonly")
	if cfg.Type != TypeArray || len(cfg.Array) != 2 || cfg.Array[0].Str != "appendonly" || cfg.Array[1].Str != "no" {
		t.Fatalf("config get: %+v", cfg)
	}
	if empty := c.Do("CONFIG", "GET", "no-such"); empty.Type != TypeArray || len(empty.Array) != 0 {
		t.Fatalf("config miss: %+v", empty)
	}
	count := c.Do("COMMAND", "COUNT")
	if count.Type != TypeInt || count.Int != int64(len(commandTable)) {
		t.Fatalf("command count: %+v", count)
	}
	cmd := c.Do("COMMAND")
	if cmd.Type != TypeArray || len(cmd.Array) != len(commandTable) {
		t.Fatalf("command: type %v len %d", cmd.Type, len(cmd.Array))
	}
	infoGet := c.Do("COMMAND", "INFO", "get", "nope")
	if infoGet.Type != TypeArray || len(infoGet.Array) != 2 || infoGet.Array[0].Array[0].Str != "get" || !infoGet.Array[1].Null {
		t.Fatalf("command info: %+v", infoGet)
	}
	wantErr(t, c.Do("NOPE"), "unknown command")
	if !strings.Contains(c.Do("NoSuch", "x").Str, "NoSuch") {
		t.Fatal("unknown command should echo the original name")
	}

	wantSimple(t, c.Do("FLUSHALL"), "OK")
	wantInt(t, c.Do("DBSIZE"), 0)
	wantNull(t, c.Do("GET", "name"))

	wantSimple(t, c.Do("SET", "short", "v", "PX", "150"), "OK")
	time.Sleep(400 * time.Millisecond)
	wantNull(t, c.Do("GET", "short"))

	// Inline telnet form, then a normal command on the same connection.
	if _, err := c.conn.Write([]byte("PING\r\n")); err != nil {
		t.Fatal(err)
	}
	wantSimple(t, c.Receive(), "PONG")
	if _, err := c.conn.Write([]byte("SET \"inline key\" inline-value\r\n")); err != nil {
		t.Fatal(err)
	}
	wantSimple(t, c.Receive(), "OK")
	wantBulk(t, c.Do("GET", "inline key"), "inline-value")

	wantSimple(t, c.Do("QUIT"), "OK")
	_ = c.conn.SetReadDeadline(time.Now().Add(time.Second))
	_, err := ReadValue(c.r, DefaultLimits())
	if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("after QUIT: %v", err)
	}
}

func TestServerCacheErrors(t *testing.T) {
	// One byte per shard. A real entry is larger than that, and a zero share
	// would mean "no byte limit" on that shard, so the total is shardCount.
	const maxMemory = int64(16)
	c := cache.New(cache.Options{
		MaxKeys:        1000,
		MaxMemory:      maxMemory,
		EvictionPolicy: cache.EvictionNoEviction,
	})
	_, addr := startServer(t, c, Options{MaxMemory: maxMemory})
	cl := dialClient(t, addr)

	mem := cl.Do("INFO", "memory")
	wantBulkContains(t, mem, "# Memory")
	wantBulkContains(t, mem, "maxmemory:16")
	wantBulkContains(t, mem, "maxmemory_policy:noeviction")
	if strings.Contains(mem.Str, "# Server") {
		t.Fatalf("INFO memory included server: %s", mem.Str)
	}

	oom := cl.Do("SET", "k", "v")
	if oom.Type != TypeError || oom.Str != "OOM command not allowed when used memory > 'maxmemory'" {
		t.Fatalf("oom: %+v", oom)
	}

	overflow := cl.Do("SET", "k", "v", "EX", "9223372037")
	if overflow.Type != TypeError || overflow.Str != "ERR invalid expire time" {
		t.Fatalf("ttl overflow: %+v", overflow)
	}
	expire := cl.Do("EXPIRE", "k", "9223372037")
	if expire.Type != TypeError || expire.Str != "ERR invalid expire time" {
		t.Fatalf("expire overflow: %+v", expire)
	}
	pexpire := cl.Do("PEXPIRE", "k", "9223372036855")
	if pexpire.Type != TypeError || pexpire.Str != "ERR invalid expire time" {
		t.Fatalf("pexpire overflow: %+v", pexpire)
	}

	key := strings.Repeat("k", cache.MaxKeyBytes+1)
	gotKey := cl.Do("SET", key, "v")
	if gotKey.Type != TypeError || gotKey.Str != "ERR "+cache.ErrKeyTooLarge.Error() {
		t.Fatalf("key limit: %+v", gotKey)
	}
	value := strings.Repeat("v", cache.MaxValueBytes+1)
	gotValue := cl.Do("SET", "k", value)
	if gotValue.Type != TypeError || gotValue.Str != "ERR "+cache.ErrValueTooLarge.Error() {
		t.Fatalf("value limit: %+v", gotValue)
	}
}

func wantBulkContains(t *testing.T, v Value, substr string) {
	t.Helper()
	if v.Type != TypeBulk || v.Null || !strings.Contains(v.Str, substr) {
		t.Fatalf("got %+v, want bulk containing %q", v, substr)
	}
}

func TestServerPipeline(t *testing.T) {
	_, addr := startServer(t, nil, Options{})
	c := dialClient(t, addr)
	c.Send("PING")
	c.Send("ECHO", "x")
	c.Send("SET", "k", "v")
	c.Send("GET", "k")
	c.Flush()
	wantSimple(t, c.Receive(), "PONG")
	wantBulk(t, c.Receive(), "x")
	wantSimple(t, c.Receive(), "OK")
	wantBulk(t, c.Receive(), "v")
}

func TestPipelineFlushesWhenBufferDrains(t *testing.T) {
	c := cache.New(cache.Options{MaxKeys: 10})
	t.Cleanup(func() { c.Close() })
	srv := NewServer(c, Options{IdleTimeout: 0})
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() { clientConn.Close() })
	counter := &writeCounter{Conn: serverConn}
	payload := bytes.Repeat([]byte("*1\r\n$4\r\nPING\r\n"), 10)
	done := make(chan struct{})
	go func() {
		srv.serveConn(counter)
		close(done)
	}()
	// net.Pipe is synchronous. The handler must be reading before this write,
	// otherwise Write blocks forever and the handler never starts.
	_ = clientConn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := clientConn.Write(payload); err != nil {
		t.Fatal(err)
	}

	_ = clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	r := bufio.NewReader(clientConn)
	for i := 0; i < 10; i++ {
		v, err := ReadValue(r, DefaultLimits())
		if err != nil {
			t.Fatalf("reply %d: %v", i, err)
		}
		wantSimple(t, v, "PONG")
	}
	_ = clientConn.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("connection handler did not return")
	}
	if n := counter.n.Load(); n < 1 || n > 2 {
		t.Fatalf("server writes = %d, want one flush for the pipeline", n)
	}
}

type writeCounter struct {
	net.Conn
	n atomic.Int32
}

func (w *writeCounter) Write(p []byte) (int, error) {
	w.n.Add(1)
	return w.Conn.Write(p)
}

func TestServerProtocolErrorClosesConnection(t *testing.T) {
	_, addr := startServer(t, nil, Options{})
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("*-2\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	r := bufio.NewReader(conn)
	v, err := ReadValue(r, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	wantErr(t, v, "Protocol error")
	_, err = ReadValue(r, DefaultLimits())
	if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("want closed connection, got %v", err)
	}

	c := dialClient(t, addr)
	wantSimple(t, c.Do("PING"), "PONG")

	bad, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer bad.Close()
	if _, err := bad.Write([]byte{0xff, '\r', '\n'}); err != nil {
		t.Fatal(err)
	}
	_ = bad.SetReadDeadline(time.Now().Add(2 * time.Second))
	br := bufio.NewReader(bad)
	v, err = ReadValue(br, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	wantErr(t, v, "unknown command")
	if _, err := bad.Write([]byte("*1\r\n$4\r\nPING\r\n")); err != nil {
		t.Fatal(err)
	}
	wantSimple(t, mustRead(t, br), "PONG")
}

func mustRead(t *testing.T, r *bufio.Reader) Value {
	t.Helper()
	v, err := ReadValue(r, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestServerIdleTimeout(t *testing.T) {
	_, addr := startServer(t, nil, Options{IdleTimeout: 400 * time.Millisecond})
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 1)
	_, err = conn.Read(buf)
	var ne net.Error
	if err == nil || (errors.As(err, &ne) && ne.Timeout()) {
		t.Fatalf("expected the server to close the idle connection, got %v", err)
	}
}

func TestServerMaxClients(t *testing.T) {
	_, addr := startServer(t, nil, Options{MaxClients: 1})
	first := dialClient(t, addr)
	wantSimple(t, first.Do("PING"), "PONG")

	second, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	_ = second.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 128)
	n, err := second.Read(buf)
	if n == 0 {
		t.Fatal(err)
	}
	if !bytes.Contains(buf[:n], []byte("max number of clients reached")) {
		t.Fatalf("got %q", buf[:n])
	}
	wantSimple(t, first.Do("PING"), "PONG")
}

func TestServerShutdown(t *testing.T) {
	srv, addr := startServer(t, nil, Options{IdleTimeout: time.Minute})
	c := dialClient(t, addr)
	wantSimple(t, c.Do("PING"), "PONG")

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err := srv.Shutdown(ctx)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown: %v", err)
	}
	_ = c.conn.SetReadDeadline(time.Now().Add(time.Second))
	_, err = c.r.ReadByte()
	var ne net.Error
	if err == nil || (errors.As(err, &ne) && ne.Timeout()) {
		t.Fatalf("expected the server to close the connection, got %v", err)
	}
	if _, err := net.DialTimeout("tcp", addr, 300*time.Millisecond); err == nil {
		t.Fatal("expected dial to fail after shutdown")
	}
}

func TestServerWritesThroughAOF(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.aof")
	c := cache.New(cache.Options{
		MaxKeys:       100,
		AppendLogPath: path,
		AppendFsync:   cache.FsyncAlways,
	})
	_, addr := startServer(t, c, Options{})
	client := dialClient(t, addr)
	wantSimple(t, client.Do("SET", "name", "tiny"), "OK")
	wantInt(t, client.Do("EXPIRE", "name", "30"), 1)
	wantInt(t, client.Do("DEL", "name"), 1)
	wantSimple(t, client.Do("MSET", "a", "1", "b", "2"), "OK")
	wantInt(t, client.Do("INCR", "counter"), 1)
	wantSimple(t, client.Do("FLUSHALL"), "OK")

	recs, err := c.ReplayRecords()
	if err != nil {
		t.Fatal(err)
	}
	var sets, expires, dels int
	for _, rec := range recs {
		switch rec.Command {
		case "set":
			sets++
		case "expire":
			expires++
		case "del":
			dels++
		default:
			t.Fatalf("unexpected aof record %+v", rec)
		}
	}
	// SET, MSET a, MSET b, INCR -> 4 sets. EXPIRE -> 1. DEL name plus FLUSHALL of a, b, counter -> 4 dels.
	if sets != 4 || expires != 1 || dels != 4 {
		t.Fatalf("aof sets=%d expires=%d dels=%d records=%+v", sets, expires, dels, recs)
	}
}

func TestServerConcurrentClients(t *testing.T) {
	_, addr := startServer(t, nil, Options{})
	errCh := make(chan error, 8)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := exerciseClient(addr, i); err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

func exerciseClient(addr string, i int) error {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	bw := bufio.NewWriter(conn)
	w := NewWriter(bw)
	r := bufio.NewReader(conn)
	key := "k" + string(rune('a'+i))
	for n := 0; n < 20; n++ {
		if err := roundTrip(conn, r, bw, w, []string{"SET", key, "v"}, func(v Value) error {
			if v.Type != TypeSimple || v.Str != "OK" {
				return errors.New("set failed")
			}
			return nil
		}); err != nil {
			return err
		}
		if err := roundTrip(conn, r, bw, w, []string{"GET", key}, func(v Value) error {
			if v.Type != TypeBulk || v.Str != "v" {
				return errors.New("get failed")
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func roundTrip(conn net.Conn, r *bufio.Reader, bw *bufio.Writer, w *Writer, args []string, check func(Value) error) error {
	if err := w.WriteCommand(args...); err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	v, err := ReadValue(r, DefaultLimits())
	if err != nil {
		return err
	}
	return check(v)
}
