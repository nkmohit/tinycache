package resp

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nkmohit/tinycache/internal/cache"
)

const (
	defaultMaxClients   = 10000
	defaultReadBuffer   = 32 << 10
	defaultWriteBuffer  = 32 << 10
	defaultWriteTimeout = 10 * time.Second

	rejectClosed = "closed"
	rejectMax    = "max"
)

// ErrClosed is returned by ListenAndServe after Shutdown closes the listener.
var ErrClosed = errors.New("resp: server closed")

// Options configures a Server. Zero IdleTimeout disables the idle timeout.
// Zero MaxClients means defaultMaxClients. Zero Limits use DefaultLimits.
type Options struct {
	Addr            string
	IdleTimeout     time.Duration
	WriteTimeout    time.Duration
	MaxClients      int
	Limits          Limits
	ReadBufferSize  int
	WriteBufferSize int
	// MaxMemory is the configured byte budget, reported by INFO as maxmemory.
	// It matches cache.Options.MaxMemory. Zero means no byte limit.
	MaxMemory int64
}

// Server is a RESP2 TCP server. Each connection runs on its own goroutine.
// Replies are buffered and flushed when the read buffer has no more complete
// input, which is what turns a pipeline into one write.
type Server struct {
	cache        *cache.Cache
	addr         string
	idle         time.Duration
	writeTimeout time.Duration
	maxClients   int
	limits       Limits
	readBuffer   int
	writeBuffer  int
	maxMemory    int64

	mu       sync.Mutex
	ln       net.Listener
	conns    map[net.Conn]struct{}
	closed   bool
	port     string
	started  time.Time
	clients  atomic.Int32
	accepted atomic.Int64
	commands atomic.Int64
	rejected atomic.Int64
	wg       sync.WaitGroup
}

// NewServer serves c. c must be non-nil.
func NewServer(c *cache.Cache, opts Options) *Server {
	readBuf := opts.ReadBufferSize
	if readBuf <= 0 {
		readBuf = defaultReadBuffer
	}
	writeBuf := opts.WriteBufferSize
	if writeBuf <= 0 {
		writeBuf = defaultWriteBuffer
	}
	writeTimeout := opts.WriteTimeout
	if writeTimeout <= 0 {
		writeTimeout = defaultWriteTimeout
	}
	return &Server{
		cache:        c,
		addr:         opts.Addr,
		idle:         opts.IdleTimeout,
		writeTimeout: writeTimeout,
		maxClients:   opts.MaxClients,
		limits:       opts.Limits.normalize(),
		readBuffer:   readBuf,
		writeBuffer:  writeBuf,
		maxMemory:    opts.MaxMemory,
		conns:        map[net.Conn]struct{}{},
	}
}

// ListenAndServe listens on the configured address and serves until Shutdown.
func (s *Server) ListenAndServe() error {
	addr := s.addr
	if addr == "" {
		addr = ":6379"
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

// Serve accepts connections from ln until Shutdown closes it.
func (s *Server) Serve(ln net.Listener) error {
	if s.cache == nil {
		return errors.New("resp: cache is nil")
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = ln.Close()
		return ErrClosed
	}
	s.ln = ln
	s.started = time.Now()
	if _, port, err := net.SplitHostPort(ln.Addr().String()); err == nil {
		s.port = port
	}
	s.mu.Unlock()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if s.isClosed() {
				return ErrClosed
			}
			return err
		}
		s.wg.Add(1)
		go s.admit(conn)
	}
}

// Shutdown stops accepting, waits for connections to finish, and closes any
// still open when ctx is done.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	ln := s.ln
	s.mu.Unlock()
	if ln != nil {
		_ = ln.Close()
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.forceClose()
		<-done
		return ctx.Err()
	}
}

// Addr is the listener address once Serve has started, otherwise the configured address.
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln != nil {
		return s.ln.Addr().String()
	}
	return s.addr
}

func (s *Server) admit(conn net.Conn) {
	defer s.wg.Done()
	ok, reason := s.track(conn)
	if !ok {
		if reason == rejectMax {
			s.rejected.Add(1)
			_, _ = io.WriteString(conn, "-ERR max number of clients reached\r\n")
		}
		_ = conn.Close()
		return
	}
	s.serveConn(conn)
}

func (s *Server) serveConn(conn net.Conn) {
	defer func() {
		if rec := recover(); rec != nil {
			log.Printf("tinycache redis: client handler panic: %v", rec)
		}
	}()
	defer conn.Close()
	defer s.forget(conn)

	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
		_ = tc.SetKeepAlive(true)
	}

	br := bufio.NewReaderSize(conn, s.readBuffer)
	bw := bufio.NewWriterSize(conn, s.writeBuffer)
	wr := NewWriter(bw)
	ctx := context.Background()

	for {
		if s.isClosed() {
			return
		}
		if s.idle > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(s.idle))
		}
		args, err := ReadCommand(br, s.limits)
		if err != nil {
			if errors.Is(err, ErrEmptyCommand) {
				continue
			}
			if errors.Is(err, io.EOF) {
				return
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				return
			}
			msg := "ERR Protocol error: incomplete request"
			var pe *ProtocolError
			if errors.As(err, &pe) {
				msg = pe.Message
			}
			_ = conn.SetWriteDeadline(time.Now().Add(s.writeTimeout))
			_ = wr.WriteError(msg)
			_ = bw.Flush()
			return
		}
		_ = conn.SetReadDeadline(time.Time{})
		_ = conn.SetWriteDeadline(time.Now().Add(s.writeTimeout))

		quit, err := s.execute(ctx, args, wr)
		if err != nil {
			_ = bw.Flush()
			return
		}
		// Flush only when the buffered input is drained (or the client is leaving).
		// A pipeline sitting in the read buffer becomes one write.
		if br.Buffered() == 0 || quit {
			if ferr := bw.Flush(); ferr != nil {
				return
			}
		}
		_ = conn.SetWriteDeadline(time.Time{})
		if quit {
			return
		}
	}
}

func (s *Server) clientLimit() int {
	if s.maxClients <= 0 {
		return defaultMaxClients
	}
	return s.maxClients
}

func (s *Server) track(conn net.Conn) (bool, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false, rejectClosed
	}
	if int(s.clients.Load()) >= s.clientLimit() {
		return false, rejectMax
	}
	s.clients.Add(1)
	s.conns[conn] = struct{}{}
	s.accepted.Add(1)
	return true, ""
}

func (s *Server) forget(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.conns[conn]; !ok {
		return
	}
	delete(s.conns, conn)
	s.clients.Add(-1)
}

func (s *Server) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *Server) forceClose() {
	s.mu.Lock()
	conns := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

func (s *Server) infoSnapshot() (port string, started time.Time, clients int32, accepted, commands, rejected int64) {
	s.mu.Lock()
	port = s.port
	started = s.started
	s.mu.Unlock()
	return port, started, s.clients.Load(), s.accepted.Load(), s.commands.Load(), s.rejected.Load()
}
