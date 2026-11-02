package cache

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

// FsyncPolicy matches Redis appendfsync.
type FsyncPolicy string

const (
	// FsyncAlways syncs the append-only log before the write returns.
	FsyncAlways FsyncPolicy = "always"
	// FsyncEverySec lets writes return after the append and syncs about once
	// a second from a background ticker. Close syncs once more on the way out.
	FsyncEverySec FsyncPolicy = "everysec"
	// FsyncNo leaves durability to the operating system.
	FsyncNo FsyncPolicy = "no"
)

var (
	// ErrInvalidFsync is returned when a policy string is not always, everysec, or no.
	ErrInvalidFsync = errors.New("appendfsync must be always, everysec, or no")
	// ErrCorruptAOF is returned when an append-only file has a bad record
	// before the final line. A torn last line is truncated instead.
	ErrCorruptAOF = errors.New("corrupt append-only file")
	// ErrAOFRewriteInProgress is returned when a rewrite is already running.
	ErrAOFRewriteInProgress = errors.New("aof rewrite already in progress")
	// ErrCacheClosed is returned when a rewrite is requested after Close.
	ErrCacheClosed = errors.New("cache is closed")
)

// aofInfof is the persistence log line. Tests replace it to read the text.
var aofInfof = log.Printf

// ParseFsyncPolicy accepts always, everysec, and no. An empty string is everysec.
func ParseFsyncPolicy(value string) (FsyncPolicy, error) {
	switch FsyncPolicy(value) {
	case "":
		return FsyncEverySec, nil
	case FsyncAlways, FsyncEverySec, FsyncNo:
		return FsyncPolicy(value), nil
	default:
		return "", ErrInvalidFsync
	}
}

// syncWriter is the part of *os.File the append log uses. Tests pass a fake.
type syncWriter interface {
	Write(p []byte) (int, error)
	Sync() error
	Close() error
}

// appendLogger is the append-only mutation log.
type appendLogger struct {
	mu       sync.Mutex
	file     syncWriter
	path     string
	policy   FsyncPolicy
	interval time.Duration
	dirty    bool
	err      error

	// size is the byte length of the live file. baseSize is the length after
	// the last rewrite, or at startup when no rewrite has run. Auto rewrite
	// compares the two the way Redis compares aof_current_size with
	// aof_rewrite_base_size.
	size     int64
	baseSize int64

	percentage int
	minSize    int64

	// inProgress covers the whole rewrite, including the unlocked file build.
	// rewriting is the window where new commands are also copied into rewriteBuf.
	inProgress bool
	rewriting  bool
	rewriteBuf []byte
	rewriteWG  sync.WaitGroup
	closed     bool
}

func openAppendLogger(path string, policy FsyncPolicy, percentage int, minSize int64) *appendLogger {
	if path == "" {
		return nil
	}
	logger := &appendLogger{
		path:       path,
		policy:     policy,
		interval:   time.Second,
		percentage: percentage,
		minSize:    minSize,
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		log.Printf("tinycache aof: open %s: %v", path, err)
		logger.err = err
		return logger
	}
	logger.file = file
	if fi, statErr := file.Stat(); statErr == nil {
		logger.size = fi.Size()
		logger.baseSize = fi.Size()
	}
	return logger
}

func (a *appendLogger) runsBackgroundSync() bool {
	if a == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.file != nil && a.policy == FsyncEverySec
}

func (a *appendLogger) write(record ReplayRecord) error {
	if a == nil {
		return nil
	}
	data, err := marshalRecord(record)
	if err != nil {
		log.Printf("tinycache aof: marshal: %v", err)
		return err
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err != nil {
		log.Printf("tinycache aof: %v", a.err)
		return a.err
	}
	if a.file == nil {
		return nil
	}
	n, err := a.file.Write(data)
	if err != nil {
		log.Printf("tinycache aof: write: %v", err)
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	a.size += int64(n)
	if a.rewriting {
		a.rewriteBuf = append(a.rewriteBuf, data...)
	}
	if a.policy == FsyncAlways {
		if err := a.file.Sync(); err != nil {
			log.Printf("tinycache aof: fsync: %v", err)
			return err
		}
		return nil
	}
	if a.policy == FsyncEverySec {
		a.dirty = true
	}
	return nil
}

func marshalRecord(record ReplayRecord) ([]byte, error) {
	data, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, len(data)+1)
	copy(buf, data)
	buf[len(data)] = '\n'
	return buf, nil
}

// commandRecord builds a log line. ExpiresAt is absolute so a replay after
// downtime does not treat a dead key as a fresh TTL.
func commandRecord(command, key, value string, expiresAt, at time.Time) ReplayRecord {
	if !at.IsZero() {
		at = at.UTC()
	}
	if !expiresAt.IsZero() {
		expiresAt = expiresAt.UTC()
	}
	rec := ReplayRecord{
		Command:   command,
		Key:       key,
		Value:     value,
		ExpiresAt: expiresAt,
		At:        at,
	}
	if !expiresAt.IsZero() && !at.IsZero() {
		if secs := int64(expiresAt.Sub(at).Seconds()); secs > 0 {
			rec.TTLSeconds = secs
		}
	}
	return rec
}

// everySecLoop syncs a dirty log on each tick and once more when stop closes.
func (a *appendLogger) everySecLoop(stop <-chan struct{}) {
	interval := a.interval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_ = a.syncIfDirty()
		case <-stop:
			_ = a.syncIfDirty()
			return
		}
	}
}

func (a *appendLogger) syncIfDirty() error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.syncLocked()
}

func (a *appendLogger) syncLocked() error {
	if a.file == nil || !a.dirty {
		return nil
	}
	a.dirty = false
	if err := a.file.Sync(); err != nil {
		a.dirty = true
		log.Printf("tinycache aof: fsync: %v", err)
		return err
	}
	return nil
}

func (a *appendLogger) syncFile() error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.file == nil {
		return nil
	}
	return a.file.Sync()
}

func (a *appendLogger) close() error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.closed = true
	syncErr := a.syncLocked()
	if a.file == nil {
		return syncErr
	}
	err := a.file.Close()
	a.file = nil
	if err != nil {
		log.Printf("tinycache aof: close: %v", err)
	}
	if syncErr != nil {
		return syncErr
	}
	return err
}

func (a *appendLogger) beginRewrite() error {
	if a == nil || a.path == "" {
		return ErrAppendLogPath
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return ErrCacheClosed
	}
	if a.inProgress {
		return ErrAOFRewriteInProgress
	}
	if a.err != nil {
		return a.err
	}
	if a.file == nil {
		return ErrAppendLogPath
	}
	a.inProgress = true
	a.rewriteWG.Add(1)
	return nil
}

func (a *appendLogger) finishRewrite() {
	a.mu.Lock()
	// A failed rewrite may still be capturing. Drop that buffer: those
	// commands are already on the previous file. A rewrite that published
	// has already stopped capturing.
	if a.rewriting {
		a.rewriting = false
		a.rewriteBuf = nil
	}
	a.inProgress = false
	a.mu.Unlock()
	a.rewriteWG.Done()
}

func (a *appendLogger) waitRewrite() {
	if a == nil {
		return
	}
	a.mu.Lock()
	a.closed = true
	a.mu.Unlock()
	a.rewriteWG.Wait()
}

func (a *appendLogger) needsRewrite() bool {
	if a == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.needsRewriteLocked()
}

func (a *appendLogger) needsRewriteLocked() bool {
	if a.percentage <= 0 || a.file == nil || a.closed || a.inProgress || a.rewriting {
		return false
	}
	if a.minSize > 0 && a.size < a.minSize {
		return false
	}
	base := a.baseSize
	if base < 1 {
		base = 1
	}
	if a.size < base {
		return false
	}
	growth := a.size - base
	return growth*100 >= base*int64(a.percentage)
}

func (a *appendLogger) takeRewriteBuf() []byte {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	chunk := a.rewriteBuf
	a.rewriteBuf = nil
	return chunk
}

func (a *appendLogger) currentSize() int64 {
	if a == nil {
		return 0
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.size
}

func (a *appendLogger) setSize(n int64) {
	if a == nil {
		return
	}
	a.mu.Lock()
	a.size = n
	a.baseSize = n
	a.mu.Unlock()
}
