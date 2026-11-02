package cache

import (
	"encoding/json"
	"errors"
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

// ErrInvalidFsync is returned when a policy string is not always, everysec, or no.
var ErrInvalidFsync = errors.New("appendfsync must be always, everysec, or no")

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
	policy   FsyncPolicy
	interval time.Duration
	dirty    bool
	err      error
}

func openAppendLogger(path string, policy FsyncPolicy) *appendLogger {
	if path == "" {
		return nil
	}
	logger := &appendLogger{policy: policy, interval: time.Second}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		log.Printf("tinycache aof: open %s: %v", path, err)
		logger.err = err
		return logger
	}
	logger.file = file
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
	data, err := json.Marshal(record)
	if err != nil {
		log.Printf("tinycache aof: marshal: %v", err)
		return err
	}
	data = append(data, '\n')

	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err != nil {
		log.Printf("tinycache aof: %v", a.err)
		return a.err
	}
	if a.file == nil {
		return nil
	}
	if _, err := a.file.Write(data); err != nil {
		log.Printf("tinycache aof: write: %v", err)
		return err
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

func (a *appendLogger) close() error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
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
