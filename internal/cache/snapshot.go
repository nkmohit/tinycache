package cache

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"hash/crc64"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const (
	snapshotMagic   = "TINYCACHE"
	snapshotVersion = 2
)

// ErrCorruptSnapshot is returned when a snapshot fails its header, checksum,
// or JSON check. The cache is not opened on this error.
var ErrCorruptSnapshot = errors.New("corrupt snapshot")

// crc64Table is CRC-64/ECMA-182, the polynomial hash/crc64 calls ECMA.
var crc64Table = crc64.MakeTable(crc64.ECMA)

type snapshotFile struct {
	Version int             `json:"version"`
	SavedAt time.Time       `json:"savedAt"`
	Entries []snapshotEntry `json:"entries"`
}

type snapshotEntry struct {
	Key            string    `json:"key"`
	Value          string    `json:"value"`
	ExpiresAt      time.Time `json:"expiresAt,omitempty"`
	CreatedAt      time.Time `json:"createdAt"`
	UpdatedAt      time.Time `json:"updatedAt"`
	LastAccessedAt time.Time `json:"lastAccessedAt"`
	AccessCount    int64     `json:"accessCount"`
}

type snapshotHeader struct {
	Magic   string    `json:"magic"`
	Version int       `json:"version"`
	SavedAt time.Time `json:"savedAt"`
	CRC32   string    `json:"crc32"`
	CRC64   string    `json:"crc64"`
	Length  int       `json:"length"`
}

type snapshotPayload struct {
	Entries []snapshotEntry `json:"entries"`
}

func (c *Cache) SaveSnapshot(path string) error {
	if path == "" {
		path = c.snapshotPath
	}
	if path == "" {
		return ErrSnapshotPath
	}

	now := time.Now().UTC()
	c.lockAll()
	entries := make([]snapshotEntry, 0)
	for i := range c.shards {
		s := &c.shards[i]
		s.removeExpired(now, c)
		for _, item := range s.items {
			entries = append(entries, snapshotEntry{
				Key:            item.key,
				Value:          item.value,
				ExpiresAt:      item.expiresAt,
				CreatedAt:      item.createdAt,
				UpdatedAt:      item.updatedAt,
				LastAccessedAt: item.lastAccessedAt,
				AccessCount:    item.accessCount,
			})
		}
	}
	c.unlockAll()

	data, err := encodeSnapshot(now, entries)
	if err != nil {
		return err
	}
	return atomicWriteFile(path, data)
}

func (c *Cache) loadSnapshot(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	entries, err := decodeSnapshot(data)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	c.lockAll()
	defer c.unlockAll()
	c.installSnapshotEntries(entries, now)
	return nil
}

func (c *Cache) installSnapshotEntries(entries []snapshotEntry, now time.Time) {
	for _, saved := range entries {
		if !saved.ExpiresAt.IsZero() && !saved.ExpiresAt.After(now) {
			continue
		}
		s := c.shard(saved.Key)
		if _, exists := s.items[saved.Key]; exists {
			continue
		}
		item := &entry{
			key:            saved.Key,
			value:          saved.Value,
			expiresAt:      saved.ExpiresAt,
			createdAt:      saved.CreatedAt,
			updatedAt:      saved.UpdatedAt,
			lastAccessedAt: saved.LastAccessedAt,
			accessCount:    saved.AccessCount,
			sizeBytes:      estimateSize(saved.Key, saved.Value),
		}
		if err := c.ensureRoom(s, item.sizeBytes, 1, nil); err != nil {
			continue
		}
		c.insertItem(s, item)
	}
}

func encodeSnapshot(savedAt time.Time, entries []snapshotEntry) ([]byte, error) {
	if entries == nil {
		entries = []snapshotEntry{}
	}
	payload, err := json.Marshal(snapshotPayload{Entries: entries})
	if err != nil {
		return nil, err
	}
	sum32 := crc32.ChecksumIEEE(payload)
	sum64 := crc64.Checksum(payload, crc64Table)
	header, err := json.Marshal(snapshotHeader{
		Magic:   snapshotMagic,
		Version: snapshotVersion,
		SavedAt: savedAt.UTC(),
		CRC32:   fmt.Sprintf("%08x", sum32),
		CRC64:   fmt.Sprintf("%016x", sum64),
		Length:  len(payload),
	})
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(header)+1+len(payload))
	out = append(out, header...)
	out = append(out, '\n')
	out = append(out, payload...)
	return out, nil
}

func decodeSnapshot(data []byte) ([]snapshotEntry, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, fmt.Errorf("%w: empty file", ErrCorruptSnapshot)
	}
	line := data
	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		line = data[:i]
	}
	// Version 2 files carry a magic header. Older files are one JSON document.
	if bytes.Contains(line, []byte(`"magic"`)) {
		return decodeSnapshotV2(data)
	}
	return decodeSnapshotV1(data)
}

func decodeSnapshotV2(data []byte) ([]snapshotEntry, error) {
	nl := bytes.IndexByte(data, '\n')
	if nl < 0 {
		return nil, fmt.Errorf("%w: truncated header", ErrCorruptSnapshot)
	}
	var header snapshotHeader
	if err := json.Unmarshal(data[:nl], &header); err != nil {
		return nil, fmt.Errorf("%w: header: %v", ErrCorruptSnapshot, err)
	}
	if header.Magic != snapshotMagic {
		return nil, fmt.Errorf("%w: bad magic %q", ErrCorruptSnapshot, header.Magic)
	}
	if header.Version != snapshotVersion {
		return nil, fmt.Errorf("%w: unsupported snapshot version %d", ErrCorruptSnapshot, header.Version)
	}
	payload := data[nl+1:]
	if header.Length != len(payload) {
		return nil, fmt.Errorf("%w: length %d does not match payload of %d bytes", ErrCorruptSnapshot, header.Length, len(payload))
	}
	want32, err := strconv.ParseUint(header.CRC32, 16, 32)
	if err != nil {
		return nil, fmt.Errorf("%w: crc32: %v", ErrCorruptSnapshot, err)
	}
	want64, err := strconv.ParseUint(header.CRC64, 16, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: crc64: %v", ErrCorruptSnapshot, err)
	}
	got32 := crc32.ChecksumIEEE(payload)
	got64 := crc64.Checksum(payload, crc64Table)
	if uint32(want32) != got32 {
		return nil, fmt.Errorf("%w: crc32 mismatch", ErrCorruptSnapshot)
	}
	if want64 != got64 {
		return nil, fmt.Errorf("%w: crc64 mismatch", ErrCorruptSnapshot)
	}
	var body snapshotPayload
	if err := json.Unmarshal(payload, &body); err != nil {
		return nil, fmt.Errorf("%w: payload: %v", ErrCorruptSnapshot, err)
	}
	return body.Entries, nil
}

func decodeSnapshotV1(data []byte) ([]snapshotEntry, error) {
	var file snapshotFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorruptSnapshot, err)
	}
	if file.Version > 1 {
		return nil, fmt.Errorf("%w: unsupported snapshot version %d", ErrCorruptSnapshot, file.Version)
	}
	if file.Version == 0 && file.Entries == nil {
		return nil, fmt.Errorf("%w: not a snapshot", ErrCorruptSnapshot)
	}
	return file.Entries, nil
}

// atomicWriteFile writes data to a temp file in the destination directory,
// fsyncs it, renames it over path, and fsyncs the directory.
func atomicWriteFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tinycache-snapshot-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	closed := false
	published := false
	defer func() {
		if !closed {
			_ = tmp.Close()
		}
		if !published {
			_ = os.Remove(tmpName)
		}
	}()
	if err := tmp.Chmod(0o644); err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	closed = true
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	published = true
	return fsyncDir(dir)
}

func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
