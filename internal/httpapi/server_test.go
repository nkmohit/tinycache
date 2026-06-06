package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/nkmohit/tinycache/internal/cache"
)

func TestCommandEndpoints(t *testing.T) {
	s := newTestServer(t)

	status, body := request(t, s, http.MethodPost, "/command/set", `{"key":"name","value":"tiny","ttlSeconds":60}`)
	if status != http.StatusOK || body["ok"] != true {
		t.Fatalf("unexpected set response status=%d body=%v", status, body)
	}

	status, body = request(t, s, http.MethodGet, "/command/get?key=name", "")
	if status != http.StatusOK || body["hit"] != true || body["value"] != "tiny" {
		t.Fatalf("unexpected get response status=%d body=%v", status, body)
	}

	status, body = request(t, s, http.MethodGet, "/command/ttl?key=name", "")
	if status != http.StatusOK || body["ttl"].(float64) < 1 {
		t.Fatalf("unexpected ttl response status=%d body=%v", status, body)
	}

	status, body = request(t, s, http.MethodPost, "/command/expire", `{"key":"name","ttlSeconds":120}`)
	if status != http.StatusOK || body["updated"] != true {
		t.Fatalf("unexpected expire response status=%d body=%v", status, body)
	}

	status, body = request(t, s, http.MethodDelete, "/command/del?key=name", "")
	if status != http.StatusOK || body["deleted"] != true {
		t.Fatalf("unexpected delete response status=%d body=%v", status, body)
	}
}

func TestGetMissing(t *testing.T) {
	s := newTestServer(t)

	status, body := request(t, s, http.MethodGet, "/command/get?key=missing", "")
	if status != http.StatusOK || body["hit"] != false {
		t.Fatalf("unexpected get miss response status=%d body=%v", status, body)
	}
}

func TestInvalidRequests(t *testing.T) {
	s := newTestServer(t)

	tests := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{name: "invalid json", method: http.MethodPost, path: "/command/set", body: `{`},
		{name: "empty set key", method: http.MethodPost, path: "/command/set", body: `{"key":"","value":"x"}`},
		{name: "negative set ttl", method: http.MethodPost, path: "/command/set", body: `{"key":"x","value":"x","ttlSeconds":-1}`},
		{name: "empty get key", method: http.MethodGet, path: "/command/get", body: ""},
		{name: "negative expire ttl", method: http.MethodPost, path: "/command/expire", body: `{"key":"x","ttlSeconds":-1}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, body := request(t, s, tt.method, tt.path, tt.body)
			if status != http.StatusBadRequest {
				t.Fatalf("expected 400, got status=%d body=%v", status, body)
			}
		})
	}
}

func TestDebugAndMetricsEndpoints(t *testing.T) {
	s := newTestServer(t)

	request(t, s, http.MethodPost, "/command/set", `{"key":"a","value":"1"}`)
	request(t, s, http.MethodPost, "/command/set", `{"key":"b","value":"2"}`)
	request(t, s, http.MethodGet, "/command/get?key=a", "")

	status, body := request(t, s, http.MethodGet, "/metrics", "")
	if status != http.StatusOK || body["keyCount"].(float64) != 2 {
		t.Fatalf("unexpected metrics response status=%d body=%v", status, body)
	}

	status, body = request(t, s, http.MethodGet, "/debug/keys", "")
	if status != http.StatusOK || len(body["keys"].([]any)) != 2 {
		t.Fatalf("unexpected debug keys response status=%d body=%v", status, body)
	}

	status, body = request(t, s, http.MethodGet, "/debug/keys?filter=a&sort=access&desc=true&limit=1", "")
	if status != http.StatusOK || len(body["keys"].([]any)) != 1 {
		t.Fatalf("unexpected queried debug keys response status=%d body=%v", status, body)
	}

	status, body = request(t, s, http.MethodGet, "/debug/summary", "")
	if status != http.StatusOK || body["metrics"] == nil || body["keys"] == nil || body["lru"] == nil || body["events"] == nil {
		t.Fatalf("unexpected debug summary response status=%d body=%v", status, body)
	}

	status, body = request(t, s, http.MethodGet, "/debug/lru", "")
	keys := body["keys"].([]any)
	if status != http.StatusOK || keys[0] != "a" {
		t.Fatalf("unexpected lru response status=%d body=%v", status, body)
	}

	status, body = request(t, s, http.MethodGet, "/debug/events", "")
	if status != http.StatusOK || len(body["events"].([]any)) == 0 {
		t.Fatalf("unexpected events response status=%d body=%v", status, body)
	}
}

func TestCORSPreflight(t *testing.T) {
	s := newTestServer(t)

	req := httptest.NewRequest(http.MethodOptions, "/command/set", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("expected wildcard cors origin, got %q", got)
	}
}

func TestStaticUIServing(t *testing.T) {
	c := cache.New(cache.Options{
		MaxKeys:         10,
		CleanupInterval: time.Hour,
		EventLogSize:    100,
	})
	t.Cleanup(c.Close)
	s := NewWithOptions(c, Options{UIDir: "../../web/cachescope"})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !bytes.Contains(rec.Body.Bytes(), []byte("CacheScope")) {
		t.Fatalf("expected CacheScope UI body, got %s", rec.Body.String())
	}
}

func TestDebugEventStream(t *testing.T) {
	s := newTestServer(t)

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/debug/events/stream", nil).WithContext(ctx)
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.ServeHTTP(rec, req)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("stream handler did not stop after request cancellation")
	}

	if !bytes.Contains(rec.Body.Bytes(), []byte("event: snapshot")) {
		t.Fatalf("expected snapshot event, got %s", rec.Body.String())
	}
}

func TestDebugReplayEndpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tinycache.aof")
	c := cache.New(cache.Options{
		MaxKeys:         10,
		CleanupInterval: time.Hour,
		EventLogSize:    100,
		AppendLogPath:   path,
	})
	t.Cleanup(c.Close)
	s := New(c)

	request(t, s, http.MethodPost, "/command/set", `{"key":"name","value":"tiny"}`)

	status, body := request(t, s, http.MethodGet, "/debug/replay", "")
	records := body["records"].([]any)
	if status != http.StatusOK || len(records) != 1 || body["source"] != "appendLog" {
		t.Fatalf("unexpected replay response status=%d body=%v", status, body)
	}
}

func TestAdminSnapshotEndpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tinycache.snapshot.json")
	c := cache.New(cache.Options{
		MaxKeys:         10,
		CleanupInterval: time.Hour,
		EventLogSize:    100,
		SnapshotPath:    path,
	})
	t.Cleanup(c.Close)
	s := New(c)

	request(t, s, http.MethodPost, "/command/set", `{"key":"name","value":"tiny"}`)
	status, body := request(t, s, http.MethodPost, "/admin/snapshot", "")
	if status != http.StatusOK || body["ok"] != true {
		t.Fatalf("unexpected snapshot response status=%d body=%v", status, body)
	}

	loaded := cache.New(cache.Options{
		MaxKeys:         10,
		CleanupInterval: time.Hour,
		EventLogSize:    100,
		SnapshotPath:    path,
	})
	t.Cleanup(loaded.Close)
	result, err := loaded.Get(context.Background(), "name")
	if err != nil || !result.Hit {
		t.Fatalf("expected snapshot to load key, result=%#v err=%v", result, err)
	}
}

func newTestServer(t *testing.T) *Server {
	t.Helper()
	c := cache.New(cache.Options{
		MaxKeys:         10,
		CleanupInterval: time.Hour,
		EventLogSize:    100,
	})
	t.Cleanup(c.Close)
	return New(c)
}

func request(t *testing.T, handler http.Handler, method, path, body string) (int, map[string]any) {
	t.Helper()

	req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	var decoded map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode response failed: %v body=%s", err, rec.Body.String())
	}
	return rec.Code, decoded
}
