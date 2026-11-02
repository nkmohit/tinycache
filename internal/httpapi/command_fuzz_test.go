package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nkmohit/tinycache/internal/cache"
)

func FuzzHTTPCommands(f *testing.F) {
	f.Add("POST", "/command/set", `{"key":"name","value":"tiny","ttlSeconds":60}`)
	f.Add("POST", "/command/set", `{"key":"name","value":"tiny","ttlSeconds":"500ms"}`)
	f.Add("POST", "/command/set", `{"key":"name","value":"tiny","ttlSeconds":9227000000}`)
	f.Add("POST", "/command/expire", `{"key":"name","ttlSeconds":-1}`)
	f.Add("GET", "/command/get?key=name", "")
	f.Add("DELETE", "/command/del?key=name", "")
	f.Add("GET", "/command/ttl?key=name", "")
	f.Add("GET", "/debug/keys?sort=access&desc=true", "")
	f.Add("GET", "/debug/summary?sort=access&desc=true", "")
	f.Add("GET", "/debug/keys?sort=nope", "")
	f.Add("POST", "/admin/snapshot", `{"path":"/tmp/evil"}`)
	f.Add("GET", "/debug/events/stream", "")
	f.Fuzz(func(t *testing.T, method, target, body string) {
		if len(method) > 16 || len(target) > 2048 || len(body) > 8<<10 {
			return
		}
		if !strings.HasPrefix(target, "/") || strings.ContainsAny(target, " \r\n") {
			return
		}
		c := cache.New(cache.Options{MaxKeys: 32, CleanupInterval: time.Hour, EventLogSize: 8})
		defer c.Close()
		ctx := context.Background()
		_ = c.Set(ctx, "a", "1", 0)
		_ = c.Set(ctx, "b", "2", 0)

		req, err := http.NewRequest(method, target, strings.NewReader(body))
		if err != nil {
			return
		}
		reqCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if strings.Contains(target, "/debug/events/stream") {
			cancel()
		}
		req = req.WithContext(reqCtx)
		rec := httptest.NewRecorder()
		New(c).ServeHTTP(rec, req)
	})
}
