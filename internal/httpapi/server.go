package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/nkmohit/tinycache/internal/cache"
)

type Server struct {
	cache *cache.Cache
	mux   *http.ServeMux
}

type setRequest struct {
	Key        string `json:"key"`
	Value      string `json:"value"`
	TTLSeconds int64  `json:"ttlSeconds,omitempty"`
}

type expireRequest struct {
	Key        string `json:"key"`
	TTLSeconds int64  `json:"ttlSeconds"`
}

func New(c *cache.Cache) *Server {
	s := &Server{
		cache: c,
		mux:   http.NewServeMux(),
	}
	s.routes()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) routes() {
	s.mux.HandleFunc("POST /command/set", s.handleSet)
	s.mux.HandleFunc("GET /command/get", s.handleGet)
	s.mux.HandleFunc("DELETE /command/del", s.handleDelete)
	s.mux.HandleFunc("POST /command/expire", s.handleExpire)
	s.mux.HandleFunc("GET /command/ttl", s.handleTTL)
	s.mux.HandleFunc("GET /metrics", s.handleMetrics)
	s.mux.HandleFunc("GET /debug/keys", s.handleDebugKeys)
	s.mux.HandleFunc("GET /debug/lru", s.handleDebugLRU)
	s.mux.HandleFunc("GET /debug/events", s.handleDebugEvents)
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
}

func (s *Server) handleSet(w http.ResponseWriter, r *http.Request) {
	var req setRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.TTLSeconds < 0 {
		writeError(w, http.StatusBadRequest, "ttlSeconds cannot be negative")
		return
	}
	if err := s.cache.Set(r.Context(), req.Key, req.Value, ttlDuration(req.TTLSeconds)); err != nil {
		writeCacheError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	result, err := s.cache.Get(r.Context(), key)
	if err != nil {
		writeCacheError(w, err)
		return
	}
	if !result.Hit {
		writeJSON(w, http.StatusOK, map[string]any{
			"hit": false,
			"key": key,
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"hit":   true,
		"key":   result.Key,
		"value": result.Value,
	})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	deleted, err := s.cache.Delete(r.Context(), key)
	if err != nil {
		writeCacheError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"deleted": deleted})
}

func (s *Server) handleExpire(w http.ResponseWriter, r *http.Request) {
	var req expireRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.TTLSeconds < 0 {
		writeError(w, http.StatusBadRequest, "ttlSeconds cannot be negative")
		return
	}
	updated, err := s.cache.Expire(r.Context(), req.Key, ttlDuration(req.TTLSeconds))
	if err != nil {
		writeCacheError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"updated": updated})
}

func (s *Server) handleTTL(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	ttl, err := s.cache.TTL(r.Context(), key)
	if err != nil {
		writeCacheError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"key": key,
		"ttl": ttl,
	})
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.cache.Metrics())
}

func (s *Server) handleDebugKeys(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.cache.SnapshotDebug())
}

func (s *Server) handleDebugLRU(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string][]string{"keys": s.cache.LRUKeys()})
}

func (s *Server) handleDebugEvents(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"events": s.cache.Events()})
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return false
	}
	return true
}

func writeCacheError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, cache.ErrEmptyKey), errors.Is(err, cache.ErrInvalidTTL):
		writeError(w, http.StatusBadRequest, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, "internal server error")
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSONStatus(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	writeJSONStatus(w, status, value)
}

func writeJSONStatus(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func ttlDuration(seconds int64) time.Duration {
	if seconds == 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}
