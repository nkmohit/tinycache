package httpapi

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/nkmohit/tinycache/internal/cache"
)

// ttlInput is the ttlSeconds field of a CacheScope command. A JSON number is
// a count of seconds. A JSON string uses ParseTTL ("500ms", "1h", "60").
type ttlInput struct {
	d time.Duration
}

func (t *ttlInput) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || bytes.Equal(data, []byte("null")) {
		t.d = 0
		return nil
	}
	var raw string
	if data[0] == '"' {
		if err := json.Unmarshal(data, &raw); err != nil {
			return err
		}
	} else {
		var num json.Number
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.UseNumber()
		if err := dec.Decode(&num); err != nil {
			return err
		}
		raw = num.String()
	}
	d, err := cache.ParseTTL(raw)
	if err != nil {
		return err
	}
	t.d = d
	return nil
}
