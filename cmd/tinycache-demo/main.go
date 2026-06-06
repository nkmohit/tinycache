package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"
)

type setRequest struct {
	Key        string `json:"key"`
	Value      string `json:"value"`
	TTLSeconds int64  `json:"ttlSeconds,omitempty"`
}

type expireRequest struct {
	Key        string `json:"key"`
	TTLSeconds int64  `json:"ttlSeconds"`
}

func main() {
	baseURL := flag.String("base-url", "http://localhost:8080", "TinyCache base URL")
	flag.Parse()

	client := &http.Client{Timeout: 5 * time.Second}
	for i := 0; i < 8; i++ {
		key := fmt.Sprintf("evict:candidate:%02d", i)
		mustPostJSON(client, *baseURL+"/command/set", setRequest{Key: key, Value: fmt.Sprintf("payload-%02d", i), TTLSeconds: 600})
	}

	seed := []setRequest{
		{Key: "session:alpha", Value: `{"user":"ava","role":"admin"}`, TTLSeconds: 120},
		{Key: "session:beta", Value: `{"user":"kai","role":"editor"}`, TTLSeconds: 90},
		{Key: "session:gamma", Value: `{"user":"maya","role":"viewer"}`, TTLSeconds: 45},
		{Key: "feature:checkout", Value: "enabled"},
		{Key: "feature:search", Value: "enabled"},
		{Key: "product:42", Value: `{"sku":"sku-42","price":1299}`},
		{Key: "product:73", Value: `{"sku":"sku-73","price":849}`},
		{Key: "rate:ip:127.0.0.1", Value: "17", TTLSeconds: 30},
		{Key: "cart:user:ava", Value: `["sku-42","sku-73"]`, TTLSeconds: 180},
	}

	for _, item := range seed {
		mustPostJSON(client, *baseURL+"/command/set", item)
	}

	for i := 0; i < 8; i++ {
		mustGet(client, *baseURL+"/command/get?key="+url.QueryEscape("session:alpha"))
	}
	for i := 0; i < 5; i++ {
		mustGet(client, *baseURL+"/command/get?key="+url.QueryEscape("product:42"))
	}
	for i := 0; i < 3; i++ {
		mustGet(client, *baseURL+"/command/get?key="+url.QueryEscape("feature:checkout"))
	}
	mustGet(client, *baseURL+"/command/get?key="+url.QueryEscape("missing:key"))
	mustPostJSON(client, *baseURL+"/command/expire", expireRequest{Key: "feature:search", TTLSeconds: 300})

	log.Printf("seeded TinyCache demo data at %s", *baseURL)
}

func mustPostJSON(client *http.Client, endpoint string, payload any) {
	body, err := json.Marshal(payload)
	if err != nil {
		log.Fatalf("marshal payload: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		log.Fatalf("create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("post %s: %v", endpoint, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(resp.Body)
		log.Fatalf("post %s failed status=%d body=%s", endpoint, resp.StatusCode, string(data))
	}
}

func mustGet(client *http.Client, endpoint string) {
	resp, err := client.Get(endpoint)
	if err != nil {
		log.Fatalf("get %s: %v", endpoint, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(resp.Body)
		log.Fatalf("get %s failed status=%d body=%s", endpoint, resp.StatusCode, string(data))
	}
}
