package uhttp

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGoCache_GetSet(t *testing.T) {
	ctx := context.Background()
	cfg := CacheConfig{
		TTL:       10 * time.Minute,
		MaxSizeMb: 10,
	}

	cache, err := NewGoCache(ctx, cfg)
	require.NoError(t, err)
	require.NotNil(t, cache)

	// Create a test request
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com/test", nil)
	require.NoError(t, err)

	// Create a test response
	body := "test response body"
	resp := &http.Response{
		Status:     "200 OK",
		StatusCode: http.StatusOK,
		Proto:      "HTTP/1.1",
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader([]byte(body))),
	}

	// First Get should return nil (cache miss)
	result, err := cache.Get(req)
	require.NoError(t, err)
	require.Nil(t, result, "First get should be a cache miss")
	if result != nil {
		defer result.Body.Close()
	}

	// Set the response
	err = cache.Set(req, resp)
	require.NoError(t, err)

	// Second Get should return the cached response
	result, err = cache.Get(req)
	require.NoError(t, err)
	require.NotNil(t, result, "Second get should hit the cache")
	require.Equal(t, http.StatusOK, result.StatusCode)
	require.Equal(t, "200 OK", result.Status)

	// Verify response body
	resultBody, err := io.ReadAll(result.Body)
	require.NoError(t, err)
	require.Equal(t, body, string(resultBody))
	err = result.Body.Close()
	require.NoError(t, err)
}

func TestGoCache_Stats(t *testing.T) {
	ctx := context.Background()
	cfg := CacheConfig{
		TTL:       10 * time.Minute,
		MaxSizeMb: 10,
	}

	cache, err := NewGoCache(ctx, cfg)
	require.NoError(t, err)

	stats := cache.Stats(ctx)
	require.Equal(t, uint64(0), stats.Hits)
	require.Equal(t, uint64(0), stats.Misses)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com/test", nil)
	require.NoError(t, err)

	// First Get - cache miss
	res, err := cache.Get(req)
	require.NoError(t, err)
	if res != nil {
		defer res.Body.Close()
	}

	stats = cache.Stats(ctx)
	require.Equal(t, uint64(0), stats.Hits, "Should have 0 hits after cache miss")
	require.Equal(t, stats.Misses, uint64(1), "Should have a miss")

	missesBefore := stats.Misses

	// Set the response
	resp := &http.Response{
		Status:     "200 OK",
		StatusCode: http.StatusOK,
		Proto:      "HTTP/1.1",
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader([]byte("test"))),
	}
	err = cache.Set(req, resp)
	require.NoError(t, err)

	// Second Get - cache hit
	res, err = cache.Get(req)
	require.NoError(t, err)
	if res != nil {
		defer res.Body.Close()
	}

	stats = cache.Stats(ctx)
	require.Equal(t, missesBefore, stats.Misses, "Misses should not increase")
	require.Equal(t, stats.Hits, uint64(1), "Should have a hit")
}

func TestGoCache_Clear(t *testing.T) {
	ctx := context.Background()
	cfg := CacheConfig{
		TTL:       10 * time.Minute,
		MaxSizeMb: 10,
	}

	cache, err := NewGoCache(ctx, cfg)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com/test", nil)
	require.NoError(t, err)

	resp := &http.Response{
		Status:     "200 OK",
		StatusCode: http.StatusOK,
		Proto:      "HTTP/1.1",
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader([]byte("test"))),
	}

	// Set a response
	err = cache.Set(req, resp)
	require.NoError(t, err)

	// Verify it's cached
	result, err := cache.Get(req)
	require.NoError(t, err)
	require.NotNil(t, result)
	defer result.Body.Close()

	// Clear the cache
	err = cache.Clear(ctx)
	require.NoError(t, err)

	// Verify it's gone
	result, err = cache.Get(req)
	require.NoError(t, err)
	require.Nil(t, result, "Should be nil after clear")
	if result != nil {
		defer result.Body.Close()
	}
}

func TestGoCache_DifferentRequests(t *testing.T) {
	ctx := context.Background()
	cfg := CacheConfig{
		TTL:       10 * time.Minute,
		MaxSizeMb: 10,
	}

	cache, err := NewGoCache(ctx, cfg)
	require.NoError(t, err)

	req1, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com/path1", nil)
	require.NoError(t, err)

	req2, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com/path2", nil)
	require.NoError(t, err)

	resp1 := &http.Response{
		Status:     "200 OK",
		StatusCode: http.StatusOK,
		Proto:      "HTTP/1.1",
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader([]byte("response1"))),
	}

	resp2 := &http.Response{
		Status:     "200 OK",
		StatusCode: http.StatusOK,
		Proto:      "HTTP/1.1",
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader([]byte("response2"))),
	}

	// Set both responses
	err = cache.Set(req1, resp1)
	require.NoError(t, err)
	err = cache.Set(req2, resp2)
	require.NoError(t, err)

	// Get both and verify they're different
	result1, err := cache.Get(req1)
	require.NoError(t, err)
	require.NotNil(t, result1)
	defer result1.Body.Close()

	result2, err := cache.Get(req2)
	require.NoError(t, err)
	require.NotNil(t, result2)
	defer result2.Body.Close()

	body1, err := io.ReadAll(result1.Body)
	require.NoError(t, err)
	body2, err := io.ReadAll(result2.Body)
	require.NoError(t, err)

	require.NotEqual(t, string(body1), string(body2), "Different requests should have different cached values")
	require.Equal(t, "response1", string(body1))
	require.Equal(t, "response2", string(body2))
}

func TestGoCache_QueryParams(t *testing.T) {
	ctx := context.Background()
	cfg := CacheConfig{
		TTL:       10 * time.Minute,
		MaxSizeMb: 10,
	}

	cache, err := NewGoCache(ctx, cfg)
	require.NoError(t, err)

	// Same path, different query params
	req1, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com/path?foo=bar", nil)
	require.NoError(t, err)

	req2, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com/path?baz=qux", nil)
	require.NoError(t, err)

	resp1 := &http.Response{
		Status:     "200 OK",
		StatusCode: http.StatusOK,
		Proto:      "HTTP/1.1",
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader([]byte("response1"))),
	}

	resp2 := &http.Response{
		Status:     "200 OK",
		StatusCode: http.StatusOK,
		Proto:      "HTTP/1.1",
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader([]byte("response2"))),
	}

	// Set both
	err = cache.Set(req1, resp1)
	require.NoError(t, err)
	err = cache.Set(req2, resp2)
	require.NoError(t, err)

	// Get both and verify they're different
	result1, err := cache.Get(req1)
	require.NoError(t, err)
	defer result1.Body.Close()
	result2, err := cache.Get(req2)
	require.NoError(t, err)
	defer result2.Body.Close()

	body1, _ := io.ReadAll(result1.Body)
	body2, _ := io.ReadAll(result2.Body)

	require.Equal(t, "response1", string(body1))
	require.Equal(t, "response2", string(body2))
}

func TestGoCache_TTLExpiration(t *testing.T) {
	ctx := context.Background()
	cfg := CacheConfig{
		TTL:       50 * time.Millisecond, // Very short TTL
		MaxSizeMb: 10,
	}

	cache, err := NewGoCache(ctx, cfg)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com/test", nil)
	require.NoError(t, err)

	resp := &http.Response{
		Status:     "200 OK",
		StatusCode: http.StatusOK,
		Proto:      "HTTP/1.1",
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader([]byte("test"))),
	}

	// Set the response
	err = cache.Set(req, resp)
	require.NoError(t, err)

	// Immediately get - should be cached
	result, err := cache.Get(req)
	require.NoError(t, err)
	require.NotNil(t, result, "Should be cached immediately")
	defer result.Body.Close()

	// Wait for TTL to expire
	time.Sleep(100 * time.Millisecond)

	// Get again - should be nil (expired)
	result, err = cache.Get(req)
	require.NoError(t, err)
	require.Nil(t, result, "Should be nil after TTL expiration")
	if result != nil {
		defer result.Body.Close()
	}
}

func TestGoCache_NilResponses(t *testing.T) {
	// Get with nil cache should not panic
	gc := &GoCache{}
	result, err := gc.Get(nil)
	require.NoError(t, err)
	require.Nil(t, result)
	if result != nil {
		defer result.Body.Close()
	}
}

func TestGoCache_Delete(t *testing.T) {
	ctx := context.Background()
	cfg := CacheConfig{
		TTL:       10 * time.Minute,
		MaxSizeMb: 10,
	}

	cache, err := NewGoCache(ctx, cfg)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com/test", nil)
	require.NoError(t, err)

	resp := &http.Response{
		Status:     "200 OK",
		StatusCode: http.StatusOK,
		Proto:      "HTTP/1.1",
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader([]byte("test"))),
	}

	// Set the response
	err = cache.Set(req, resp)
	require.NoError(t, err)

	// Verify it's cached
	result, err := cache.Get(req)
	require.NoError(t, err)
	require.NotNil(t, result)
	defer result.Body.Close()

	// Create cache key for deletion
	key, err := CreateCacheKey(req)
	require.NoError(t, err)

	// Delete it
	err = cache.Delete(key)
	require.NoError(t, err)

	// Verify it's gone
	result, err = cache.Get(req)
	require.NoError(t, err)
	require.Nil(t, result, "Should be nil after delete")
	if result != nil {
		defer result.Body.Close()
	}
}

func TestGoCache_Has(t *testing.T) {
	ctx := context.Background()
	cfg := CacheConfig{
		TTL:       10 * time.Minute,
		MaxSizeMb: 10,
	}

	cache, err := NewGoCache(ctx, cfg)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com/test", nil)
	require.NoError(t, err)

	key, err := CreateCacheKey(req)
	require.NoError(t, err)

	// Initially should not exist
	has := cache.Has(key)
	require.False(t, has, "Should not exist initially")

	// Set the response
	resp := &http.Response{
		Status:     "200 OK",
		StatusCode: http.StatusOK,
		Proto:      "HTTP/1.1",
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader([]byte("test"))),
	}

	err = cache.Set(req, resp)
	require.NoError(t, err)

	// Should now exist
	has = cache.Has(key)
	require.True(t, has, "Should exist after Set")

	// Clear the cache
	err = cache.Clear(ctx)
	require.NoError(t, err)

	// Should no longer exist
	has = cache.Has(key)
	require.False(t, has, "Should not exist after Clear")
}

func TestGoCache_ServerIntegration(t *testing.T) {
	// Start a test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("server response"))
	}))
	defer server.Close()

	ctx := context.Background()
	cfg := CacheConfig{
		TTL:       10 * time.Minute,
		MaxSizeMb: 10,
	}

	cache, err := NewGoCache(ctx, cfg)
	require.NoError(t, err)

	// Make a request
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/test", nil)
	require.NoError(t, err)

	// Get from server
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Cache the response
	err = cache.Set(req, resp)
	require.NoError(t, err)

	// Get from cache
	cachedResp, err := cache.Get(req)
	require.NoError(t, err)
	require.NotNil(t, cachedResp)
	defer cachedResp.Body.Close()
	require.Equal(t, http.StatusOK, cachedResp.StatusCode)

	cachedBody, err := io.ReadAll(cachedResp.Body)
	require.NoError(t, err)
	require.Equal(t, "server response", string(cachedBody))
}

func TestNewGoCache_InvalidConfig(t *testing.T) {
	ctx := context.Background()

	// Test with 0 TTL - should still work but might not cache properly
	cfg := CacheConfig{
		TTL:       0,
		MaxSizeMb: 10,
	}

	_, err := NewGoCache(ctx, cfg)
	// With otter v2, TTL of 0 might still work but cache won't expire
	// The behavior depends on otter's implementation
	require.NoError(t, err)
}

func TestGoCache_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	cfg := CacheConfig{
		TTL:       10 * time.Minute,
		MaxSizeMb: 10,
	}

	cache, err := NewGoCache(ctx, cfg)
	require.NoError(t, err)

	// Create multiple goroutines that access the cache
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com/"+string(rune(id)), nil)
			resp := &http.Response{
				Status:     "200 OK",
				StatusCode: http.StatusOK,
				Proto:      "HTTP/1.1",
				Header:     make(http.Header),
				Body:       io.NopCloser(bytes.NewReader([]byte("test"))),
			}

			_ = cache.Set(req, resp)
			res, _ := cache.Get(req)
			if res != nil {
				defer res.Body.Close()
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
}
