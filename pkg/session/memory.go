package session

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/jellydator/ttlcache/v3"
)

// MemorySessionCache implements SessionCache interface using an in-memory store with TTL.
type MemorySessionCache struct {
	cache      map[string]*ttlcache.Cache[string, []byte] // namespace -> cache
	mu         sync.RWMutex
	defaultTTL time.Duration
}

// NewMemorySessionCache creates a new in-memory session cache with default TTL of 1 hour.
func NewMemorySessionCache(ctx context.Context, opt ...types.SessionCacheOption) (types.SessionCache, error) {
	cache := &MemorySessionCache{
		cache:      make(map[string]*ttlcache.Cache[string, []byte]),
		defaultTTL: time.Hour, // Default TTL
	}

	// Apply options
	for _, option := range opt {
		if err := option(ctx, cache); err != nil {
			return nil, fmt.Errorf("failed to apply session cache option: %w", err)
		}
	}

	return cache, nil
}

// NewMemorySessionCacheWithTTL creates a new in-memory session cache with custom TTL.
func NewMemorySessionCacheWithTTL(ctx context.Context, ttl time.Duration) (types.SessionCache, error) {
	return &MemorySessionCache{
		cache:      make(map[string]*ttlcache.Cache[string, []byte]),
		defaultTTL: ttl,
	}, nil
}

// getOrCreateNamespaceCache returns the cache for a namespace, creating it if needed.
func (m *MemorySessionCache) getOrCreateNamespaceCache(namespace string) *ttlcache.Cache[string, []byte] {
	c, ok := m.cache[namespace]
	if !ok {
		c = ttlcache.New[string, []byte](ttlcache.WithTTL[string, []byte](m.defaultTTL))
		go c.Start()
		m.cache[namespace] = c
	}
	return c
}

// Get retrieves a value from the cache by namespace and key.
func (m *MemorySessionCache) Get(ctx context.Context, namespace, key string) ([]byte, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c, ok := m.cache[namespace]
	if !ok || c == nil {
		return nil, false, nil
	}
	item := c.Get(key)
	if item == nil {
		return nil, false, nil
	}

	return item.Value(), true, nil
}

// Set stores a value in the cache with the given namespace and key.
func (m *MemorySessionCache) Set(ctx context.Context, namespace, key string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	c := m.getOrCreateNamespaceCache(namespace)
	c.Set(key, value, ttlcache.DefaultTTL)
	return nil
}

// Delete removes a value from the cache by namespace and key.
func (m *MemorySessionCache) Delete(ctx context.Context, namespace, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.cache[namespace]
	if ok && c != nil {
		c.Delete(key)
	}
	return nil
}

// Clear removes all values from the cache.
func (m *MemorySessionCache) Clear(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.cache {
		c.DeleteAll()
	}
	return nil
}

// GetAll returns all key-value pairs in a namespace.
func (m *MemorySessionCache) GetAll(ctx context.Context, namespace string) (map[string][]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c, ok := m.cache[namespace]
	if !ok || c == nil {
		return map[string][]byte{}, nil
	}
	result := make(map[string][]byte)
	for _, item := range c.Items() {
		result[item.Key()] = item.Value()
	}
	return result, nil
}

// GetMany retrieves multiple values from the cache by namespace and keys.
func (m *MemorySessionCache) GetMany(ctx context.Context, namespace string, keys []string) (map[string][]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	c, ok := m.cache[namespace]
	if !ok || c == nil {
		return map[string][]byte{}, nil
	}

	result := make(map[string][]byte)
	for _, key := range keys {
		item := c.Get(key)
		if item != nil {
			result[key] = item.Value()
		}
	}

	return result, nil
}

// SetMany stores multiple values in the cache with the given namespace.
func (m *MemorySessionCache) SetMany(ctx context.Context, namespace string, values map[string][]byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	c := m.getOrCreateNamespaceCache(namespace)
	for key, value := range values {
		c.Set(key, value, ttlcache.DefaultTTL)
	}

	return nil
}

// Close performs any necessary cleanup when the cache is no longer needed.
func (m *MemorySessionCache) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.cache {
		c.Stop()
	}
	m.cache = nil
	return nil
}
