package session

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/conductorone/baton-sdk/pkg/types"
)

// MemorySessionCache implements SessionCache interface using an in-memory store with TTL.
type MemorySessionCache struct {
	cache      map[string]map[string][]byte // syncID -> key -> value
	mu         sync.RWMutex
	defaultTTL time.Duration
}

// NewMemorySessionCache creates a new in-memory session cache with default TTL of 1 hour.
func NewMemorySessionCache(ctx context.Context, opt ...types.SessionCacheConstructorOption) (types.SessionCache, error) {
	return NewMemorySessionCacheWithTTL(ctx, time.Hour, opt...)
}

// NewMemorySessionCacheWithTTL creates a new in-memory session cache with custom TTL.
func NewMemorySessionCacheWithTTL(ctx context.Context, ttl time.Duration, opt ...types.SessionCacheConstructorOption) (types.SessionCache, error) {
	// Apply constructor options
	for _, option := range opt {
		var err error
		ctx, err = option(ctx)
		if err != nil {
			return nil, err
		}
	}

	return &MemorySessionCache{
		cache:      make(map[string]map[string][]byte),
		defaultTTL: ttl,
	}, nil
}

// Get retrieves a value from the cache by key.
func (m *MemorySessionCache) Get(ctx context.Context, key string, opt ...types.SessionCacheOption) ([]byte, bool, error) {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return nil, false, err
	}

	if bag.Prefix != "" {
		key = bag.Prefix + KeyPrefixDelimiter + key
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	syncCache, ok := m.cache[bag.SyncID]
	if !ok {
		return nil, false, nil
	}

	value, found := syncCache[key]
	if !found {
		return nil, false, nil
	}
	dst := make([]byte, len(value)) // allocate destination
	_ = copy(dst, value)
	return dst, true, nil
}

// Set stores a value in the cache with the given key.
func (m *MemorySessionCache) Set(ctx context.Context, key string, value []byte, opt ...types.SessionCacheOption) error {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return err
	}

	if bag.Prefix != "" {
		key = bag.Prefix + KeyPrefixDelimiter + key
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Get or create the sync cache
	syncCache, ok := m.cache[bag.SyncID]
	if !ok {
		syncCache = make(map[string][]byte)
		m.cache[bag.SyncID] = syncCache
	}

	syncCache[key] = value
	return nil
}

// Delete removes a value from the cache by key.
func (m *MemorySessionCache) Delete(ctx context.Context, key string, opt ...types.SessionCacheOption) error {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if bag.Prefix != "" {
		key = bag.Prefix + KeyPrefixDelimiter + key
	}

	syncCache, ok := m.cache[bag.SyncID]
	if ok {
		delete(syncCache, key)
	}
	return nil
}

// Clear removes all values from the cache.
func (m *MemorySessionCache) Clear(ctx context.Context, opt ...types.SessionCacheOption) error {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.cache, bag.SyncID)
	return nil
}

// GetAll returns all key-value pairs.
func (m *MemorySessionCache) GetAll(ctx context.Context, opt ...types.SessionCacheOption) (map[string][]byte, error) {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return nil, err
	}

	if bag.Prefix != "" {
		return nil, fmt.Errorf("prefix is not supported for GetAll in memory session cache")
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	syncCache, ok := m.cache[bag.SyncID]
	if !ok {
		return map[string][]byte{}, nil
	}

	result := make(map[string][]byte)
	for key, value := range syncCache {
		dst := make([]byte, len(value)) // allocate destination
		_ = copy(dst, value)
		result[key] = dst
	}
	return result, nil
}

// GetMany retrieves multiple values from the cache by keys.
func (m *MemorySessionCache) GetMany(ctx context.Context, keys []string, opt ...types.SessionCacheOption) (map[string][]byte, error) {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	syncCache, ok := m.cache[bag.SyncID]
	if !ok {
		return map[string][]byte{}, nil
	}

	result := make(map[string][]byte)
	for _, key := range keys {
		if value, found := syncCache[key]; found {
			k := strings.TrimPrefix(key, bag.Prefix+KeyPrefixDelimiter)
			dst := make([]byte, len(value)) // allocate destination
			_ = copy(dst, value)
			result[k] = dst
		}
	}

	return result, nil
}

// SetMany stores multiple values in the cache.
func (m *MemorySessionCache) SetMany(ctx context.Context, values map[string][]byte, opt ...types.SessionCacheOption) error {
	// Apply options to get syncID
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Get or create the sync cache
	syncCache, ok := m.cache[bag.SyncID]
	if !ok {
		syncCache = make(map[string][]byte)
		m.cache[bag.SyncID] = syncCache
	}

	for key, value := range values {
		if bag.Prefix != "" {
			key = bag.Prefix + KeyPrefixDelimiter + key
		}
		syncCache[key] = value
	}

	return nil
}

// Close performs any necessary cleanup when the cache is no longer needed.
func (m *MemorySessionCache) Close(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Clear all data
	m.cache = make(map[string]map[string][]byte)
	return nil
}
