package session

import (
	"context"
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
		key = bag.Prefix + "::" + key
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	syncCache, ok := m.cache[bag.SyncID]
	if !ok {
		return nil, false, nil
	}

	value, found := syncCache[key]
	return value, found, nil
}

// Set stores a value in the cache with the given key.
func (m *MemorySessionCache) Set(ctx context.Context, key string, value []byte, opt ...types.SessionCacheOption) error {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return err
	}

	if bag.Prefix != "" {
		key = bag.Prefix + "::" + key
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
		key = bag.Prefix + "::" + key
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

	m.mu.RLock()
	defer m.mu.RUnlock()

	syncCache, ok := m.cache[bag.SyncID]
	if !ok {
		return map[string][]byte{}, nil
	}

	result := make(map[string][]byte)
	for key, value := range syncCache {
		if bag.Prefix != "" {
			key = bag.Prefix + "::" + key
		}
		result[key] = value
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
		if bag.Prefix != "" {
			key = bag.Prefix + "::" + key
		}
		if value, found := syncCache[key]; found {
			result[key] = value
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
			key = bag.Prefix + "::" + key
		}
		syncCache[key] = value
	}

	return nil
}

// Close performs any necessary cleanup when the cache is no longer needed.
func (m *MemorySessionCache) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Clear all data
	m.cache = make(map[string]map[string][]byte)
	return nil
}

// namespacedSessionCache wraps a SessionCache to operate within a fixed namespace.
type namespacedSessionCache struct {
	cache     types.SessionCache
	namespace string
}

// prefixKey adds the namespace as a prefix to the key.
func (n *namespacedSessionCache) prefixKey(key string) string {
	if n.namespace == "" {
		return key
	}
	return n.namespace + "::" + key
}

// Get retrieves a value from the cache by key (namespace is fixed).
func (n *namespacedSessionCache) Get(ctx context.Context, key string, opt ...types.SessionCacheOption) ([]byte, bool, error) {
	// Prefix the key with the namespace for isolation
	prefixedKey := n.prefixKey(key)
	return n.cache.Get(ctx, prefixedKey, opt...)
}

// GetMany retrieves multiple values from the cache by keys (namespace is fixed).
func (n *namespacedSessionCache) GetMany(ctx context.Context, keys []string, opt ...types.SessionCacheOption) (map[string][]byte, error) {
	// Prefix all keys with the namespace for isolation
	prefixedKeys := make([]string, len(keys))
	for i, key := range keys {
		prefixedKeys[i] = n.prefixKey(key)
	}

	result, err := n.cache.GetMany(ctx, prefixedKeys, opt...)
	if err != nil {
		return nil, err
	}

	// Remove the namespace prefix from the result keys
	unprefixedResult := make(map[string][]byte)
	for prefixedKey, value := range result {
		unprefixedKey := strings.TrimPrefix(prefixedKey, n.namespace+"::")
		unprefixedResult[unprefixedKey] = value
	}

	return unprefixedResult, nil
}

// Set stores a value in the cache with the given key (namespace is fixed).
func (n *namespacedSessionCache) Set(ctx context.Context, key string, value []byte, opt ...types.SessionCacheOption) error {
	// Prefix the key with the namespace for isolation
	prefixedKey := n.prefixKey(key)
	return n.cache.Set(ctx, prefixedKey, value, opt...)
}

// SetMany stores multiple values in the cache (namespace is fixed).
func (n *namespacedSessionCache) SetMany(ctx context.Context, values map[string][]byte, opt ...types.SessionCacheOption) error {
	// Prefix all keys with the namespace for isolation
	prefixedValues := make(map[string][]byte)
	for key, value := range values {
		prefixedKey := n.prefixKey(key)
		prefixedValues[prefixedKey] = value
	}
	return n.cache.SetMany(ctx, prefixedValues, opt...)
}

// Delete removes a value from the cache by key (namespace is fixed).
func (n *namespacedSessionCache) Delete(ctx context.Context, key string, opt ...types.SessionCacheOption) error {
	// Prefix the key with the namespace for isolation
	prefixedKey := n.prefixKey(key)
	return n.cache.Delete(ctx, prefixedKey, opt...)
}

// GetAll returns all key-value pairs in the fixed namespace.
func (n *namespacedSessionCache) GetAll(ctx context.Context, opt ...types.SessionCacheOption) (map[string][]byte, error) {
	// Get all values and filter by namespace prefix
	allValues, err := n.cache.GetAll(ctx, opt...)
	if err != nil {
		return nil, err
	}

	// Filter to only include keys with our namespace prefix
	namespacePrefix := n.namespace + "::"
	result := make(map[string][]byte)
	for key, value := range allValues {
		if strings.HasPrefix(key, namespacePrefix) {
			unprefixedKey := strings.TrimPrefix(key, namespacePrefix)
			result[unprefixedKey] = value
		}
	}

	return result, nil
}

// Clear removes all values from the cache.
func (n *namespacedSessionCache) Clear(ctx context.Context, opt ...types.SessionCacheOption) error {
	// Get all values and delete only those with our namespace prefix
	allValues, err := n.cache.GetAll(ctx, opt...)
	if err != nil {
		return err
	}

	namespacePrefix := n.namespace + "::"
	for key := range allValues {
		if strings.HasPrefix(key, namespacePrefix) {
			err := n.cache.Delete(ctx, key, opt...)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Close performs any necessary cleanup when the cache is no longer needed.
func (n *namespacedSessionCache) Close() error {
	return n.cache.Close()
}

// WithNamespace returns a curried session cache that operates within a fixed namespace.
func (n *namespacedSessionCache) WithNamespace(namespace string) types.SessionCache {
	// For a namespaced cache, we can create another namespaced cache with the new namespace
	return &namespacedSessionCache{
		cache:     n.cache,
		namespace: namespace,
	}
}

// WithNamespace returns a curried session cache that operates within a fixed namespace.
func (m *MemorySessionCache) WithNamespace(namespace string) types.SessionCache {
	return &namespacedSessionCache{
		cache:     m,
		namespace: namespace,
	}
}
