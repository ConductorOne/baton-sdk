package types

import (
	"context"
)

// SessionCache defines the interface for session cache operations.
type SessionCache interface {
	// Get retrieves a value from the cache by namespace and key.
	Get(ctx context.Context, namespace, key string) ([]byte, bool, error)
	GetMany(ctx context.Context, namespace string, keys []string) (map[string][]byte, error)
	// Set stores a value in the cache with the given namespace and key.
	Set(ctx context.Context, namespace, key string, value []byte) error
	SetMany(ctx context.Context, namespace string, values map[string][]byte) error
	// Delete removes a value from the cache by namespace and key.
	Delete(ctx context.Context, namespace, key string) error
	// Clear removes all values from the cache.
	Clear(ctx context.Context) error
	// GetAll returns all key-value pairs in a namespace.
	GetAll(ctx context.Context, namespace string) (map[string][]byte, error)
	// Close performs any necessary cleanup when the cache is no longer needed.
	Close() error
}

// SessionCacheOption is a function type that configures a session cache.
type SessionCacheOption func(ctx context.Context, cache SessionCache) error

// SessionCacheConstructor is a function type that creates a new session cache instance.
type SessionCacheConstructor func(ctx context.Context, opt ...SessionCacheOption) (SessionCache, error)
