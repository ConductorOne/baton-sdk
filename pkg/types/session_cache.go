package types

import (
	"context"
)

// SessionCache defines the interface for session cache operations.
type SessionCache interface {
	// Get retrieves a value from the cache by key.
	Get(ctx context.Context, key string, opt ...SessionCacheOption) ([]byte, bool, error)
	GetMany(ctx context.Context, keys []string, opt ...SessionCacheOption) (map[string][]byte, error)
	// Set stores a value in the cache with the given key.
	Set(ctx context.Context, key string, value []byte, opt ...SessionCacheOption) error
	SetMany(ctx context.Context, values map[string][]byte, opt ...SessionCacheOption) error
	// Delete removes a value from the cache by key.
	Delete(ctx context.Context, key string, opt ...SessionCacheOption) error
	// Clear removes all values from the cache.
	Clear(ctx context.Context, opt ...SessionCacheOption) error
	// GetAll returns all key-value pairs.
	GetAll(ctx context.Context, opt ...SessionCacheOption) (map[string][]byte, error)
	// Close performs any necessary cleanup when the cache is no longer needed.
	Close() error
}

type SessionCacheBag struct {
	SyncID string
	Prefix string
}

// SessionCacheOption is a function type that configures a session cache operation.
type SessionCacheOption func(ctx context.Context, bag *SessionCacheBag) error

// SessionCacheConstructorOption is a function type that configures a session cache constructor.
type SessionCacheConstructorOption func(ctx context.Context) (context.Context, error)

// SessionCacheConstructor is a function type that creates a new session cache instance.
type SessionCacheConstructor func(ctx context.Context, opt ...SessionCacheConstructorOption) (SessionCache, error)

// SyncIDKey is the context key for storing the current sync ID.
type SyncIDKey struct{}

// WithSyncID returns a SessionCacheOption that sets the sync ID for the operation.
func WithSyncID(syncID string) SessionCacheOption {
	return func(ctx context.Context, bag *SessionCacheBag) error {
		bag.SyncID = syncID
		return nil
	}
}

// GetSyncID retrieves the sync ID from the context, returning empty string if not found.
func GetSyncID(ctx context.Context) string {
	if syncID, ok := ctx.Value(SyncIDKey{}).(string); ok {
		return syncID
	}
	return ""
}
