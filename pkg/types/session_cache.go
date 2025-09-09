package types

import (
	"context"
)

// SessionCacheKey is the context key for storing the session cache instance.
type SessionCacheKey struct{}

// SessionCache is an interface for caching session data.
type SessionCache interface {
	Get(ctx context.Context, key string, opt ...SessionCacheOption) ([]byte, bool, error)
	GetMany(ctx context.Context, keys []string, opt ...SessionCacheOption) (map[string][]byte, error)
	Set(ctx context.Context, key string, value []byte, opt ...SessionCacheOption) error
	SetMany(ctx context.Context, values map[string][]byte, opt ...SessionCacheOption) error
	Delete(ctx context.Context, key string, opt ...SessionCacheOption) error
	Clear(ctx context.Context, opt ...SessionCacheOption) error
	GetAll(ctx context.Context, opt ...SessionCacheOption) (map[string][]byte, error)
	Close(ctx context.Context) error
}

// SessionCacheOption is a function that modifies a SessionCacheBag.
type SessionCacheOption func(ctx context.Context, bag *SessionCacheBag) error

// SessionCacheConstructor is a function that creates a SessionCache instance.
type SessionCacheConstructor func(ctx context.Context, opt ...SessionCacheConstructorOption) (SessionCache, error)

// SessionCacheConstructorOption is a function that modifies the context for session cache construction.
type SessionCacheConstructorOption func(ctx context.Context) (context.Context, error)

// SessionCacheBag holds the configuration for session cache operations.
type SessionCacheBag struct {
	SyncID string
	Prefix string
}

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

func SetSyncIDInContext(ctx context.Context, syncID string) context.Context {
	return context.WithValue(ctx, SyncIDKey{}, syncID)
}
