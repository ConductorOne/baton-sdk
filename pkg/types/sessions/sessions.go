package sessions

import (
	"context"
)

type SessionStoreKey struct{}

type SessionStore interface {
	Get(ctx context.Context, key string, opt ...SessionStoreOption) ([]byte, bool, error)
	GetMany(ctx context.Context, keys []string, opt ...SessionStoreOption) (map[string][]byte, error)
	Set(ctx context.Context, key string, value []byte, opt ...SessionStoreOption) error
	SetMany(ctx context.Context, values map[string][]byte, opt ...SessionStoreOption) error
	Delete(ctx context.Context, key string, opt ...SessionStoreOption) error
	Clear(ctx context.Context, opt ...SessionStoreOption) error
	GetAll(ctx context.Context, opt ...SessionStoreOption) (map[string][]byte, error)
	Close(ctx context.Context) error
}

type SessionStoreOption func(ctx context.Context, bag *SessionStoreBag) error

type SessionStoreConstructor func(ctx context.Context, opt ...SessionStoreConstructorOption) (SessionStore, error)

type SessionStoreConstructorOption func(ctx context.Context) (context.Context, error)

type SessionStoreBag struct {
	SyncID string
	Prefix string
}

// SyncIDKey is the context key for storing the current sync ID.
type SyncIDKey struct{}

// WithSyncID returns a SessionCacheOption that sets the sync ID for the operation.
func WithSyncID(syncID string) SessionStoreOption {
	return func(ctx context.Context, bag *SessionStoreBag) error {
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
