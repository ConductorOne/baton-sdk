package sessions

import (
	"context"

	"github.com/maypok86/otter/v2"
)

type SessionStoreKey struct{}

type SessionStore interface {
	Get(ctx context.Context, key string, opt ...SessionStoreOption) ([]byte, bool, error)
	GetMany(ctx context.Context, keys []string, opt ...SessionStoreOption) (map[string][]byte, error)
	Set(ctx context.Context, key string, value []byte, opt ...SessionStoreOption) error
	SetMany(ctx context.Context, values map[string][]byte, opt ...SessionStoreOption) error
	Delete(ctx context.Context, key string, opt ...SessionStoreOption) error
	Clear(ctx context.Context, opt ...SessionStoreOption) error
	GetAll(ctx context.Context, pageToken string, opt ...SessionStoreOption) (map[string][]byte, string, error)
}

type SessionStoreOption func(ctx context.Context, bag *SessionStoreBag) error

type SessionStoreConstructor func(ctx context.Context, opt ...SessionStoreConstructorOption) (SessionStore, error)

type SessionStoreConstructorOption func(ctx context.Context) (context.Context, error)

type WeightedValue struct {
	V interface{}
	W uint32
}

type Cache = otter.Cache[string, *WeightedValue]

type SessionStoreBag struct {
	SyncID    string
	Prefix    string
	PageToken string
	Cache     *Cache
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

func WithPrefix(prefix string) SessionStoreOption {
	return func(ctx context.Context, bag *SessionStoreBag) error {
		bag.Prefix = prefix
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

func WithPageToken(pageToken string) SessionStoreOption {
	return func(ctx context.Context, bag *SessionStoreBag) error {
		bag.PageToken = pageToken
		return nil
	}
}

func WithCache(cache *Cache) SessionStoreOption {
	return func(ctx context.Context, bag *SessionStoreBag) error {
		bag.Cache = cache
		return nil
	}
}

func SetSyncIDInContext(ctx context.Context, syncID string) context.Context {
	return context.WithValue(ctx, SyncIDKey{}, syncID)
}

type SetSessionStore interface {
	SetSessionStore(ctx context.Context, store SessionStore)
}
