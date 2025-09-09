package types

import (
	"context"
)

type SessionCacheKey struct{}

type SessionStore interface {
	Get(ctx context.Context, key string, opt ...SessionOption) ([]byte, bool, error)
	GetMany(ctx context.Context, keys []string, opt ...SessionOption) (map[string][]byte, error)
	Set(ctx context.Context, key string, value []byte, opt ...SessionOption) error
	SetMany(ctx context.Context, values map[string][]byte, opt ...SessionOption) error
	Delete(ctx context.Context, key string, opt ...SessionOption) error
	Clear(ctx context.Context, opt ...SessionOption) error
	GetAll(ctx context.Context, opt ...SessionOption) (map[string][]byte, error)
	Close() error
}

type SessionOption func(ctx context.Context, bag *SessionBag) error

type SessionConstructor func(ctx context.Context, opt ...SessionConstructorOption) (SessionStore, error)

type SessionConstructorOption func(ctx context.Context) (context.Context, error)

type SessionBag struct {
	SyncID string
	Prefix string
}

type SyncIDKey struct{}

func WithSyncID(syncID string) SessionOption {
	return func(ctx context.Context, bag *SessionBag) error {
		bag.SyncID = syncID
		return nil
	}
}

func GetSyncID(ctx context.Context) string {
	if syncID, ok := ctx.Value(SyncIDKey{}).(string); ok {
		return syncID
	}
	return ""
}

func SetSyncIDInContext(ctx context.Context, syncID string) context.Context {
	return context.WithValue(ctx, SyncIDKey{}, syncID)
}
