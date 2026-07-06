package dotc1z

import (
	"context"

	"github.com/conductorone/baton-sdk/pkg/types/sessions"
)

func (s *pebbleStore) SessionStore() sessions.SessionStore {
	return pebbleStoreSessionStore{store: s}
}

type pebbleStoreSessionStore struct {
	store *pebbleStore
}

func (s pebbleStoreSessionStore) Get(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
	return s.store.engine.SessionGet(ctx, key, opt...)
}

func (s pebbleStoreSessionStore) Set(ctx context.Context, key string, value []byte, opt ...sessions.SessionStoreOption) error {
	return s.store.markDirty(s.store.engine.SessionSet(ctx, key, value, opt...))
}

func (s pebbleStoreSessionStore) GetMany(ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string][]byte, []string, error) {
	return s.store.engine.SessionGetMany(ctx, keys, opt...)
}

func (s pebbleStoreSessionStore) GetAll(ctx context.Context, pageToken string, opt ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
	return s.store.engine.SessionGetAll(ctx, pageToken, opt...)
}

func (s pebbleStoreSessionStore) SetMany(ctx context.Context, values map[string][]byte, opt ...sessions.SessionStoreOption) error {
	return s.store.markDirty(s.store.engine.SessionSetMany(ctx, values, opt...))
}

func (s pebbleStoreSessionStore) Delete(ctx context.Context, key string, opt ...sessions.SessionStoreOption) error {
	return s.store.markDirty(s.store.engine.SessionDelete(ctx, key, opt...))
}

func (s pebbleStoreSessionStore) Clear(ctx context.Context, opt ...sessions.SessionStoreOption) error {
	return s.store.markDirty(s.store.engine.SessionClear(ctx, opt...))
}
