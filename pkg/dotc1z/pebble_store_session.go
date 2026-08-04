package dotc1z

import (
	"context"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
)

func (s *pebbleStore) SessionStore() sessions.SessionStore {
	return pebbleStoreSessionStore{store: s}
}

type pebbleStoreSessionStore struct {
	store *pebbleStore
}

func (s pebbleStoreSessionStore) Get(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
	engine := s.store.Engine
	return engine.SessionGet(ctx, key, opt...)
}

func (s pebbleStoreSessionStore) Set(ctx context.Context, key string, value []byte, opt ...sessions.SessionStoreOption) error {
	return s.store.withMutation(func(e *pebble.Engine) error {
		return e.SessionSet(ctx, key, value, opt...)
	})
}

func (s pebbleStoreSessionStore) GetMany(ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string][]byte, []string, error) {
	engine := s.store.Engine
	return engine.SessionGetMany(ctx, keys, opt...)
}

func (s pebbleStoreSessionStore) GetAll(ctx context.Context, pageToken string, opt ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
	engine := s.store.Engine
	return engine.SessionGetAll(ctx, pageToken, opt...)
}

func (s pebbleStoreSessionStore) SetMany(ctx context.Context, values map[string][]byte, opt ...sessions.SessionStoreOption) error {
	return s.store.withMutation(func(e *pebble.Engine) error {
		return e.SessionSetMany(ctx, values, opt...)
	})
}

func (s pebbleStoreSessionStore) Delete(ctx context.Context, key string, opt ...sessions.SessionStoreOption) error {
	return s.store.withMutation(func(e *pebble.Engine) error {
		return e.SessionDelete(ctx, key, opt...)
	})
}

func (s pebbleStoreSessionStore) Clear(ctx context.Context, opt ...sessions.SessionStoreOption) error {
	return s.store.withMutation(func(e *pebble.Engine) error {
		return e.SessionClear(ctx, opt...)
	})
}
