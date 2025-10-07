package session

import (
	"context"

	"github.com/conductorone/baton-sdk/pkg/types/sessions"
)

// Test helper functions to maintain compatibility with existing tests

func (m *MemorySessionCache) GetWithSyncID(ctx context.Context, syncID, key string) ([]byte, bool, error) {
	return m.Get(ctx, key, sessions.WithSyncID(syncID))
}

func (m *MemorySessionCache) SetWithSyncID(ctx context.Context, syncID, key string, value []byte) error {
	return m.Set(ctx, key, value, sessions.WithSyncID(syncID))
}

func (m *MemorySessionCache) DeleteWithSyncID(ctx context.Context, syncID, key string) error {
	return m.Delete(ctx, key, sessions.WithSyncID(syncID))
}

func (m *MemorySessionCache) ClearWithSyncID(ctx context.Context, syncID string) error {
	return m.Clear(ctx, sessions.WithSyncID(syncID))
}

func (m *MemorySessionCache) GetAllWithSyncID(ctx context.Context, syncID string) (map[string][]byte, error) {
	return m.GetAll(ctx, sessions.WithSyncID(syncID))
}

func (m *MemorySessionCache) GetManyWithSyncID(ctx context.Context, syncID string, keys []string) (map[string][]byte, error) {
	return m.GetMany(ctx, keys, sessions.WithSyncID(syncID))
}

func (m *MemorySessionCache) SetManyWithSyncID(ctx context.Context, syncID string, values map[string][]byte) error {
	return m.SetMany(ctx, values, sessions.WithSyncID(syncID))
}
