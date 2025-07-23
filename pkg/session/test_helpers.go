package session

import (
	"context"

	"github.com/conductorone/baton-sdk/pkg/types"
)

// Test helper functions to maintain compatibility with existing tests

func (m *MemorySessionCache) GetWithSyncID(ctx context.Context, syncID, key string) ([]byte, bool, error) {
	return m.Get(ctx, key, types.WithSyncID(syncID))
}

func (m *MemorySessionCache) SetWithSyncID(ctx context.Context, syncID, key string, value []byte) error {
	return m.Set(ctx, key, value, types.WithSyncID(syncID))
}

func (m *MemorySessionCache) DeleteWithSyncID(ctx context.Context, syncID, key string) error {
	return m.Delete(ctx, key, types.WithSyncID(syncID))
}

func (m *MemorySessionCache) ClearWithSyncID(ctx context.Context, syncID string) error {
	return m.Clear(ctx, types.WithSyncID(syncID))
}

func (m *MemorySessionCache) GetAllWithSyncID(ctx context.Context, syncID string) (map[string][]byte, error) {
	return m.GetAll(ctx, types.WithSyncID(syncID))
}

func (m *MemorySessionCache) GetManyWithSyncID(ctx context.Context, syncID string, keys []string) (map[string][]byte, error) {
	return m.GetMany(ctx, keys, types.WithSyncID(syncID))
}

func (m *MemorySessionCache) SetManyWithSyncID(ctx context.Context, syncID string, values map[string][]byte) error {
	return m.SetMany(ctx, values, types.WithSyncID(syncID))
}

func (n *namespacedSessionCache) GetWithSyncID(ctx context.Context, syncID, key string) ([]byte, bool, error) {
	return n.Get(ctx, key, types.WithSyncID(syncID))
}

func (n *namespacedSessionCache) SetWithSyncID(ctx context.Context, syncID, key string, value []byte) error {
	return n.Set(ctx, key, value, types.WithSyncID(syncID))
}

func (n *namespacedSessionCache) DeleteWithSyncID(ctx context.Context, syncID, key string) error {
	return n.Delete(ctx, key, types.WithSyncID(syncID))
}

func (n *namespacedSessionCache) GetAllWithSyncID(ctx context.Context, syncID string) (map[string][]byte, error) {
	return n.GetAll(ctx, types.WithSyncID(syncID))
}

func (n *namespacedSessionCache) GetManyWithSyncID(ctx context.Context, syncID string, keys []string) (map[string][]byte, error) {
	return n.GetMany(ctx, keys, types.WithSyncID(syncID))
}

func (n *namespacedSessionCache) SetManyWithSyncID(ctx context.Context, syncID string, values map[string][]byte) error {
	return n.SetMany(ctx, values, types.WithSyncID(syncID))
}
