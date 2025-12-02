package connectorbuilder

import (
	"context"

	"github.com/conductorone/baton-sdk/pkg/types/sessions"
)

var _ sessions.SessionStore = (*SessionStoreWithSyncID)(nil)

// SessionStoreWithSyncID wraps a SessionStore to automatically inject sync ID into all operations.
type SessionStoreWithSyncID struct {
	ss     sessions.SessionStore
	syncID string
}

// WithSyncId creates a new SessionStore wrapper that prepends sync ID to all operations.
func WithSyncId(ss sessions.SessionStore, syncID string) sessions.SessionStore {
	return &SessionStoreWithSyncID{
		ss:     ss,
		syncID: syncID,
	}
}

func (w *SessionStoreWithSyncID) Get(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
	opts := append([]sessions.SessionStoreOption{sessions.WithSyncID(w.syncID)}, opt...)
	return w.ss.Get(ctx, key, opts...)
}

func (w *SessionStoreWithSyncID) GetMany(ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string][]byte, []string, error) {
	opts := append([]sessions.SessionStoreOption{sessions.WithSyncID(w.syncID)}, opt...)
	return w.ss.GetMany(ctx, keys, opts...)
}

func (w *SessionStoreWithSyncID) Set(ctx context.Context, key string, value []byte, opt ...sessions.SessionStoreOption) error {
	opts := append([]sessions.SessionStoreOption{sessions.WithSyncID(w.syncID)}, opt...)
	return w.ss.Set(ctx, key, value, opts...)
}

func (w *SessionStoreWithSyncID) SetMany(ctx context.Context, values map[string][]byte, opt ...sessions.SessionStoreOption) error {
	opts := append([]sessions.SessionStoreOption{sessions.WithSyncID(w.syncID)}, opt...)
	return w.ss.SetMany(ctx, values, opts...)
}

func (w *SessionStoreWithSyncID) Delete(ctx context.Context, key string, opt ...sessions.SessionStoreOption) error {
	opts := append([]sessions.SessionStoreOption{sessions.WithSyncID(w.syncID)}, opt...)
	return w.ss.Delete(ctx, key, opts...)
}

func (w *SessionStoreWithSyncID) Clear(ctx context.Context, opt ...sessions.SessionStoreOption) error {
	opts := append([]sessions.SessionStoreOption{sessions.WithSyncID(w.syncID)}, opt...)
	return w.ss.Clear(ctx, opts...)
}

func (w *SessionStoreWithSyncID) GetAll(ctx context.Context, pageToken string, opt ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
	opts := append([]sessions.SessionStoreOption{sessions.WithSyncID(w.syncID)}, opt...)
	return w.ss.GetAll(ctx, pageToken, opts...)
}
