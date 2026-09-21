package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
)

type ledgerSessionStore struct{ sessions.SessionStore }

func ledgerSessionContext(ctx context.Context) context.Context {
	return c1zstore.WithPageWriteBypass(ctx, "connector session state is independent of committed sync records and action progress")
}
func (s ledgerSessionStore) Set(ctx context.Context, key string, value []byte, opts ...sessions.SessionStoreOption) error {
	return s.SessionStore.Set(ledgerSessionContext(ctx), key, value, opts...)
}
func (s ledgerSessionStore) SetMany(ctx context.Context, values map[string][]byte, opts ...sessions.SessionStoreOption) error {
	return s.SessionStore.SetMany(ledgerSessionContext(ctx), values, opts...)
}
func (s ledgerSessionStore) Delete(ctx context.Context, key string, opts ...sessions.SessionStoreOption) error {
	return s.SessionStore.Delete(ledgerSessionContext(ctx), key, opts...)
}
func (s ledgerSessionStore) Clear(ctx context.Context, opts ...sessions.SessionStoreOption) error {
	return s.SessionStore.Clear(ledgerSessionContext(ctx), opts...)
}
