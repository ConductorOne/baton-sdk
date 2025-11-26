package session

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

var _ sessions.SessionStore = (*NoOpSessionStore)(nil)

// Don't panic in dev (ideally).
type NoOpSessionStore struct{}

var ErrSessionStoreDisabled = fmt.Errorf("session store is disabled by connector author. It must be explicitly enabled via RunConnector WithSessionStoreEnabled()")

func (n *NoOpSessionStore) logAndError(ctx context.Context, operation string) error {
	l := ctxzap.Extract(ctx)
	l.Warn("NoOpSessionStore operation ignored", zap.String("operation", operation))
	return fmt.Errorf("%w: operation %s is not supported", ErrSessionStoreDisabled, operation)
}

func (n *NoOpSessionStore) Get(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
	return nil, false, n.logAndError(ctx, "Get")
}

func (n *NoOpSessionStore) GetMany(ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string][]byte, []string, error) {
	return nil, nil, n.logAndError(ctx, "GetMany")
}

func (n *NoOpSessionStore) Set(ctx context.Context, key string, value []byte, opt ...sessions.SessionStoreOption) error {
	return n.logAndError(ctx, "Set")
}

func (n *NoOpSessionStore) SetMany(ctx context.Context, values map[string][]byte, opt ...sessions.SessionStoreOption) error {
	return n.logAndError(ctx, "SetMany")
}

func (n *NoOpSessionStore) Delete(ctx context.Context, key string, opt ...sessions.SessionStoreOption) error {
	return n.logAndError(ctx, "Delete")
}

func (n *NoOpSessionStore) Clear(ctx context.Context, opt ...sessions.SessionStoreOption) error {
	// NOTE: we call this unconditionally for cleanup, so don't throw.
	return nil
}

func (n *NoOpSessionStore) GetAll(ctx context.Context, pageToken string, opt ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
	return nil, "", n.logAndError(ctx, "GetAll")
}
