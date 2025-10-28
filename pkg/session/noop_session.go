package session

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

var _ sessions.SessionStore = (*NoOpSessionStore)(nil)

// NoOpSessionStore is a SessionStore implementation that either returns errors
// or logs warnings and does nothing (no-op). Useful for testing or disabling
// session storage functionality.
type NoOpSessionStore struct {
	returnErrors bool
	errorMessage string
}

// NoOpSessionStoreOption configures a NoOpSessionStore.
type NoOpSessionStoreOption func(*NoOpSessionStore)

// WithErrors configures the store to return errors instead of logging warnings.
func WithErrors(message string) NoOpSessionStoreOption {
	return func(s *NoOpSessionStore) {
		s.returnErrors = true
		s.errorMessage = message
	}
}

// NewNoOpSessionStore creates a new NoOpSessionStore.
// By default, it logs warnings and does nothing. Use WithErrors() to make it return errors instead.
func NewNoOpSessionStore(opts ...NoOpSessionStoreOption) *NoOpSessionStore {
	s := &NoOpSessionStore{
		returnErrors: false,
		errorMessage: "session store is disabled",
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (n *NoOpSessionStore) logOrError(ctx context.Context, operation string) error {
	if n.returnErrors {
		return fmt.Errorf("%s: %s", operation, n.errorMessage)
	}
	l := ctxzap.Extract(ctx)
	l.Warn("NoOpSessionStore operation ignored",
		zap.String("operation", operation),
		zap.String("message", n.errorMessage),
	)
	return nil
}

func (n *NoOpSessionStore) Get(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
	err := n.logOrError(ctx, "Get")
	if err != nil {
		return nil, false, err
	}
	return nil, false, nil
}

func (n *NoOpSessionStore) GetMany(ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string][]byte, error) {
	err := n.logOrError(ctx, "GetMany")
	if err != nil {
		return nil, err
	}
	return make(map[string][]byte), nil
}

func (n *NoOpSessionStore) Set(ctx context.Context, key string, value []byte, opt ...sessions.SessionStoreOption) error {
	return n.logOrError(ctx, "Set")
}

func (n *NoOpSessionStore) SetMany(ctx context.Context, values map[string][]byte, opt ...sessions.SessionStoreOption) error {
	return n.logOrError(ctx, "SetMany")
}

func (n *NoOpSessionStore) Delete(ctx context.Context, key string, opt ...sessions.SessionStoreOption) error {
	return n.logOrError(ctx, "Delete")
}

func (n *NoOpSessionStore) Clear(ctx context.Context, opt ...sessions.SessionStoreOption) error {
	return n.logOrError(ctx, "Clear")
}

func (n *NoOpSessionStore) GetAll(ctx context.Context, pageToken string, opt ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
	err := n.logOrError(ctx, "GetAll")
	if err != nil {
		return nil, "", err
	}
	return make(map[string][]byte), "", nil
}
