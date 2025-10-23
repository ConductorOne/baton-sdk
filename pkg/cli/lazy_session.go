package cli

import (
	"context"
	"sync"

	"github.com/conductorone/baton-sdk/pkg/types/sessions"
)

var _ sessions.SessionStore = (*lazySessionStore)(nil)

// lazySessionStore implements types.SessionStore interface but only creates the actual session
// when a method is called for the first time.
type lazySessionStore struct {
	constructor sessions.SessionStoreConstructor
	once        sync.Once
	session     sessions.SessionStore
	err         error
}

// ensureSession creates the actual session store if it hasn't been created yet.
func (l *lazySessionStore) ensureSession(ctx context.Context) error {
	l.once.Do(func() {
		l.session, l.err = l.constructor(ctx)
	})
	return l.err
}

// Get implements types.SessionStore.
func (l *lazySessionStore) Get(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
	if err := l.ensureSession(ctx); err != nil {
		return nil, false, err
	}
	return l.session.Get(ctx, key, opt...)
}

// GetMany implements types.SessionStore.
func (l *lazySessionStore) GetMany(ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string][]byte, error) {
	if err := l.ensureSession(ctx); err != nil {
		return nil, err
	}
	return l.session.GetMany(ctx, keys, opt...)
}

// Set implements types.SessionStore.
func (l *lazySessionStore) Set(ctx context.Context, key string, value []byte, opt ...sessions.SessionStoreOption) error {
	if err := l.ensureSession(ctx); err != nil {
		return err
	}
	return l.session.Set(ctx, key, value, opt...)
}

// SetMany implements types.SessionStore.
func (l *lazySessionStore) SetMany(ctx context.Context, values map[string][]byte, opt ...sessions.SessionStoreOption) error {
	if err := l.ensureSession(ctx); err != nil {
		return err
	}
	return l.session.SetMany(ctx, values, opt...)
}

// Delete implements types.SessionStore.
func (l *lazySessionStore) Delete(ctx context.Context, key string, opt ...sessions.SessionStoreOption) error {
	if err := l.ensureSession(ctx); err != nil {
		return err
	}
	return l.session.Delete(ctx, key, opt...)
}

// Clear implements types.SessionStore.
func (l *lazySessionStore) Clear(ctx context.Context, opt ...sessions.SessionStoreOption) error {
	if err := l.ensureSession(ctx); err != nil {
		return err
	}
	return l.session.Clear(ctx, opt...)
}

// GetAll implements types.SessionStore.
func (l *lazySessionStore) GetAll(ctx context.Context, pageToken string, opt ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
	if err := l.ensureSession(ctx); err != nil {
		return nil, "", err
	}
	return l.session.GetAll(ctx, pageToken, opt...)
}
