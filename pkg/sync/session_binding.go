package sync

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/session"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
)

// sessionBinding is this sync's binding on the connector's session server,
// held from the moment the sync ID is known until Close.
type sessionBinding struct {
	unbind sessions.UnbindSessionStore
}

func (b *sessionBinding) bind(ctx context.Context, binder sessions.SessionStoreBinder, syncID string, store sessions.SessionStore) error {
	if binder == nil {
		return nil
	}
	if err := b.release(ctx); err != nil {
		return err
	}
	unbind, err := binder.BindSessionStore(ctx, syncID, store)
	if err != nil {
		return fmt.Errorf("binding session store for sync %s: %w", syncID, err)
	}
	b.unbind = unbind
	return nil
}

func (b *sessionBinding) release(ctx context.Context) error {
	if b.unbind == nil {
		return nil
	}
	unbind := b.unbind
	b.unbind = nil
	return unbind(ctx)
}

func (s *syncer) bindSessionStore(ctx context.Context) error {
	if s.cfg.sessionStoreBinder == nil {
		return nil
	}
	sessionStore := s.store.SessionStore()
	if s.ledgered {
		sessionStore = ledgerSessionStore{SessionStore: sessionStore}
	}
	// Instrumented so session-store cost is attributable in the sync
	// stats instead of vanishing into inflated connector-call latency
	// (e.g. a broken backend whose every request times out before the
	// connector falls back to real work).
	kind := "c1z_" + s.store.Metadata().Engine
	return s.sessionBinding.bind(ctx, s.cfg.sessionStoreBinder, s.syncID, session.NewInstrumentedSessionStore(sessionStore, kind, "", s.recordSessionOp))
}

func resolveSessionStoreBinder(setter sessions.SetSessionStore) sessions.SessionStoreBinder {
	switch v := setter.(type) {
	case nil:
		return nil
	case sessions.SessionStoreBinder:
		return v
	default:
		return attachOnlySessionStore{setter: setter}
	}
}

// attachOnlySessionStore adapts a setter that predates SessionStoreBinder. It
// keeps serving the last store it was given after that sync closes.
type attachOnlySessionStore struct {
	setter sessions.SetSessionStore
}

func (a attachOnlySessionStore) BindSessionStore(ctx context.Context, _ string, store sessions.SessionStore) (sessions.UnbindSessionStore, error) {
	a.setter.SetSessionStore(ctx, store)
	return func(context.Context) error { return nil }, nil
}
