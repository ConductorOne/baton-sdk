package sync

import (
	"context"
	"fmt"
	"time"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/session"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
)

const sessionDrainTimeout = 30 * time.Second

type sessionBinding struct {
	unbind sessions.UnbindSessionStore
}

func (b *sessionBinding) bind(
	ctx context.Context,
	binder sessions.SessionStoreBinder,
	syncID string,
	store c1zstore.Store,
	ledgered bool,
	onOp session.SessionOpObserver,
) error {
	if binder == nil {
		return nil
	}
	if err := b.release(ctx); err != nil {
		return err
	}
	sessionStore := store.SessionStore()
	if ledgered {
		sessionStore = ledgerSessionStore{SessionStore: sessionStore}
	}
	kind := "c1z_" + store.Metadata().Engine
	unbind, err := binder.BindSessionStore(ctx, syncID, session.NewInstrumentedSessionStore(sessionStore, kind, "", onOp))
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
	ctx, cancel := context.WithTimeout(ctx, sessionDrainTimeout)
	defer cancel()
	return unbind(ctx)
}
