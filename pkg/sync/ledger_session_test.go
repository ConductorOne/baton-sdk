package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/stretchr/testify/require"
)

func TestLedgerSessionMutationsSurviveFailedPage(t *testing.T) {
	for _, method := range []string{"set", "set-many", "delete", "clear"} {
		t.Run(method, func(t *testing.T) {
			f := newLedgerFixture(t)
			syncID := f.engine.CurrentSyncID()
			opts := []sessions.SessionStoreOption{sessions.WithSyncID(syncID), sessions.WithPrefix("selected/")}
			other := []sessions.SessionStoreOption{sessions.WithSyncID(syncID), sessions.WithPrefix("other/")}
			session := ledgerSessionStore{SessionStore: f.store.SessionStore()}
			require.NoError(t, session.Set(t.Context(), "seed", []byte("before"), opts...))
			require.NoError(t, session.Set(t.Context(), "seed", []byte("untouched"), other...))
			runtime, err := newTestLedgerRuntime(t.Context(), f.ledger, "attempt")
			require.NoError(t, err)
			failed := errors.New("connector failed after session mutation")
			id := c1zstore.LedgerActionIdentity{Op: SyncResourceTypesOp.String()}
			f.audit.enter(ledgerHandler)
			_, err = runtime.runPage(t.Context(), 0, id, func(ctx context.Context, page *ledgerPage) error {
				require.NoError(t, page.writer.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "discard"}.Build()))
				switch method {
				case "set":
					require.NoError(t, session.Set(ctx, "seed", []byte("after"), opts...))
				case "set-many":
					require.NoError(t, session.SetMany(ctx, map[string][]byte{"seed": []byte("after"), "second": []byte("added")}, opts...))
				case "delete":
					require.NoError(t, session.Delete(ctx, "seed", opts...))
				case "clear":
					require.NoError(t, session.Clear(ctx, opts...))
				}
				return failed
			})
			f.audit.enter(ledgerLifecycle)
			require.ErrorIs(t, err, failed)
			require.NoError(t, f.store.Close(t.Context()))
			f = openLedgerFixtureAt(t, f.path, false)
			require.NoError(t, f.store.SetCurrentSync(t.Context(), syncID))
			_, found, err := f.ledger.GetLedgerRow(t.Context(), id)
			require.NoError(t, err)
			require.False(t, found)
			records, err := f.store.ListResourceTypes(t.Context(), &v2.ResourceTypesServiceListResourceTypesRequest{})
			require.NoError(t, err)
			require.Empty(t, records.GetList())
			value, found, err := f.store.SessionStore().Get(t.Context(), "seed", opts...)
			require.NoError(t, err)
			require.Equal(t, method == "set" || method == "set-many", found)
			if found {
				require.Equal(t, []byte("after"), value)
			}
			value, found, err = f.store.SessionStore().Get(t.Context(), "seed", other...)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, []byte("untouched"), value)
			if method == "set-many" {
				value, found, err = f.store.SessionStore().Get(t.Context(), "second", opts...)
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, []byte("added"), value)
			}
		})
	}
}
