package pebble

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
)

// TestSessionWritesAllowedWhileSealed pins the post-EndSync session
// lifecycle: the production sync flow calls connector Cleanup AFTER EndSync
// seals the engine, and Cleanup clears the session keyspace so connector
// scratch state doesn't ship in the saved c1z. Session writes are scratch
// state, not sync records — they must ride the AllowSealed barrier like the
// sync-run metadata stamps, not be refused with ErrEngineSealed (which
// silently baked sessions into every saved artifact).
func TestSessionWritesAllowedWhileSealed(t *testing.T) {
	ctx := context.Background()
	e, err := Open(ctx, t.TempDir())
	require.NoError(t, err)
	defer e.Close()
	a := NewAdapter(e)
	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	sid := sessions.WithSyncID(syncID)

	require.NoError(t, e.SessionSet(ctx, "cache-key", []byte("cache-value"), sid))

	require.NoError(t, a.EndSync(ctx))
	require.True(t, e.IsSealed(), "EndSync must seal")

	// The Cleanup-path clear must succeed on the sealed engine.
	require.NoError(t, e.SessionClear(ctx, sid), "SessionClear after EndSync (connector Cleanup path)")
	_, found, err := e.SessionGet(ctx, "cache-key", sid)
	require.NoError(t, err)
	require.False(t, found, "cleared session key must be gone from the sealed store")

	// The other session writers share the same lifecycle contract.
	require.NoError(t, e.SessionSet(ctx, "k2", []byte("v2"), sid), "SessionSet while sealed")
	require.NoError(t, e.SessionSetMany(ctx, map[string][]byte{"k3": []byte("v3")}, sid), "SessionSetMany while sealed")
	require.NoError(t, e.SessionDelete(ctx, "k2", sid), "SessionDelete while sealed")

	// Sanity: record writes stay refused — the seal still means what it
	// meant.
	require.ErrorIs(t, e.withWrite(func() error { return nil }), ErrEngineSealed,
		"record writes must still be refused while sealed")
}
