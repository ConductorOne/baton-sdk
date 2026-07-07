package pebble

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestDeferredIndexPendingSurvivesReopen pins the durable deferred-index
// marker: a sync whose grant writes deferred the by_principal index and was
// interrupted BEFORE EndSync must, after a process restart (simulated by
// close + reopen) and an idempotent resume that writes nothing, still run
// the by_principal rebuild at EndSync. Before the marker was persisted,
// the in-memory flag died with the process and the resumed EndSync saved a
// "finished" store whose by_principal index was silently missing every
// deferred-written grant.
func TestDeferredIndexPendingSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Process A: start a sync, write expanded grants (deferred by_principal),
	// then stop WITHOUT EndSync — the interrupt.
	e, err := Open(ctx, dir)
	require.NoError(t, err)
	a := NewAdapter(e)
	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	app := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "app", Resource: "github"}.Build(),
	}.Build()
	grants := make([]*v2.Grant, 0, 8)
	for _, principal := range []string{"alice", "bob", "carol", "dave"} {
		grants = append(grants, v2.Grant_builder{
			Id:          "app:github:read:user:" + principal,
			Entitlement: v2.Entitlement_builder{Id: "app:github:read", Resource: app}.Build(),
			Principal: v2.Resource_builder{
				Id: v2.ResourceId_builder{ResourceType: "user", Resource: principal}.Build(),
			}.Build(),
		}.Build())
	}
	require.NoError(t, a.Grants().StoreExpandedGrants(ctx, grants...))
	require.True(t, e.deferredIdxPending.Load(), "expanded writes must arm the deferred index flag")
	require.NoError(t, e.Close())

	// Process B: reopen, resume the same sync, write NOTHING (the idempotent
	// re-run), and EndSync.
	e2, err := Open(ctx, dir)
	require.NoError(t, err)
	defer e2.Close()
	require.True(t, e2.deferredIdxPending.Load(), "the pending marker must survive the reopen")

	a2 := NewAdapter(e2)
	require.NoError(t, a2.SetCurrentSync(ctx, syncID))
	require.NoError(t, a2.EndSync(ctx))

	// The by_principal index must serve every deferred-written grant.
	for _, principal := range []string{"alice", "bob", "carol", "dave"} {
		n := 0
		require.NoError(t, e2.IterateGrantsByPrincipal(ctx, "user", principal, func(*v3.GrantRecord) bool {
			n++
			return true
		}))
		require.Equalf(t, 1, n, "by_principal must index %s's grant after the resumed EndSync", principal)
	}

	// The marker is consumed: a third open owes nothing.
	require.NoError(t, e2.Close())
	e3, err := Open(ctx, dir)
	require.NoError(t, err)
	defer e3.Close()
	require.False(t, e3.deferredIdxPending.Load(), "a successful rebuild must clear the durable marker")
}
