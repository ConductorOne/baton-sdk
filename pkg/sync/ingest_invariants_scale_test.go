package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// Scale and inertness pins for the dangling-reference drops:
//
//   - the drop paths must stream chunked batches (a large dangling
//     population costs seconds, not one fsync per row);
//   - the invariants must be COMPLETELY inert on SQLite stores and on
//     partial syncs, in both modes.

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/logging"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

// TestIngestInvariantI8DropScale drops 100k grants under one dangling
// entitlement. With the streaming chunked-batch implementation this is
// seconds; a per-row-fsync implementation would blow the test timeout
// (100k synced commits), so completion itself pins the batching.
func TestIngestInvariantI8DropScale(t *testing.T) {
	if testing.Short() {
		t.Skip("scale test")
	}
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	repo, err := rs.NewResource("Repo r1", equivRepoRT, "r1")
	require.NoError(t, err)

	// No eq_repo TYPE row: a disabled-type gap, which is the drop arm of
	// the replay policy (an enabled-type dangling would fail instead).
	cur := newRepairTestStore(ctx, t, filepath.Join(tmpDir, "cur.c1z"), tmpDir)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, cur.PutResources(ctx, repo))

	const grantCount = 100_000
	batch := make([]*v2.Grant, 0, 1000)
	for i := 0; i < grantCount; i++ {
		principal := v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: fmt.Sprintf("u-%06d", i)}.Build(),
		}.Build()
		// All grants reference the same entitlement, which has no row.
		batch = append(batch, grant.NewGrant(repo, "ghost", principal.GetId()))
		if len(batch) == cap(batch) {
			require.NoError(t, cur.PutGrants(ctx, batch...))
			batch = batch[:0]
		}
	}
	require.NoError(t, cur.PutGrants(ctx, batch...))

	s := &syncer{store: cur, syncType: connectorstore.SyncTypeFull}
	start := time.Now()
	require.NoError(t, s.checkGrantEntitlementReferences(ctx))
	elapsed := time.Since(start)
	t.Logf("dropped %d dangling grants in %v", grantCount, elapsed)

	resp, err := cur.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageSize: 1}.Build())
	require.NoError(t, err)
	require.Empty(t, resp.GetList(), "every dangling grant must be dropped")

	// Idempotent second pass, and fail-fast agrees nothing remains.
	require.NoError(t, s.checkGrantEntitlementReferences(ctx))
	s.failFastInvariants = true
	require.NoError(t, s.checkGrantEntitlementReferences(ctx))

	require.NoError(t, cur.Close(ctx))
}

// TestReferentialInvariantsInertOnSQLite pins the SQLite gate: the
// referential invariants require IngestInvariantStore (Pebble-only), so on a
// SQLite store they are complete no-ops in both modes — no error, no
// drop — even with flagrantly dangling data present.
func TestReferentialInvariantsInertOnSQLite(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	store, err := dotc1z.NewStore(ctx, filepath.Join(tmpDir, "sqlite.c1z"),
		dotc1z.WithEngine(c1zstore.EngineSQLite),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	_, ok := any(store).(dotc1z.IngestInvariantStore)
	require.False(t, ok, "SQLite store must not implement IngestInvariantStore; if it grows the capability, revisit the drop gates")

	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	repo, err := rs.NewResource("Repo r1", equivRepoRT, "r1")
	require.NoError(t, err)
	require.NoError(t, store.PutResourceTypes(ctx, equivRepoRT))
	// Dangling everything: entitlement without its resource, grant
	// without its entitlement or principal.
	danglingEnt := et.NewAssignmentEntitlement(repo, "admin")
	require.NoError(t, store.PutEntitlements(ctx, danglingEnt))
	ghost := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "ghost"}.Build(),
	}.Build()
	g := grant.NewGrant(repo, "ghost-slug", ghost.GetId())
	require.NoError(t, store.PutGrants(ctx, g))

	for _, failFast := range []bool{false, true} {
		s := &syncer{store: store, syncType: connectorstore.SyncTypeFull, failFastInvariants: failFast}
		require.NoError(t, s.checkEntitlementResourceReferences(ctx))
		require.NoError(t, s.checkGrantResourceReferences(ctx))
		require.NoError(t, s.checkGrantEntitlementReferences(ctx))
		require.NoError(t, s.checkGrantPrincipalReferences(ctx))
	}

	// Nothing was dropped.
	_, err = store.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
		EntitlementId: danglingEnt.GetId(),
	}.Build())
	require.NoError(t, err)
	_, err = store.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{
		GrantId: g.GetId(),
	}.Build())
	require.NoError(t, err)

	require.NoError(t, store.Close(ctx))
}

// TestReferentialInvariantsInertOnPartialSync pins the sync-type gate:
// a partial sync's store legitimately lacks rows that live in the base
// full sync, so the referential invariants must not evaluate at all —
// no error even in fail-fast mode, no drops.
func TestReferentialInvariantsInertOnPartialSync(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	cur := newRepairTestStore(ctx, t, filepath.Join(tmpDir, "cur.c1z"), tmpDir)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
	require.NoError(t, err)
	repo, err := rs.NewResource("Repo r1", equivRepoRT, "r1")
	require.NoError(t, err)
	require.NoError(t, cur.PutResourceTypes(ctx, equivRepoRT))
	ghost := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "ghost"}.Build(),
	}.Build()
	g := grant.NewGrant(repo, "ghost-slug", ghost.GetId())
	require.NoError(t, cur.PutGrants(ctx, g))

	for _, failFast := range []bool{false, true} {
		s := &syncer{store: cur, syncType: connectorstore.SyncTypePartial, failFastInvariants: failFast}
		require.NoError(t, s.checkEntitlementResourceReferences(ctx))
		require.NoError(t, s.checkGrantResourceReferences(ctx))
		require.NoError(t, s.checkGrantEntitlementReferences(ctx))
		require.NoError(t, s.checkGrantPrincipalReferences(ctx))
	}

	_, err = cur.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{
		GrantId: g.GetId(),
	}.Build())
	require.NoError(t, err, "partial syncs must never drop")

	require.NoError(t, cur.Close(ctx))
}
