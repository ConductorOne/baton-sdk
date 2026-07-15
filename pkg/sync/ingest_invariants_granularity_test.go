package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// Granularity tests for the invariant exemptions and gates:
//
//   - I8's InsertResourceGrants exemption is per GRANT (the ownership
//     rule's unit), not per entitlement resource — one machinery-owned
//     IRG grant must not launder connector-owned dangling references
//     that share its resource.
//   - I6 (scope/manifest consistency) is skipped for expand-grants-only
//     syncs: compaction's fold output deliberately clears the manifest
//     while keeping scope stamps, so on that path every stamped scope of
//     a healthy artifact would read as an orphan.

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

// TestIngestInvariantI8ExemptionIsPerGrant pins the granularity: a
// resource holding BOTH a machinery-owned IRG grant (dangling by design)
// and a connector-owned grant referencing a different missing entitlement
// must keep the former and drop (or fail on) the latter.
func TestIngestInvariantI8ExemptionIsPerGrant(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	repo, err := rs.NewResource("Repo r1", equivRepoRT, "r1")
	require.NoError(t, err)
	irgGrant := repairTestGrant(t, repo) // "admin" slug, carries InsertResourceGrants
	ghost := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "bob"}.Build(),
	}.Build()
	// Same resource, different missing entitlement, NO annotation:
	// connector-owned dangling reference.
	plainGrant := grant.NewGrant(repo, "viewer", ghost.GetId())

	cur := newRepairTestStore(ctx, t, filepath.Join(tmpDir, "cur.c1z"), tmpDir)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, cur.PutResourceTypes(ctx, equivRepoRT))
	require.NoError(t, cur.PutResources(ctx, repo))
	require.NoError(t, cur.PutGrants(ctx, irgGrant, plainGrant))

	s := &syncer{store: cur, syncType: connectorstore.SyncTypeFull}

	// Fail-fast: the connector-owned dangling grant must fail the sync —
	// the sibling IRG grant on the same resource must not exempt it.
	s.failFastInvariants = true
	err = s.checkGrantEntitlementReferences(ctx)
	require.Error(t, err, "an IRG grant on the resource must not exempt an unrelated dangling reference")
	require.Contains(t, err.Error(), "ingest invariant I8")
	require.Contains(t, err.Error(), "viewer")
	require.NotErrorIs(t, err, ErrReplayIntegrity)

	// Default mode: the connector-owned grant drops, the IRG grant stays.
	s.failFastInvariants = false
	require.NoError(t, s.checkGrantEntitlementReferences(ctx))
	_, err = cur.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{
		GrantId: plainGrant.GetId(),
	}.Build())
	require.Error(t, err, "the connector-owned dangling grant must be dropped")
	_, err = cur.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{
		GrantId: irgGrant.GetId(),
	}.Build())
	require.NoError(t, err, "the machinery-owned IRG grant must never be dropped")

	// Idempotent: after the drop, both modes find nothing.
	require.NoError(t, s.checkGrantEntitlementReferences(ctx))
	s.failFastInvariants = true
	require.NoError(t, s.checkGrantEntitlementReferences(ctx))

	require.NoError(t, cur.Close(ctx))
}

// TestIngestInvariantI8MixedGrantsUnderOneEntitlement pins the per-grant
// rule within a single missing entitlement: IRG carriers and plain grants
// can share the SAME entitlement identity (an IRG page and a normal page
// both emitted grants for it); only the plain grants drop.
func TestIngestInvariantI8MixedGrantsUnderOneEntitlement(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	repo, err := rs.NewResource("Repo r1", equivRepoRT, "r1")
	require.NoError(t, err)
	irgGrant := repairTestGrant(t, repo) // "admin" entitlement, alice, annotated
	ghost := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "bob"}.Build(),
	}.Build()
	plainGrant := grant.NewGrant(repo, "admin", ghost.GetId()) // same entitlement, no annotation

	cur := newRepairTestStore(ctx, t, filepath.Join(tmpDir, "cur.c1z"), tmpDir)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, cur.PutResourceTypes(ctx, equivRepoRT))
	require.NoError(t, cur.PutResources(ctx, repo))
	require.NoError(t, cur.PutGrants(ctx, irgGrant, plainGrant))

	s := &syncer{store: cur, syncType: connectorstore.SyncTypeFull}

	s.failFastInvariants = true
	err = s.checkGrantEntitlementReferences(ctx)
	require.Error(t, err, "a mixed entitlement (IRG + plain grants) must fail fast on the plain grant")

	s.failFastInvariants = false
	require.NoError(t, s.checkGrantEntitlementReferences(ctx))
	_, err = cur.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{
		GrantId: plainGrant.GetId(),
	}.Build())
	require.Error(t, err, "the plain grant under the mixed entitlement must be dropped")
	_, err = cur.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{
		GrantId: irgGrant.GetId(),
	}.Build())
	require.NoError(t, err, "the IRG carrier under the mixed entitlement must be kept")

	require.NoError(t, cur.Close(ctx))
}

// TestIngestInvariantI6SkippedForExpandOnlySyncs pins the expand-grants
// gate: an orphan scope (stamped rows, no manifest entry) fails a normal
// sealed sync's I6, but an expansion-only run — compaction's pass over a
// fold output whose manifest was deliberately cleared — must not evaluate
// it at all.
func TestIngestInvariantI6SkippedForExpandOnlySyncs(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	repo, err := rs.NewResource("Repo r1", equivRepoRT, "r1")
	require.NoError(t, err)
	ghost := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
	}.Build()
	g := grant.NewGrant(repo, "admin", ghost.GetId())

	// Stamped rows with NO manifest entry: the orphan shape.
	cur := newRepairTestStore(ctx, t, filepath.Join(tmpDir, "cur.c1z"), tmpDir)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, cur.PutResourceTypes(ctx, equivRepoRT))
	require.NoError(t, cur.PutResources(ctx, repo))
	scope := sourcecache.HashScope("grants:r1")
	require.NoError(t, cur.PutGrants(sourcecache.WithScope(ctx, scope), g))

	s := &syncer{store: cur, syncType: connectorstore.SyncTypeFull, failFastInvariants: true}

	// Sanity: a normal sync detects the orphan (replay-integrity class).
	err = s.checkSourceCacheScopeConsistency(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrReplayIntegrity)
	require.Contains(t, err.Error(), "ingest invariant I6")

	// The expansion-only pass must skip the check entirely.
	s.onlyExpandGrants = true
	require.NoError(t, s.checkSourceCacheScopeConsistency(ctx),
		"expand-grants-only syncs must not evaluate I6: fold outputs legitimately hold stamps without a manifest")

	require.NoError(t, cur.Close(ctx))
}
