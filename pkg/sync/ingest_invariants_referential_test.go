package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// I3/I7/I8/I9 referential-invariant tests over a pebble-backed store
// (the engine that carries the inspection surface). Current policy:
// default mode AGGREGATES dangling references into one warning per
// invariant and never mutates the store; fail-fast mode hard-fails
// with a named, plainly-attributed error. The exemptions
// (InsertResourceGrants per grant for I8, ExternalResourceMatch*
// carriers for I9) hold in every mode. The drop arms for
// never-synced-type danglings arrive with the dangling-reference drop
// policy and will extend these fixtures.

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

var invariantRepoRT = v2.ResourceType_builder{
	Id:          "repo",
	DisplayName: "Repo",
}.Build()

// newPebbleInvariantStore opens a pebble-backed store (the engine with
// the IngestInvariantStore surface) with an open full sync, plus a
// pass fixture pointed at it.
func newPebbleInvariantStore(ctx context.Context, t *testing.T) (c1zstore.Store, *ingestInvariantsPass) {
	t.Helper()
	tmpDir := t.TempDir()
	store, err := dotc1z.NewStore(ctx, filepath.Join(tmpDir, "invariants.c1z"),
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	facts, ok := store.(dotc1z.IngestInvariantStore)
	require.True(t, ok, "the pebble store must carry the invariant inspection surface")
	pass := &ingestInvariantsPass{
		store: store,
		facts: facts,
		p: &IngestInvariantsPolicy{
			ActiveSyncID: syncID,
			SyncType:     connectorstore.SyncTypeFull,
		},
	}
	return store, pass
}

func TestIngestInvariantI7DanglingEntitlements(t *testing.T) {
	ctx := context.Background()
	store, pass := newPebbleInvariantStore(ctx, t)

	repo, err := rs.NewResource("Repo r1", invariantRepoRT, "r1")
	require.NoError(t, err)
	ent := et.NewAssignmentEntitlement(repo, "admin")

	// The violating shape: an entitlement row whose resource has no row.
	require.NoError(t, store.PutEntitlements(ctx, ent))

	// Fail-fast: named failure.
	pass.p.FailFast = true
	err = pass.checkEntitlementResourceReferences(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ingest invariant I7")

	// Default mode: aggregated warning only — no error, no mutation,
	// idempotent.
	pass.p.FailFast = false
	require.NoError(t, pass.checkEntitlementResourceReferences(ctx))
	_, err = store.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
		EntitlementId: ent.GetId(),
	}.Build())
	require.NoError(t, err, "default mode must not drop the dangling entitlement row")
	require.NoError(t, pass.checkEntitlementResourceReferences(ctx))

	// Healing the reference (putting the resource row) clears the
	// verdict in every mode.
	require.NoError(t, store.PutResources(ctx, repo))
	pass.p.FailFast = true
	require.NoError(t, pass.checkEntitlementResourceReferences(ctx))
}

func TestIngestInvariantI8DanglingGrants(t *testing.T) {
	ctx := context.Background()
	store, pass := newPebbleInvariantStore(ctx, t)

	repo, err := rs.NewResource("Repo r1", invariantRepoRT, "r1")
	require.NoError(t, err)
	ent := et.NewAssignmentEntitlement(repo, "admin")
	principal := v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build()
	g := grant.NewGrant(repo, "admin", principal)

	// The violating shape: a grant row whose entitlement has no row.
	require.NoError(t, store.PutResources(ctx, repo))
	require.NoError(t, store.PutGrants(ctx, g))

	// Fail-fast: named failure carrying the exact reconstructed id.
	pass.p.FailFast = true
	err = pass.checkGrantEntitlementReferences(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ingest invariant I8")
	require.Contains(t, err.Error(), ent.GetId())

	// Default mode: aggregated warning only — grant retained, idempotent.
	pass.p.FailFast = false
	require.NoError(t, pass.checkGrantEntitlementReferences(ctx))
	_, err = store.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{
		GrantId: g.GetId(),
	}.Build())
	require.NoError(t, err, "default mode must not drop the dangling grant row")
	require.NoError(t, pass.checkGrantEntitlementReferences(ctx))

	// Healing the reference (putting the entitlement row) clears the
	// verdict in every mode.
	require.NoError(t, store.PutEntitlements(ctx, ent))
	pass.p.FailFast = true
	require.NoError(t, pass.checkGrantEntitlementReferences(ctx))
}

func TestIngestInvariantI8ExemptsInsertResourceGrants(t *testing.T) {
	ctx := context.Background()
	store, pass := newPebbleInvariantStore(ctx, t)

	repo, err := rs.NewResource("Repo r1", invariantRepoRT, "r1")
	require.NoError(t, err)
	principal := v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build()
	g := grant.NewGrant(repo, "admin", principal, grant.WithAnnotation(&v2.InsertResourceGrants{}))

	require.NoError(t, store.PutResourceTypes(ctx, invariantRepoRT))
	require.NoError(t, store.PutResources(ctx, repo))
	require.NoError(t, store.PutGrants(ctx, g))

	// Exempt in both modes: the machinery-owned shape (grants exist,
	// entitlement rows never do).
	pass.p.FailFast = true
	require.NoError(t, pass.checkGrantEntitlementReferences(ctx))
	pass.p.FailFast = false
	require.NoError(t, pass.checkGrantEntitlementReferences(ctx))
	_, err = store.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{
		GrantId: g.GetId(),
	}.Build())
	require.NoError(t, err)

	// The exemption is per GRANT: one plain grant under the same
	// missing entitlement makes it a violation again.
	plain := grant.NewGrant(repo, "admin", v2.ResourceId_builder{ResourceType: "user", Resource: "bob"}.Build())
	require.NoError(t, store.PutGrants(ctx, plain))
	pass.p.FailFast = true
	err = pass.checkGrantEntitlementReferences(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ingest invariant I8")
}

func TestIngestInvariantI9DanglingPrincipals(t *testing.T) {
	ctx := context.Background()
	store, pass := newPebbleInvariantStore(ctx, t)

	repo, err := rs.NewResource("Repo r1", invariantRepoRT, "r1")
	require.NoError(t, err)
	ent := et.NewAssignmentEntitlement(repo, "admin")
	ghost := v2.ResourceId_builder{ResourceType: "user", Resource: "ghost"}.Build()
	g := grant.NewGrant(repo, "admin", ghost)

	// The violating shape: resource + entitlement exist, principal not.
	require.NoError(t, store.PutResourceTypes(ctx, invariantRepoRT))
	require.NoError(t, store.PutResources(ctx, repo))
	require.NoError(t, store.PutEntitlements(ctx, ent))
	require.NoError(t, store.PutGrants(ctx, g))

	// Fail-fast: named failure.
	pass.p.FailFast = true
	err = pass.checkGrantPrincipalReferences(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ingest invariant I9")
	require.Contains(t, err.Error(), "user/ghost")

	// Default mode: aggregated warning only — grant retained, idempotent.
	pass.p.FailFast = false
	require.NoError(t, pass.checkGrantPrincipalReferences(ctx))
	_, err = store.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{
		GrantId: g.GetId(),
	}.Build())
	require.NoError(t, err, "default mode must not drop the dangling-principal grant")
	require.NoError(t, pass.checkGrantPrincipalReferences(ctx))
}

// TestIngestInvariantI9SeesDeferredRegimeGrants pins the
// EnsureGrantIndexes leg of I9: expansion-written grants
// (StoreExpandedGrants, the DEFERRED index regime) have no by_principal
// entries until the deferred build runs, so a dangling principal
// written through that path is invisible to the scan unless the pass
// forces the build first. Without the build-and-clear branch, this
// violation would silently pass.
func TestIngestInvariantI9SeesDeferredRegimeGrants(t *testing.T) {
	ctx := context.Background()
	store, pass := newPebbleInvariantStore(ctx, t)

	repo, err := rs.NewResource("Repo r1", invariantRepoRT, "r1")
	require.NoError(t, err)
	ent := et.NewAssignmentEntitlement(repo, "admin")
	ghost := v2.ResourceId_builder{ResourceType: "user", Resource: "ghost"}.Build()

	require.NoError(t, store.PutResourceTypes(ctx, invariantRepoRT))
	require.NoError(t, store.PutResources(ctx, repo))
	require.NoError(t, store.PutEntitlements(ctx, ent))
	// The deferred regime: by_principal is NOT maintained inline here.
	require.NoError(t, store.Grants().StoreExpandedGrants(ctx, grant.NewGrant(repo, "admin", ghost)))

	pass.p.FailFast = true
	err = pass.checkGrantPrincipalReferences(ctx)
	require.Error(t, err, "the pass must force the deferred index build or the scan misses expansion-written grants")
	require.Contains(t, err.Error(), "ingest invariant I9")
	require.Contains(t, err.Error(), "user/ghost")

	// The build ran and the marker cleared: a re-run takes the
	// no-pending fast path and still sees the same violation.
	require.NoError(t, pass.facts.EnsureGrantIndexes(ctx))
	err = pass.checkGrantPrincipalReferences(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "user/ghost")
}

func TestIngestInvariantI9ExemptsExternalMatchCarriers(t *testing.T) {
	ctx := context.Background()
	store, pass := newPebbleInvariantStore(ctx, t)

	repo, err := rs.NewResource("Repo r1", invariantRepoRT, "r1")
	require.NoError(t, err)
	ent := et.NewAssignmentEntitlement(repo, "admin")
	viewerEnt := et.NewAssignmentEntitlement(repo, "viewer")
	carrier := v2.ResourceId_builder{ResourceType: "external-user", Resource: "someone@example.com"}.Build()
	// Two carrier grants for the SAME dangling principal, so the
	// carrier-grant count is distinguishable from the principal count.
	newCarrierGrant := func(slug string) *v2.Grant {
		g := grant.NewGrant(repo, slug, carrier)
		annos := annotations.Annotations(g.GetAnnotations())
		annos.Update(v2.ExternalResourceMatch_builder{Key: "email", Value: "someone@example.com"}.Build())
		g.SetAnnotations(annos)
		return g
	}
	g := newCarrierGrant("admin")
	g2 := newCarrierGrant("viewer")

	// The carrier shape: a match-annotated grant whose stub principal
	// has no row because no external resource file was configured (the
	// match op never ran to transform-and-delete it).
	require.NoError(t, store.PutResourceTypes(ctx, invariantRepoRT))
	require.NoError(t, store.PutResources(ctx, repo))
	require.NoError(t, store.PutEntitlements(ctx, ent, viewerEnt))
	require.NoError(t, store.PutGrants(ctx, g, g2))

	// Exempt in both modes: evidence of a config gap, not bad data.
	pass.p.FailFast = true
	require.NoError(t, pass.checkGrantPrincipalReferences(ctx))
	pass.p.FailFast = false
	require.NoError(t, pass.checkGrantPrincipalReferences(ctx))
	for _, kept := range []string{g.GetId(), g2.GetId()} {
		_, err = store.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{
			GrantId: kept,
		}.Build())
		require.NoError(t, err, "unprocessed external-match carriers must never be dropped")
	}

	// The carrier count is per GRANT, not per principal: one dangling
	// principal with two carrier grants must report carrierGrants == 2.
	require.NoError(t, pass.facts.EnsureGrantIndexes(ctx))
	visits := 0
	require.NoError(t, pass.facts.ForEachDanglingGrantPrincipal(ctx, func(rt, rid string, matchOnly bool, carrierGrants int64) error {
		visits++
		require.True(t, matchOnly)
		require.Equal(t, int64(2), carrierGrants)
		return nil
	}))
	require.Equal(t, 1, visits, "one dangling principal expected")
}

func TestIngestInvariantI3GrantResourceReferences(t *testing.T) {
	ctx := context.Background()
	store, pass := newPebbleInvariantStore(ctx, t)

	repo, err := rs.NewResource("Repo r1", invariantRepoRT, "r1")
	require.NoError(t, err)
	principal := v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build()

	// Unannotated arm: a grant whose entitlement RESOURCE has no row and
	// no InsertResourceGrants annotation — tolerated with a warning,
	// promoted under fail-fast.
	plain := grant.NewGrant(repo, "admin", principal)
	require.NoError(t, store.PutGrants(ctx, plain))

	pass.p.FailFast = false
	require.NoError(t, pass.checkGrantResourceReferences(ctx))
	pass.p.FailFast = true
	err = pass.checkGrantResourceReferences(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ingest invariant I3")
	require.Contains(t, err.Error(), "promoted under fail-fast")

	// Annotated arm: the same dangling resource with an
	// InsertResourceGrants grant is the machinery's own establishment
	// guarantee — a hard violation in BOTH modes.
	annotated := grant.NewGrant(repo, "writer", principal, grant.WithAnnotation(&v2.InsertResourceGrants{}))
	require.NoError(t, store.PutGrants(ctx, annotated))
	pass.p.FailFast = false
	err = pass.checkGrantResourceReferences(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "InsertResourceGrants")

	// The compaction MERGE is exempt from the annotated hard arm in
	// default mode: keep-newer merges can strand an annotated grant
	// whose resource row lost the merge. (Expansion-only alone does NOT
	// exempt — rollback-expansion replays a single non-merged artifact
	// and keeps full strictness.)
	pass.p.CompactionMerge = true
	require.NoError(t, pass.checkGrantResourceReferences(ctx))
	pass.p.CompactionMerge = false

	// Healing the reference (putting the resource row) clears every arm.
	require.NoError(t, store.PutResources(ctx, repo))
	pass.p.FailFast = true
	require.NoError(t, pass.checkGrantResourceReferences(ctx))
}
