package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// I7/I8/I9 referential-invariant tests: entitlements referencing
// vanished resources (I7), grants referencing vanished entitlements
// (I8), and grants referencing vanished principals (I9) — connector
// magic-id bugs and the holes type-granularity scopes open. All are
// connector-attributable: default mode DROPS the rows (platform uplift
// discards them anyway) with one aggregated warning, fail-fast mode fails,
// and none carry ErrReplayIntegrity (a cold retry is clean on a
// dishonest validator, so the sentinel would loop forever).

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/logging"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

func TestIngestInvariantI7DropsDanglingEntitlements(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	repo, err := rs.NewResource("Repo r1", equivRepoRT, "r1")
	require.NoError(t, err)
	ent := et.NewAssignmentEntitlement(repo, "admin")

	// The violating shape: an entitlement row whose resource has no row.
	cur := newRepairTestStore(ctx, t, filepath.Join(tmpDir, "cur.c1z"), tmpDir)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, cur.PutResourceTypes(ctx, equivRepoRT))
	require.NoError(t, cur.PutEntitlements(ctx, ent))

	s := &syncer{store: cur, syncType: connectorstore.SyncTypeFull}

	// Fail-fast: named failure, no sentinel, no drop.
	s.failFastInvariants = true
	err = s.checkEntitlementResourceReferences(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ingest invariant I7")
	require.NotErrorIs(t, err, ErrReplayIntegrity)
	_, err = cur.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
		EntitlementId: ent.GetId(),
	}.Build())
	require.NoError(t, err, "fail-fast mode must not drop rows")

	// Default mode: the row is DROPPED, and the pass is idempotent.
	s.failFastInvariants = false
	require.NoError(t, s.checkEntitlementResourceReferences(ctx))
	_, err = cur.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
		EntitlementId: ent.GetId(),
	}.Build())
	require.Error(t, err, "default mode must drop the dangling entitlement row")
	require.NoError(t, s.checkEntitlementResourceReferences(ctx))

	// After the drop, fail-fast finds nothing.
	s.failFastInvariants = true
	require.NoError(t, s.checkEntitlementResourceReferences(ctx))

	require.NoError(t, cur.Close(ctx))
}

func TestIngestInvariantI8DropsDanglingGrants(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	repo, err := rs.NewResource("Repo r1", equivRepoRT, "r1")
	require.NoError(t, err)
	ent := et.NewAssignmentEntitlement(repo, "admin")
	principal := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
	}.Build()
	g := grant.NewGrant(repo, "admin", principal.GetId())

	// The violating shape: a grant row whose entitlement has no row.
	cur := newRepairTestStore(ctx, t, filepath.Join(tmpDir, "cur.c1z"), tmpDir)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, cur.PutResourceTypes(ctx, equivRepoRT))
	require.NoError(t, cur.PutResources(ctx, repo))
	require.NoError(t, cur.PutGrants(ctx, g))

	s := &syncer{store: cur, syncType: connectorstore.SyncTypeFull}

	// Fail-fast: named failure carrying the exact reconstructed id; no drop.
	s.failFastInvariants = true
	err = s.checkGrantEntitlementReferences(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ingest invariant I8")
	require.Contains(t, err.Error(), ent.GetId())
	require.NotErrorIs(t, err, ErrReplayIntegrity)

	// Default mode: the grant is DROPPED; the pass is idempotent; fail-fast then
	// finds nothing.
	s.failFastInvariants = false
	require.NoError(t, s.checkGrantEntitlementReferences(ctx))
	_, err = cur.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{
		GrantId: g.GetId(),
	}.Build())
	require.Error(t, err, "default mode must drop the dangling grant row")
	require.NoError(t, s.checkGrantEntitlementReferences(ctx))
	s.failFastInvariants = true
	require.NoError(t, s.checkGrantEntitlementReferences(ctx))

	require.NoError(t, cur.Close(ctx))
}

func TestIngestInvariantI8ExemptsInsertResourceGrants(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	repo, err := rs.NewResource("Repo r1", equivRepoRT, "r1")
	require.NoError(t, err)
	g := repairTestGrant(t, repo) // carries InsertResourceGrants

	cur := newRepairTestStore(ctx, t, filepath.Join(tmpDir, "cur.c1z"), tmpDir)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, cur.PutResourceTypes(ctx, equivRepoRT))
	require.NoError(t, cur.PutResources(ctx, repo))
	require.NoError(t, cur.PutGrants(ctx, g))

	s := &syncer{store: cur, syncType: connectorstore.SyncTypeFull}

	// Exempt in both modes: no failure, no drop.
	s.failFastInvariants = true
	require.NoError(t, s.checkGrantEntitlementReferences(ctx))
	s.failFastInvariants = false
	require.NoError(t, s.checkGrantEntitlementReferences(ctx))
	_, err = cur.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{
		GrantId: g.GetId(),
	}.Build())
	require.NoError(t, err, "InsertResourceGrants shape must never be dropped")

	require.NoError(t, cur.Close(ctx))
}

func TestIngestInvariantI9DropsDanglingPrincipalGrants(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	repo, err := rs.NewResource("Repo r1", equivRepoRT, "r1")
	require.NoError(t, err)
	ent := et.NewAssignmentEntitlement(repo, "admin")
	ghost := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "ghost"}.Build(),
	}.Build()
	g := grant.NewGrant(repo, "admin", ghost.GetId())

	// The violating shape: resource + entitlement exist, principal not.
	cur := newRepairTestStore(ctx, t, filepath.Join(tmpDir, "cur.c1z"), tmpDir)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, cur.PutResourceTypes(ctx, equivRepoRT))
	require.NoError(t, cur.PutResources(ctx, repo))
	require.NoError(t, cur.PutEntitlements(ctx, ent))
	require.NoError(t, cur.PutGrants(ctx, g))

	s := &syncer{store: cur, syncType: connectorstore.SyncTypeFull}

	// Fail-fast: named failure, no drop.
	s.failFastInvariants = true
	err = s.checkGrantPrincipalReferences(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ingest invariant I9")
	require.Contains(t, err.Error(), "user/ghost")
	require.NotErrorIs(t, err, ErrReplayIntegrity)

	// Default mode: the grant is DROPPED; the pass is idempotent; fail-fast then
	// finds nothing.
	s.failFastInvariants = false
	require.NoError(t, s.checkGrantPrincipalReferences(ctx))
	_, err = cur.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{
		GrantId: g.GetId(),
	}.Build())
	require.Error(t, err, "default mode must drop the dangling-principal grant")
	require.NoError(t, s.checkGrantPrincipalReferences(ctx))
	s.failFastInvariants = true
	require.NoError(t, s.checkGrantPrincipalReferences(ctx))

	require.NoError(t, cur.Close(ctx))
}

func TestIngestInvariantI9ExemptsExternalMatchCarriers(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	repo, err := rs.NewResource("Repo r1", equivRepoRT, "r1")
	require.NoError(t, err)
	ent := et.NewAssignmentEntitlement(repo, "admin")
	viewerEnt := et.NewAssignmentEntitlement(repo, "viewer")
	carrier := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "external-user", Resource: "someone@example.com"}.Build(),
	}.Build()
	// Two carrier grants for the SAME dangling principal, so the
	// carrier-grant count is distinguishable from the principal count.
	newCarrierGrant := func(slug string) *v2.Grant {
		g := grant.NewGrant(repo, slug, carrier.GetId())
		annos := annotations.Annotations(g.GetAnnotations())
		annos.Update(v2.ExternalResourceMatch_builder{Key: "email", Value: "someone@example.com"}.Build())
		g.SetAnnotations(annos)
		return g
	}
	g := newCarrierGrant("admin")
	g2 := newCarrierGrant("viewer")

	// The carrier shape: a match-annotated grant whose stub principal has
	// no row because no external resource file was configured (the match
	// op never ran to transform-and-delete it).
	cur := newRepairTestStore(ctx, t, filepath.Join(tmpDir, "cur.c1z"), tmpDir)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, cur.PutResourceTypes(ctx, equivRepoRT))
	require.NoError(t, cur.PutResources(ctx, repo))
	require.NoError(t, cur.PutEntitlements(ctx, ent, viewerEnt))
	require.NoError(t, cur.PutGrants(ctx, g, g2))

	s := &syncer{store: cur, syncType: connectorstore.SyncTypeFull}

	// Exempt in both modes: evidence of a config gap, not bad data.
	s.failFastInvariants = true
	require.NoError(t, s.checkGrantPrincipalReferences(ctx))
	s.failFastInvariants = false
	require.NoError(t, s.checkGrantPrincipalReferences(ctx))
	for _, kept := range []string{g.GetId(), g2.GetId()} {
		_, err = cur.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{
			GrantId: kept,
		}.Build())
		require.NoError(t, err, "unprocessed external-match carriers must never be dropped")
	}

	// The carrier count is per GRANT, not per principal: one dangling
	// principal with two carrier grants must report carrierGrants == 2
	// (units consistent with the mixed-principal path's skip count).
	facts, ok := cur.(dotc1z.IngestInvariantStore)
	require.True(t, ok)
	require.NoError(t, facts.EnsureGrantIndexes(ctx))
	visits := 0
	require.NoError(t, facts.ForEachDanglingGrantPrincipal(ctx, func(rt, rid string, matchOnly bool, carrierGrants int64) error {
		visits++
		require.True(t, matchOnly)
		require.Equal(t, int64(2), carrierGrants)
		return nil
	}))
	require.Equal(t, 1, visits, "one dangling principal expected")

	require.NoError(t, cur.Close(ctx))
}

// TestDishonestValidatorStrandsGrantEntitlements is the end-to-end I8
// scenario: a connector whose grants validator says "unchanged" (so the
// scope replays) while its entitlement listing was refreshed and dropped
// the entitlement. Fail-fast mode names it without the replay sentinel;
// default mode drops the stranded grants and the sync converges to what
// a cold sync would have produced.
func TestDishonestValidatorStrandsGrantEntitlements(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	mc := newSourceCacheMockConnector()
	group, memberEnt, err := mc.AddGroup(ctx, "g1")
	require.NoError(t, err)
	user, err := mc.AddUser(ctx, "u1")
	require.NoError(t, err)
	stranded := mc.AddGroupMember(ctx, group, user)
	mc.etagByResource[group.GetId().GetResource()] = "v1"

	sync1 := filepath.Join(tmpDir, "s1.c1z")
	runSourceCacheSync(ctx, t, mc, sync1, "", tmpDir)

	// The dishonesty: the entitlement listing drops the member
	// entitlement while the grants validator still reports "unchanged".
	mc.entDB[group.GetId().GetResource()] = nil

	// Fail-fast warm sync: fails, named, not replay-classified.
	store2, err := dotc1z.NewStore(ctx, filepath.Join(tmpDir, "s2.c1z"),
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	warmStrict, err := NewSyncer(ctx, mc,
		WithConnectorStore(store2),
		WithTmpDir(tmpDir),
		WithPreviousSyncC1ZPath(sync1),
		WithFailFastInvariants(),
	)
	require.NoError(t, err)
	err = warmStrict.Sync(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ingest invariant I8")
	require.NotErrorIs(t, err, ErrReplayIntegrity,
		"a dishonest validator must not trigger the cold-retry ladder: the cold run is clean, "+
			"so the retry would mask the connector bug on every sync forever")
	require.NoError(t, warmStrict.Close(ctx))

	// Default-mode warm sync: succeeds, and the stranded grants are DROPPED —
	// the artifact converges to cold-sync semantics.
	sync3 := filepath.Join(tmpDir, "s3.c1z")
	store3, err := dotc1z.NewStore(ctx, sync3,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	warmLenient, err := NewSyncer(ctx, mc,
		WithConnectorStore(store3),
		WithTmpDir(tmpDir),
		WithPreviousSyncC1ZPath(sync1),
	)
	require.NoError(t, err)
	require.NoError(t, warmLenient.Sync(ctx))
	require.NoError(t, warmLenient.Close(ctx))

	grants3 := listGrantsInFile(ctx, t, sync3)
	require.NotContains(t, grants3, stranded.GetId(),
		"default mode must drop grants stranded by the dishonest validator")
	_ = memberEnt
}
