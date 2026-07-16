package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// I7/I8/I9 referential-invariant tests, under the replay policy's
// attribution split:
//
//   - DISABLED-TYPE gaps (the referent's resource type was never
//     synced): expected configuration shapes. Default mode DROPS the
//     dependent rows with one aggregated warning and seals a
//     referentially consistent artifact; fail-fast mode fails. The drop
//     fixtures below deliberately omit the referent's type row.
//   - ENABLED-TYPE danglings (the type IS synced, the row is not):
//     never sealed away. Warm syncs fail with ErrReplayIntegrity
//     (discard + cold retry); cold syncs fail plainly (connector
//     data/behavior). See the *_EnabledTypeFails tests.

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

	// The violating shape: an entitlement row whose resource has no row —
	// and no TYPE row either (disabled-type gap: the drop arm).
	cur := newRepairTestStore(ctx, t, filepath.Join(tmpDir, "cur.c1z"), tmpDir)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
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

	// The violating shape: a grant row whose entitlement has no row —
	// and no TYPE row for the entitlement's resource type (disabled-type
	// gap: the drop arm).
	cur := newRepairTestStore(ctx, t, filepath.Join(tmpDir, "cur.c1z"), tmpDir)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
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
// the entitlement. The stranded grants dangle under an ENABLED type, so
// the replay policy applies: the warm output is DISCARDED
// (ErrReplayIntegrity — the runners' cold-retry ladder) and the cold
// re-run, whose fresh fetch has no stranded grants, seals clean. The
// artifact is never silently sanitized.
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
	// The membership grants also vanish upstream (the fresh fetch has
	// none) — only the REPLAY carries the stranded grants forward.
	mc.entDB[group.GetId().GetResource()] = nil
	mc.grantDB[group.GetId().GetResource()] = nil

	// Warm sync (default mode): the stranded grants dangle under the
	// SYNCED group type — replay is implicated, the output must be
	// discarded and retried cold, never sealed sanitized.
	store2, err := dotc1z.NewStore(ctx, filepath.Join(tmpDir, "s2.c1z"),
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	warm, err := NewSyncer(ctx, mc,
		WithConnectorStore(store2),
		WithTmpDir(tmpDir),
		WithPreviousSyncC1ZPath(sync1),
	)
	require.NoError(t, err)
	err = warm.Sync(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ingest invariant I8")
	require.ErrorIs(t, err, ErrReplayIntegrity,
		"stranded grants under an enabled type on a warm sync must route to the cold-retry ladder")
	require.NoError(t, warm.Close(ctx))

	// The cold retry (what the runners do with ErrReplayIntegrity):
	// clean — the fresh fetch never had the stranded grants.
	sync3 := filepath.Join(tmpDir, "s3.c1z")
	runSourceCacheSync(ctx, t, mc, sync3, "", tmpDir)
	grants3 := listGrantsInFile(ctx, t, sync3)
	require.NotContains(t, grants3, stranded.GetId(),
		"the cold retry's artifact must not contain the stranded grant")
	_ = memberEnt
}

// TestIngestInvariantEnabledTypeDanglingsFail pins the replay policy's
// strict arm for all three referential invariants: a missing referent
// whose resource TYPE is synced is never sealed away. Cold syncs fail
// plainly (connector data/behavior — the retry could not help); warm
// syncs fail with ErrReplayIntegrity (discard + cold retry); the
// compaction expand pass is exempt (keep-newer merges legitimately
// manufacture dangling refs, and its drop pass converges the artifact).
func TestIngestInvariantEnabledTypeDanglingsFail(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	repo, err := rs.NewResource("Repo r1", equivRepoRT, "r1")
	require.NoError(t, err)
	ent := et.NewAssignmentEntitlement(repo, "admin")
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	ghost := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "ghost"}.Build(),
	}.Build()

	// All three shapes, every referent under a SYNCED type: an
	// entitlement whose resource row is missing (I7), a grant whose
	// entitlement row is missing (I8), and a grant whose principal row
	// is missing (I9).
	cur := newRepairTestStore(ctx, t, filepath.Join(tmpDir, "cur.c1z"), tmpDir)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, cur.PutResourceTypes(ctx, equivRepoRT, userRT))

	// I7 shape: entitlement row, no resource row, type row present.
	require.NoError(t, cur.PutEntitlements(ctx, ent))

	s := &syncer{store: cur, syncType: connectorstore.SyncTypeFull}

	// COLD: plain failure, no sentinel, nothing dropped.
	err = s.checkEntitlementResourceReferences(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ingest invariant I7")
	require.Contains(t, err.Error(), "SYNCED type")
	require.NotErrorIs(t, err, ErrReplayIntegrity)
	_, err = cur.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
		EntitlementId: ent.GetId(),
	}.Build())
	require.NoError(t, err, "an enabled-type dangling must never be dropped")

	// WARM: same evidence carries the replay-integrity sentinel.
	s.sourceCache.prev = struct{ dotc1z.SourceCacheStore }{}
	err = s.checkEntitlementResourceReferences(ctx)
	require.ErrorIs(t, err, ErrReplayIntegrity)
	s.sourceCache.prev = nil

	// COMPACTION EXPAND PASS: exempt — the drop arm converges the
	// merged artifact (keep-newer merges manufacture these by design).
	s.onlyExpandGrants = true
	require.NoError(t, s.checkEntitlementResourceReferences(ctx),
		"the compaction expand pass must drop, not fail: merges manufacture dangling refs no input contained")
	s.onlyExpandGrants = false

	// I8 + I9 shapes: resource row present now; a grant under a missing
	// entitlement (enabled repo type) and a grant to a missing principal
	// (enabled user type).
	require.NoError(t, cur.PutResources(ctx, repo))
	gEnt := grant.NewGrant(repo, "missing-ent", ghost.GetId())
	require.NoError(t, cur.PutGrants(ctx, gEnt))

	err = s.checkGrantEntitlementReferences(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ingest invariant I8")
	require.Contains(t, err.Error(), "SYNCED type")
	require.NotErrorIs(t, err, ErrReplayIntegrity)
	s.sourceCache.prev = struct{ dotc1z.SourceCacheStore }{}
	require.ErrorIs(t, s.checkGrantEntitlementReferences(ctx), ErrReplayIntegrity)
	s.sourceCache.prev = nil

	// Heal I8's shape (put its entitlement) so I9 is isolated: the
	// grant's principal "user/ghost" has a type row but no resource row.
	missingEnt := et.NewAssignmentEntitlement(repo, "missing-ent")
	require.NoError(t, cur.PutEntitlements(ctx, missingEnt))
	err = s.checkGrantPrincipalReferences(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ingest invariant I9")
	require.Contains(t, err.Error(), "SYNCED type")
	require.NotErrorIs(t, err, ErrReplayIntegrity)
	s.sourceCache.prev = struct{ dotc1z.SourceCacheStore }{}
	require.ErrorIs(t, s.checkGrantPrincipalReferences(ctx), ErrReplayIntegrity)
	s.sourceCache.prev = nil
	_, err = cur.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{
		GrantId: gEnt.GetId(),
	}.Build())
	require.NoError(t, err, "enabled-type danglings must never be dropped in any failing mode")

	require.NoError(t, cur.Close(ctx))
}
