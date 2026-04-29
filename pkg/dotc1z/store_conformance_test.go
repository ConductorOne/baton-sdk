package dotc1z

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestC1ZStoreConformance exercises every method on C1ZStore and its
// sub-interfaces (GrantStore, SyncMeta, FileOps) against a real *C1File.
// Each subtest is self-contained with its own fresh store to keep cases
// independent.
func TestC1ZStoreConformance(t *testing.T) {
	t.Run("Grants/StoreExpandedGrants strips annotation", testStoreExpandedGrantsStripsAnnotation)
	t.Run("Grants/StoreExpandedGrants preserves expansion columns", testStoreExpandedGrantsPreservesExpansionColumns)
	t.Run("Grants/PendingExpansion returns only needs_expansion rows", testPendingExpansionNeedsOnly)
	t.Run("Grants/PendingExpansion early termination", testPendingExpansionEarlyTermination)
	t.Run("Grants/ListWithAnnotations yields nil for non-expandable", testListWithAnnotationsNilForNonExpandable)
	t.Run("Grants/ListWithAnnotations yields annotation for expandable", testListWithAnnotationsAnnotationPopulated)
	t.Run("SyncMeta/MarkSyncSupportsDiff persists across reopen", testMarkSyncSupportsDiffPersists)
	t.Run("SyncMeta/LatestFullSync returns nil when no runs", testLatestFullSyncEmptyReturnsNil)
	t.Run("SyncMeta/LatestFullSync ignores in-progress and non-full", testLatestFullSyncIgnoresInProgressAndPartial)
	t.Run("SyncMeta/LatestFinishedSyncOfAnyType includes diff types", testLatestFinishedSyncOfAnyTypeIncludesDiff)
	t.Run("SyncMeta/Stats reports row counts", testStatsReportsRowCounts)
	t.Run("FileOps/CloneSync produces readable c1z", testCloneSyncProducesReadableC1Z)
	t.Run("FileOps/GenerateSyncDiff creates partial sync", testGenerateSyncDiffCreatesPartial)
}

// -----------------------------------------------------------------------------
// GrantStore tests
// -----------------------------------------------------------------------------

func testStoreExpandedGrantsStripsAnnotation(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	grant := expandableGrant(t, "g-strip", "ent1", "group", "g1", "user", "u1", []string{"ent2"}, false)
	require.NoError(t, c1f.Grants().StoreExpandedGrants(ctx, grant))

	// Read the raw data blob and verify the GrantExpandable annotation was stripped.
	var data []byte
	err := c1f.db.QueryRowContext(ctx,
		"SELECT data FROM "+grants.Name()+" WHERE external_id=?", "g-strip",
	).Scan(&data)
	require.NoError(t, err)

	got := &v2.Grant{}
	require.NoError(t, proto.Unmarshal(data, got))
	require.False(t, hasGrantExpandable(got),
		"StoreExpandedGrants should strip GrantExpandable from the persisted grant")
}

func testStoreExpandedGrantsPreservesExpansionColumns(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	// Step 1: seed via PutGrants so the expansion columns get populated.
	seed := expandableGrant(t, "g-preserve", "ent1", "group", "g1", "user", "u1", []string{"ent2"}, false)
	require.NoError(t, c1f.PutGrants(ctx, seed))

	rawBefore := getRawGrantRow(ctx, t, c1f, "g-preserve")
	require.NotNil(t, rawBefore.expansion, "seed grant should have expansion column populated")
	require.Equal(t, 1, rawBefore.needsExpansion)

	// Step 2: rewrite via StoreExpandedGrants — same grant id, different payload.
	// Expansion columns should not change. The payload (stripped) is what the
	// method writes.
	rewrite := expandableGrant(t, "g-preserve", "ent1", "group", "g1", "user", "u1",
		[]string{"this-should-be-ignored"}, true)
	require.NoError(t, c1f.Grants().StoreExpandedGrants(ctx, rewrite))

	rawAfter := getRawGrantRow(ctx, t, c1f, "g-preserve")
	require.Equal(t, rawBefore.expansion, rawAfter.expansion,
		"expansion column must be unchanged by StoreExpandedGrants")
	require.Equal(t, rawBefore.needsExpansion, rawAfter.needsExpansion,
		"needs_expansion column must be unchanged by StoreExpandedGrants")
}

func testPendingExpansionNeedsOnly(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	// Seed three grants:
	//   needs_expansion=1 — should appear
	//   needs_expansion=0 — shouldn't appear (not tested here; it requires
	//     post-expansion bookkeeping to flip the flag, which is beyond this test)
	//   no expansion annotation — shouldn't appear
	needsOne := expandableGrant(t, "g-needs-1", "ent1", "group", "g1", "user", "u1", []string{"ent2"}, false)
	needsTwo := expandableGrant(t, "g-needs-2", "ent1", "group", "g1", "user", "u1", []string{"ent2"}, true)
	plain := plainGrant(t, "g-plain", "ent1", "group", "g1", "user", "u1")
	require.NoError(t, c1f.PutGrants(ctx, needsOne, needsTwo, plain))

	seen := make(map[string]PendingExpansion)
	for pe, err := range c1f.Grants().PendingExpansion(ctx) {
		require.NoError(t, err)
		seen[pe.GrantExternalID] = pe
	}

	require.Contains(t, seen, "g-needs-1")
	require.Contains(t, seen, "g-needs-2")
	require.NotContains(t, seen, "g-plain",
		"non-expandable grant must not appear in PendingExpansion")

	got := seen["g-needs-1"]
	require.Equal(t, []string{"ent2"}, got.Annotation.GetEntitlementIds())
	require.False(t, got.Annotation.GetShallow())
	require.Equal(t, "ent1", got.TargetEntitlementID)
	require.Equal(t, "user", got.PrincipalResourceTypeID)
	require.Equal(t, "u1", got.PrincipalResourceID)

	shallow := seen["g-needs-2"]
	require.True(t, shallow.Annotation.GetShallow())
}

func testPendingExpansionEarlyTermination(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	// Seed enough rows to cross a page boundary (maxPageSize is 1000 on main).
	// We seed 3 rows and verify we only observe 1 when we break early.
	grants := make([]*v2.Grant, 3)
	for i := range grants {
		grants[i] = expandableGrant(t, "g-term-"+string(rune('a'+i)), "ent1", "group", "g1", "user", "u"+string(rune('1'+i)), []string{"ent2"}, false)
	}
	require.NoError(t, c1f.PutGrants(ctx, grants...))

	count := 0
	for pe, err := range c1f.Grants().PendingExpansion(ctx) {
		require.NoError(t, err)
		_ = pe
		count++
		if count == 1 {
			break
		}
	}
	require.Equal(t, 1, count, "iterator should stop after one yield if caller breaks")
}

func testListWithAnnotationsNilForNonExpandable(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	g := plainGrant(t, "g-la-plain", "ent1", "group", "g1", "user", "u1")
	require.NoError(t, c1f.PutGrants(ctx, g))

	var seen []GrantAnnotation
	for ga, err := range c1f.Grants().ListWithAnnotations(ctx) {
		require.NoError(t, err)
		seen = append(seen, ga)
	}
	require.Len(t, seen, 1)
	require.Nil(t, seen[0].Annotation, "plain grant must yield Annotation=nil")
	require.Equal(t, "g-la-plain", seen[0].Grant.GetId())
}

func testListWithAnnotationsAnnotationPopulated(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	g := expandableGrant(t, "g-la-exp", "ent1", "group", "g1", "user", "u1", []string{"ent2"}, true)
	require.NoError(t, c1f.PutGrants(ctx, g))

	var seen []GrantAnnotation
	for ga, err := range c1f.Grants().ListWithAnnotations(ctx) {
		require.NoError(t, err)
		seen = append(seen, ga)
	}
	require.Len(t, seen, 1)
	require.NotNil(t, seen[0].Annotation)
	require.Equal(t, []string{"ent2"}, seen[0].Annotation.GetEntitlementIds())
	require.True(t, seen[0].Annotation.GetShallow())
	require.Equal(t, "g-la-exp", seen[0].GrantExternalID)
	require.Equal(t, "ent1", seen[0].TargetEntitlementID)
}

// -----------------------------------------------------------------------------
// SyncMeta tests
// -----------------------------------------------------------------------------

func testMarkSyncSupportsDiffPersists(t *testing.T) {
	ctx := context.Background()

	// Use our own temp file so we control close/reopen separately from the
	// shared setupTestC1Z cleanup contract.
	tmp, err := os.CreateTemp("", "test-marksupportsdiff-*.c1z")
	require.NoError(t, err)
	require.NoError(t, tmp.Close())
	path := tmp.Name()
	defer func() { _ = os.Remove(path) }()

	c1f, err := NewC1ZFile(ctx, path)
	require.NoError(t, err)

	syncID, err := c1f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, c1f.EndSync(ctx))
	require.NoError(t, c1f.SyncMeta().MarkSyncSupportsDiff(ctx, syncID))
	require.NoError(t, c1f.Close(ctx))

	// Reopen and verify persistence.
	reopened, err := NewC1ZFile(ctx, path)
	require.NoError(t, err)
	defer func() { _ = reopened.Close(ctx) }()

	run, err := reopened.getSync(ctx, syncID)
	require.NoError(t, err)
	require.NotNil(t, run)
	require.True(t, run.SupportsDiff)
}

func testLatestFullSyncEmptyReturnsNil(t *testing.T) {
	ctx := context.Background()
	tmp, err := os.CreateTemp("", "test-latest-empty-*.c1z")
	require.NoError(t, err)
	require.NoError(t, tmp.Close())
	defer func() { _ = os.Remove(tmp.Name()) }()

	c1f, err := NewC1ZFile(ctx, tmp.Name())
	require.NoError(t, err)
	defer func() { _ = c1f.Close(ctx) }()

	got, err := c1f.SyncMeta().LatestFullSync(ctx)
	require.NoError(t, err)
	require.Nil(t, got, "empty store should return nil, not zero-valued SyncRun")
}

func testLatestFullSyncIgnoresInProgressAndPartial(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	fullID := c1f.currentSyncID
	require.NotEmpty(t, fullID)
	require.NoError(t, c1f.EndSync(ctx))

	// A later partial sync — must NOT be returned by LatestFullSync.
	partialID, err := c1f.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
	require.NoError(t, err)
	require.NoError(t, c1f.EndSync(ctx))

	// Another still-in-progress full sync — must NOT be returned (ended_at is null).
	_, err = c1f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	got, err := c1f.SyncMeta().LatestFullSync(ctx)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, fullID, got.ID, "LatestFullSync must return the finished full sync, not the partial or in-progress")
	require.NotEqual(t, partialID, got.ID)
}

func testLatestFinishedSyncOfAnyTypeIncludesDiff(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	fullID := c1f.currentSyncID
	require.NoError(t, c1f.EndSync(ctx))

	// Start a later partial sync so there are multiple sync types in the store.
	partialID, err := c1f.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
	require.NoError(t, err)
	require.NoError(t, c1f.EndSync(ctx))

	got, err := c1f.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, partialID, got.ID,
		"LatestFinishedSyncOfAnyType must return the most recent finished sync of any type")
	require.NotEqual(t, fullID, got.ID)
	require.Equal(t, connectorstore.SyncTypePartial, got.Type)
}

func testStatsReportsRowCounts(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	syncID := c1f.currentSyncID

	// Add one grant so there's something to count.
	require.NoError(t, c1f.PutGrants(ctx,
		plainGrant(t, "g-stats", "ent1", "group", "g1", "user", "u1")))
	require.NoError(t, c1f.EndSync(ctx))

	stats, err := c1f.SyncMeta().Stats(ctx, connectorstore.SyncTypeFull, syncID)
	require.NoError(t, err)
	require.NotEmpty(t, stats)
	// Whatever the key is, the grants table should have a non-zero count.
	hasNonZero := false
	for _, v := range stats {
		if v > 0 {
			hasNonZero = true
			break
		}
	}
	require.True(t, hasNonZero, "Stats should report at least one non-zero row count")
}

// -----------------------------------------------------------------------------
// FileOps tests
// -----------------------------------------------------------------------------

func testCloneSyncProducesReadableC1Z(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	syncID := c1f.currentSyncID
	require.NoError(t, c1f.PutGrants(ctx,
		plainGrant(t, "g-clone", "ent1", "group", "g1", "user", "u1")))
	require.NoError(t, c1f.EndSync(ctx))

	clonePath := t.TempDir() + "/cloned.c1z"
	require.NoError(t, c1f.FileOps().CloneSync(ctx, clonePath, syncID))

	cloned, err := NewC1ZFile(ctx, clonePath)
	require.NoError(t, err)
	defer func() { _ = cloned.Close(ctx) }()

	run, err := cloned.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
	require.NoError(t, err)
	require.NotNil(t, run)
}

func testGenerateSyncDiffCreatesPartial(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	baseID := c1f.currentSyncID
	require.NoError(t, c1f.PutGrants(ctx,
		plainGrant(t, "g-base", "ent1", "group", "g1", "user", "u1")))
	require.NoError(t, c1f.EndSync(ctx))

	// Start a second full sync that adds a grant.
	appliedID, err := c1f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, c1f.PutGrants(ctx,
		plainGrant(t, "g-base", "ent1", "group", "g1", "user", "u1"),
		plainGrant(t, "g-new", "ent1", "group", "g1", "user", "u2")))
	require.NoError(t, c1f.EndSync(ctx))

	diffID, err := c1f.FileOps().GenerateSyncDiff(ctx, baseID, appliedID)
	require.NoError(t, err)
	require.NotEmpty(t, diffID)
	require.NotEqual(t, baseID, diffID)
	require.NotEqual(t, appliedID, diffID)
}

// -----------------------------------------------------------------------------
// test helpers
// -----------------------------------------------------------------------------

// expandableGrant constructs a grant with a GrantExpandable annotation.
func expandableGrant(
	t *testing.T,
	grantID, entID, rtID, resID, prtID, pRID string,
	srcEnts []string,
	shallow bool,
) *v2.Grant {
	t.Helper()
	g := plainGrant(t, grantID, entID, rtID, resID, prtID, pRID)
	annos := annotations.Annotations(g.GetAnnotations())
	annos.Update(v2.GrantExpandable_builder{
		EntitlementIds: srcEnts,
		Shallow:        shallow,
	}.Build())
	g.SetAnnotations(annos)
	return g
}

// plainGrant constructs a grant with no expansion annotation.
func plainGrant(t *testing.T, grantID, entID, rtID, resID, prtID, pRID string) *v2.Grant {
	t.Helper()
	return v2.Grant_builder{
		Id: grantID,
		Entitlement: v2.Entitlement_builder{
			Id: entID,
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{ResourceType: rtID, Resource: resID}.Build(),
			}.Build(),
		}.Build(),
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: prtID, Resource: pRID}.Build(),
		}.Build(),
	}.Build()
}
