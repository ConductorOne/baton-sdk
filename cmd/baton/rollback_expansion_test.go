package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/sdk"
	"github.com/conductorone/baton-sdk/pkg/sync"
)

// buildPreExpansionC1Z writes a c1z modeling nested groups before
// expansion: g2 is a member of g1 (an expandable grant), alice is a
// member of g2, plus an external-resource grant carrying GrantImmutable
// and an external-principal grant carrying GrantSources — neither created
// by the expander. The sync is started but NOT finished and NOT expanded.
func buildPreExpansionC1Z(t *testing.T, ctx context.Context, path string) string {
	t.Helper()
	f, err := dotc1z.NewC1ZFile(ctx, path)
	require.NoError(t, err)
	syncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	require.NoError(t, f.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "group", DisplayName: "Group", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP}}.Build(),
		v2.ResourceType_builder{Id: "user", DisplayName: "User", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build(),
	))
	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	alice := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(), DisplayName: "Alice"}.Build()
	bob := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "bob"}.Build(), DisplayName: "Bob"}.Build()
	require.NoError(t, f.PutResources(ctx, g1, g2, alice, bob))

	g1Member := v2.Entitlement_builder{Id: "group:g1:member", DisplayName: "g1 member", Resource: g1, Slug: "member"}.Build()
	g2Member := v2.Entitlement_builder{Id: "group:g2:member", DisplayName: "g2 member", Resource: g2, Slug: "member"}.Build()
	require.NoError(t, f.PutEntitlements(ctx, g1Member, g2Member))

	expandableAnno, err := anypb.New(v2.GrantExpandable_builder{EntitlementIds: []string{"group:g2:member"}}.Build())
	require.NoError(t, err)
	immutableAnno, err := anypb.New(v2.GrantImmutable_builder{}.Build())
	require.NoError(t, err)

	// g2 is a member of g1, expandable from g2:member (src) into g1:member (dst).
	nesting := v2.Grant_builder{
		Id:          "group:g1:member:group:g2",
		Entitlement: g1Member,
		Principal:   g2,
		Annotations: []*anypb.Any{expandableAnno},
	}.Build()
	// alice is a direct member of g2.
	aliceG2 := v2.Grant_builder{
		Id:          "group:g2:member:user:alice",
		Entitlement: g2Member,
		Principal:   alice,
	}.Build()
	// External-resource grant: GrantImmutable but synced, not derived.
	externalImmutable := v2.Grant_builder{
		Id:          "group:g2:member:user:bob",
		Entitlement: g2Member,
		Principal:   bob,
		Annotations: []*anypb.Any{immutableAnno},
	}.Build()
	// External-principal grant: GrantSources but synced, not derived.
	externalSourced := v2.Grant_builder{
		Id:          "group:g2:member:group:g2",
		Entitlement: g2Member,
		Principal:   g2,
		Sources: v2.GrantSources_builder{Sources: map[string]*v2.GrantSources_GrantSource{
			"group:g2:member": v2.GrantSources_GrantSource_builder{IsDirect: true}.Build(),
		}}.Build(),
	}.Build()
	require.NoError(t, f.PutGrants(ctx, nesting, aliceG2, externalImmutable, externalSourced))

	require.NoError(t, f.Close(ctx))
	return syncID
}

// expandViaSyncer runs the real syncer's grant-expansion over an existing
// c1z, exactly as production does. This finishes the sync with a
// non-empty completed sync token, sets the supports_diff marker, and
// creates the expander-derived grants — the production shape a hand-built
// fixture cannot reproduce.
func expandViaSyncer(t *testing.T, ctx context.Context, path, syncID string) {
	t.Helper()
	conn, err := sdk.NewEmptyConnector()
	require.NoError(t, err)
	syncer, err := sync.NewSyncer(ctx, conn,
		sync.WithC1ZPath(path),
		sync.WithSyncID(syncID),
		sync.WithOnlyExpandGrants(),
		sync.WithTmpDir(t.TempDir()),
	)
	require.NoError(t, err)
	require.NoError(t, syncer.Sync(ctx))
	require.NoError(t, syncer.Close(ctx))
}

// buildExpandedFixture produces a finished, expanded, production-shaped
// c1z (non-empty completed token) and returns (syncID, deletedCount).
func buildExpandedFixture(t *testing.T, ctx context.Context, path string) (string, int) {
	t.Helper()
	syncID := buildPreExpansionC1Z(t, ctx, path)
	expandViaSyncer(t, ctx, path, syncID)
	return syncID, rollbackDeletes(t, ctx, path, syncID)
}

// rollbackDeletes returns how many grants a rollback would delete on the
// sync, via a dry run (GrantsDeleted counts purely expander-derived rows).
func rollbackDeletes(t *testing.T, ctx context.Context, path, syncID string) int {
	t.Helper()
	ro, err := dotc1z.NewC1ZFile(ctx, path, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	defer ro.Close(ctx)
	res, err := ro.RollbackExpansion(ctx, syncID, true)
	require.NoError(t, err)
	return res.GrantsDeleted
}

func TestRollbackExpansionDryRun(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "src.c1z")
	syncID, deletes := buildExpandedFixture(t, ctx, path)
	require.Positive(t, deletes, "the real syncer expansion must produce at least one derived grant")

	ro, err := dotc1z.NewC1ZFile(ctx, path, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	defer ro.Close(ctx)
	res, err := ro.RollbackExpansion(ctx, syncID, true)
	require.NoError(t, err)
	require.True(t, res.DryRun)
	require.Equal(t, deletes, res.GrantsDeleted)
}

func TestRollbackExpansionUnfinishedRefused(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "src.c1z")
	syncID := buildPreExpansionC1Z(t, ctx, path) // started, never finished/expanded

	store, err := dotc1z.NewC1ZFile(ctx, path)
	require.NoError(t, err)
	defer store.Close(ctx)
	_, err = store.RollbackExpansion(ctx, syncID, false)
	require.ErrorIs(t, err, dotc1z.ErrSyncNotFinished)
}

func TestRollbackExpansionCommand(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	inPath := filepath.Join(tmp, "src.c1z")
	outPath := filepath.Join(tmp, "out.c1z")
	syncID, deletes := buildExpandedFixture(t, ctx, inPath)
	require.Positive(t, deletes)

	require.NoError(t, runRollback(ctx, inPath, "--out", outPath, "--sync-id", syncID))
	require.FileExists(t, outPath)

	// Input untouched.
	require.Equal(t, deletes, rollbackDeletes(t, ctx, inPath, syncID))
	// Output (no --replay): derived grants gone, synced grants survive —
	// including the external GrantImmutable and external GrantSources
	// grants and the expandable original. A second rollback finds nothing
	// to delete and nothing to clear.
	require.Equal(t, 0, rollbackDeletes(t, ctx, outPath, syncID))
	requireGrantPresence(t, ctx, outPath, syncID, "group:g1:member:group:g2", true) // expandable original
	requireGrantPresence(t, ctx, outPath, syncID, "group:g2:member:user:alice", true)
	requireGrantPresence(t, ctx, outPath, syncID, "group:g2:member:user:bob", true) // external + GrantImmutable
	requireGrantPresence(t, ctx, outPath, syncID, "group:g2:member:group:g2", true) // external + GrantSources
	// The surviving direct grant that carried a self-source has had its
	// sources cleared back to the pre-expansion shape.
	requireGrantSourcesEmpty(t, ctx, outPath, syncID, "group:g2:member:group:g2")
}

func TestRollbackExpansionRequiresOut(t *testing.T) {
	ctx := context.Background()
	inPath := filepath.Join(t.TempDir(), "src.c1z")
	syncID, _ := buildExpandedFixture(t, ctx, inPath)
	require.Error(t, runRollback(ctx, inPath, "--sync-id", syncID), "a write without --out must be refused")
}

// TestRollbackExpansionReplayRederives is the real-sync-shaped regression
// guard: the fixture has a non-empty completed token (produced by the
// syncer), so a replay that failed to reset the token would no-op and
// leave zero derived grants. It asserts the EXACT derived count is
// restored, which a partial-expansion regression would fail.
func TestRollbackExpansionReplayRederives(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	inPath := filepath.Join(tmp, "src.c1z")
	outPath := filepath.Join(tmp, "out.c1z")
	syncID, deletes := buildExpandedFixture(t, ctx, inPath)
	require.Positive(t, deletes)

	require.NoError(t, runRollback(ctx, inPath, "--out", outPath, "--sync-id", syncID, "--replay"))
	require.FileExists(t, outPath)

	require.Equal(t, deletes, rollbackDeletes(t, ctx, outPath, syncID),
		"replay must restore exactly the derived grants the rollback deleted")
	requireGrantPresence(t, ctx, outPath, syncID, "group:g1:member:user:alice", true)
}

// buildMergedConflictC1Z writes a finished, expansion-marked c1z carrying
// the exact shape a compactor keep-newer merge legitimately seals: two
// entitlements in one exclusion group BOTH marked is_default (the group
// default moved between input syncs and the union kept both rows). This
// is a hard I5 conflict for a strict invariant pass. It also carries an
// expandable nesting grant so a replay has real expansion work to do.
func buildMergedConflictC1Z(t *testing.T, ctx context.Context, path string) string {
	t.Helper()
	f, err := dotc1z.NewC1ZFile(ctx, path)
	require.NoError(t, err)
	syncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	require.NoError(t, f.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "group", DisplayName: "Group", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP}}.Build(),
		v2.ResourceType_builder{Id: "user", DisplayName: "User", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build(),
	))
	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	alice := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(), DisplayName: "Alice"}.Build()
	require.NoError(t, f.PutResources(ctx, g1, g2, alice))

	defaultAnno := func() *anypb.Any {
		a, err := anypb.New(v2.EntitlementExclusionGroup_builder{ExclusionGroupId: "eg-1", IsDefault: true}.Build())
		require.NoError(t, err)
		return a
	}
	g1Member := v2.Entitlement_builder{
		Id: "group:g1:member", DisplayName: "g1 member", Resource: g1, Slug: "member",
		Annotations: []*anypb.Any{defaultAnno()},
	}.Build()
	g2Member := v2.Entitlement_builder{
		Id: "group:g2:member", DisplayName: "g2 member", Resource: g2, Slug: "member",
		Annotations: []*anypb.Any{defaultAnno()},
	}.Build()
	require.NoError(t, f.PutEntitlements(ctx, g1Member, g2Member))

	expandableAnno, err := anypb.New(v2.GrantExpandable_builder{EntitlementIds: []string{"group:g2:member"}}.Build())
	require.NoError(t, err)
	nesting := v2.Grant_builder{
		Id:          "group:g1:member:group:g2",
		Entitlement: g1Member,
		Principal:   g2,
		Annotations: []*anypb.Any{expandableAnno},
	}.Build()
	aliceG2 := v2.Grant_builder{
		Id:          "group:g2:member:user:alice",
		Entitlement: g2Member,
		Principal:   alice,
	}.Build()
	require.NoError(t, f.PutGrants(ctx, nesting, aliceG2))

	require.NoError(t, f.SetSupportsDiff(ctx, syncID))
	require.NoError(t, f.EndSync(ctx))
	require.NoError(t, f.Close(ctx))
	return syncID
}

// TestRollbackExpansionReplayToleratesMergedConflicts is the round-5
// regression guard: rollback-expansion's inputs include compaction-merged
// artifacts, and the compactor legitimately seals keep-newer unions that
// hold I5 exclusion-group conflicts (two is_default rows in one group —
// see TestCompactionExpandToleratesMergeManufacturedExclusionConflicts).
// A replay that runs the invariant pass at full strictness hard-fails I5
// AFTER RollbackExpandedGrants already mutated the output, bricking the
// recovery tool and leaving the file rolled-back-but-unexpanded. The
// replay syncer must soften to attributed warnings
// (WithCompactionMergedStore) and complete.
func TestRollbackExpansionReplayToleratesMergedConflicts(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	inPath := filepath.Join(tmp, "src.c1z")
	outPath := filepath.Join(tmp, "out.c1z")
	syncID := buildMergedConflictC1Z(t, ctx, inPath)

	require.NoError(t, runRollback(ctx, inPath, "--out", outPath, "--sync-id", syncID, "--replay"),
		"replay over a compaction-merged artifact must warn on the sealed conflict, not hard-fail I5")
	require.FileExists(t, outPath)

	// The replay actually expanded: alice's membership in g2 propagated
	// through the expandable nesting grant into g1.
	requireGrantPresence(t, ctx, outPath, syncID, "group:g1:member:user:alice", true)
}

// buildSyntheticExpandedC1Z writes a finished, expansion-marked c1z whose
// grants carry hand-set Sources maps, so each rollback classification case
// is exercised in isolation without driving the real expander:
//   - derived: on g1:member, Sources only foreign (g2:member), GrantImmutable → deleted.
//   - directExpanded: on g2:member, Sources include self (g2:member) → kept, sources cleared.
//   - plainDirect: on g2:member, no Sources → untouched.
//   - suspect: on g1:member, Sources only foreign, NO GrantImmutable → deleted + flagged.
func buildSyntheticExpandedC1Z(t *testing.T, ctx context.Context, path string) string {
	t.Helper()
	f, err := dotc1z.NewC1ZFile(ctx, path)
	require.NoError(t, err)
	syncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	require.NoError(t, f.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "group", DisplayName: "Group", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP}}.Build(),
		v2.ResourceType_builder{Id: "user", DisplayName: "User", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build(),
	))
	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	alice := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(), DisplayName: "Alice"}.Build()
	bob := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "bob"}.Build(), DisplayName: "Bob"}.Build()
	carol := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "carol"}.Build(), DisplayName: "Carol"}.Build()
	dave := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "dave"}.Build(), DisplayName: "Dave"}.Build()
	require.NoError(t, f.PutResources(ctx, g1, g2, alice, bob, carol, dave))

	g1Member := v2.Entitlement_builder{Id: "group:g1:member", DisplayName: "g1 member", Resource: g1, Slug: "member"}.Build()
	g2Member := v2.Entitlement_builder{Id: "group:g2:member", DisplayName: "g2 member", Resource: g2, Slug: "member"}.Build()
	require.NoError(t, f.PutEntitlements(ctx, g1Member, g2Member))

	immutableAnno, err := anypb.New(v2.GrantImmutable_builder{}.Build())
	require.NoError(t, err)

	derived := v2.Grant_builder{
		Id:          "group:g1:member:user:alice",
		Entitlement: g1Member,
		Principal:   alice,
		Annotations: []*anypb.Any{immutableAnno},
		Sources: v2.GrantSources_builder{Sources: map[string]*v2.GrantSources_GrantSource{
			"group:g2:member": v2.GrantSources_GrantSource_builder{IsDirect: false}.Build(),
		}}.Build(),
	}.Build()
	directExpanded := v2.Grant_builder{
		Id:          "group:g2:member:user:bob",
		Entitlement: g2Member,
		Principal:   bob,
		Sources: v2.GrantSources_builder{Sources: map[string]*v2.GrantSources_GrantSource{
			"group:g2:member": v2.GrantSources_GrantSource_builder{IsDirect: true}.Build(),
			"group:g1:member": v2.GrantSources_GrantSource_builder{IsDirect: false}.Build(),
		}}.Build(),
	}.Build()
	plainDirect := v2.Grant_builder{
		Id:          "group:g2:member:user:carol",
		Entitlement: g2Member,
		Principal:   carol,
	}.Build()
	suspect := v2.Grant_builder{
		Id:          "group:g1:member:user:dave",
		Entitlement: g1Member,
		Principal:   dave,
		Sources: v2.GrantSources_builder{Sources: map[string]*v2.GrantSources_GrantSource{
			"group:g2:member": v2.GrantSources_GrantSource_builder{IsDirect: true}.Build(),
		}}.Build(),
	}.Build()
	require.NoError(t, f.PutGrants(ctx, derived, directExpanded, plainDirect, suspect))

	require.NoError(t, f.SetSupportsDiff(ctx, syncID))
	require.NoError(t, f.EndSync(ctx))
	require.NoError(t, f.Close(ctx))
	return syncID
}

func TestRollbackExpansionClassification(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "src.c1z")
	syncID := buildSyntheticExpandedC1Z(t, ctx, path)

	// Dry run: counts without mutating.
	ro, err := dotc1z.NewC1ZFile(ctx, path, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	res, err := ro.RollbackExpansion(ctx, syncID, true)
	require.NoError(t, err)
	require.NoError(t, ro.Close(ctx))
	require.True(t, res.DryRun)
	require.Equal(t, 2, res.GrantsDeleted, "derived + suspect deleted")
	require.Equal(t, 1, res.SourcesCleared, "directExpanded cleared")
	require.Equal(t, 1, res.SuspectConnectorSourced, "suspect lacks GrantImmutable")
	// Dry run mutated nothing.
	requireGrantPresence(t, ctx, path, syncID, "group:g1:member:user:alice", true)

	// Real rollback on a writable copy.
	store, err := dotc1z.NewC1ZFile(ctx, path)
	require.NoError(t, err)
	res, err = store.RollbackExpansion(ctx, syncID, false)
	require.NoError(t, err)
	require.NoError(t, store.Close(ctx))
	require.Equal(t, 2, res.GrantsDeleted)
	require.Equal(t, 1, res.SourcesCleared)
	require.Equal(t, 1, res.SuspectConnectorSourced)

	requireGrantPresence(t, ctx, path, syncID, "group:g1:member:user:alice", false) // derived → deleted
	requireGrantPresence(t, ctx, path, syncID, "group:g1:member:user:dave", false)  // suspect → deleted
	requireGrantPresence(t, ctx, path, syncID, "group:g2:member:user:bob", true)    // directExpanded → kept
	requireGrantPresence(t, ctx, path, syncID, "group:g2:member:user:carol", true)  // plainDirect → untouched
	requireGrantSourcesEmpty(t, ctx, path, syncID, "group:g2:member:user:bob")
}

func TestRollbackExpansionNonExpandedRefused(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "src.c1z")

	f, err := dotc1z.NewC1ZFile(ctx, path)
	require.NoError(t, err)
	syncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, f.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "user", DisplayName: "User", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build(),
	))
	require.NoError(t, f.EndSync(ctx)) // finished but supports_diff never set
	require.NoError(t, f.Close(ctx))

	store, err := dotc1z.NewC1ZFile(ctx, path)
	require.NoError(t, err)
	defer store.Close(ctx)
	_, err = store.RollbackExpansion(ctx, syncID, false)
	require.ErrorIs(t, err, dotc1z.ErrSyncNotExpanded)
}

// buildEmptyExpandedC1Z writes a finished, expansion-marked sync that holds
// resources but no grants — the resource-only shape a rollback has nothing
// to do on.
func buildEmptyExpandedC1Z(t *testing.T, ctx context.Context, path string) string {
	t.Helper()
	f, err := dotc1z.NewC1ZFile(ctx, path)
	require.NoError(t, err)
	syncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, f.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "user", DisplayName: "User", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build(),
	))
	require.NoError(t, f.SetSupportsDiff(ctx, syncID))
	require.NoError(t, f.EndSync(ctx))
	require.NoError(t, f.Close(ctx))
	return syncID
}

func TestRollbackExpansionNoGrantsEarlyReturn(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	inPath := filepath.Join(tmp, "src.c1z")
	syncID := buildEmptyExpandedC1Z(t, ctx, inPath)

	// Both the dry run and the real rollback short-circuit to a zero-count
	// result without error: a grant-less sync has nothing to roll back.
	store, err := dotc1z.NewC1ZFile(ctx, inPath)
	require.NoError(t, err)
	dry, err := store.RollbackExpansion(ctx, syncID, true)
	require.NoError(t, err)
	require.Equal(t, 0, dry.GrantsDeleted)
	require.Equal(t, 0, dry.SourcesCleared)
	got, err := store.RollbackExpansion(ctx, syncID, false)
	require.NoError(t, err)
	require.Equal(t, 0, got.GrantsDeleted)
	require.Equal(t, 0, got.SourcesCleared)
	require.NoError(t, store.Close(ctx))

	// The command path with --replay --validate no-ops cleanly: nothing to
	// re-derive, and the before/after source snapshots are both empty.
	outPath := filepath.Join(tmp, "out.c1z")
	require.NoError(t, runRollback(ctx, inPath, "--out", outPath, "--sync-id", syncID, "--replay", "--validate"))
	require.FileExists(t, outPath)
}

func runRollback(ctx context.Context, inPath string, args ...string) error {
	root := &cobra.Command{Use: "baton"}
	root.PersistentFlags().StringP("file", "f", "sync.c1z", "")
	root.AddCommand(rollbackExpansionCmd())
	root.SilenceUsage, root.SilenceErrors = true, true
	root.SetArgs(append([]string{"rollback-expansion", "--file", inPath}, args...))
	return root.ExecuteContext(ctx)
}

func requireGrantPresence(t *testing.T, ctx context.Context, path, syncID, grantID string, want bool) {
	t.Helper()
	_, found := loadGrant(t, ctx, path, syncID, grantID)
	require.Equalf(t, want, found, "grant %q presence in %s", grantID, filepath.Base(path))
}

func requireGrantSourcesEmpty(t *testing.T, ctx context.Context, path, syncID, grantID string) {
	t.Helper()
	g, found := loadGrant(t, ctx, path, syncID, grantID)
	require.Truef(t, found, "grant %q expected present in %s", grantID, filepath.Base(path))
	require.Emptyf(t, g.GetSources().GetSources(), "grant %q sources should be cleared", grantID)
}

func loadGrant(t *testing.T, ctx context.Context, path, syncID, grantID string) (*v2.Grant, bool) {
	t.Helper()
	ro, err := dotc1z.NewC1ZFile(ctx, path, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	defer ro.Close(ctx)
	require.NoError(t, ro.SetCurrentSync(ctx, syncID))
	pageToken := ""
	for {
		resp, err := ro.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageSize: 1000, PageToken: pageToken}.Build())
		require.NoError(t, err)
		for _, g := range resp.GetList() {
			if g.GetId() == grantID {
				return g, true
			}
		}
		if resp.GetNextPageToken() == "" {
			return nil, false
		}
		pageToken = resp.GetNextPageToken()
	}
}
