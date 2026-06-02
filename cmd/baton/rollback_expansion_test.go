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
// c1z (non-empty completed token) and returns (syncID, derivedCount).
func buildExpandedFixture(t *testing.T, ctx context.Context, path string) (string, int) {
	t.Helper()
	syncID := buildPreExpansionC1Z(t, ctx, path)
	expandViaSyncer(t, ctx, path, syncID)
	return syncID, derivedCount(t, ctx, path, syncID)
}

// derivedCount returns how many expander-derived rows the sync has, via a
// dry-run rollback (DerivedDeleted counts derived_by_expansion=1 rows).
func derivedCount(t *testing.T, ctx context.Context, path, syncID string) int {
	t.Helper()
	ro, err := dotc1z.NewC1ZFile(ctx, path, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	defer ro.Close(ctx)
	res, err := ro.RollbackExpansion(ctx, syncID, true)
	require.NoError(t, err)
	return res.DerivedDeleted
}

func TestRollbackExpansionDryRun(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "src.c1z")
	syncID, derived := buildExpandedFixture(t, ctx, path)
	require.Positive(t, derived, "the real syncer expansion must produce at least one derived grant")

	ro, err := dotc1z.NewC1ZFile(ctx, path, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	defer ro.Close(ctx)
	res, err := ro.RollbackExpansion(ctx, syncID, true)
	require.NoError(t, err)
	require.True(t, res.DryRun)
	require.Equal(t, derived, res.DerivedDeleted)
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
	syncID, derived := buildExpandedFixture(t, ctx, inPath)
	require.Positive(t, derived)

	require.NoError(t, runRollback(ctx, inPath, "--out", outPath, "--sync-id", syncID))
	require.FileExists(t, outPath)

	// Input untouched.
	require.Equal(t, derived, derivedCount(t, ctx, inPath, syncID))
	// Output (no --replay): derived grants gone, synced grants survive —
	// including the external GrantImmutable (blocker #1) and external
	// GrantSources (blocker #2) grants and the expandable original.
	require.Equal(t, 0, derivedCount(t, ctx, outPath, syncID))
	requireGrantPresence(t, ctx, outPath, syncID, "group:g1:member:group:g2", true) // expandable original
	requireGrantPresence(t, ctx, outPath, syncID, "group:g2:member:user:alice", true)
	requireGrantPresence(t, ctx, outPath, syncID, "group:g2:member:user:bob", true) // external + GrantImmutable
	requireGrantPresence(t, ctx, outPath, syncID, "group:g2:member:group:g2", true) // external + GrantSources
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
	syncID, derived := buildExpandedFixture(t, ctx, inPath)
	require.Positive(t, derived)

	require.NoError(t, runRollback(ctx, inPath, "--out", outPath, "--sync-id", syncID, "--replay"))
	require.FileExists(t, outPath)

	require.Equal(t, derived, derivedCount(t, ctx, outPath, syncID),
		"replay must restore exactly the derived grants the rollback deleted")
	requireGrantPresence(t, ctx, outPath, syncID, "group:g1:member:user:alice", true)
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
	ro, err := dotc1z.NewC1ZFile(ctx, path, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	defer ro.Close(ctx)
	require.NoError(t, ro.SetCurrentSync(ctx, syncID))
	found := false
	pageToken := ""
	for {
		resp, err := ro.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageSize: 1000, PageToken: pageToken}.Build())
		require.NoError(t, err)
		for _, g := range resp.GetList() {
			if g.GetId() == grantID {
				found = true
			}
		}
		if resp.GetNextPageToken() == "" {
			break
		}
		pageToken = resp.GetNextPageToken()
	}
	require.Equalf(t, want, found, "grant %q presence in %s", grantID, filepath.Base(path))
}
