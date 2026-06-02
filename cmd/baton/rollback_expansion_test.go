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
)

// buildExpandedFixture writes a finished sync containing one expandable
// original grant (GrantExpandable → needs_expansion=1), one
// expansion-derived grant (GrantImmutable + GrantSources), and one
// pre-existing connector grant the expander mutated in place
// (GrantSources, no GrantImmutable). Returns the sync id.
func buildExpandedFixture(t *testing.T, ctx context.Context, path string, finish bool) string {
	t.Helper()
	f, err := dotc1z.NewC1ZFile(ctx, path)
	require.NoError(t, err)
	syncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	require.NoError(t, f.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "group", DisplayName: "Group", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP}}.Build(),
		v2.ResourceType_builder{Id: "user", DisplayName: "User", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build(),
	))
	src := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "src"}.Build(), DisplayName: "Src"}.Build()
	dst := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "dst"}.Build(), DisplayName: "Dst"}.Build()
	alice := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(), DisplayName: "Alice"}.Build()
	bob := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "bob"}.Build(), DisplayName: "Bob"}.Build()
	require.NoError(t, f.PutResources(ctx, src, dst, alice, bob))

	srcEnt := v2.Entitlement_builder{Id: "group:src:member", DisplayName: "src member", Resource: src, Slug: "member"}.Build()
	dstEnt := v2.Entitlement_builder{Id: "group:dst:member", DisplayName: "dst member", Resource: dst, Slug: "member"}.Build()
	require.NoError(t, f.PutEntitlements(ctx, srcEnt, dstEnt))

	expandableAnno, err := anypb.New(v2.GrantExpandable_builder{EntitlementIds: []string{"group:src:member"}}.Build())
	require.NoError(t, err)
	immutableAnno, err := anypb.New(v2.GrantImmutable_builder{}.Build())
	require.NoError(t, err)

	// Original expandable grant on the source entitlement.
	original := v2.Grant_builder{
		Id:          "group:src:member:user:alice",
		Entitlement: srcEnt,
		Principal:   alice,
		Annotations: []*anypb.Any{expandableAnno},
	}.Build()
	// Derived grant the expander created (Case A): GrantImmutable + Sources.
	derived := v2.Grant_builder{
		Id:          "group:dst:member:user:alice",
		Entitlement: dstEnt,
		Principal:   alice,
		Sources: v2.GrantSources_builder{Sources: map[string]*v2.GrantSources_GrantSource{
			"group:src:member": v2.GrantSources_GrantSource_builder{IsDirect: false}.Build(),
		}}.Build(),
		Annotations: []*anypb.Any{immutableAnno},
	}.Build()
	// Pre-existing connector grant the expander mutated (Case B): Sources, no GrantImmutable.
	caseB := v2.Grant_builder{
		Id:          "group:dst:member:user:bob",
		Entitlement: dstEnt,
		Principal:   bob,
		Sources: v2.GrantSources_builder{Sources: map[string]*v2.GrantSources_GrantSource{
			"group:dst:member": v2.GrantSources_GrantSource_builder{IsDirect: true}.Build(),
			"group:src:member": v2.GrantSources_GrantSource_builder{IsDirect: false}.Build(),
		}}.Build(),
	}.Build()
	require.NoError(t, f.PutGrants(ctx, original, derived, caseB))

	if finish {
		require.NoError(t, f.EndSync(ctx))
	}
	require.NoError(t, f.Close(ctx))
	return syncID
}

func TestRollbackExpansionDryRun(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "src.c1z")
	syncID := buildExpandedFixture(t, ctx, path, true)

	ro, err := dotc1z.NewC1ZFile(ctx, path, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	defer ro.Close(ctx)

	res, err := ro.RollbackExpansion(ctx, syncID, true)
	require.NoError(t, err)
	require.True(t, res.DryRun)
	require.Equal(t, 1, res.DerivedDeleted, "one GrantImmutable-derived grant")
	require.Equal(t, 1, res.SourcesCleared, "one Case-B mutated grant (no GrantImmutable, has sources)")
}

func TestRollbackExpansionUnfinishedRefused(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "src.c1z")
	syncID := buildExpandedFixture(t, ctx, path, false) // no EndSync → unfinished

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
	syncID := buildExpandedFixture(t, ctx, inPath, true)

	root := &cobra.Command{Use: "baton"}
	root.PersistentFlags().StringP("file", "f", "sync.c1z", "")
	root.AddCommand(rollbackExpansionCmd())
	root.SetArgs([]string{"rollback-expansion", "--file", inPath, "--out", outPath, "--sync-id", syncID})
	require.NoError(t, root.ExecuteContext(ctx))

	require.FileExists(t, outPath)
	// Input untouched: still has the derived grant.
	requireGrantPresence(t, ctx, inPath, syncID, "group:dst:member:user:alice", true)
	// Output: derived grant gone, original expandable grant intact, Case-B survives.
	requireGrantPresence(t, ctx, outPath, syncID, "group:dst:member:user:alice", false)
	requireGrantPresence(t, ctx, outPath, syncID, "group:src:member:user:alice", true)
	requireGrantPresence(t, ctx, outPath, syncID, "group:dst:member:user:bob", true)
}

func TestRollbackExpansionRequiresOut(t *testing.T) {
	ctx := context.Background()
	inPath := filepath.Join(t.TempDir(), "src.c1z")
	syncID := buildExpandedFixture(t, ctx, inPath, true)

	root := &cobra.Command{Use: "baton"}
	root.PersistentFlags().StringP("file", "f", "sync.c1z", "")
	root.AddCommand(rollbackExpansionCmd())
	root.SilenceUsage, root.SilenceErrors = true, true
	root.SetArgs([]string{"rollback-expansion", "--file", inPath, "--sync-id", syncID})
	require.Error(t, root.ExecuteContext(ctx), "a write without --out must be refused")
}

func TestReplayExpansionRuns(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	inPath := filepath.Join(tmp, "src.c1z")
	outPath := filepath.Join(tmp, "out.c1z")
	syncID := buildExpandedFixture(t, ctx, inPath, true)

	root := &cobra.Command{Use: "baton"}
	root.PersistentFlags().StringP("file", "f", "sync.c1z", "")
	root.AddCommand(rollbackExpansionCmd())
	root.SetArgs([]string{"rollback-expansion", "--file", inPath, "--out", outPath, "--sync-id", syncID, "--replay"})
	require.NoError(t, root.ExecuteContext(ctx), "rollback + 3-phase replay should run clean")
	require.FileExists(t, outPath)
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
