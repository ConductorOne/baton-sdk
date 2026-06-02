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

// buildExpandedFixture writes a finished, expansion-marked sync modeling
// nested groups: g2 is a member of g1, alice is a member of g2, so
// expansion derives alice -> g1:member. The fixture also includes two
// grants that carry expansion-looking annotations but were NOT created
// by the expander — an external-resource grant with GrantImmutable and
// an external-principal grant with GrantSources — both of which rollback
// must leave intact (they are synced, derived_by_expansion=0).
//
// The expander-derived grant is written via StoreExpandedGrants so it
// gets derived_by_expansion=1; everything else via PutGrants (=0).
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
	// External-resource grant: carries GrantImmutable but was synced, not derived.
	externalImmutable := v2.Grant_builder{
		Id:          "group:g2:member:user:bob",
		Entitlement: g2Member,
		Principal:   bob,
		Annotations: []*anypb.Any{immutableAnno},
	}.Build()
	// External-principal grant: carries GrantSources but was synced, not derived.
	externalSourced := v2.Grant_builder{
		Id:          "group:g1:member:user:bob",
		Entitlement: g1Member,
		Principal:   bob,
		Sources: v2.GrantSources_builder{Sources: map[string]*v2.GrantSources_GrantSource{
			"group:g1:member": v2.GrantSources_GrantSource_builder{IsDirect: true}.Build(),
		}}.Build(),
	}.Build()
	require.NoError(t, f.PutGrants(ctx, nesting, aliceG2, externalImmutable, externalSourced))

	// The expander-created derived grant (alice -> g1:member).
	derived := v2.Grant_builder{
		Id:          "group:g1:member:user:alice",
		Entitlement: g1Member,
		Principal:   alice,
		Sources: v2.GrantSources_builder{Sources: map[string]*v2.GrantSources_GrantSource{
			"group:g2:member": v2.GrantSources_GrantSource_builder{IsDirect: false}.Build(),
		}}.Build(),
		Annotations: []*anypb.Any{immutableAnno},
	}.Build()
	require.NoError(t, f.StoreExpandedGrants(ctx, derived))

	if finish {
		require.NoError(t, f.SetSupportsDiff(ctx, syncID))
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
	require.Equal(t, 1, res.DerivedDeleted, "only the StoreExpandedGrants-written derived grant is derived")
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

	require.NoError(t, runRollback(ctx, inPath, "--out", outPath, "--sync-id", syncID))

	require.FileExists(t, outPath)
	// Input untouched: still has the derived grant.
	requireGrantPresence(t, ctx, inPath, syncID, "group:g1:member:user:alice", true)
	// Output: derived grant gone; everything synced survives — including
	// the external GrantImmutable grant (blocker #1) and the external
	// GrantSources grant (blocker #2).
	requireGrantPresence(t, ctx, outPath, syncID, "group:g1:member:user:alice", false)
	requireGrantPresence(t, ctx, outPath, syncID, "group:g1:member:group:g2", true) // expandable original
	requireGrantPresence(t, ctx, outPath, syncID, "group:g2:member:user:alice", true)
	requireGrantPresence(t, ctx, outPath, syncID, "group:g2:member:user:bob", true) // external + GrantImmutable
	requireGrantPresence(t, ctx, outPath, syncID, "group:g1:member:user:bob", true) // external + GrantSources
}

func TestRollbackExpansionRequiresOut(t *testing.T) {
	ctx := context.Background()
	inPath := filepath.Join(t.TempDir(), "src.c1z")
	syncID := buildExpandedFixture(t, ctx, inPath, true)
	require.Error(t, runRollback(ctx, inPath, "--sync-id", syncID), "a write without --out must be refused")
}

func TestRollbackExpansionReplayRederives(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	inPath := filepath.Join(tmp, "src.c1z")
	outPath := filepath.Join(tmp, "out.c1z")
	syncID := buildExpandedFixture(t, ctx, inPath, true)

	require.NoError(t, runRollback(ctx, inPath, "--out", outPath, "--sync-id", syncID, "--replay"))
	require.FileExists(t, outPath)
	// Replay re-runs the full expansion, so the derived grant returns.
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
