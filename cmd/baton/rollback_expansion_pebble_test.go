package main

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/synccompactor"
)

// pebbleGroupsShape is a nested-groups fixture: g2 is a member of g1 through
// an expandable grant, alice and bob are members of g2 (bob's grant carries
// GrantImmutable), and carol is a direct member of both. Expansion derives
// alice and bob on g1 and adds Sources to carol's g1 grant.
type pebbleGroupsShape struct {
	// connectorSourced adds g2's membership in itself with a connector-set
	// self-source, which rollback clears and replay cannot restore.
	connectorSourced bool
}

func (s pebbleGroupsShape) put(t *testing.T, ctx context.Context, w connectorstore.Writer) {
	t.Helper()
	require.NoError(t, w.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "group", DisplayName: "Group", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP}}.Build(),
		v2.ResourceType_builder{Id: "user", DisplayName: "User", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build(),
	))
	resource := func(rt, id string) *v2.Resource {
		return v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: rt, Resource: id}.Build(), DisplayName: id}.Build()
	}
	g1, g2 := resource("group", "g1"), resource("group", "g2")
	alice, bob, carol := resource("user", "alice"), resource("user", "bob"), resource("user", "carol")
	require.NoError(t, w.PutResources(ctx, g1, g2, alice, bob, carol))
	g1Member := v2.Entitlement_builder{Id: "group:g1:member", DisplayName: "g1 member", Resource: g1, Slug: "member"}.Build()
	g2Member := v2.Entitlement_builder{Id: "group:g2:member", DisplayName: "g2 member", Resource: g2, Slug: "member"}.Build()
	require.NoError(t, w.PutEntitlements(ctx, g1Member, g2Member))

	expandable, err := anypb.New(v2.GrantExpandable_builder{EntitlementIds: []string{"group:g2:member"}}.Build())
	require.NoError(t, err)
	immutable, err := anypb.New(v2.GrantImmutable_builder{}.Build())
	require.NoError(t, err)
	grant := func(ent *v2.Entitlement, principal *v2.Resource, annos ...*anypb.Any) *v2.Grant {
		return v2.Grant_builder{
			Id:          ent.GetId() + ":" + principal.GetId().GetResourceType() + ":" + principal.GetId().GetResource(),
			Entitlement: ent,
			Principal:   principal,
			Annotations: annos,
		}.Build()
	}
	grants := []*v2.Grant{
		grant(g1Member, g2, expandable),
		grant(g2Member, alice),
		grant(g2Member, bob, immutable),
		grant(g2Member, carol),
		grant(g1Member, carol),
	}
	if s.connectorSourced {
		g := grant(g2Member, g2)
		g.SetSources(v2.GrantSources_builder{Sources: map[string]*v2.GrantSources_GrantSource{
			"group:g2:member": v2.GrantSources_GrantSource_builder{IsDirect: true}.Build(),
		}}.Build())
		grants = append(grants, g)
	}
	require.NoError(t, w.PutGrants(ctx, grants...))
}

// buildPebbleC1Z writes a finished, unexpanded Pebble sync populated by put.
func buildPebbleC1Z(t *testing.T, ctx context.Context, path string, put func(connectorstore.Writer)) string {
	t.Helper()
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	put(store)
	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))
	return syncID
}

func buildPebbleExpandedFixture(t *testing.T, ctx context.Context, path string, shape pebbleGroupsShape) string {
	t.Helper()
	syncID := buildPebbleC1Z(t, ctx, path, func(w connectorstore.Writer) { shape.put(t, ctx, w) })
	expandViaSyncer(t, ctx, path, syncID)
	return syncID
}

// grantSnapshot renders every grant's id, Sources, annotations, and
// needs_expansion bit so two files' grant sets compare as strings.
func grantSnapshot(t *testing.T, ctx context.Context, path, syncID string) map[string]string {
	t.Helper()
	ro, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer ro.Close(ctx)
	require.NoError(t, ro.SetCurrentSync(ctx, syncID))

	needsExpansion := map[string]bool{}
	for p, err := range ro.Grants().PendingExpansion(ctx) {
		require.NoError(t, err)
		needsExpansion[p.GrantExternalID] = true
	}
	out := map[string]string{}
	rs, ok := ro.(connectorstore.StreamingReader)
	require.True(t, ok)
	for g, err := range rs.StreamGrants(ctx, syncID, connectorstore.StreamGrantsOptions{}) {
		require.NoError(t, err)
		annos := make([]string, 0, len(g.GetAnnotations()))
		for _, a := range g.GetAnnotations() {
			b, err := proto.MarshalOptions{Deterministic: true}.Marshal(a)
			require.NoError(t, err)
			annos = append(annos, fmt.Sprintf("%x", b))
		}
		sort.Strings(annos)
		out[g.GetId()] = fmt.Sprintf("sources=[%s] annotations=[%s] needs_expansion=%t",
			canonicalGrantSources(g), strings.Join(annos, ","), needsExpansion[g.GetId()])
	}
	return out
}

func pebbleSyncFinished(t *testing.T, ctx context.Context, path, syncID string) bool {
	t.Helper()
	ro, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer ro.Close(ctx)
	run, err := ro.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
	require.NoError(t, err)
	return run != nil && run.ID == syncID
}

func pebbleStats(t *testing.T, ctx context.Context, path, syncID string) map[string]int64 {
	t.Helper()
	ro, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer ro.Close(ctx)
	stats, err := ro.SyncMeta().Stats(ctx, connectorstore.SyncTypeAny, syncID)
	require.NoError(t, err)
	return stats
}

func TestRollbackExpansionPebbleRestoresPreExpansionGrants(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	inPath := filepath.Join(tmp, "src.c1z")
	outPath := filepath.Join(tmp, "out.c1z")
	shape := pebbleGroupsShape{}
	syncID := buildPebbleC1Z(t, ctx, inPath, func(w connectorstore.Writer) { shape.put(t, ctx, w) })
	preExpansion := grantSnapshot(t, ctx, inPath, syncID)
	expandViaSyncer(t, ctx, inPath, syncID)
	expanded := grantSnapshot(t, ctx, inPath, syncID)
	require.NotEqual(t, preExpansion, expanded, "the fixture must expand")

	dry, err := rollbackDryRun(t, ctx, inPath, syncID)
	require.NoError(t, err)
	require.Positive(t, dry.GrantsDeleted)
	require.Positive(t, dry.SourcesCleared)

	require.NoError(t, runRollback(ctx, inPath, "--out", outPath, "--sync-id", syncID))

	require.Equal(t, preExpansion, grantSnapshot(t, ctx, outPath, syncID), "rollback restores the pre-expansion grant set")
	require.Equal(t, len(expanded)-len(preExpansion), dry.GrantsDeleted, "dry-run deletes equal the grants the write removed")
	require.Equal(t, expanded, grantSnapshot(t, ctx, inPath, syncID), "the input is never modified")
	require.True(t, pebbleSyncFinished(t, ctx, outPath, syncID), "the output is resealed")
	require.Less(t, pebbleStats(t, ctx, outPath, syncID)["grants"], pebbleStats(t, ctx, inPath, syncID)["grants"],
		"stats are recomputed against the rolled-back rows")

	_, err = rollbackDryRun(t, ctx, outPath, syncID)
	require.ErrorIs(t, err, dotc1z.ErrSyncNotExpanded, "a rolled-back output has nothing left to roll back")
}

func TestRollbackExpansionPebbleReplayValidates(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	inPath := filepath.Join(tmp, "src.c1z")
	outPath := filepath.Join(tmp, "out.c1z")
	syncID := buildPebbleExpandedFixture(t, ctx, inPath, pebbleGroupsShape{})

	require.NoError(t, runRollback(ctx, inPath, "--out", outPath, "--sync-id", syncID, "--replay", "--validate"))

	require.Equal(t, grantSnapshot(t, ctx, inPath, syncID), grantSnapshot(t, ctx, outPath, syncID),
		"replay reproduces the expanded grant set")
	require.Equal(t, pebbleStats(t, ctx, inPath, syncID), pebbleStats(t, ctx, outPath, syncID),
		"rollback and replay reproduce the expanded sync's stats")
	require.True(t, pebbleSyncFinished(t, ctx, outPath, syncID))
}

func TestRollbackExpansionPebbleReplayRederives(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	inPath := filepath.Join(tmp, "src.c1z")
	outPath := filepath.Join(tmp, "out.c1z")
	syncID := buildPebbleExpandedFixture(t, ctx, inPath, pebbleGroupsShape{connectorSourced: true})
	deletes := rollbackDeletes(t, ctx, inPath, syncID)
	require.Positive(t, deletes)

	require.NoError(t, runRollback(ctx, inPath, "--out", outPath, "--sync-id", syncID, "--replay"))

	require.Equal(t, deletes, rollbackDeletes(t, ctx, outPath, syncID), "replay restores exactly the derived grants")
	requireGrantPresence(t, ctx, outPath, syncID, "group:g1:member:user:alice", true)
	requireGrantSourcesEmpty(t, ctx, outPath, syncID, "group:g2:member:group:g2")
}

func TestRollbackExpansionPebbleValidateDetectsDivergence(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	inPath := filepath.Join(tmp, "src.c1z")
	outPath := filepath.Join(tmp, "out.c1z")
	syncID := buildPebbleExpandedFixture(t, ctx, inPath, pebbleGroupsShape{connectorSourced: true})

	err := runRollback(ctx, inPath, "--out", outPath, "--sync-id", syncID, "--replay", "--validate")
	require.ErrorContains(t, err, "changed grant sources")
	require.NoFileExists(t, outPath)
}

func TestRollbackExpansionPebbleNotExpanded(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	inPath := filepath.Join(tmp, "src.c1z")
	outPath := filepath.Join(tmp, "out.c1z")
	shape := pebbleGroupsShape{}
	syncID := buildPebbleC1Z(t, ctx, inPath, func(w connectorstore.Writer) { shape.put(t, ctx, w) })

	require.ErrorIs(t, runRollback(ctx, inPath, "--dry-run", "--sync-id", syncID), dotc1z.ErrSyncNotExpanded)
	require.ErrorIs(t, runRollback(ctx, inPath, "--out", outPath, "--sync-id", syncID), dotc1z.ErrSyncNotExpanded)
	require.NoFileExists(t, outPath)
}

func TestRollbackExpansionPebbleNoGrants(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	inPath := filepath.Join(tmp, "src.c1z")
	outPath := filepath.Join(tmp, "out.c1z")
	syncID := buildPebbleC1Z(t, ctx, inPath, func(w connectorstore.Writer) {
		require.NoError(t, w.PutResourceTypes(ctx,
			v2.ResourceType_builder{Id: "user", DisplayName: "User", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build(),
		))
	})

	require.NoError(t, runRollback(ctx, inPath, "--dry-run", "--sync-id", syncID))
	require.NoError(t, runRollback(ctx, inPath, "--out", outPath, "--sync-id", syncID, "--replay", "--validate"))
	require.True(t, pebbleSyncFinished(t, ctx, outPath, syncID))
}

func TestRollbackExpansionPebbleSuspectGrants(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	inPath := filepath.Join(tmp, "src.c1z")
	syncID := buildPebbleC1Z(t, ctx, inPath, func(w connectorstore.Writer) {
		pebbleGroupsShape{}.put(t, ctx, w)
		dave := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "dave"}.Build(), DisplayName: "dave"}.Build()
		g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
		require.NoError(t, w.PutResources(ctx, dave))
		require.NoError(t, w.PutGrants(ctx, v2.Grant_builder{
			Id:          "group:g1:member:user:dave",
			Entitlement: v2.Entitlement_builder{Id: "group:g1:member", Resource: g1}.Build(),
			Principal:   dave,
			Sources: v2.GrantSources_builder{Sources: map[string]*v2.GrantSources_GrantSource{
				"group:g2:member": v2.GrantSources_GrantSource_builder{IsDirect: true}.Build(),
			}}.Build(),
		}.Build()))
	})

	res, err := rollbackDryRun(t, ctx, inPath, syncID)
	require.NoError(t, err)
	require.Equal(t, 1, res.SuspectConnectorSourced)
	require.Equal(t, 1, res.GrantsDeleted)

	deletedOut := filepath.Join(tmp, "deleted.c1z")
	require.NoError(t, runRollback(ctx, inPath, "--out", deletedOut, "--sync-id", syncID))
	requireGrantPresence(t, ctx, deletedOut, syncID, "group:g1:member:user:dave", false)

	preservedOut := filepath.Join(tmp, "preserved.c1z")
	require.NoError(t, runRollback(ctx, inPath, "--out", preservedOut, "--sync-id", syncID, "--preserve-suspect-grants"))
	g, found := loadGrant(t, ctx, preservedOut, syncID, "group:g1:member:user:dave")
	require.True(t, found)
	require.Contains(t, g.GetSources().GetSources(), "group:g2:member")
}

func TestRollbackExpansionPebbleUnfinishedRefused(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	inPath := filepath.Join(tmp, "src.c1z")
	outPath := filepath.Join(tmp, "out.c1z")
	store, err := dotc1z.NewStore(ctx, inPath, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	pebbleGroupsShape{}.put(t, ctx, store)
	require.NoError(t, store.Close(ctx))

	require.ErrorIs(t, runRollback(ctx, inPath, "--dry-run", "--sync-id", syncID), dotc1z.ErrSyncNotFinished)
	require.Error(t, runRollback(ctx, inPath, "--out", outPath, "--sync-id", syncID))
	require.NoFileExists(t, outPath)
}

func TestRollbackExpansionPebbleCompactedInput(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	basePath := filepath.Join(tmp, "base.c1z")
	partialPath := filepath.Join(tmp, "partial.c1z")
	baseID := buildPebbleExpandedFixture(t, ctx, basePath, pebbleGroupsShape{})
	partialID := buildPebbleC1Z(t, ctx, partialPath, func(w connectorstore.Writer) {
		pebbleGroupsShape{}.put(t, ctx, w)
	})
	expandViaSyncer(t, ctx, partialPath, partialID)

	t.Setenv("BATON_EXPERIMENTAL_PEBBLE_COMPACTOR", "fold")
	c, cleanup, err := synccompactor.NewCompactor(ctx, t.TempDir(), []*synccompactor.CompactableSync{
		{FilePath: basePath, SyncID: baseID},
		{FilePath: partialPath, SyncID: partialID},
	}, synccompactor.WithTmpDir(t.TempDir()), synccompactor.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	defer func() { require.NoError(t, cleanup()) }()
	compacted, err := c.Compact(ctx)
	require.NoError(t, err)
	ro, err := dotc1z.NewStore(ctx, compacted.FilePath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	require.Equal(t, string(c1zstore.EnginePebble), ro.Metadata().Engine)
	require.NoError(t, ro.Close(ctx))
	require.Positive(t, rollbackDeletes(t, ctx, compacted.FilePath, compacted.SyncID))

	outPath := filepath.Join(tmp, "out.c1z")
	require.NoError(t, runRollback(ctx, compacted.FilePath, "--out", outPath, "--sync-id", compacted.SyncID, "--replay", "--validate"))
	require.Equal(t, grantSnapshot(t, ctx, compacted.FilePath, compacted.SyncID), grantSnapshot(t, ctx, outPath, compacted.SyncID))
}
