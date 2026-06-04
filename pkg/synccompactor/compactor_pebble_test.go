package synccompactor

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cockroachdb/pebble/v2"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// buildPebbleInput writes a minimal Pebble (v3) c1z at path with the
// given sync type and one grant per id (a fixed user→member graph),
// then ends + closes it. Returns the sync id.
func buildPebbleInput(t *testing.T, ctx context.Context, path string, st connectorstore.SyncType, grantIDs ...string) string {
	t.Helper()
	require.NoError(t, ensurePebbleRegistered())

	w, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(dotc1z.EnginePebble))
	require.NoError(t, err)
	store, ok := w.(dotc1z.C1ZStore)
	require.True(t, ok)

	syncID, err := store.StartNewSync(ctx, st, "")
	require.NoError(t, err)

	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	require.NoError(t, store.PutResourceTypes(ctx, userRT, groupRT))

	group := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
		DisplayName: "Group One",
	}.Build()
	user := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(),
		DisplayName: "User One",
	}.Build()
	require.NoError(t, store.PutResources(ctx, group, user))

	member := v2.Entitlement_builder{
		Id:       "member",
		Resource: group,
		Purpose:  v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
	}.Build()
	require.NoError(t, store.PutEntitlements(ctx, member))

	for _, id := range grantIDs {
		g := v2.Grant_builder{Id: id, Principal: user, Entitlement: member}.Build()
		require.NoError(t, store.PutGrants(ctx, g))
	}

	// Every input carries an asset so the asset-drop parity can be
	// asserted on the compacted output (the merge must not copy it).
	require.NoError(t, store.PutAsset(ctx, v2.AssetRef_builder{Id: "asset-1"}.Build(), "text/plain", []byte("payload")))

	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))
	return syncID
}

// countPebbleAssets returns the number of asset records stored under
// syncID in the Pebble c1z at path (read directly off the engine's
// asset keyspace).
func countPebbleAssets(t *testing.T, ctx context.Context, path, syncID string) int {
	t.Helper()
	w, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	defer w.Close(ctx)
	eng, ok := enginepkg.AsEngine(w)
	require.True(t, ok, "store at %s is not a pebble engine", path)
	sb, err := codec.EncodeSyncID(syncID)
	require.NoError(t, err)
	it, err := eng.DB().NewIter(&pebble.IterOptions{
		LowerBound: enginepkg.AssetSyncLowerBound(sb),
		UpperBound: enginepkg.AssetSyncUpperBound(sb),
	})
	require.NoError(t, err)
	defer it.Close()
	n := 0
	for it.First(); it.Valid(); it.Next() {
		n++
	}
	require.NoError(t, it.Error())
	return n
}

// latestEndedAt returns the ended_at of the latest finished sync in the
// c1z at path.
func latestEndedAt(t *testing.T, ctx context.Context, path string) time.Time {
	t.Helper()
	w, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	defer w.Close(ctx)
	store, ok := w.(dotc1z.C1ZStore)
	require.True(t, ok)
	resp, err := store.GetLatestFinishedSync(ctx, reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest_builder{}.Build())
	require.NoError(t, err)
	return resp.GetSync().GetEndedAt().AsTime()
}

func verifyCompacted(t *testing.T, ctx context.Context, path, syncID string) (int, string) {
	t.Helper()
	w, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	store, ok := w.(dotc1z.C1ZStore)
	require.True(t, ok)
	defer store.Close(ctx)

	require.NoError(t, store.SetCurrentSync(ctx, syncID))

	count := 0
	pageToken := ""
	for {
		resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
			PageSize:  1000,
			PageToken: pageToken,
		}.Build())
		require.NoError(t, err)
		count += len(resp.GetList())
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}

	syncResp, err := store.GetLatestFinishedSync(ctx, reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest_builder{}.Build())
	require.NoError(t, err)
	return count, syncResp.GetSync().GetSyncType()
}

// TestCompactPebbleEndToEnd is the wired-in-option integration: two v3
// Pebble inputs compacted with WithEngine(EnginePebble) yield ONE
// merged Pebble sync whose grants are the union (overlapping ids
// deduped), with the union sync type.
func TestCompactPebbleEndToEnd(t *testing.T) {
	ctx := context.Background()
	inDir := t.TempDir()
	outDir := t.TempDir()

	p1 := filepath.Join(inDir, "in1.c1z")
	p2 := filepath.Join(inDir, "in2.c1z")
	s1 := buildPebbleInput(t, ctx, p1, connectorstore.SyncTypeFull, "g-shared", "g-only1")
	s2 := buildPebbleInput(t, ctx, p2, connectorstore.SyncTypePartial, "g-shared", "g-only2")

	entries := []*CompactableSync{{FilePath: p1, SyncID: s1}, {FilePath: p2, SyncID: s2}}
	c, cleanup, err := NewCompactor(ctx, outDir, entries, WithTmpDir(t.TempDir()), WithEngine(dotc1z.EnginePebble))
	require.NoError(t, err)
	defer func() { _ = cleanup() }()

	out, err := c.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, out)

	count, st := verifyCompacted(t, ctx, out.FilePath, out.SyncID)
	require.Equal(t, 3, count, "union of {g-shared,g-only1} and {g-shared,g-only2} deduped = 3 grants")
	require.Equal(t, string(connectorstore.SyncTypeFull), st, "union of full + partial = full")
}

// buildSQLiteInput writes a minimal SQLite (v1) c1z with one grant per
// id, for the default-engine regression.
func buildSQLiteInput(t *testing.T, ctx context.Context, path string, st connectorstore.SyncType, grantIDs ...string) string {
	t.Helper()
	store, err := dotc1z.NewC1ZFile(ctx, path)
	require.NoError(t, err)
	syncID, err := store.StartNewSync(ctx, st, "")
	require.NoError(t, err)
	require.NoError(t, store.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build(),
		v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()))
	group := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "Group One"}.Build()
	user := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "User One"}.Build()
	require.NoError(t, store.PutResources(ctx, group, user))
	member := v2.Entitlement_builder{Id: "member", Resource: group, Purpose: v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT}.Build()
	require.NoError(t, store.PutEntitlements(ctx, member))
	for _, id := range grantIDs {
		require.NoError(t, store.PutGrants(ctx, v2.Grant_builder{Id: id, Principal: user, Entitlement: member}.Build()))
	}
	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))
	return syncID
}

// TestCompactPebbleDropsAssets pins the asset-drop parity: the SQLite
// path folds only resource_types/resources/entitlements/grants and
// drops assets, so the Pebble path must too — the compacted sync has
// zero assets even though every input had one.
func TestCompactPebbleDropsAssets(t *testing.T) {
	ctx := context.Background()
	inDir := t.TempDir()
	p1 := filepath.Join(inDir, "in1.c1z")
	p2 := filepath.Join(inDir, "in2.c1z")
	s1 := buildPebbleInput(t, ctx, p1, connectorstore.SyncTypeFull, "g1")
	s2 := buildPebbleInput(t, ctx, p2, connectorstore.SyncTypePartial, "g2")

	// Sanity: the inputs actually carry assets, so a zero on the output
	// proves a drop, not an empty fixture.
	require.Positive(t, countPebbleAssets(t, ctx, p1, s1), "input must carry an asset")

	c, cleanup, err := NewCompactor(ctx, t.TempDir(), []*CompactableSync{{FilePath: p1, SyncID: s1}, {FilePath: p2, SyncID: s2}}, WithTmpDir(t.TempDir()), WithEngine(dotc1z.EnginePebble))
	require.NoError(t, err)
	defer func() { _ = cleanup() }()
	out, err := c.Compact(ctx)
	require.NoError(t, err)

	require.Equal(t, 0, countPebbleAssets(t, ctx, out.FilePath, out.SyncID), "compacted pebble sync must contain zero assets (parity with sqlite)")
}

// TestCompactPebbleEndedAtIsMaxOfInputs pins the sync_run ended_at
// parity: the compacted sync's ended_at is the max across the inputs.
func TestCompactPebbleEndedAtIsMaxOfInputs(t *testing.T) {
	ctx := context.Background()
	inDir := t.TempDir()
	p1 := filepath.Join(inDir, "in1.c1z")
	p2 := filepath.Join(inDir, "in2.c1z")
	// Both partial so the union type is partial and grant expansion is
	// skipped — expansion re-ends the sync with a fresh ended_at, so
	// the "ended_at == max(inputs)" invariant is asserted on the
	// no-expansion path (the same path that holds it for sqlite).
	s1 := buildPebbleInput(t, ctx, p1, connectorstore.SyncTypePartial, "g1")
	s2 := buildPebbleInput(t, ctx, p2, connectorstore.SyncTypePartial, "g2")

	e1 := latestEndedAt(t, ctx, p1)
	e2 := latestEndedAt(t, ctx, p2)
	want := e1
	if e2.After(want) {
		want = e2
	}

	c, cleanup, err := NewCompactor(ctx, t.TempDir(), []*CompactableSync{{FilePath: p1, SyncID: s1}, {FilePath: p2, SyncID: s2}}, WithTmpDir(t.TempDir()), WithEngine(dotc1z.EnginePebble))
	require.NoError(t, err)
	defer func() { _ = cleanup() }()
	out, err := c.Compact(ctx)
	require.NoError(t, err)

	require.True(t, latestEndedAt(t, ctx, out.FilePath).Equal(want), "compacted ended_at must equal max(inputs' ended_at)")
}

// TestCompactSQLiteDefaultUnchanged is the default-engine regression:
// with WithEngine unset, compaction still produces a SQLite (v1) c1z
// (openable by the SQLite-only NewC1ZFile) with the unioned grants.
func TestCompactSQLiteDefaultUnchanged(t *testing.T) {
	ctx := context.Background()
	inDir := t.TempDir()
	p1 := filepath.Join(inDir, "in1.c1z")
	p2 := filepath.Join(inDir, "in2.c1z")
	s1 := buildSQLiteInput(t, ctx, p1, connectorstore.SyncTypeFull, "g-shared", "g-only1")
	s2 := buildSQLiteInput(t, ctx, p2, connectorstore.SyncTypePartial, "g-shared", "g-only2")

	c, cleanup, err := NewCompactor(ctx, t.TempDir(), []*CompactableSync{{FilePath: p1, SyncID: s1}, {FilePath: p2, SyncID: s2}}, WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer func() { _ = cleanup() }()
	out, err := c.Compact(ctx)
	require.NoError(t, err)

	// The default output is SQLite: NewC1ZFile (the SQLite-only
	// constructor) must open it. A pebble v3 artifact would fail here.
	store, err := dotc1z.NewC1ZFile(ctx, out.FilePath, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	defer store.Close(ctx)
	require.NoError(t, store.SetCurrentSync(ctx, out.SyncID))
	resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageSize: 1000}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 3, "default sqlite compaction must union grants to 3")
}
