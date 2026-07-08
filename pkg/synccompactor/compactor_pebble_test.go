package synccompactor

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	batonGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
)

// buildPebbleInput writes a minimal Pebble (v3) c1z at path with the
// given sync type and one grant per id (a fixed user→member graph),
// then ends + closes it. Returns the sync id.
func buildPebbleInput(t testing.TB, ctx context.Context, path string, st connectorstore.SyncType, grantIDs ...string) string {
	t.Helper()

	w, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)

	syncID, err := w.StartNewSync(ctx, st, "")
	require.NoError(t, err)

	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	require.NoError(t, w.PutResourceTypes(ctx, userRT, groupRT))

	group := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
		DisplayName: "Group One",
	}.Build()
	users := make([]*v2.Resource, 0, len(grantIDs))
	usersByGrantID := make(map[string]*v2.Resource, len(grantIDs))
	for _, id := range grantIDs {
		principalID := compactFixturePrincipalID(id)
		if _, ok := usersByGrantID[id]; ok {
			continue
		}
		user := v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "user", Resource: principalID}.Build(),
			DisplayName: "User " + principalID,
		}.Build()
		users = append(users, user)
		usersByGrantID[id] = user
	}
	require.NoError(t, w.PutResources(ctx, append([]*v2.Resource{group}, users...)...))

	member := v2.Entitlement_builder{
		Id:       "member",
		Resource: group,
		Purpose:  v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
	}.Build()
	require.NoError(t, w.PutEntitlements(ctx, member))

	for _, id := range grantIDs {
		user := usersByGrantID[id]
		g := v2.Grant_builder{Id: batonGrant.NewGrantID(user, member), Principal: user, Entitlement: member}.Build()
		require.NoError(t, w.PutGrants(ctx, g))
	}

	// Every input carries an asset so the asset-drop parity can be
	// asserted on the compacted output (the merge must not copy it).
	require.NoError(t, w.PutAsset(ctx, v2.AssetRef_builder{Id: "asset-1"}.Build(), "text/plain", []byte("payload")))

	require.NoError(t, w.EndSync(ctx))
	require.NoError(t, w.Close(ctx))
	return syncID
}

// countPebbleAssets returns the number of asset records stored under
// syncID in the Pebble c1z at path (read directly off the engine's
// asset keyspace).
func countPebbleAssets(t *testing.T, ctx context.Context, path string) int {
	t.Helper()
	w, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	defer w.Close(ctx)
	eng, ok := enginepkg.AsEngine(w)
	require.True(t, ok, "store at %s is not a pebble engine", path)
	it, err := eng.DB().NewIter(&pebble.IterOptions{
		LowerBound: enginepkg.AssetLowerBound(),
		UpperBound: enginepkg.AssetUpperBound(),
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
	resp, err := w.GetLatestFinishedSync(ctx, reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest_builder{}.Build())
	require.NoError(t, err)
	return resp.GetSync().GetEndedAt().AsTime()
}

func verifyCompacted(t *testing.T, ctx context.Context, path, syncID string) (int, string) {
	t.Helper()
	w, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	defer w.Close(ctx)

	require.NoError(t, w.SetCurrentSync(ctx, syncID))

	count := 0
	pageToken := ""
	for {
		resp, err := w.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
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

	syncResp, err := w.GetLatestFinishedSync(ctx, reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest_builder{}.Build())
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
	c, cleanup, err := NewCompactor(ctx, outDir, entries, WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	defer func() { _ = cleanup() }()

	out, err := c.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, out)

	count, st := verifyCompacted(t, ctx, out.FilePath, out.SyncID)
	require.Equal(t, 3, count, "union of {g-shared,g-only1} and {g-shared,g-only2} deduped = 3 grants")
	require.Equal(t, string(connectorstore.SyncTypeFull), st, "union of full + partial = full")

	store, err := dotc1z.NewStore(ctx, out.FilePath, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	defer store.Close(ctx)
	require.NoError(t, store.SetCurrentSync(ctx, out.SyncID))

	group := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
	}.Build()
	byResource, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
		Resource: group,
		PageSize: 1000,
	}.Build())
	require.NoError(t, err)
	require.Len(t, byResource.GetList(), 3, "compacted pebble output must materialize grant_by_entitlement_resource index")
}

// requirePebbleOutput asserts the compacted output at path is a Pebble store.
func requirePebbleOutput(t *testing.T, ctx context.Context, path string) {
	t.Helper()
	w, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	defer w.Close(ctx)
	_, ok := enginepkg.AsEngine(w)
	require.True(t, ok, "compacted output at %s must be a pebble engine", path)
}

// TestCompactMixedInputsAutoSelectsPebble exercises multi-engine compaction:
// when the incremental chain mixes a SQLite (v1) input and a Pebble (v3)
// input and no engine is forced, the run auto-selects Pebble, converts the
// SQLite input, and produces a single Pebble output with the deduped union.
func TestCompactMixedInputsAutoSelectsPebble(t *testing.T) {
	ctx := context.Background()
	inDir := t.TempDir()
	outDir := t.TempDir()

	sqlitePath := filepath.Join(inDir, "in-sqlite.c1z")
	pebblePath := filepath.Join(inDir, "in-pebble.c1z")
	s1 := buildSQLiteInput(t, ctx, sqlitePath, connectorstore.SyncTypeFull, "g-shared", "g-only1")
	s2 := buildPebbleInput(t, ctx, pebblePath, connectorstore.SyncTypePartial, "g-shared", "g-only2")

	// Base (entries[0]) is the SQLite input, so the conversion path covers
	// the base, not just a partial.
	entries := []*CompactableSync{{FilePath: sqlitePath, SyncID: s1}, {FilePath: pebblePath, SyncID: s2}}
	c, cleanup, err := NewCompactor(ctx, outDir, entries, WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer func() { _ = cleanup() }()

	out, err := c.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, out)

	requirePebbleOutput(t, ctx, out.FilePath)
	count, st := verifyCompacted(t, ctx, out.FilePath, out.SyncID)
	require.Equal(t, 3, count, "union of {g-shared,g-only1} and {g-shared,g-only2} deduped = 3 grants")
	require.Equal(t, string(connectorstore.SyncTypeFull), st, "union of full + partial = full")
}

// TestCompactExplicitPebbleConvertsAllSQLiteInputs pins that an explicit
// WithEngine(EnginePebble) request over all-SQLite inputs converts every
// input to Pebble and merges them, rather than rejecting the run.
func TestCompactExplicitPebbleConvertsAllSQLiteInputs(t *testing.T) {
	ctx := context.Background()
	inDir := t.TempDir()
	outDir := t.TempDir()

	p1 := filepath.Join(inDir, "in1.c1z")
	p2 := filepath.Join(inDir, "in2.c1z")
	s1 := buildSQLiteInput(t, ctx, p1, connectorstore.SyncTypeFull, "g-shared", "g-only1")
	s2 := buildSQLiteInput(t, ctx, p2, connectorstore.SyncTypePartial, "g-shared", "g-only2")

	entries := []*CompactableSync{{FilePath: p1, SyncID: s1}, {FilePath: p2, SyncID: s2}}
	c, cleanup, err := NewCompactor(ctx, outDir, entries, WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	defer func() { _ = cleanup() }()

	out, err := c.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, out)

	requirePebbleOutput(t, ctx, out.FilePath)
	count, st := verifyCompacted(t, ctx, out.FilePath, out.SyncID)
	require.Equal(t, 3, count, "union of all-sqlite inputs deduped = 3 grants")
	require.Equal(t, string(connectorstore.SyncTypeFull), st, "union of full + partial = full")
}

// TestCompactExplicitPebbleConvertsSQLitePartialWithEmptySyncID preserves the
// SQLite compactor's empty-SyncID behavior: it selects the latest finished
// compactable sync, including partial-only inputs. ToPebble's own "" fallback
// is full-only, so conversion must resolve the compactable sync first.
func TestCompactExplicitPebbleConvertsSQLitePartialWithEmptySyncID(t *testing.T) {
	ctx := context.Background()
	inDir := t.TempDir()
	outDir := t.TempDir()

	p1 := filepath.Join(inDir, "full.c1z")
	p2 := filepath.Join(inDir, "partial-only.c1z")
	s1 := buildSQLiteInput(t, ctx, p1, connectorstore.SyncTypeFull, "g-shared", "g-only1")
	_ = buildSQLiteInput(t, ctx, p2, connectorstore.SyncTypePartial, "g-shared", "g-only2")

	entries := []*CompactableSync{
		{FilePath: p1, SyncID: s1},
		{FilePath: p2, SyncID: ""},
	}
	c, cleanup, err := NewCompactor(ctx, outDir, entries, WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	defer func() { _ = cleanup() }()

	out, err := c.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, out)

	requirePebbleOutput(t, ctx, out.FilePath)
	count, st := verifyCompacted(t, ctx, out.FilePath, out.SyncID)
	require.Equal(t, 3, count, "empty SyncID partial-only sqlite input must be selected and converted")
	require.Equal(t, string(connectorstore.SyncTypeFull), st, "union of full + partial = full")
}

func TestCompactExplicitPebbleConvertsSQLiteEmptySyncIDTiebreaksBySyncID(t *testing.T) {
	ctx := context.Background()
	inDir := t.TempDir()
	outDir := t.TempDir()

	p1 := filepath.Join(inDir, "base.c1z")
	p2 := filepath.Join(inDir, "tied-source.c1z")
	s1 := buildSQLiteInput(t, ctx, p1, connectorstore.SyncTypePartial, "g-base")
	_, partialSyncID := buildSQLiteInputWithTiedCompactableSyncs(t, ctx, p2)

	entries := []*CompactableSync{
		{FilePath: p1, SyncID: s1},
		{FilePath: p2, SyncID: ""},
	}
	c, cleanup, err := NewCompactor(ctx, outDir, entries, WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble), WithSkipGrantExpansion())
	require.NoError(t, err)
	defer func() { _ = cleanup() }()

	out, err := c.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, out)

	store, err := dotc1z.NewStore(ctx, out.FilePath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer store.Close(ctx)
	require.NoError(t, store.SetCurrentSync(ctx, out.SyncID))

	resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageSize: 1000}.Build())
	require.NoError(t, err)
	grantIDs := make(map[string]bool, len(resp.GetList()))
	for _, grant := range resp.GetList() {
		grantIDs[grant.GetId()] = true
	}
	require.True(t, grantIDs[compactInputGrantID("g-partial")], "empty SyncID must select tied sync with greater sync_id %s", partialSyncID)
	require.False(t, grantIDs[compactInputGrantID("g-full")], "lower sync_id full sync must not be selected just because full is checked first")
}

// TestCompactFoldConvertsSQLitePartial pins that the in-place fold strategy
// only requires the BASE to be Pebble: with a Pebble base and a SQLite
// partial (fold forced), the SQLite partial is converted to Pebble and
// folded into the base copy, yielding the deduped union.
func TestCompactFoldConvertsSQLitePartial(t *testing.T) {
	ctx := context.Background()
	inDir := t.TempDir()
	outDir := t.TempDir()

	basePath := filepath.Join(inDir, "base-pebble.c1z")
	partialPath := filepath.Join(inDir, "partial-sqlite.c1z")
	s1 := buildPebbleInput(t, ctx, basePath, connectorstore.SyncTypeFull, "g-shared", "g-only1")
	s2 := buildSQLiteInput(t, ctx, partialPath, connectorstore.SyncTypePartial, "g-shared", "g-only2")

	entries := []*CompactableSync{{FilePath: basePath, SyncID: s1}, {FilePath: partialPath, SyncID: s2}}
	c, cleanup, err := NewCompactor(ctx, outDir, entries,
		WithTmpDir(t.TempDir()),
		WithEngine(c1zstore.EnginePebble),
		WithPebbleCompactorMode(PebbleCompactorModeFold),
	)
	require.NoError(t, err)
	defer func() { _ = cleanup() }()

	out, err := c.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, out)

	requirePebbleOutput(t, ctx, out.FilePath)
	count, st := verifyCompacted(t, ctx, out.FilePath, out.SyncID)
	require.Equal(t, 3, count, "fold of pebble base + sqlite partial deduped = 3 grants")
	require.Equal(t, string(connectorstore.SyncTypeFull), st, "union of full + partial = full")
}

// TestCompactFoldPebbleBaseMixedPartials covers the canonical multi-engine
// fold chain: a Pebble base plus two partials, one Pebble and one SQLite.
// The SQLite partial is converted to Pebble and both partials are folded
// into the base copy (fold forced), yielding the deduped union across all
// three inputs.
func TestCompactFoldPebbleBaseMixedPartials(t *testing.T) {
	ctx := context.Background()
	inDir := t.TempDir()
	outDir := t.TempDir()

	basePath := filepath.Join(inDir, "base-pebble.c1z")
	partialPebblePath := filepath.Join(inDir, "partial-pebble.c1z")
	partialSQLitePath := filepath.Join(inDir, "partial-sqlite.c1z")

	// base: {g-shared, g-base1}; pebble partial: {g-shared, g-p1};
	// sqlite partial: {g-shared, g-p2}. Deduped union = 4 grants.
	sBase := buildPebbleInput(t, ctx, basePath, connectorstore.SyncTypeFull, "g-shared", "g-base1")
	sPebble := buildPebbleInput(t, ctx, partialPebblePath, connectorstore.SyncTypePartial, "g-shared", "g-p1")
	sSQLite := buildSQLiteInput(t, ctx, partialSQLitePath, connectorstore.SyncTypePartial, "g-shared", "g-p2")

	// entries[0] is the base; the two partials follow.
	entries := []*CompactableSync{
		{FilePath: basePath, SyncID: sBase},
		{FilePath: partialPebblePath, SyncID: sPebble},
		{FilePath: partialSQLitePath, SyncID: sSQLite},
	}
	c, cleanup, err := NewCompactor(ctx, outDir, entries,
		WithTmpDir(t.TempDir()),
		WithEngine(c1zstore.EnginePebble),
		WithPebbleCompactorMode(PebbleCompactorModeFold),
	)
	require.NoError(t, err)
	defer func() { _ = cleanup() }()

	out, err := c.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, out)

	requirePebbleOutput(t, ctx, out.FilePath)
	count, st := verifyCompacted(t, ctx, out.FilePath, out.SyncID)
	require.Equal(t, 4, count, "deduped union of base + pebble partial + sqlite partial = 4 grants")
	require.Equal(t, string(connectorstore.SyncTypeFull), st, "union of full + partial + partial = full")
}

// TestCompactPebbleBaseMixedPartialsAutoMode is the same Pebble-base +
// mixed-partial chain but with the strategy auto-selected rather than fold
// forced, ensuring the overlay/kway merge path also handles a SQLite partial.
func TestCompactPebbleBaseMixedPartialsAutoMode(t *testing.T) {
	ctx := context.Background()
	inDir := t.TempDir()
	outDir := t.TempDir()

	basePath := filepath.Join(inDir, "base-pebble.c1z")
	partialPebblePath := filepath.Join(inDir, "partial-pebble.c1z")
	partialSQLitePath := filepath.Join(inDir, "partial-sqlite.c1z")

	sBase := buildPebbleInput(t, ctx, basePath, connectorstore.SyncTypeFull, "g-shared", "g-base1")
	sPebble := buildPebbleInput(t, ctx, partialPebblePath, connectorstore.SyncTypePartial, "g-shared", "g-p1")
	sSQLite := buildSQLiteInput(t, ctx, partialSQLitePath, connectorstore.SyncTypePartial, "g-shared", "g-p2")

	entries := []*CompactableSync{
		{FilePath: basePath, SyncID: sBase},
		{FilePath: partialPebblePath, SyncID: sPebble},
		{FilePath: partialSQLitePath, SyncID: sSQLite},
	}
	c, cleanup, err := NewCompactor(ctx, outDir, entries, WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer func() { _ = cleanup() }()

	out, err := c.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, out)

	requirePebbleOutput(t, ctx, out.FilePath)
	count, st := verifyCompacted(t, ctx, out.FilePath, out.SyncID)
	require.Equal(t, 4, count, "deduped union of base + pebble partial + sqlite partial = 4 grants")
	require.Equal(t, string(connectorstore.SyncTypeFull), st, "union of full + partial + partial = full")
}

// recencyStore is the Put* surface shared by the SQLite C1File and the
// engine-agnostic C1ZStore, so one data builder can seed either input.
type recencyStore interface {
	PutResourceTypes(ctx context.Context, resourceTypes ...*v2.ResourceType) error
	PutResources(ctx context.Context, resources ...*v2.Resource) error
	PutEntitlements(ctx context.Context, entitlements ...*v2.Entitlement) error
	PutGrants(ctx context.Context, grants ...*v2.Grant) error
}

// putRecencyData writes the fixed group/member graph with ONE grant on the
// shared (member@g1, user/shared-user) structural identity, whose stored
// external id is "g-" + marker — the marker identifies which input's
// version of the overlapping grant won the merge.
//
// Under the structural-identity layout, grants are keyed by
// (entitlement, principal) refs, NOT by their public id string: inputs
// overlap only when their refs match, and the merge's recency rule picks
// one whole row per identity. The stored external id rides along on the
// winning row, which is what makes it a usable winner marker — and what a
// conversion that re-stamped discovered_at would flip. (The pre-identity
// fixtures marked winners with the principal and overlapped rows by a
// shared id string; two grants sharing a public id across DIFFERENT
// principals are two distinct grants now, so that shape no longer
// overlaps at all.)
//
// includeBaseOnly adds a second, distinct identity (user/base-only) that
// no partial carries, pinning that non-overlapping base rows survive.
func putRecencyData(t testing.TB, ctx context.Context, store recencyStore, marker string, includeBaseOnly bool) {
	t.Helper()
	require.NoError(t, store.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build(),
		v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()))
	group := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "Group One"}.Build()
	shared := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "shared-user"}.Build(), DisplayName: "Shared User"}.Build()
	resources := []*v2.Resource{group, shared}
	baseOnly := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "base-only"}.Build(), DisplayName: "Base Only"}.Build()
	if includeBaseOnly {
		resources = append(resources, baseOnly)
	}
	require.NoError(t, store.PutResources(ctx, resources...))
	member := v2.Entitlement_builder{Id: "member", Resource: group, Purpose: v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT}.Build()
	require.NoError(t, store.PutEntitlements(ctx, member))
	require.NoError(t, store.PutGrants(ctx, v2.Grant_builder{Id: "g-" + marker, Principal: shared, Entitlement: member}.Build()))
	if includeBaseOnly {
		require.NoError(t, store.PutGrants(ctx, v2.Grant_builder{Id: "g-base", Principal: baseOnly, Entitlement: member}.Build()))
	}
}

// buildPebbleRecencyInput writes a Pebble (v3) c1z whose records carry
// real sync-time discovered_at stamps.
func buildPebbleRecencyInput(t testing.TB, ctx context.Context, path string, st connectorstore.SyncType, marker string, includeBaseOnly bool) string {
	t.Helper()
	w, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	syncID, err := w.StartNewSync(ctx, st, "")
	require.NoError(t, err)
	putRecencyData(t, ctx, w, marker, includeBaseOnly)
	require.NoError(t, w.EndSync(ctx))
	require.NoError(t, w.Close(ctx))
	return syncID
}

// buildSQLiteRecencyInput writes a SQLite (v1) c1z and backdates every
// record's discovered_at column to the given instant, so the input's
// true record recency is unambiguous relative to the other inputs.
func buildSQLiteRecencyInput(
	t testing.TB, ctx context.Context, path string, st connectorstore.SyncType, marker string, discoveredAt time.Time, includeBaseOnly bool,
) string {
	t.Helper()
	store, err := dotc1z.NewC1ZFile(ctx, path)
	require.NoError(t, err)
	syncID, err := store.StartNewSync(ctx, st, "")
	require.NoError(t, err)
	putRecencyData(t, ctx, store, marker, includeBaseOnly)
	db := rawSQLiteDBForTest(t, store)
	stamp := discoveredAt.Format("2006-01-02 15:04:05.999999999")
	for _, table := range []string{"v1_resource_types", "v1_resources", "v1_entitlements", "v1_grants"} {
		_, err := db.ExecContext(ctx, "UPDATE "+table+" SET discovered_at = ?", stamp) //nolint:gosec // Table names are fixed test fixtures.
		require.NoError(t, err)
	}
	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))
	return syncID
}

// winningMarker returns the stored external id of the grant on the shared
// (member@g1, user/shared-user) identity in the compacted output — i.e.
// which input's version of the overlapping grant won the recency merge.
func winningMarker(t *testing.T, ctx context.Context, out *CompactableSync) string {
	t.Helper()
	store := openCompactedPebble(t, ctx, out)
	defer store.Close(ctx)
	var winner string
	found := 0
	resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageSize: 1000}.Build())
	require.NoError(t, err)
	require.Empty(t, resp.GetNextPageToken(), "recency fixtures are single-page")
	for _, g := range resp.GetList() {
		if g.GetPrincipal().GetId().GetResource() != "shared-user" {
			continue
		}
		found++
		winner = g.GetId()
	}
	require.Equal(t, 1, found, "exactly one grant must survive on the shared identity")
	return winner
}

// TestCompactMixedInputsPreservesRecordRecency pins that converting a
// SQLite input during multi-engine compaction preserves each record's
// original discovered_at: the merge picks winners by newest
// discovered_at, so a conversion that re-stamped records with the
// conversion wall clock would make the OLD SQLite base override the
// newer Pebble partial on overlapping keys.
//
// Chain: an old SQLite full (records discovered in 2020, marker alice) +
// a fresh Pebble partial (marker bob), overlapping on one structural
// identity. The partial is newer, so bob's row must win.
func TestCompactMixedInputsPreservesRecordRecency(t *testing.T) {
	ctx := context.Background()
	inDir := t.TempDir()
	outDir := t.TempDir()

	sqlitePath := filepath.Join(inDir, "base-sqlite.c1z")
	pebblePath := filepath.Join(inDir, "partial-pebble.c1z")
	oldDiscovery := time.Date(2020, time.January, 2, 3, 4, 5, 0, time.UTC)
	s1 := buildSQLiteRecencyInput(t, ctx, sqlitePath, connectorstore.SyncTypeFull, "alice", oldDiscovery, true)
	s2 := buildPebbleRecencyInput(t, ctx, pebblePath, connectorstore.SyncTypePartial, "bob", false)

	entries := []*CompactableSync{
		{FilePath: sqlitePath, SyncID: s1},
		{FilePath: pebblePath, SyncID: s2},
	}
	c, cleanup, err := NewCompactor(ctx, outDir, entries, WithTmpDir(t.TempDir()), WithSkipGrantExpansion())
	require.NoError(t, err)
	defer func() { _ = cleanup() }()

	out, err := c.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, out)
	requirePebbleOutput(t, ctx, out.FilePath)

	require.Equal(t, "g-bob", winningMarker(t, ctx, out),
		"the newer pebble partial must win the overlapping grant; the converted sqlite base's 2020 records must not be re-stamped to conversion time")
}

// TestCompactFoldMixedPartialsPreservesRecordRecency is the fold-path
// variant: a Pebble base plus an OLD SQLite partial (2020 records) and
// a fresh Pebble partial, overlapping on one structural identity. Fold
// applies partials newest-first with strictly-newer-wins, so the fresh
// Pebble partial (bob) must win; a conversion that re-stamped the SQLite
// partial's records to conversion time would let carol override bob.
func TestCompactFoldMixedPartialsPreservesRecordRecency(t *testing.T) {
	ctx := context.Background()
	inDir := t.TempDir()
	outDir := t.TempDir()

	basePath := filepath.Join(inDir, "base-pebble.c1z")
	sqlitePartialPath := filepath.Join(inDir, "partial-sqlite.c1z")
	pebblePartialPath := filepath.Join(inDir, "partial-pebble.c1z")

	oldDiscovery := time.Date(2020, time.January, 2, 3, 4, 5, 0, time.UTC)
	sBase := buildPebbleRecencyInput(t, ctx, basePath, connectorstore.SyncTypeFull, "alice", true)
	sSQLite := buildSQLiteRecencyInput(t, ctx, sqlitePartialPath, connectorstore.SyncTypePartial, "carol", oldDiscovery, false)
	sPebble := buildPebbleRecencyInput(t, ctx, pebblePartialPath, connectorstore.SyncTypePartial, "bob", false)

	// Chain order: base, then partials oldest-first.
	entries := []*CompactableSync{
		{FilePath: basePath, SyncID: sBase},
		{FilePath: sqlitePartialPath, SyncID: sSQLite},
		{FilePath: pebblePartialPath, SyncID: sPebble},
	}
	c, cleanup, err := NewCompactor(ctx, outDir, entries,
		WithTmpDir(t.TempDir()),
		WithEngine(c1zstore.EnginePebble),
		WithPebbleCompactorMode(PebbleCompactorModeFold),
		WithSkipGrantExpansion(),
	)
	require.NoError(t, err)
	defer func() { _ = cleanup() }()

	out, err := c.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, out)
	requirePebbleOutput(t, ctx, out.FilePath)

	require.Equal(t, "g-bob", winningMarker(t, ctx, out),
		"the fresh pebble partial must win; the converted 2020 sqlite partial must not be re-stamped to conversion time and override it")
}

// TestCompactExplicitSQLiteRejectsPebbleInput pins the policy conflict: an
// explicit SQLite request with a Pebble input is rejected rather than
// silently downgrading the Pebble data.
func TestCompactExplicitSQLiteRejectsPebbleInput(t *testing.T) {
	ctx := context.Background()
	inDir := t.TempDir()
	outDir := t.TempDir()

	sqlitePath := filepath.Join(inDir, "in-sqlite.c1z")
	pebblePath := filepath.Join(inDir, "in-pebble.c1z")
	s1 := buildSQLiteInput(t, ctx, sqlitePath, connectorstore.SyncTypeFull, "g1")
	s2 := buildPebbleInput(t, ctx, pebblePath, connectorstore.SyncTypePartial, "g2")

	entries := []*CompactableSync{{FilePath: sqlitePath, SyncID: s1}, {FilePath: pebblePath, SyncID: s2}}
	c, cleanup, err := NewCompactor(ctx, outDir, entries, WithTmpDir(t.TempDir()), WithEngine(c1zstore.EngineSQLite))
	require.NoError(t, err)
	defer func() { _ = cleanup() }()

	_, err = c.Compact(ctx)
	require.ErrorIs(t, err, ErrEnginePolicyConflict)
}

// buildSQLiteInput writes a minimal SQLite (v1) c1z with one grant per
// id, for the default-engine regression.
func buildSQLiteInput(t testing.TB, ctx context.Context, path string, st connectorstore.SyncType, grantIDs ...string) string {
	t.Helper()
	store, err := dotc1z.NewC1ZFile(ctx, path)
	require.NoError(t, err)
	syncID, err := store.StartNewSync(ctx, st, "")
	require.NoError(t, err)
	putSQLiteInputData(t, ctx, store, grantIDs...)
	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))
	return syncID
}

func buildSQLiteInputWithTiedCompactableSyncs(t testing.TB, ctx context.Context, path string) (string, string) {
	t.Helper()
	store, err := dotc1z.NewC1ZFile(ctx, path)
	require.NoError(t, err)

	fullOriginalID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	putSQLiteInputData(t, ctx, store, "g-full")
	require.NoError(t, store.EndSync(ctx))

	partialOriginalID, err := store.StartNewSync(ctx, connectorstore.SyncTypePartial, fullOriginalID)
	require.NoError(t, err)
	putSQLiteInputData(t, ctx, store, "g-partial")
	require.NoError(t, store.EndSync(ctx))

	fullSyncID, partialSyncID := orderedTestSyncIDs()
	db := rawSQLiteDBForTest(t, store)
	retargetSQLiteSyncID(t, ctx, db, fullOriginalID, fullSyncID)
	retargetSQLiteSyncID(t, ctx, db, partialOriginalID, partialSyncID)
	_, err = db.ExecContext(ctx, "UPDATE v1_sync_runs SET parent_sync_id = ? WHERE sync_id = ?", fullSyncID, partialSyncID)
	require.NoError(t, err)

	tiedEndedAt := time.Date(2026, time.June, 29, 12, 0, 0, 0, time.UTC).Format("2006-01-02 15:04:05.999999999")
	_, err = db.ExecContext(ctx, "UPDATE v1_sync_runs SET ended_at = ? WHERE sync_id IN (?, ?)", tiedEndedAt, fullSyncID, partialSyncID)
	require.NoError(t, err)

	require.NoError(t, store.Close(ctx))
	return fullSyncID, partialSyncID
}

func putSQLiteInputData(t testing.TB, ctx context.Context, store *dotc1z.C1File, grantIDs ...string) {
	t.Helper()
	require.NoError(t, store.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build(),
		v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()))
	group := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "Group One"}.Build()
	users := make([]*v2.Resource, 0, len(grantIDs))
	usersByGrantID := make(map[string]*v2.Resource, len(grantIDs))
	for _, id := range grantIDs {
		principalID := compactFixturePrincipalID(id)
		if _, ok := usersByGrantID[id]; ok {
			continue
		}
		user := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: principalID}.Build(), DisplayName: "User " + principalID}.Build()
		users = append(users, user)
		usersByGrantID[id] = user
	}
	require.NoError(t, store.PutResources(ctx, append([]*v2.Resource{group}, users...)...))
	member := v2.Entitlement_builder{Id: "member", Resource: group, Purpose: v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT}.Build()
	require.NoError(t, store.PutEntitlements(ctx, member))
	for _, id := range grantIDs {
		require.NoError(t, store.PutGrants(ctx, v2.Grant_builder{Id: id, Principal: usersByGrantID[id], Entitlement: member}.Build()))
	}
}

func compactFixturePrincipalID(grantID string) string {
	if strings.Contains(grantID, "shared") {
		return "shared"
	}
	return grantID
}

// compactInputGrantID: stored external ids now round-trip verbatim, so the
// converted output emits the fixture's own grant id.
func compactInputGrantID(grantID string) string {
	return grantID
}

func overlayGrantID(entitlementID, principalID string) string {
	group := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "engineering"}.Build()}.Build()
	user := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: principalID}.Build(), ParentResourceId: group.GetId()}.Build()
	ent := v2.Entitlement_builder{Id: entitlementID, Resource: group, Purpose: v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT}.Build()
	return batonGrant.NewGrantID(user, ent)
}

func overlayEntitlementID(entitlementID string) string {
	return entitlementID
}

func orderedTestSyncIDs() (string, string) {
	a := ksuid.New().String()
	b := ksuid.New().String()
	if a > b {
		return b, a
	}
	return a, b
}

func rawSQLiteDBForTest(t testing.TB, store *dotc1z.C1File) *sql.DB {
	t.Helper()
	field := reflect.ValueOf(store).Elem().FieldByName("rawDb")
	require.True(t, field.IsValid())
	require.False(t, field.IsNil())
	return (*sql.DB)(unsafe.Pointer(field.Pointer()))
}

func retargetSQLiteSyncID(t testing.TB, ctx context.Context, db *sql.DB, oldID, newID string) {
	t.Helper()
	for _, table := range []string{"v1_sync_runs", "v1_resource_types", "v1_resources", "v1_entitlements", "v1_grants"} {
		_, err := db.ExecContext(ctx, "UPDATE "+table+" SET sync_id = ? WHERE sync_id = ?", newID, oldID) //nolint:gosec // Table names are fixed test fixtures.
		require.NoError(t, err)
	}
	_, err := db.ExecContext(ctx, "UPDATE v1_sync_runs SET parent_sync_id = ? WHERE parent_sync_id = ?", newID, oldID)
	require.NoError(t, err)
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
	require.Positive(t, countPebbleAssets(t, ctx, p1), "input must carry an asset")

	c, cleanup, err := NewCompactor(ctx, t.TempDir(), []*CompactableSync{{FilePath: p1, SyncID: s1}, {FilePath: p2, SyncID: s2}}, WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	defer func() { _ = cleanup() }()
	out, err := c.Compact(ctx)
	require.NoError(t, err)

	require.Equal(t, 0, countPebbleAssets(t, ctx, out.FilePath), "compacted pebble sync must contain zero assets (parity with sqlite)")
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

	c, cleanup, err := NewCompactor(ctx, t.TempDir(), []*CompactableSync{{FilePath: p1, SyncID: s1}, {FilePath: p2, SyncID: s2}}, WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble))
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

type overlayInputSpec struct {
	syncType connectorstore.SyncType
	suffix   string
	grants   []overlayGrantSpec
}

type overlayGrantSpec struct {
	id              string
	principalID     string
	entitlementID   string
	needsExpansion  bool
	skipEntitlement bool
}

func buildOverlayInput(t testing.TB, ctx context.Context, path string, spec overlayInputSpec) string {
	t.Helper()
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	syncID, err := store.StartNewSync(ctx, spec.syncType, "")
	require.NoError(t, err)

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	require.NoError(t, store.PutResourceTypes(ctx, groupRT, userRT))

	group := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "group", Resource: "engineering"}.Build(),
		DisplayName: "Engineering",
	}.Build()
	alice := v2.Resource_builder{
		Id:               v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
		DisplayName:      "Alice " + spec.suffix,
		ParentResourceId: group.GetId(),
	}.Build()
	bob := v2.Resource_builder{
		Id:               v2.ResourceId_builder{ResourceType: "user", Resource: "bob"}.Build(),
		DisplayName:      "Bob " + spec.suffix,
		ParentResourceId: group.GetId(),
	}.Build()
	require.NoError(t, store.PutResources(ctx, group, alice, bob))

	entitlementsByID := map[string]*v2.Entitlement{}
	for _, g := range spec.grants {
		if g.skipEntitlement {
			continue
		}
		if _, ok := entitlementsByID[g.entitlementID]; ok {
			continue
		}
		entitlementsByID[g.entitlementID] = v2.Entitlement_builder{
			Id:       g.entitlementID,
			Resource: group,
			Purpose:  v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
		}.Build()
	}
	for _, ent := range entitlementsByID {
		require.NoError(t, store.PutEntitlements(ctx, ent))
	}

	for _, g := range spec.grants {
		principalID := alice
		if g.principalID == "bob" {
			principalID = bob
		}
		// SDK-shaped grant id (the concat) so bare-id GetGrant queries in
		// these tests resolve; spec ids only label expansion assertions.
		grant := v2.Grant_builder{
			Id:          overlayGrantID(g.entitlementID, g.principalID),
			Principal:   principalID,
			Entitlement: v2.Entitlement_builder{Id: g.entitlementID, Resource: group, Purpose: v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT}.Build(),
		}.Build()
		if g.needsExpansion {
			grant.SetAnnotations([]*anypb.Any{mustAny(t, v2.GrantExpandable_builder{
				EntitlementIds: []string{"member"},
				Shallow:        true,
			}.Build())})
		}
		require.NoError(t, store.PutGrants(ctx, grant))
	}
	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))
	return syncID
}

func mustAny(t testing.TB, msg proto.Message) *anypb.Any {
	t.Helper()
	out, err := anypb.New(msg)
	require.NoError(t, err)
	return out
}

func compactPebbleOverlay(t testing.TB, ctx context.Context, entries []*CompactableSync, extra ...Option) *CompactableSync {
	t.Helper()
	opts := append([]Option{
		WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble), WithSkipGrantExpansion(),
		WithPebbleCompactorMode(PebbleCompactorModeOverlay),
	}, extra...)
	c, cleanup, err := NewCompactor(ctx, t.TempDir(), entries, opts...)
	require.NoError(t, err)
	defer func() { require.NoError(t, cleanup()) }()
	out, err := c.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, out)
	return out
}

func openCompactedPebble(t testing.TB, ctx context.Context, out *CompactableSync) c1zstore.Store {
	t.Helper()
	store, err := dotc1z.NewStore(ctx, out.FilePath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	require.NoError(t, store.SetCurrentSync(ctx, out.SyncID))
	return store
}

func TestCompactPebbleOverlayMaterializesAllQueryIndexes(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.c1z")
	incPath := filepath.Join(dir, "inc.c1z")
	baseSync := buildOverlayInput(t, ctx, basePath, overlayInputSpec{
		syncType: connectorstore.SyncTypeFull,
		suffix:   "base",
		grants: []overlayGrantSpec{
			{id: "g-alice-member", principalID: "alice", entitlementID: "member"},
			{id: "g-bob-admin", principalID: "bob", entitlementID: "admin", needsExpansion: true},
		},
	})
	incSync := buildOverlayInput(t, ctx, incPath, overlayInputSpec{
		syncType: connectorstore.SyncTypePartial,
		suffix:   "incremental",
		grants: []overlayGrantSpec{
			{id: "g-inc-bob-member", principalID: "bob", entitlementID: "member"},
		},
	})
	out := compactPebbleOverlay(t, ctx, []*CompactableSync{{FilePath: basePath, SyncID: baseSync}, {FilePath: incPath, SyncID: incSync}})
	store := openCompactedPebble(t, ctx, out)
	defer store.Close(ctx)

	group := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "engineering"}.Build()}.Build()
	alice := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(), ParentResourceId: group.GetId()}.Build()

	byResource, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{Resource: group, PageSize: 100}.Build())
	require.NoError(t, err)
	requireGrantIDs(t, byResource.GetList(), overlayGrantID("member", "alice"), overlayGrantID("admin", "bob"), overlayGrantID("member", "bob"))

	member := v2.Entitlement_builder{Id: "member", Resource: group}.Build()
	byEntitlement, err := store.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement: member,
		PageSize:    100,
	}.Build())
	require.NoError(t, err)
	requireGrantIDs(t, byEntitlement.GetList(), overlayGrantID("member", "alice"), overlayGrantID("member", "bob"))

	byPrincipalAndEntitlement, err := store.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement: member,
		PrincipalId: alice.GetId(),
		PageSize:    100,
	}.Build())
	require.NoError(t, err)
	requireGrantIDs(t, byPrincipalAndEntitlement.GetList(), overlayGrantID("member", "alice"))

	users, err := store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{ParentResourceId: group.GetId(), PageSize: 100}.Build())
	require.NoError(t, err)
	requireResourceIDs(t, users.GetList(), "alice", "bob")

	ents, err := store.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{Resource: group, PageSize: 100}.Build())
	require.NoError(t, err)
	requireEntitlementIDs(t, ents.GetList(), overlayEntitlementID("admin"), overlayEntitlementID("member"))

	pending, next, err := store.Grants().PendingExpansionPage(ctx, "")
	require.NoError(t, err)
	require.Empty(t, next)
	require.Len(t, pending, 1)
	require.Equal(t, overlayGrantID("admin", "bob"), pending[0].GrantExternalID)
}

func TestCompactPebbleOverlayFirstSeenWinsForOverlappingGrant(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.c1z")
	incPath := filepath.Join(dir, "inc.c1z")
	baseSync := buildOverlayInput(t, ctx, basePath, overlayInputSpec{
		syncType: connectorstore.SyncTypeFull,
		suffix:   "base",
		grants:   []overlayGrantSpec{{id: "shared", principalID: "alice", entitlementID: "member"}},
	})
	incSync := buildOverlayInput(t, ctx, incPath, overlayInputSpec{
		syncType: connectorstore.SyncTypePartial,
		suffix:   "incremental",
		grants:   []overlayGrantSpec{{id: "shared", principalID: "bob", entitlementID: "member"}},
	})
	out := compactPebbleOverlay(t, ctx, []*CompactableSync{{FilePath: basePath, SyncID: baseSync}, {FilePath: incPath, SyncID: incSync}})
	store := openCompactedPebble(t, ctx, out)
	defer store.Close(ctx)
	grant, err := store.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{GrantId: overlayGrantID("member", "bob")}.Build())
	require.NoError(t, err)
	require.Equal(t, "bob", grant.GetGrant().GetPrincipal().GetId().GetResource(), "newest/applied source must win overlay duplicate")
}

func TestCompactPebbleOverlayFallsBackToKWayAboveSeenLimit(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.c1z")
	incPath := filepath.Join(dir, "inc.c1z")
	baseSync := buildOverlayInput(t, ctx, basePath, overlayInputSpec{
		syncType: connectorstore.SyncTypeFull,
		suffix:   "base",
		grants:   []overlayGrantSpec{{id: "shared", principalID: "alice", entitlementID: "member"}},
	})
	incSync := buildOverlayInput(t, ctx, incPath, overlayInputSpec{
		syncType: connectorstore.SyncTypePartial,
		suffix:   "incremental",
		grants:   []overlayGrantSpec{{id: "shared", principalID: "bob", entitlementID: "member"}},
	})
	out := compactPebbleOverlay(t, ctx, []*CompactableSync{{FilePath: basePath, SyncID: baseSync}, {FilePath: incPath, SyncID: incSync}}, WithOverlaySeenKeyLimit(1))
	store := openCompactedPebble(t, ctx, out)
	defer store.Close(ctx)
	grant, err := store.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{GrantId: overlayGrantID("member", "bob")}.Build())
	require.NoError(t, err)
	require.Equal(t, "bob", grant.GetGrant().GetPrincipal().GetId().GetResource(), "fallback must preserve compaction semantics")
}

func TestCompactPebbleOverlayFallsBackToKWayWhenMissingStatsPreflightExceedsLimit(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.c1z")
	incPath := filepath.Join(dir, "inc.c1z")
	baseSync := buildOverlayInput(t, ctx, basePath, overlayInputSpec{
		syncType: connectorstore.SyncTypeFull,
		suffix:   "base",
		grants:   []overlayGrantSpec{{id: "shared", principalID: "alice", entitlementID: "member"}},
	})
	incSync := buildOverlayInput(t, ctx, incPath, overlayInputSpec{
		syncType: connectorstore.SyncTypePartial,
		suffix:   "incremental",
		grants:   []overlayGrantSpec{{id: "shared", principalID: "bob", entitlementID: "member"}},
	})
	deletePebbleStatsSidecar(t, ctx, basePath)
	deletePebbleStatsSidecar(t, ctx, incPath)

	out := compactPebbleOverlay(t, ctx, []*CompactableSync{{FilePath: basePath, SyncID: baseSync}, {FilePath: incPath, SyncID: incSync}}, WithOverlaySeenKeyLimit(1))
	store := openCompactedPebble(t, ctx, out)
	defer store.Close(ctx)
	grant, err := store.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{GrantId: overlayGrantID("member", "bob")}.Build())
	require.NoError(t, err)
	require.Equal(t, "bob", grant.GetGrant().GetPrincipal().GetId().GetResource(), "missing cached stats preflight fallback must preserve compaction semantics")
}

func TestCompactPebbleOverlayMissingStatsPreflightAllowsSmallInputs(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.c1z")
	incPath := filepath.Join(dir, "inc.c1z")
	baseSync := buildOverlayInput(t, ctx, basePath, overlayInputSpec{
		syncType: connectorstore.SyncTypeFull,
		suffix:   "base",
		grants:   []overlayGrantSpec{{id: "base-only", principalID: "alice", entitlementID: "member"}},
	})
	incSync := buildOverlayInput(t, ctx, incPath, overlayInputSpec{
		syncType: connectorstore.SyncTypePartial,
		suffix:   "incremental",
		grants:   nil,
	})
	deletePebbleStatsSidecar(t, ctx, basePath)
	deletePebbleStatsSidecar(t, ctx, incPath)

	out := compactPebbleOverlay(t, ctx, []*CompactableSync{{FilePath: basePath, SyncID: baseSync}, {FilePath: incPath, SyncID: incSync}}, WithOverlaySeenKeyLimit(100))
	store := openCompactedPebble(t, ctx, out)
	defer store.Close(ctx)
	resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageSize: 100}.Build())
	require.NoError(t, err)
	requireGrantIDs(t, resp.GetList(), overlayGrantID("member", "alice"))
}

func TestCompactPebbleOverlayWholeBaseBucketFastPathIndexesBaseGrants(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.c1z")
	incPath := filepath.Join(dir, "inc.c1z")
	baseSync := buildOverlayInput(t, ctx, basePath, overlayInputSpec{
		syncType: connectorstore.SyncTypeFull,
		suffix:   "base",
		grants: []overlayGrantSpec{
			{id: "base-alice", principalID: "alice", entitlementID: "member"},
			{id: "base-bob", principalID: "bob", entitlementID: "admin", needsExpansion: true},
		},
	})
	incSync := buildOverlayInput(t, ctx, incPath, overlayInputSpec{
		syncType: connectorstore.SyncTypePartial,
		suffix:   "incremental-no-grants",
		grants:   nil,
	})
	out := compactPebbleOverlay(t, ctx, []*CompactableSync{{FilePath: basePath, SyncID: baseSync}, {FilePath: incPath, SyncID: incSync}})
	store := openCompactedPebble(t, ctx, out)
	defer store.Close(ctx)
	group := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "engineering"}.Build()}.Build()
	byResource, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{Resource: group, PageSize: 100}.Build())
	require.NoError(t, err)
	requireGrantIDs(t, byResource.GetList(), overlayGrantID("member", "alice"), overlayGrantID("admin", "bob"))
	pending, _, err := store.Grants().PendingExpansionPage(ctx, "")
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, overlayGrantID("admin", "bob"), pending[0].GrantExternalID)
}

// TestCompactPebbleStatsSidecarMatchesRecompute pins that compaction
// persists merge-accumulated stats that exactly match a full recompute
// of the compacted output, for both the K-way path and the overlay
// path (each forced explicitly via WithPebbleCompactorMode).
func TestCompactPebbleStatsSidecarMatchesRecompute(t *testing.T) {
	for _, mode := range []PebbleCompactorMode{PebbleCompactorModeKWay, PebbleCompactorModeOverlay} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			inDir := t.TempDir()
			p1 := filepath.Join(inDir, "in1.c1z")
			p2 := filepath.Join(inDir, "in2.c1z")
			s1 := buildPebbleInput(t, ctx, p1, connectorstore.SyncTypePartial, "g-shared", "g-only1")
			s2 := buildPebbleInput(t, ctx, p2, connectorstore.SyncTypePartial, "g-shared", "g-only2")

			c, cleanup, err := NewCompactor(ctx, t.TempDir(),
				[]*CompactableSync{{FilePath: p1, SyncID: s1}, {FilePath: p2, SyncID: s2}},
				WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble), WithSkipGrantExpansion(),
				WithPebbleCompactorMode(mode))
			require.NoError(t, err)
			defer func() { _ = cleanup() }()
			out, err := c.Compact(ctx)
			require.NoError(t, err)
			require.NotNil(t, out)

			w, err := dotc1z.NewStore(ctx, out.FilePath, dotc1z.WithTmpDir(t.TempDir()))
			require.NoError(t, err)
			defer w.Close(ctx)
			eng, ok := enginepkg.AsEngine(w)
			require.True(t, ok)

			stored, err := enginepkg.ReadSyncStatsRecord(ctx, eng, out.SyncID)
			require.NoError(t, err)
			require.NotNil(t, stored, "compaction must persist a stats sidecar")

			// Sanity-pin the expected fixture counts so a dual-sided
			// systematic error can't slip through the parity check.
			require.Equal(t, int64(2), stored.GetResourceTypes())
			require.Equal(t, int64(4), stored.GetResources())
			require.Equal(t, int64(1), stored.GetEntitlements())
			require.Equal(t, int64(3), stored.GetGrants(), "union of {g-shared,g-only1} and {g-shared,g-only2}")
			require.Equal(t, int64(0), stored.GetAssets(), "compaction drops assets")
			require.Equal(t, map[string]int64{"group": 1, "user": 3}, stored.GetResourcesByResourceType())
			require.Equal(t, map[string]int64{"group": 3}, stored.GetGrantsByEntitlementResourceType())

			require.NoError(t, eng.PersistSyncStats(ctx, out.SyncID))
			recomputed, err := enginepkg.ReadSyncStatsRecord(ctx, eng, out.SyncID)
			require.NoError(t, err)
			require.NotNil(t, recomputed)
			require.Equal(t, recomputed.GetResourceTypes(), stored.GetResourceTypes())
			require.Equal(t, recomputed.GetResources(), stored.GetResources())
			require.Equal(t, recomputed.GetEntitlements(), stored.GetEntitlements())
			require.Equal(t, recomputed.GetGrants(), stored.GetGrants())
			require.Equal(t, recomputed.GetAssets(), stored.GetAssets())
			require.Equal(t, recomputed.GetResourcesByResourceType(), stored.GetResourcesByResourceType())
			require.Equal(t, recomputed.GetGrantsByEntitlementResourceType(), stored.GetGrantsByEntitlementResourceType())
		})
	}
}

func requireGrantIDs(t testing.TB, grants []*v2.Grant, want ...string) {
	t.Helper()
	got := make([]string, 0, len(grants))
	for _, grant := range grants {
		got = append(got, grant.GetId())
	}
	require.ElementsMatch(t, want, got)
}

func requireResourceIDs(t testing.TB, resources []*v2.Resource, want ...string) {
	t.Helper()
	got := make([]string, 0, len(resources))
	for _, resource := range resources {
		got = append(got, resource.GetId().GetResource())
	}
	require.ElementsMatch(t, want, got)
}

func requireEntitlementIDs(t testing.TB, entitlements []*v2.Entitlement, want ...string) {
	t.Helper()
	got := make([]string, 0, len(entitlements))
	for _, entitlement := range entitlements {
		got = append(got, entitlement.GetId())
	}
	require.ElementsMatch(t, want, got)
}

func deletePebbleStatsSidecar(t testing.TB, ctx context.Context, path string) {
	t.Helper()
	w, err := dotc1z.NewStore(ctx, path, dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	eng, ok := enginepkg.AsEngine(w)
	require.True(t, ok)
	iter, err := eng.DB().NewIter(&pebble.IterOptions{
		LowerBound: enginepkg.SyncStatsSidecarLowerBound(),
		UpperBound: enginepkg.SyncStatsSidecarUpperBound(),
	})
	require.NoError(t, err)
	var keys [][]byte
	for iter.First(); iter.Valid(); iter.Next() {
		keys = append(keys, append([]byte(nil), iter.Key()...))
	}
	require.NoError(t, iter.Error())
	require.NoError(t, iter.Close())
	for _, key := range keys {
		require.NoError(t, eng.DB().Delete(key, nil))
	}
	require.NoError(t, w.Close(ctx))
}
