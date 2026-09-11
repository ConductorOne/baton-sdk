package synccompactor

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	batonGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
)

// foldLedgerPageID is the action the ledgered base fixture records.
var foldLedgerPageID = c1zstore.LedgerActionIdentity{
	Op:             "SyncGrants",
	ResourceTypeID: "group",
	ResourceID:     "g1",
}

const (
	foldLedgerRunID = "base-run"
	// foldLedgerCounterKey is distinctive so a non-zero reading on the
	// fold output can only have come from the base's bucket.
	foldLedgerCounterKey = "fold-ledger-fixture-grants"
)

// buildLedgeredPebbleInput writes a Pebble c1z whose grants were
// ingested through BeginPage, so the saved file carries ledger rows and
// a counter bucket alongside the records. Sealing goes through
// EndSyncWithStats because a ledgered sync refuses EndSync
// (ErrLedgeredSyncNeedsStats). Returns the sync id.
func buildLedgeredPebbleInput(t *testing.T, ctx context.Context, path string, st connectorstore.SyncType, grantIDs ...string) string {
	t.Helper()

	w, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	ledger, ok := w.(c1zstore.PageLedgerStore)
	require.True(t, ok, "the pebble store implements the page ledger")
	stats, ok := w.(c1zstore.SyncStatsStore)
	require.True(t, ok, "the pebble store implements the stats side of the ledger")

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
		if _, ok := usersByGrantID[id]; ok {
			continue
		}
		principalID := compactFixturePrincipalID(id)
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

	// The grants ride a page, so the commit stages the ledger row and the
	// counter bucket in the same batch as the records.
	page := ledger.BeginPage()
	for _, id := range grantIDs {
		user := usersByGrantID[id]
		g := v2.Grant_builder{Id: batonGrant.NewGrantID(user, member), Principal: user, Entitlement: member}.Build()
		require.NoError(t, page.PutGrants(ctx, g))
	}
	require.NoError(t, page.SetCounterBucket(foldLedgerRunID, 0, c1zstore.LedgerCounters{
		Counters: map[string]uint64{foldLedgerCounterKey: uint64(len(grantIDs))},
	}))
	require.NoError(t, page.Commit(ctx, foldLedgerPageID, nil))

	// Prove the fixture is what the test needs before it is sealed: an
	// absent row here would make every assertion below vacuous.
	_, found, err := ledger.GetLedgerRow(ctx, foldLedgerPageID)
	require.NoError(t, err)
	require.True(t, found, "fixture did not record a ledger row")

	require.NoError(t, w.PutAsset(ctx, v2.AssetRef_builder{Id: "asset-1"}.Build(), "text/plain", []byte("payload")))
	require.NoError(t, stats.EndSyncWithStats(ctx, c1zstore.SyncStats{}))
	require.NoError(t, w.Close(ctx))
	return syncID
}

// TestCompactPebbleFoldDropsInheritedBaseLedger covers the in-place
// fold's inheritance of a ledgered base. copyFileForFold byte-copies the
// base, and encodeLedgerKey carries no sync id (one ledger per file), so
// the base's rows, facts, and counter buckets land in the output while
// the fold renames the sync-run record to a fresh id. They then describe
// an ingest the output no longer claims.
//
// Two things go wrong if they stay: ledgerActive reports true, so a
// later rebind that writes a checkpoint token fails with
// ErrLedgeredSyncWritesNoToken; and LedgerCounters reports the base
// ingest's totals as this artifact's.
func TestCompactPebbleFoldDropsInheritedBaseLedger(t *testing.T) {
	ctx := context.Background()
	inDir := t.TempDir()

	basePath := filepath.Join(inDir, "base.c1z")
	partialPath := filepath.Join(inDir, "partial.c1z")
	baseSyncID := buildLedgeredPebbleInput(t, ctx, basePath, connectorstore.SyncTypeFull, "g-shared", "g-base-only")
	partialSyncID := buildPebbleInput(t, ctx, partialPath, connectorstore.SyncTypePartial, "g-shared", "g-partial-only")
	markFoldInputVerified(t, ctx, basePath, baseSyncID)

	// The base really does carry a ledger into the fold; without this the
	// drop below could pass against a file that never had one.
	requireLedgerRow(t, ctx, basePath, true, "the fold's base input")
	requireLedgerCounter(t, ctx, basePath, 2, "the fold's base input")

	t.Setenv("BATON_EXPERIMENTAL_PEBBLE_COMPACTOR", "fold")
	c, cleanup, err := NewCompactor(ctx, t.TempDir(), []*CompactableSync{
		{FilePath: basePath, SyncID: baseSyncID},
		{FilePath: partialPath, SyncID: partialSyncID},
	}, WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble), WithSkipGrantExpansion())
	require.NoError(t, err)
	defer func() { require.NoError(t, cleanup()) }()

	out, err := c.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, out)
	require.NotEqual(t, baseSyncID, out.SyncID)

	// The records survive the fold; only the ledger describing how they
	// were collected is gone.
	count, _ := verifyCompacted(t, ctx, out.FilePath, out.SyncID)
	require.Equal(t, 3, count, "the fold must keep the union of grants")

	requireLedgerRow(t, ctx, out.FilePath, false, "the fold output")

	store, err := dotc1z.NewStore(ctx, out.FilePath, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()
	ledger, ok := store.(c1zstore.PageLedgerStore)
	require.True(t, ok)

	counters, err := ledger.LedgerCounters(ctx)
	require.NoError(t, err)
	require.Zero(t, counters.Counters[foldLedgerCounterKey],
		"the fold output reports the base ingest's counter totals as its own")

	// The consequence that bites in production: a rebound sync must be
	// able to write a checkpoint token. An inherited ledger makes
	// ledgerActive true and this fails with ErrLedgeredSyncWritesNoToken.
	require.NoError(t, store.SetCurrentSync(ctx, out.SyncID))
	require.NoError(t, store.CheckpointSync(ctx, "token-after-fold"),
		"an inherited ledger blocks the fold output from checkpointing a later sync")
}

// requireLedgerRow asserts whether the c1z at path carries the fixture's
// ledger row.
func requireLedgerRow(t *testing.T, ctx context.Context, path string, want bool, what string) {
	t.Helper()
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()
	ledger, ok := store.(c1zstore.PageLedgerStore)
	require.True(t, ok)
	_, found, err := ledger.GetLedgerRow(ctx, foldLedgerPageID)
	require.NoError(t, err)
	require.Equal(t, want, found, "%s: ledger row present = %v, want %v", what, found, want)
}

// requireLedgerCounter asserts the fixture's counter bucket reading in
// the c1z at path.
func requireLedgerCounter(t *testing.T, ctx context.Context, path string, want uint64, what string) {
	t.Helper()
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()
	ledger, ok := store.(c1zstore.PageLedgerStore)
	require.True(t, ok)
	counters, err := ledger.LedgerCounters(ctx)
	require.NoError(t, err)
	require.Equal(t, want, counters.Counters[foldLedgerCounterKey], "%s: ledger counter", what)
}
