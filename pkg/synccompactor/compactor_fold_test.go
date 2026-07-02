package synccompactor

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
)

// grantDiscoveredAt reads one grant's discovered_at from the Pebble c1z
// at path, under syncID. Fixture grant ids are connector-custom (no concat
// shape), so the row is addressed by refs via the by_principal index.
func grantDiscoveredAt(t *testing.T, ctx context.Context, path, syncID, grantID string) time.Time {
	t.Helper()
	w, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer w.Close(ctx)
	eng, ok := enginepkg.AsEngine(w)
	require.True(t, ok, "store at %s is not a pebble engine", path)
	// One grant per principal in these fixtures (entitlement is always
	// "member"), so the principal scan identifies the row exactly.
	var rec *v3.GrantRecord
	require.NoError(t, eng.IterateGrantsByPrincipal(ctx, "user", compactFixturePrincipalID(grantID), func(r *v3.GrantRecord) bool {
		rec = r
		return false
	}))
	require.NotNil(t, rec, "grant %s not found in %s", grantID, path)
	return rec.GetDiscoveredAt().AsTime()
}

// TestCompactPebbleFoldMintsFreshSync covers the in-place fold strategy
// (BATON_EXPERIMENTAL_PEBBLE_COMPACTOR=fold): the compacted output is a
// copy of the base input into which the partials' winners are folded
// via the keep-newer path. Base keys are never rewritten (the data
// keyspace carries no sync_id), but the single sync-run record is
// re-keyed to a FRESH sync id, so C1's compaction bookkeeping sees a
// new sync.
func TestCompactPebbleFoldMintsFreshSync(t *testing.T) {
	ctx := context.Background()
	inDir := t.TempDir()

	basePath := filepath.Join(inDir, "base.c1z")
	partialPath := filepath.Join(inDir, "partial.c1z")
	// The partial is built after the base, so its "g-shared" carries a
	// strictly newer discovered_at and must win the fold.
	baseSyncID := buildPebbleInput(t, ctx, basePath, connectorstore.SyncTypeFull, "g-shared", "g-base-only")
	partialSyncID := buildPebbleInput(t, ctx, partialPath, connectorstore.SyncTypePartial, "g-shared", "g-partial-only")

	t.Setenv("BATON_EXPERIMENTAL_PEBBLE_COMPACTOR", "fold")
	c, cleanup, err := NewCompactor(ctx, t.TempDir(), []*CompactableSync{
		{FilePath: basePath, SyncID: baseSyncID},
		{FilePath: partialPath, SyncID: partialSyncID},
	}, WithTmpDir(t.TempDir()), WithEngine(dotc1z.EnginePebble), WithSkipGrantExpansion())
	require.NoError(t, err)
	defer func() { require.NoError(t, cleanup()) }()

	out, err := c.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, out)

	// Fresh sync id (not the base's).
	require.NotEqual(t, baseSyncID, out.SyncID, "fold output must mint a fresh sync id")
	require.NotEmpty(t, out.SyncID)

	// Union of grants, union sync type (full beats partial).
	count, syncType := verifyCompacted(t, ctx, out.FilePath, out.SyncID)
	require.Equal(t, 3, count)
	require.Equal(t, string(connectorstore.SyncTypeFull), syncType)

	// The overlapping grant's winner is the partial's record (strictly
	// newer discovered_at); the base-only grant keeps the base's stamp.
	// (Reads ignore the sync id arg — the keyspace carries one sync.)
	require.Equal(t,
		grantDiscoveredAt(t, ctx, partialPath, partialSyncID, "g-shared"),
		grantDiscoveredAt(t, ctx, out.FilePath, out.SyncID, "g-shared"),
		"fold winner for overlapping grant must be the partial's (newer) record")
	require.Equal(t,
		grantDiscoveredAt(t, ctx, basePath, baseSyncID, "g-base-only"),
		grantDiscoveredAt(t, ctx, out.FilePath, out.SyncID, "g-base-only"),
		"base-only grant must be preserved untouched")

	// ended_at advances to the max across inputs (the partial's).
	require.Equal(t,
		latestEndedAt(t, ctx, partialPath).UTC(),
		latestEndedAt(t, ctx, out.FilePath).UTC())

	// Fold semantics: the base sync's data is preserved wholesale, so
	// the base's asset survives (unlike the rebuild strategies, which
	// drop assets). Partial assets are not copied.
	require.Equal(t, 1, countPebbleAssets(t, ctx, out.FilePath))

	// The folded sync-run record stands alone (no dangling parent link —
	// the base sync's record was overwritten by the rename), and its
	// stats sidecar was recomputed under the fresh id.
	statsW, err := dotc1z.NewStore(ctx, out.FilePath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer statsW.Close(ctx)
	statsEng, ok := enginepkg.AsEngine(statsW)
	require.True(t, ok)
	rec, err := statsEng.GetSyncRunRecord(ctx, out.SyncID)
	require.NoError(t, err)
	require.Empty(t, rec.GetParentSyncId(), "folded sync must not carry a parent link to the overwritten base sync")
	stats, err := enginepkg.ReadSyncStatsRecord(ctx, statsEng, out.SyncID)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(3), stats.GetGrants())

	// Header-only contract: the fresh sync id and its stats must be
	// readable from the envelope manifest WITHOUT unpacking the payload
	// (this is what compaction source selection and any "what's in this
	// c1z?" tooling rely on). The renamed-away base id must be gone.
	f, err := os.Open(out.FilePath)
	require.NoError(t, err)
	defer f.Close()
	m, err := formatv3.ReadManifestHeader(f)
	require.NoError(t, err)
	runs := m.GetSyncRuns()
	require.Len(t, runs, 1, "folded c1z manifest must project exactly one sync run")
	require.Equal(t, out.SyncID, runs[0].GetSyncId(), "manifest projection must carry the fresh sync id")
	require.NotEqual(t, baseSyncID, runs[0].GetSyncId(), "the overwritten base sync id must not appear in the manifest")
	require.NotNil(t, runs[0].GetStats(), "manifest projection must carry the recomputed stats sidecar")
	require.Equal(t, int64(3), runs[0].GetStats().GetGrants())

	// The fold overrode g-shared, so its incumbent's bytes are dead
	// weight inside the spliced base frames — the manifest's waste
	// counter must record them.
	require.Positive(t, m.GetFoldDeadBytes(), "fold must record the overridden incumbent's bytes in fold_dead_bytes")
}

// TestCompactPebbleFoldWasteAccumulates covers the fold-waste
// accounting lifecycle: each fold adds the raw bytes it shadowed to
// the manifest's fold_dead_bytes (carried forward from the base it
// folded into), and a rebuild resets the counter to zero.
func TestCompactPebbleFoldWasteAccumulates(t *testing.T) {
	ctx := context.Background()
	inDir := t.TempDir()

	basePath := filepath.Join(inDir, "base.c1z")
	p1Path := filepath.Join(inDir, "p1.c1z")
	p2Path := filepath.Join(inDir, "p2.c1z")
	// Built in order, so each partial's g-shared is strictly newer than
	// everything before it and overrides the incumbent at fold time.
	baseSyncID := buildPebbleInput(t, ctx, basePath, connectorstore.SyncTypeFull, "g-shared", "g-base-only")
	p1SyncID := buildPebbleInput(t, ctx, p1Path, connectorstore.SyncTypePartial, "g-shared")
	p2SyncID := buildPebbleInput(t, ctx, p2Path, connectorstore.SyncTypePartial, "g-shared")

	t.Setenv("BATON_EXPERIMENTAL_PEBBLE_COMPACTOR", "fold")

	// Fold #1: p1 overrides the base's g-shared.
	out1 := compactPairOnce(t, ctx,
		&CompactableSync{FilePath: basePath, SyncID: baseSyncID},
		&CompactableSync{FilePath: p1Path, SyncID: p1SyncID})
	dead1 := readFoldDeadBytes(t, out1.FilePath)
	require.Positive(t, dead1, "first fold must record dead bytes for the overridden grant")

	// Fold #2 into the folded output: p2 overrides g-shared again. The
	// counter is cumulative — inherited from out1's manifest, plus this
	// fold's own dead bytes.
	out2 := compactPairOnce(t, ctx, out1, &CompactableSync{FilePath: p2Path, SyncID: p2SyncID})
	dead2 := readFoldDeadBytes(t, out2.FilePath)
	require.Greater(t, dead2, dead1, "second fold must accumulate on top of the inherited counter")

	// A rebuild garbage-collects every shadowed record, so its output
	// resets the counter.
	t.Setenv("BATON_EXPERIMENTAL_PEBBLE_COMPACTOR", "overlay")
	out3 := compactPairOnce(t, ctx, out2, &CompactableSync{FilePath: p2Path, SyncID: p2SyncID})
	require.Zero(t, readFoldDeadBytes(t, out3.FilePath), "a rebuild must reset fold_dead_bytes")
}

// TestFoldWasteCarryForwardAndAutoCutover covers the two waste-debt
// behaviors the synthetic-envelope gate test can't: the counter
// surviving a plain (non-fold) open/save cycle of a real Pebble c1z,
// and the AUTO mode gate reading a real folded file's manifest +
// indexed trailer to decide fold vs overlay — including the full
// Compact run that rebuilds and resets the debt once it's injected
// past the cutover. (Organic fixture-scale waste is far below 15% of
// the payload, so the over-threshold leg injects debt through the
// same AddFoldDeadBytes accessor the fold compactor uses.)
func TestFoldWasteCarryForwardAndAutoCutover(t *testing.T) {
	ctx := context.Background()
	inDir := t.TempDir()

	basePath := filepath.Join(inDir, "base.c1z")
	pPath := filepath.Join(inDir, "p.c1z")
	baseSyncID := buildPebbleInput(t, ctx, basePath, connectorstore.SyncTypeFull, "g-shared", "g-base-only")
	pSyncID := buildPebbleInput(t, ctx, pPath, connectorstore.SyncTypePartial, "g-shared")
	partial := &CompactableSync{FilePath: pPath, SyncID: pSyncID}

	// Produce a real folded c1z carrying organic dead bytes.
	t.Setenv("BATON_EXPERIMENTAL_PEBBLE_COMPACTOR", "fold")
	out := compactPairOnce(t, ctx, &CompactableSync{FilePath: basePath, SyncID: baseSyncID}, partial)
	dead := readFoldDeadBytes(t, out.FilePath)
	require.Positive(t, dead)

	// Carry-forward: a writable open + dirty save that folds nothing
	// must preserve the inherited counter verbatim (the store seeds it
	// from the source manifest at open and writes it back at save).
	w, err := dotc1z.NewStore(ctx, out.FilePath, dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	require.True(t, enginepkg.MarkStoreDirty(w))
	require.NoError(t, w.Close(ctx))
	require.Equal(t, dead, readFoldDeadBytes(t, out.FilePath),
		"a non-fold save must carry the inherited fold_dead_bytes forward unchanged")

	// AUTO mode against the real file: organic waste is far below the
	// cutover, so the gate stays on fold. The fixture partial is not
	// small relative to the fixture base, so widen the size leg to
	// isolate the waste leg.
	t.Setenv("BATON_EXPERIMENTAL_PEBBLE_COMPACTOR", "")
	t.Setenv("BATON_PEBBLE_FOLD_MAX_PARTIAL_PCT", "10000")
	autoC := &Compactor{entries: []*CompactableSync{{FilePath: out.FilePath, SyncID: out.SyncID}, partial}}
	require.Equal(t, PebbleCompactorModeFold, autoC.resolvePebbleMode(ctx),
		"real folded file below the waste cutover must stay on fold")

	// Inject debt past the cutover through the production accessor,
	// persisted by the same dirty-save path.
	w, err = dotc1z.NewStore(ctx, out.FilePath, dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	require.True(t, enginepkg.AddFoldDeadBytes(w, 1<<30))
	require.True(t, enginepkg.MarkStoreDirty(w))
	require.NoError(t, w.Close(ctx))
	require.Equal(t, dead+1<<30, readFoldDeadBytes(t, out.FilePath))

	require.Equal(t, PebbleCompactorModeOverlay, autoC.resolvePebbleMode(ctx),
		"debt past the cutover must force the rebuild")

	// Full AUTO compaction: the run must route to the rebuild and the
	// rebuilt output must reset the counter.
	rebuilt := compactPairOnce(t, ctx, &CompactableSync{FilePath: out.FilePath, SyncID: out.SyncID}, partial)
	require.Zero(t, readFoldDeadBytes(t, rebuilt.FilePath),
		"the auto-selected rebuild must reset fold_dead_bytes")
}

// compactPairOnce compacts base + partial with the pebble engine and
// returns the output.
func compactPairOnce(t *testing.T, ctx context.Context, base, partial *CompactableSync, opts ...Option) *CompactableSync {
	t.Helper()
	opts = append([]Option{WithTmpDir(t.TempDir()), WithEngine(dotc1z.EnginePebble), WithSkipGrantExpansion()}, opts...)
	c, cleanup, err := NewCompactor(ctx, t.TempDir(), []*CompactableSync{base, partial}, opts...)
	require.NoError(t, err)
	defer func() { require.NoError(t, cleanup()) }()
	out, err := c.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, out)
	return out
}

// readFoldDeadBytes returns the envelope manifest's fold_dead_bytes
// counter via a header-only read.
func readFoldDeadBytes(t *testing.T, path string) int64 {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	m, err := formatv3.ReadManifestHeader(f)
	require.NoError(t, err)
	return m.GetFoldDeadBytes()
}
