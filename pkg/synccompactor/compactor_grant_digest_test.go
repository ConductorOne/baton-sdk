package synccompactor

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
)

// capturingCore/newCapturingLogger let a test observe the zap messages
// a compaction run logs, without hooking any production code — used
// here to prove the fold's "no grant writes, skip the rebuild" branch
// actually fired, which the resulting digest bytes alone can't show
// (a rebuild over unchanged data would produce the identical bytes).
type capturingCore struct {
	zapcore.LevelEnabler
	mu       *sync.Mutex
	messages *[]string
}

func newCapturingLogger() (*zap.Logger, func() []string) {
	mu := &sync.Mutex{}
	messages := &[]string{}
	core := capturingCore{LevelEnabler: zapcore.DebugLevel, mu: mu, messages: messages}
	return zap.New(core), func() []string {
		mu.Lock()
		defer mu.Unlock()
		out := make([]string, len(*messages))
		copy(out, *messages)
		return out
	}
}

func (c capturingCore) With([]zapcore.Field) zapcore.Core { return c }
func (c capturingCore) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(e.Level) {
		return ce.AddCore(e, c)
	}
	return ce
}
func (c capturingCore) Write(e zapcore.Entry, _ []zapcore.Field) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	*c.messages = append(*c.messages, e.Message)
	return nil
}
func (c capturingCore) Sync() error { return nil }

// buildOracleFromCompacted copies every record the compacted output at
// out actually holds into a brand-new Pebble store (via the public v2
// reader/writer surface — no shared state, no internal helpers) and
// seals it there. Comparing the oracle's independently-built grant
// digest against the compacted output's is a from-scratch parity
// check: if compaction's digest build were wrong (built over stale or
// partial data), it would not byte-match a digest built from a fresh
// engine over the same final records.
func buildOracleFromCompacted(t testing.TB, ctx context.Context, out *CompactableSync, path string) string {
	t.Helper()
	src, err := dotc1z.NewStore(ctx, out.FilePath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer src.Close(ctx)
	require.NoError(t, src.SetCurrentSync(ctx, out.SyncID))

	dst, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	syncID, err := dst.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	rtResp, err := src.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{PageSize: 1000}.Build())
	require.NoError(t, err)
	require.NoError(t, dst.PutResourceTypes(ctx, rtResp.GetList()...))

	rResp, err := src.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{PageSize: 1000}.Build())
	require.NoError(t, err)
	require.NoError(t, dst.PutResources(ctx, rResp.GetList()...))

	eResp, err := src.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{PageSize: 1000}.Build())
	require.NoError(t, err)
	require.NoError(t, dst.PutEntitlements(ctx, eResp.GetList()...))

	gResp, err := src.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageSize: 1000}.Build())
	require.NoError(t, err)
	require.NoError(t, dst.PutGrants(ctx, gResp.GetList()...))

	require.NoError(t, dst.EndSync(ctx))
	require.NoError(t, dst.Close(ctx))
	return syncID
}

// TestCompactPebbleGrantDigestsBuiltPerMode pins Deliverable 1 (RFC
// 0003): a compacted output — under every merge strategy — must carry
// grant-digest state (the by_entitlement_principal_hash index + digest
// nodes) matching a from-scratch build over its final grant set, and
// the saved envelope's manifest must carry a matching whole-file
// GrantDigestRoot. Before this, compaction shipped digest-free outputs
// in every mode, defeating the digest feature for exactly the files
// downstream diff consumers ingest most.
func TestCompactPebbleGrantDigestsBuiltPerMode(t *testing.T) {
	for _, mode := range []PebbleCompactorMode{PebbleCompactorModeFold, PebbleCompactorModeKWay, PebbleCompactorModeOverlay} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			inDir := t.TempDir()
			outDir := t.TempDir()

			p1 := filepath.Join(inDir, "in1.c1z")
			p2 := filepath.Join(inDir, "in2.c1z")
			s1 := buildPebbleInput(t, ctx, p1, connectorstore.SyncTypeFull, "g-shared", "g-only1")
			s2 := buildPebbleInput(t, ctx, p2, connectorstore.SyncTypePartial, "g-shared", "g-only2")

			entries := []*CompactableSync{{FilePath: p1, SyncID: s1}, {FilePath: p2, SyncID: s2}}
			c, cleanup, err := NewCompactor(ctx, outDir, entries,
				WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble), WithPebbleCompactorMode(mode))
			require.NoError(t, err)
			defer func() { _ = cleanup() }()

			out, err := c.Compact(ctx)
			require.NoError(t, err)
			require.NotNil(t, out)

			store, err := dotc1z.NewStore(ctx, out.FilePath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
			require.NoError(t, err)
			defer store.Close(ctx)
			eng, ok := enginepkg.AsEngine(store)
			require.True(t, ok, "compacted output must be a pebble engine")
			require.NoError(t, store.SetCurrentSync(ctx, out.SyncID))

			gotRoot, ok, err := eng.GetGrantDigestGlobalRoot(ctx)
			require.NoError(t, err)
			require.True(t, ok, "compacted output must carry a built grant digest")
			require.EqualValues(t, 3, gotRoot.Count, "digest built over the deduped union of 3 grants")

			// Oracle: an independent from-scratch build over the same
			// final records.
			oraclePath := filepath.Join(outDir, "oracle-"+string(mode)+".c1z")
			oracleSyncID := buildOracleFromCompacted(t, ctx, out, oraclePath)
			oracleStore, err := dotc1z.NewStore(ctx, oraclePath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
			require.NoError(t, err)
			defer oracleStore.Close(ctx)
			oracleEng, ok := enginepkg.AsEngine(oracleStore)
			require.True(t, ok)
			require.NoError(t, oracleStore.SetCurrentSync(ctx, oracleSyncID))

			wantRoot, ok, err := oracleEng.GetGrantDigestGlobalRoot(ctx)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, wantRoot.Count, gotRoot.Count, "compacted vs. from-scratch grant count")
			require.True(t, bytes.Equal(wantRoot.Hash, gotRoot.Hash),
				"compacted digest %x must byte-equal a from-scratch build over the same final grants %x", gotRoot.Hash, wantRoot.Hash)

			// The saved manifest header carries the same root, readable
			// without unpacking the payload.
			f, err := os.Open(out.FilePath)
			require.NoError(t, err)
			defer f.Close()
			m, err := formatv3.ReadManifestHeader(f)
			require.NoError(t, err)
			mRoot := m.GetGrantDigestRoot()
			require.NotNil(t, mRoot, "manifest must carry the grant digest root")
			require.Equal(t, gotRoot.Count, mRoot.GetCount())
			require.True(t, bytes.Equal(gotRoot.Hash, mRoot.GetXorDigest()))
		})
	}
}

// grantDigestGlobalRootOf opens path read-only, binds syncID, and
// returns its stored whole-file grant digest root.
func grantDigestGlobalRootOf(t testing.TB, ctx context.Context, path, syncID string) enginepkg.DigestRoot {
	t.Helper()
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer store.Close(ctx)
	eng, ok := enginepkg.AsEngine(store)
	require.True(t, ok, "%s is not a pebble engine", path)
	require.NoError(t, store.SetCurrentSync(ctx, syncID))
	root, ok, err := eng.GetGrantDigestGlobalRoot(ctx)
	require.NoError(t, err)
	require.True(t, ok, "%s has no built grant digest", path)
	return root
}

// TestCompactPebbleFoldSkipsDigestRebuildWhenGrantsUnchanged pins the
// fix for the "every fold throws away and regenerates the whole grant
// digest, even when nothing changed" gap: a fold whose partial touches
// zero grants must leave the base's already-correct digest state
// completely untouched — no drop, no rebuild — rather than paying a
// full from-scratch rebuild for zero benefit. The resulting bytes
// alone can't distinguish "left alone" from "rebuilt to the same
// answer" (both are byte-identical for unchanged data), so this
// captures the compactor's log output to confirm the skip branch
// actually fired, and separately confirms the base's own root survives
// into the compacted output unchanged.
func TestCompactPebbleFoldSkipsDigestRebuildWhenGrantsUnchanged(t *testing.T) {
	logger, capture := newCapturingLogger()
	ctx := ctxzap.ToContext(context.Background(), logger)
	inDir := t.TempDir()
	outDir := t.TempDir()

	basePath := filepath.Join(inDir, "base.c1z")
	partialPath := filepath.Join(inDir, "partial-no-grants.c1z")
	baseSync := buildPebbleInput(t, ctx, basePath, connectorstore.SyncTypeFull, "g1", "g2")
	// A partial that shares the base's resource/entitlement shape but
	// carries ZERO grants: the fold's grants-bucket scan iterates
	// nothing, so FoldStats.GrantWrites is exactly zero.
	partialSync := buildPebbleInput(t, ctx, partialPath, connectorstore.SyncTypePartial)

	wantRoot := grantDigestGlobalRootOf(t, ctx, basePath, baseSync)

	entries := []*CompactableSync{{FilePath: basePath, SyncID: baseSync}, {FilePath: partialPath, SyncID: partialSync}}
	c, cleanup, err := NewCompactor(ctx, outDir, entries,
		WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble), WithPebbleCompactorMode(PebbleCompactorModeFold))
	require.NoError(t, err)
	defer func() { _ = cleanup() }()

	out, err := c.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, out)

	var sawSkip bool
	for _, msg := range capture() {
		if msg == "compactPebbleFold: no grant writes; base grant digest state left untouched" {
			sawSkip = true
		}
	}
	require.True(t, sawSkip, "expected the fold to log that it skipped the grant digest rebuild")

	gotRoot := grantDigestGlobalRootOf(t, ctx, out.FilePath, out.SyncID)
	require.Equal(t, wantRoot.Count, gotRoot.Count, "grant count must be exactly the base's, unchanged")
	require.True(t, bytes.Equal(wantRoot.Hash, gotRoot.Hash),
		"compacted root %x must byte-equal the base's untouched root %x", gotRoot.Hash, wantRoot.Hash)
}

// TestCompactPebbleFoldRepairsOnlyTouchedEntitlements is the
// end-to-end companion to
// TestRepairMissingGrantDigestsHealsOnlyInvalidatedPartition (which
// pins the byte-exact per-entitlement scoping at the engine level):
// a fold whose partial adds a grant under only ONE of two base
// entitlements must take the targeted invalidate+repair path (not the
// full-file rebuild, not the no-op skip), and the result must still
// match a from-scratch build over the final data.
func TestCompactPebbleFoldRepairsOnlyTouchedEntitlements(t *testing.T) {
	logger, capture := newCapturingLogger()
	ctx := ctxzap.ToContext(context.Background(), logger)
	inDir := t.TempDir()
	outDir := t.TempDir()

	basePath := filepath.Join(inDir, "base.c1z")
	partialPath := filepath.Join(inDir, "partial.c1z")
	baseSync := buildOverlayInput(t, ctx, basePath, overlayInputSpec{
		syncType: connectorstore.SyncTypeFull,
		suffix:   "base",
		grants: []overlayGrantSpec{
			{id: "g-ent1-alice", principalID: "alice", entitlementID: "ent-1"},
			{id: "g-ent2-bob", principalID: "bob", entitlementID: "ent-2"},
		},
	})
	// The partial adds a NEW grant only under ent-1; ent-2 is never
	// mentioned, so its partition must never be touched by the fold.
	partialSync := buildOverlayInput(t, ctx, partialPath, overlayInputSpec{
		syncType: connectorstore.SyncTypePartial,
		suffix:   "partial",
		grants: []overlayGrantSpec{
			{id: "g-ent1-bob", principalID: "bob", entitlementID: "ent-1"},
		},
	})

	entries := []*CompactableSync{{FilePath: basePath, SyncID: baseSync}, {FilePath: partialPath, SyncID: partialSync}}
	c, cleanup, err := NewCompactor(ctx, outDir, entries,
		WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble), WithPebbleCompactorMode(PebbleCompactorModeFold),
		WithSkipGrantExpansion())
	require.NoError(t, err)
	defer func() { _ = cleanup() }()

	out, err := c.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, out)

	var repairedMsg bool
	for _, msg := range capture() {
		if msg == "compactPebbleFold: repaired grant digests for touched entitlements" {
			repairedMsg = true
		}
	}
	require.True(t, repairedMsg, "expected the fold to log a targeted repair, not a full rebuild or a skip")

	// Oracle: the compacted output's whole-file root must still match
	// a from-scratch build over the same final grants, proving the
	// targeted repair produced a correct result even though it only
	// touched ent-1.
	oraclePath := filepath.Join(outDir, "oracle.c1z")
	oracleSyncID := buildOracleFromCompacted(t, ctx, out, oraclePath)
	gotRoot := grantDigestGlobalRootOf(t, ctx, out.FilePath, out.SyncID)
	wantRoot := grantDigestGlobalRootOf(t, ctx, oraclePath, oracleSyncID)
	require.Equal(t, wantRoot.Count, gotRoot.Count)
	require.True(t, bytes.Equal(wantRoot.Hash, gotRoot.Hash),
		"compacted root %x must byte-equal a from-scratch build %x", gotRoot.Hash, wantRoot.Hash)
}

// requireEmptyKeyRange asserts the engine's raw keyspace holds nothing
// in [lo, hi).
func requireEmptyKeyRange(t testing.TB, eng *enginepkg.Engine, lo, hi []byte, what string) {
	t.Helper()
	iter, err := eng.DB().NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	require.NoError(t, err)
	defer iter.Close()
	require.False(t, iter.First(), "%s keyspace must be empty, found key %x", what, iter.Key())
	require.NoError(t, iter.Error())
}

// TestCompactPebbleFoldDigestIndexDisabledDropsDigests pins the fold's
// disabled-digest-index escape hatch: the dest starts as a byte copy of
// the sealed base INCLUDING its digest + hash-index keyspaces, readers
// trust whatever is stored regardless of the writer's flag, and with
// WithGrantDigestIndex(false) every rebuild path (the targeted repair,
// EndSync's finalize) is gated off — so once the merge writes a grant,
// the copied digests are stale and nothing downstream can heal them.
// The fold must DROP the copied digest state instead (absent is always
// safe; present-but-stale silently corrupts grant diffs). Before this,
// the output shipped the base's untouched-but-wrong digest root.
func TestCompactPebbleFoldDigestIndexDisabledDropsDigests(t *testing.T) {
	logger, capture := newCapturingLogger()
	ctx := ctxzap.ToContext(context.Background(), logger)
	inDir := t.TempDir()
	outDir := t.TempDir()

	basePath := filepath.Join(inDir, "base.c1z")
	partialPath := filepath.Join(inDir, "partial.c1z")
	baseSync := buildOverlayInput(t, ctx, basePath, overlayInputSpec{
		syncType: connectorstore.SyncTypeFull,
		suffix:   "base",
		grants: []overlayGrantSpec{
			{id: "g-ent1-alice", principalID: "alice", entitlementID: "ent-1"},
			{id: "g-ent2-bob", principalID: "bob", entitlementID: "ent-2"},
		},
	})
	// The partial adds a grant under ent-1, so the fold's merge writes
	// into the grants keyspace and ent-1's copied digest goes stale.
	partialSync := buildOverlayInput(t, ctx, partialPath, overlayInputSpec{
		syncType: connectorstore.SyncTypePartial,
		suffix:   "partial",
		grants: []overlayGrantSpec{
			{id: "g-ent1-bob", principalID: "bob", entitlementID: "ent-1"},
		},
	})
	// Sanity: the sealed base carries the digest state the fold copies.
	grantDigestGlobalRootOf(t, ctx, basePath, baseSync)

	entries := []*CompactableSync{{FilePath: basePath, SyncID: baseSync}, {FilePath: partialPath, SyncID: partialSync}}
	c, cleanup, err := NewCompactor(ctx, outDir, entries,
		WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble), WithPebbleCompactorMode(PebbleCompactorModeFold),
		WithSkipGrantExpansion(),
		WithC1ZOptions(dotc1z.WithGrantDigestIndex(false)))
	require.NoError(t, err)
	defer func() { _ = cleanup() }()

	out, err := c.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, out)

	var sawDrop bool
	for _, msg := range capture() {
		if msg == "compactPebbleFold: grant writes with digest index disabled; dropped the base's copied digest state" {
			sawDrop = true
		}
	}
	require.True(t, sawDrop, "expected the fold to log dropping the copied digest state")

	store, err := dotc1z.NewStore(ctx, out.FilePath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer store.Close(ctx)
	eng, ok := enginepkg.AsEngine(store)
	require.True(t, ok, "compacted output must be a pebble engine")
	require.NoError(t, store.SetCurrentSync(ctx, out.SyncID))

	// A default-options reader must see digests as ABSENT (recalculate),
	// never the base's stale-but-present state.
	_, ok, err = eng.GetGrantDigestGlobalRoot(ctx)
	require.NoError(t, err)
	require.False(t, ok, "output must carry no whole-file digest root")
	requireEmptyKeyRange(t, eng, enginepkg.DigestLowerBound(), enginepkg.DigestUpperBound(), "digest node")
	requireEmptyKeyRange(t, eng, enginepkg.GrantByEntPrincHashLowerBound(), enginepkg.GrantByEntPrincHashUpperBound(), "grant hash index")

	// And the saved envelope header must not stamp a stale root either.
	f, err := os.Open(out.FilePath)
	require.NoError(t, err)
	defer f.Close()
	m, err := formatv3.ReadManifestHeader(f)
	require.NoError(t, err)
	require.Nil(t, m.GetGrantDigestRoot(), "manifest must not carry a grant digest root after the drop")
}

// TestCompactPebbleFoldWithExpansionRebuildsFullyRegardless closes the
// loop on a real question about the targeted repair's safety: grant
// expansion (run by Compact whenever it isn't skipped — here, because
// the union sync type is Full) writes grants through its OWN paths
// (PutExpandedGrantRecords / PutSynthesizedGrantRecords / the
// layer-session ingest), never through compactPebbleFold's tracked
// merge. This does not need those paths to invalidate perfectly on
// their own: expansion's syncer.Sync always calls store.EndSync
// afterward, and EndSync's finalize always runs a full digest rebuild
// when the digest index is enabled, unconditionally superseding
// whatever the targeted repair produced. This test exercises the full
// Compact() flow (not compactPebbleFold directly) with expansion NOT
// skipped, and asserts the final output's whole-file root still
// matches a from-scratch build.
func TestCompactPebbleFoldWithExpansionRebuildsFullyRegardless(t *testing.T) {
	ctx := context.Background()
	inDir := t.TempDir()
	outDir := t.TempDir()

	p1 := filepath.Join(inDir, "in1.c1z")
	p2 := filepath.Join(inDir, "in2.c1z")
	s1 := buildPebbleInput(t, ctx, p1, connectorstore.SyncTypeFull, "g-shared", "g-only1")
	s2 := buildPebbleInput(t, ctx, p2, connectorstore.SyncTypePartial, "g-shared", "g-only2")

	entries := []*CompactableSync{{FilePath: p1, SyncID: s1}, {FilePath: p2, SyncID: s2}}
	// No WithSkipGrantExpansion(): the base is Full, so the compacted
	// sync's union type is Full too, skipExpansion is false in
	// Compact(), and a real grant-expansion syncer.Sync + EndSync runs
	// on the compacted output before it is saved.
	c, cleanup, err := NewCompactor(ctx, outDir, entries,
		WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble), WithPebbleCompactorMode(PebbleCompactorModeFold))
	require.NoError(t, err)
	defer func() { _ = cleanup() }()

	out, err := c.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, out)

	oraclePath := filepath.Join(outDir, "oracle.c1z")
	oracleSyncID := buildOracleFromCompacted(t, ctx, out, oraclePath)
	gotRoot := grantDigestGlobalRootOf(t, ctx, out.FilePath, out.SyncID)
	wantRoot := grantDigestGlobalRootOf(t, ctx, oraclePath, oracleSyncID)
	require.Equal(t, wantRoot.Count, gotRoot.Count)
	require.True(t, bytes.Equal(wantRoot.Hash, gotRoot.Hash),
		"post-expansion compacted root %x must byte-equal a from-scratch build %x", gotRoot.Hash, wantRoot.Hash)
}
