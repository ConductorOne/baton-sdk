package synccompactor

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
)

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

	dst, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(dotc1z.EnginePebble), dotc1z.WithTmpDir(t.TempDir()))
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
				WithTmpDir(t.TempDir()), WithEngine(dotc1z.EnginePebble), WithPebbleCompactorMode(mode))
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
