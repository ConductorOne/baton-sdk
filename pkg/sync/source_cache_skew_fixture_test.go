package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// Frozen-artifact version-skew test. Unlike the synthesized
// mixed-version test (source_cache_mixed_version_test.go), this one
// reads REAL bytes produced by the merge-base SDK's fold compactor —
// see testdata/README-old-fold.md — so it also verifies the parts of
// old-binary behavior we would otherwise have to guess (measured: the
// old fold carries the base's source-cache manifest through INTACT,
// validators and all).

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/logging"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
)

func TestSourceCache_FrozenOldFoldArtifactRefusedForReplay(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	// Copy the frozen artifact out of testdata (syncs open the previous
	// file next to their own tmp state).
	prev := filepath.Join(tmpDir, "old-fold.c1z")
	src, err := os.Open(filepath.Join("testdata", "old-fold-mixed-version.c1z"))
	require.NoError(t, err)
	dst, err := os.Create(prev)
	require.NoError(t, err)
	_, err = io.Copy(dst, src)
	require.NoError(t, err)
	require.NoError(t, src.Close())
	require.NoError(t, dst.Close())

	// Sanity: this fixture is the dangerous shape — every record-level
	// gate passes and the manifest is intact, so ONLY the token
	// provenance can refuse it. If any of these fail, the fixture no
	// longer proves what it claims (see testdata/README-old-fold.md).
	store, err := dotc1z.NewStore(ctx, prev,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithReadOnly(true),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	run, err := store.SyncMeta().LatestFullSync(ctx)
	require.NoError(t, err)
	require.NotNil(t, run)
	require.False(t, run.Compacted, "fixture: old binary cannot set the compacted flag")
	require.True(t, run.UsableAsReplaySource(), "fixture: the record-level gate must PASS, or this test proves nothing")
	comp, err := CompactionStatsFromToken(run.SyncToken)
	require.NoError(t, err)
	require.NotNil(t, comp, "fixture: the token must carry compaction provenance — the one honest signal")
	inspector, ok := store.(dotc1z.SourceCacheInspector)
	require.True(t, ok)
	manifest, err := inspector.SourceCacheManifestSnapshot(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, manifest, "fixture: the old fold carries the base manifest intact — the manifest belt must PASS too")
	require.NoError(t, store.Close(ctx))

	// The trap: a connector whose current validator MATCHES the carried
	// manifest entry. A broken gate would 304 and replay merged rows.
	group, ent, alice, _ := sourceCacheTestFixtures(t)
	mc := newSourceCacheMockConnector()
	mc.AddResource(ctx, group)
	mc.AddResource(ctx, alice)
	mc.entDB[group.GetId().GetResource()] = []*v2.Entitlement{ent}
	mc.grantDB[group.GetId().GetResource()] = []*v2.Grant{gt.NewGrant(group, "member", alice)}
	mc.etagByResource[group.GetId().GetResource()] = `W/"skew-v1"` // matches the carried validator

	out := filepath.Join(tmpDir, "out.c1z")
	runSourceCacheSync(ctx, t, mc, out, prev, tmpDir)
	require.Zero(t, mc.lookupHits,
		"an old-binary fold artifact must never serve replay: its rows are a merge no upstream validator describes")
	require.Positive(t, mc.lookupMisses, "sanity: lookups must have been offered and missed")
}
