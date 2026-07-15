package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// Mixed-version compaction artifacts and replay eligibility.
//
// A fold compactor built from an SDK that predates the run record's
// compacted flag produces an artifact that LOOKS replay-eligible to the
// metadata gate: the record carries a full type and no compacted flag
// (the old proto has no such field), and the old fold never cleared the
// base copy's source-cache manifest (the old engine has no source-cache
// code), so the manifest belt passes too. The one signal every compactor
// version has always written is the sync TOKEN's compaction provenance
// section (BuildCompactedToken) — so the syncer's replay gate must read
// it and refuse the artifact.

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	pebbleengine "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/logging"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
)

// TestSourceCache_PreCompactedFlagFoldArtifactDegradesToCold pins the
// token-provenance gate: a previous sync whose run record says
// full/non-compacted but whose sync token carries a compaction section
// (the shape an old-binary fold leaves on a new-format base) must never
// serve replay, even with its manifest fully intact.
func TestSourceCache_PreCompactedFlagFoldArtifactDegradesToCold(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	group, ent, alice, _ := sourceCacheTestFixtures(t)
	g1 := gt.NewGrant(group, "member", alice)

	mc := newSourceCacheMockConnector()
	mc.AddResource(ctx, group)
	mc.AddResource(ctx, alice)
	mc.entDB[group.GetId().GetResource()] = []*v2.Entitlement{ent}
	mc.grantDB[group.GetId().GetResource()] = []*v2.Grant{g1}
	mc.etagByResource[group.GetId().GetResource()] = `W/"etag-v1"`

	sync1 := filepath.Join(tmpDir, "sync1.c1z")
	runSourceCacheSync(ctx, t, mc, sync1, "", tmpDir)

	// Rewrite sync 1's run record into the old-fold shape: keep the full
	// type, leave compacted UNSET (the old proto has no field to set),
	// and give it the compaction-provenance token every fold — old or
	// new — writes. The source-cache manifest stays intact, exactly as
	// an old fold (no ClearSourceCacheEntries) would leave it.
	marker, err := dotc1z.NewStore(ctx, sync1,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	run, err := marker.SyncMeta().LatestFullSync(ctx)
	require.NoError(t, err)
	require.NotNil(t, run)
	engineHolder, ok := any(marker).(interface{ PebbleEngine() *pebbleengine.Engine })
	require.True(t, ok)
	rec, err := engineHolder.PebbleEngine().GetSyncRunRecord(ctx, run.ID)
	require.NoError(t, err)
	require.False(t, rec.GetCompacted(), "sanity: the record must NOT carry the compacted flag for this test to prove the token gate")
	compactedToken, err := BuildCompactedToken(rec.GetSyncToken(), CompactionTokenInput{
		Mode:           "fold",
		BaseSyncID:     run.ID,
		PartialSyncIDs: []string{"old-partial-1"},
	})
	require.NoError(t, err)
	rec.SetSyncToken(compactedToken)
	require.NoError(t, engineHolder.PebbleEngine().PutSyncRunRecord(ctx, rec))
	require.True(t, pebbleengine.MarkStoreDirty(marker), "the token write must persist through Close")
	require.NoError(t, marker.Close(ctx))

	// Warm sync against the doctored file: every lookup must MISS
	// despite the intact manifest and the clean-looking run record.
	sync2 := filepath.Join(tmpDir, "sync2.c1z")
	missesBefore := mc.lookupMisses
	runSourceCacheSync(ctx, t, mc, sync2, sync1, tmpDir)
	require.Zero(t, mc.lookupHits,
		"a previous sync whose token carries compaction provenance must never produce a source-cache lookup hit, even without the compacted flag")
	require.Greater(t, mc.lookupMisses, missesBefore,
		"sanity: the connector must have looked up and missed (no-op lookup installed)")
}
