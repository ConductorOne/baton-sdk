package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// I3 repair-arm tests: a grant-inserted resource lost from the current
// sync (the replay-loss class) is repaired by copying the previous
// sync's row in lenient mode, and hard-fails under ErrReplayIntegrity in
// strict mode (so mutation tests still name the machinery bug) or when
// no previous source can supply the row.

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

func repairTestGrant(t *testing.T, repo *v2.Resource) *v2.Grant {
	t.Helper()
	principal := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
	}.Build()
	g := grant.NewGrant(repo, "admin", principal.GetId())
	annos := annotations.Annotations(g.GetAnnotations())
	annos.Update(&v2.InsertResourceGrants{})
	g.SetAnnotations(annos)
	return g
}

func newRepairTestStore(ctx context.Context, t *testing.T, path, tmpDir string) c1zstore.Store {
	t.Helper()
	store, err := dotc1z.NewStore(ctx, path,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	return store
}

func TestIngestInvariantI3RepairsLostRelatedResource(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	repo, err := rs.NewResource("Repo r1", equivRepoRT, "r1")
	require.NoError(t, err)
	g := repairTestGrant(t, repo)

	// Previous sync: healthy state — annotated grant AND its resource.
	prev := newRepairTestStore(ctx, t, filepath.Join(tmpDir, "prev.c1z"), tmpDir)
	_, err = prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, prev.PutResourceTypes(ctx, equivRepoRT))
	require.NoError(t, prev.PutResources(ctx, repo))
	require.NoError(t, prev.PutGrants(ctx, g))
	require.NoError(t, prev.EndSync(ctx))

	// Current sync: the replay-loss shape — annotated grant, NO resource.
	cur := newRepairTestStore(ctx, t, filepath.Join(tmpDir, "cur.c1z"), tmpDir)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, cur.PutResourceTypes(ctx, equivRepoRT))
	require.NoError(t, cur.PutGrants(ctx, g))

	s := &syncer{store: cur, syncType: connectorstore.SyncTypeFull}
	prevSC, ok := any(prev).(dotc1z.SourceCacheStore)
	require.True(t, ok)
	s.sourceCache.prev = prevSC
	s.sourceCache.prevReader = prev

	// Strict mode: no repair — the machinery bug must be NAMED, and the
	// failure classified as replay-integrity.
	s.strictIngestionInvariants = true
	err = s.checkGrantResourceReferences(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrReplayIntegrity)

	// Lenient mode: repaired from the previous sync's row, full fidelity.
	s.strictIngestionInvariants = false
	require.NoError(t, s.checkGrantResourceReferences(ctx))
	got, err := cur.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
		ResourceId: repo.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, "Repo r1", got.GetResource().GetDisplayName(),
		"repair must restore the previous sync's full-fidelity row, not a stub")

	// Idempotent: a second pass finds nothing dangling.
	require.NoError(t, s.checkGrantResourceReferences(ctx))

	require.NoError(t, cur.Close(ctx))
	require.NoError(t, prev.Close(ctx))
}

// TestWithoutPreviousSyncForcesCold pins the runners' cold-retry
// mechanism: appending WithoutPreviousSync AFTER a previous-sync path
// option must fully disable replay — the connector sees only lookup
// misses and the sync completes cold.
func TestWithoutPreviousSyncForcesCold(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	mc := newSourceCacheMockConnector()
	res, _, err := mc.AddGroup(ctx, "g1")
	require.NoError(t, err)
	_, err = mc.AddUser(ctx, "u1")
	require.NoError(t, err)
	mc.etagByResource[res.GetId().GetResource()] = "v1"

	// Warm-capable chain: sync 1 stamps, sync 2 (control) replays.
	sync1 := filepath.Join(tmpDir, "s1.c1z")
	runSourceCacheSync(ctx, t, mc, sync1, "", tmpDir)
	sync2 := filepath.Join(tmpDir, "s2.c1z")
	runSourceCacheSync(ctx, t, mc, sync2, sync1, tmpDir)
	require.Positive(t, mc.lookupHits, "control: the warm sync must have replayed")

	// The retry shape: previous path SET, WithoutPreviousSync appended
	// last. The connector must see only misses.
	hitsBefore := mc.lookupHits
	sync3 := filepath.Join(tmpDir, "s3.c1z")
	runSourceCacheSync(ctx, t, mc, sync3, sync1, tmpDir, WithoutPreviousSync())
	require.Equal(t, hitsBefore, mc.lookupHits, "WithoutPreviousSync must force every lookup to miss")
}

func TestIngestInvariantI3UnrepairableFailsWithReplayIntegrity(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	repo, err := rs.NewResource("Repo r1", equivRepoRT, "r1")
	require.NoError(t, err)
	g := repairTestGrant(t, repo)

	// Previous sync exists but is ALSO missing the resource: repair has
	// no source, so even lenient mode must fail — and classify as
	// replay-integrity so the runners' cold retry can take over.
	prev := newRepairTestStore(ctx, t, filepath.Join(tmpDir, "prev.c1z"), tmpDir)
	_, err = prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, prev.PutResourceTypes(ctx, equivRepoRT))
	require.NoError(t, prev.EndSync(ctx))

	cur := newRepairTestStore(ctx, t, filepath.Join(tmpDir, "cur.c1z"), tmpDir)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, cur.PutResourceTypes(ctx, equivRepoRT))
	require.NoError(t, cur.PutGrants(ctx, g))

	s := &syncer{store: cur, syncType: connectorstore.SyncTypeFull}
	prevSC, ok := any(prev).(dotc1z.SourceCacheStore)
	require.True(t, ok)
	s.sourceCache.prev = prevSC
	s.sourceCache.prevReader = prev

	// WARM + unrepairable: replay is implicated → replay-integrity, so
	// the runners retry cold (the attribution experiment).
	err = s.checkGrantResourceReferences(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrReplayIntegrity)

	// COLD (no previous source): replay cannot be the culprit — the
	// error must blame connector data and must NOT carry the replay
	// sentinel, or the runners would burn a pointless cold retry every
	// sync on a recurring connector bug.
	s.sourceCache.prev = nil
	s.sourceCache.prevReader = nil
	err = s.checkGrantResourceReferences(ctx)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrReplayIntegrity)
	require.Contains(t, err.Error(), "COLD sync")

	require.NoError(t, cur.Close(ctx))
	require.NoError(t, prev.Close(ctx))
}
