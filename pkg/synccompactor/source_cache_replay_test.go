package synccompactor

// Compacted artifacts and source-cache replay.
//
// The production compaction paths (fold's keep-newer MergeInto, the
// overlay/kway rebuild) are UPSERT-merges: base rows the newer sync
// didn't touch survive, and deletions don't propagate. A compacted
// artifact is therefore NOT a faithful snapshot of its newest input
// sync. Carrying the newest sync's source-cache manifest entries into
// such an artifact would be dangerous: a future sync could 304-validate
// a scope against the carried etag and replay a row set that silently
// includes resurrected base rows — stale data presented as current.
//
// The safe semantic — and the one these tests PIN — is that compacted
// artifacts carry NO source-cache manifest entries: every lookup against
// a compacted replay source misses, the connector fetches fresh, and the
// sync is cold-correct. (The IngestAndExcise compactor in
// pkg/synccompactor/pebble replaces whole key ranges and could preserve
// entries soundly — see its bucket_plans.go — but it is not the
// production path.)
//
// Consequence worth knowing operationally: if the previous-sync c1z fed
// to a syncer is a compaction artifact, ETag replay silently never
// engages. Feed raw sync files to WithPreviousSyncC1ZPath when replay
// matters.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	pebbleengine "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

var (
	scGroupRT = v2.ResourceType_builder{Id: "group", DisplayName: "Group", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP}}.Build()
	scUserRT  = v2.ResourceType_builder{Id: "user", DisplayName: "User", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build()
)

// writeScopedSyncFile writes one finished Pebble sync whose grants are
// stamped with scope and whose manifest carries (grants, scope, etag).
func writeScopedSyncFile(ctx context.Context, t *testing.T, path, tmpDir, scope, etag string, memberIDs []string) string {
	t.Helper()
	store, err := dotc1z.NewStore(ctx, path,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)

	syncID, isNew, err := store.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.True(t, isNew)

	group, err := rs.NewGroupResource("g1", scGroupRT, "g1", nil)
	require.NoError(t, err)
	require.NoError(t, store.PutResourceTypes(ctx, scGroupRT, scUserRT))
	require.NoError(t, store.PutResources(ctx, group))

	grants := make([]*v2.Grant, 0, len(memberIDs))
	for _, uid := range memberIDs {
		user, err := rs.NewUserResource(uid, scUserRT, uid, nil)
		require.NoError(t, err)
		require.NoError(t, store.PutResources(ctx, user))
		grants = append(grants, gt.NewGrant(group, "member", user))
	}
	putCtx := sourcecache.WithScope(ctx, scope)
	require.NoError(t, store.PutGrants(putCtx, grants...))

	scStore, ok := any(store).(dotc1z.SourceCacheStore)
	require.True(t, ok)
	require.NoError(t, scStore.PutSourceCacheEntry(ctx, sourcecache.RowKindGrants, scope, etag))

	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))
	return syncID
}

// TestCompactedFileIsColdReplaySource pins the compaction/replay safety
// contract for both production Pebble modes: the compacted output must
// carry NO source-cache manifest entries, so a syncer replaying from it
// misses every lookup and goes cold — never a 304 against a merged row
// set that upsert-merge semantics cannot guarantee matches the etag.
func TestCompactedFileIsColdReplaySource(t *testing.T) {
	for _, mode := range []PebbleCompactorMode{PebbleCompactorModeFold, PebbleCompactorModeOverlay} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := t.Context()
			tmpDir := t.TempDir()
			scope := sourcecache.HashScope("mock://grants/g1")

			olderPath := filepath.Join(tmpDir, "older.c1z")
			newerPath := filepath.Join(tmpDir, "newer.c1z")
			olderID := writeScopedSyncFile(ctx, t, olderPath, tmpDir, scope, "v1", []string{"u1", "u2"})
			newerID := writeScopedSyncFile(ctx, t, newerPath, tmpDir, scope, "v2", []string{"u1", "u3"})

			// Sanity: the inputs DO carry manifest entries, or the
			// empty-output assertion below tests nothing.
			for _, p := range []string{olderPath, newerPath} {
				in, err := dotc1z.NewStore(ctx, p, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(tmpDir))
				require.NoError(t, err)
				holder, ok := any(in).(interface{ PebbleEngine() *pebbleengine.Engine })
				require.True(t, ok)
				manifest, err := holder.PebbleEngine().SourceCacheManifestSnapshot(ctx)
				require.NoError(t, err)
				require.Len(t, manifest, 1, "sanity: input %s must carry its manifest entry", p)
				require.NoError(t, in.Close(ctx))
			}

			outDir := filepath.Join(tmpDir, "out")
			require.NoError(t, os.MkdirAll(outDir, 0o755))
			compactor, cleanup, err := NewCompactor(ctx, outDir, []*CompactableSync{
				{FilePath: olderPath, SyncID: olderID},
				{FilePath: newerPath, SyncID: newerID},
			}, WithTmpDir(tmpDir), WithPebbleCompactorMode(mode))
			require.NoError(t, err)
			defer func() { require.NoError(t, cleanup()) }()
			out, err := compactor.Compact(ctx)
			require.NoError(t, err)
			require.NotNil(t, out)

			compacted, err := dotc1z.NewStore(ctx, out.FilePath,
				dotc1z.WithReadOnly(true),
				dotc1z.WithTmpDir(tmpDir),
			)
			require.NoError(t, err)
			defer func() { _ = compacted.Close(ctx) }()
			run, err := compacted.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
			require.NoError(t, err)
			require.NotNil(t, run)
			require.NoError(t, compacted.SetCurrentSync(ctx, run.ID))

			// Provenance metadata: the artifact must SAY it was
			// compacted, so orchestrators (and the syncer's replay gate)
			// can answer "can this be used for replay?" without
			// inspecting keyspaces. The answer is no.
			require.True(t, run.Compacted,
				"compaction must stamp the output sync run's compacted flag")

			// The safety property: NO manifest entries survive, so a
			// syncer using this artifact as its previous sync misses
			// every lookup (cold-correct), never replays a merged row
			// set against a carried validator.
			holder, ok := any(compacted).(interface{ PebbleEngine() *pebbleengine.Engine })
			require.True(t, ok, "compacted output must be a pebble store")
			manifest, err := holder.PebbleEngine().SourceCacheManifestSnapshot(ctx)
			require.NoError(t, err)
			require.Empty(t, manifest,
				"compacted artifacts must carry no source-cache manifest entries: upsert-merge output cannot be safely validated by any input's etag")

			scStore, ok := any(compacted).(dotc1z.SourceCacheStore)
			require.True(t, ok)
			_, found, err := scStore.LookupSourceCacheEntry(ctx, sourcecache.RowKindGrants, scope)
			require.NoError(t, err)
			require.False(t, found, "a lookup against a compacted replay source must miss")

			// The merged DATA is still there (upsert semantics: newest
			// wins per key; base-only rows survive).
			grants, err := compacted.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
			require.NoError(t, err)
			ids := map[string]bool{}
			for _, g := range grants.GetList() {
				ids[g.GetId()] = true
			}
			for _, uid := range []string{"u1", "u3"} {
				require.True(t, ids[fmt.Sprintf("group:g1:member:user:%s", uid)],
					"newest sync's row for %s must be present", uid)
			}
		})
	}
}
