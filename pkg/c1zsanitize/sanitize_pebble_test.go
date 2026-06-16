package c1zsanitize

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// TestSanitizePebbleEndToEnd is the core-invariant check on a
// Pebble-in -> Pebble-out run: identity is stripped, cardinalities are
// preserved across all entity kinds and assets, and the supports_diff marker
// carries over.
func TestSanitizePebbleEndToEnd(t *testing.T) {
	ctx := context.Background()
	secret := bytes32("pebble-e2e")
	tmp := t.TempDir()
	srcPath := filepath.Join(tmp, "src.c1z")
	dstPath := filepath.Join(tmp, "dst.c1z")

	var srcSyncID string
	func() {
		src := newEngineStore(t, ctx, srcPath, dotc1z.EnginePebble)
		// Reuse the shared fixture, then mark the sync diff-capable.
		buildParityFixture(t, ctx, src)
		runs, _, err := src.(syncRunMetadataReader).ListSyncRuns(ctx, "", 100)
		require.NoError(t, err)
		require.Len(t, runs, 1, "a pebble c1z holds exactly one sync")
		srcSyncID = runs[0].ID
		require.NoError(t, src.(supportsDiffWriter).SetSupportsDiff(ctx, srcSyncID))
		require.NoError(t, src.Close(ctx))
	}()

	src := openEngineStoreRO(t, ctx, srcPath)
	dst := newEngineStore(t, ctx, dstPath, dotc1z.EnginePebble)
	require.NoError(t, Sanitize(ctx, src, dst, Options{Secret: secret, TimestampAnchor: fixedAnchor}))
	require.NoError(t, dst.Close(ctx))
	require.NoError(t, src.Close(ctx))

	ro := openEngineStoreRO(t, ctx, dstPath)
	defer ro.Close(ctx)

	// Cardinality preserved.
	require.Equal(t, 2, resourceTypeCount(t, ctx, ro))
	require.Equal(t, 2, resourceCount(t, ctx, ro))
	require.Equal(t, 2, entitlementCount(t, ctx, ro))
	require.Equal(t, 5, grantCount(t, ctx, ro))

	// Identity stripped: the source resource id/display must not survive; the
	// transformed id must be present.
	resp, err := ro.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{PageSize: 100}.Build())
	require.NoError(t, err)
	wantAlice := SanitizeID(secret, "alice")
	var sawTransformed bool
	for _, r := range resp.GetList() {
		require.NotEqual(t, "alice", r.GetId().GetResource(), "raw source resource id must not survive")
		require.NotEqual(t, "Alice", r.GetDisplayName(), "raw source display name must not survive")
		if r.GetId().GetResource() == wantAlice {
			sawTransformed = true
		}
	}
	require.True(t, sawTransformed, "the transformed resource id must be present")

	// supports_diff carried over to the sanitized sync.
	dstRuns, _, err := ro.(syncRunMetadataReader).ListSyncRuns(ctx, "", 100)
	require.NoError(t, err)
	require.Len(t, dstRuns, 1)
	require.True(t, dstRuns[0].SupportsDiff, "supports_diff marker must carry to the pebble output")
}

// TestSanitizeMultiSyncIntoPebbleIsRejected proves the live-dst guard: a
// multi-sync SQLite source cannot be sanitized into a single-sync Pebble
// destination, while a single-sync source into Pebble succeeds.
func TestSanitizeMultiSyncIntoPebbleIsRejected(t *testing.T) {
	ctx := context.Background()
	secret := bytes32("multisync-guard")
	tmp := t.TempDir()

	// Multi-sync SQLite source: two independent finished full syncs.
	multiPath := filepath.Join(tmp, "multi.c1z")
	func() {
		f, err := dotc1z.NewC1ZFile(ctx, multiPath)
		require.NoError(t, err)
		for i := 0; i < 2; i++ {
			_, err = f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			require.NoError(t, f.PutResourceTypes(ctx,
				v2.ResourceType_builder{Id: "user", DisplayName: "User", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build()))
			require.NoError(t, f.EndSync(ctx))
		}
		require.NoError(t, f.Close(ctx))
	}()

	src := openEngineStoreRO(t, ctx, multiPath)
	dst := newEngineStore(t, ctx, filepath.Join(tmp, "dst-pebble.c1z"), dotc1z.EnginePebble)
	err := Sanitize(ctx, src, dst, Options{Secret: secret, TimestampAnchor: fixedAnchor})
	require.Error(t, err, "multi-sync source into a pebble destination must be rejected")
	require.Contains(t, err.Error(), "pebble holds exactly one sync")
	_ = dst.Close(ctx)
	require.NoError(t, src.Close(ctx))

	// Sanity: the same multi-sync source into a SQLite destination is allowed.
	src2 := openEngineStoreRO(t, ctx, multiPath)
	dstSQLite := newEngineStore(t, ctx, filepath.Join(tmp, "dst-sqlite.c1z"), dotc1z.EngineSQLite)
	require.NoError(t, Sanitize(ctx, src2, dstSQLite, Options{Secret: secret, TimestampAnchor: fixedAnchor}))
	require.NoError(t, dstSQLite.Close(ctx))
	require.NoError(t, src2.Close(ctx))
}

// TestSanitizeResumableIntoPebbleIsRejected proves the resumable guard is an
// explicit destination-engine check, not a type assertion. Pebble now
// implements ListSyncRuns (added for source-side metadata reads), so the prior
// dst.(dstSyncLister) assertion no longer rejects it; resume into a pebble
// destination must still fail closed because pebble cannot rehydrate a
// checkpoint.
func TestSanitizeResumableIntoPebbleIsRejected(t *testing.T) {
	ctx := context.Background()
	secret := bytes32("resumable-pebble-guard")
	tmp := t.TempDir()
	srcPath := filepath.Join(tmp, "src.c1z")
	dstPath := filepath.Join(tmp, "dst.c1z")

	func() {
		src := newEngineStore(t, ctx, srcPath, dotc1z.EnginePebble)
		buildParityFixture(t, ctx, src)
		require.NoError(t, src.Close(ctx))
	}()

	src := openEngineStoreRO(t, ctx, srcPath)
	dst := newEngineStore(t, ctx, dstPath, dotc1z.EnginePebble)
	err := Sanitize(ctx, src, dst, Options{Secret: secret, TimestampAnchor: fixedAnchor, Resumable: true})
	require.Error(t, err, "resumable sanitize into a pebble destination must be rejected")
	require.Contains(t, err.Error(), "pebble destination")
	_ = dst.Close(ctx)
	require.NoError(t, src.Close(ctx))

	// Control: the same source is accepted into a sqlite destination.
	src2 := openEngineStoreRO(t, ctx, srcPath)
	dstSQLite := newEngineStore(t, ctx, filepath.Join(tmp, "dst-sqlite.c1z"), dotc1z.EngineSQLite)
	require.NoError(t, Sanitize(ctx, src2, dstSQLite, Options{Secret: secret, TimestampAnchor: fixedAnchor, Resumable: true}))
	require.NoError(t, dstSQLite.Close(ctx))
	require.NoError(t, src2.Close(ctx))
}

// TestSanitizeRealExpanderEngineParity hardens the first-write precondition
// against being subtly incomplete: it sanitizes a c1z whose grants were
// produced by the REAL expander (not hand-built) into both a SQLite and a
// Pebble destination and asserts the needs_expansion enumeration, the full
// GrantExpandable blobs, and the expansion-source edges are identical across
// the two engines.
func TestSanitizeRealExpanderEngineParity(t *testing.T) {
	ctx := context.Background()
	secret := bytes32("real-expander-parity")
	tmp := t.TempDir()

	// Build a nested expandable graph and run the production expander over it.
	srcPath := filepath.Join(tmp, "expanded.c1z")
	syncID := buildNestedExpandableC1Z(t, ctx, srcPath)
	expandViaRealSyncer(t, ctx, srcPath, syncID)

	sanitizeTo := func(eng dotc1z.Engine, name string) (map[string]expandBlob, map[string][]string, int) {
		dstPath := filepath.Join(tmp, "dst-"+name+".c1z")
		src := openEngineStoreRO(t, ctx, srcPath)
		dst := newEngineStore(t, ctx, dstPath, eng)
		require.NoError(t, Sanitize(ctx, src, dst, Options{Secret: secret, TimestampAnchor: fixedAnchor}))
		require.NoError(t, dst.Close(ctx))
		require.NoError(t, src.Close(ctx))

		ro := openEngineStoreRO(t, ctx, dstPath)
		defer ro.Close(ctx)
		return pendingExpansionBlobs(t, ctx, ro), grantSourcesCanonical(t, ctx, ro), grantCount(t, ctx, ro)
	}

	sqlBlobs, sqlSources, sqlGrants := sanitizeTo(dotc1z.EngineSQLite, "sqlite")
	pebBlobs, pebSources, pebGrants := sanitizeTo(dotc1z.EnginePebble, "pebble")

	require.Equal(t, sqlGrants, pebGrants, "real-expander grant count must match across engines")
	require.Greater(t, sqlGrants, 4, "the expander must have derived additional grants")
	require.Equal(t, sqlBlobs, pebBlobs, "needs_expansion enumeration + GrantExpandable blobs must match across engines")
	require.Equal(t, sqlSources, pebSources, "expansion-source edges must match across engines")
	require.NotEmpty(t, sqlSources, "real-expander output must carry derived grant sources")
}
