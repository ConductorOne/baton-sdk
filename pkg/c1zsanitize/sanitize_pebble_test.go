package c1zsanitize

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	sdksync "github.com/conductorone/baton-sdk/pkg/sync"
	"github.com/conductorone/baton-sdk/pkg/sync/expand"
)

type writerWithoutPageLedger struct {
	connectorstore.Writer
}

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
		src := newEngineStore(t, ctx, srcPath, c1zstore.EnginePebble)
		// Reuse the shared fixture, then stamp the supports_diff marker.
		buildParityFixture(t, ctx, src)
		runs, _, err := src.(syncRunMetadataReader).ListSyncRuns(ctx, "", 100)
		require.NoError(t, err)
		require.Len(t, runs, 1, "a pebble c1z holds exactly one sync")
		srcSyncID = runs[0].ID
		require.NoError(t, src.(supportsDiffWriter).SetSupportsDiff(ctx, srcSyncID))
		require.NoError(t, src.Close(ctx))
	}()

	src := openEngineStoreRO(t, ctx, srcPath)
	dst := newEngineStore(t, ctx, dstPath, c1zstore.EnginePebble)
	require.NoError(t, Sanitize(ctx, src, dst, Options{Secret: secret, TimestampAnchor: fixedAnchor}))
	require.NoError(t, dst.Close(ctx))
	require.NoError(t, src.Close(ctx))

	ro := openEngineStoreRO(t, ctx, dstPath)
	defer ro.Close(ctx)

	// Cardinality preserved.
	require.Equal(t, 2, resourceTypeCount(t, ctx, ro))
	require.Equal(t, 6, resourceCount(t, ctx, ro))
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

	// Stats sidecar matches the listed cardinalities.
	eng, ok := pebble.AsEngine(ro)
	require.True(t, ok, "pebble output must expose its engine")
	stats, err := eng.Stats(ctx, connectorstore.SyncTypeAny, dstRuns[0].ID)
	require.NoError(t, err)
	require.Equal(t, int64(2), stats["resource_types"])
	require.Equal(t, int64(6), stats["resources"])
	require.Equal(t, int64(2), stats["entitlements"])
	require.Equal(t, int64(5), stats["grants"])
	require.Equal(t, int64(1), stats["assets"])
}

func TestSanitizeDropsEntitlementGraphSidecar(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.c1z")
	dstPath := filepath.Join(dir, "dst.c1z")

	src := newEngineStore(t, ctx, srcPath, c1zstore.EnginePebble)
	buildParityFixture(t, ctx, src)
	runs, _, err := src.(syncRunMetadataReader).ListSyncRuns(ctx, "", 100)
	require.NoError(t, err)
	require.Len(t, runs, 1)
	syncID := runs[0].ID
	digest, found, err := src.(c1zstore.GrantGenerationDigestReader).GrantGenerationDigest(ctx)
	require.NoError(t, err)
	require.True(t, found)
	graph := expand.NewEntitlementGraph(ctx)
	graph.MarkExpansionComplete()
	graph.Loaded = true
	graph.HasNoCycles = true
	blob, err := expand.MarshalGraphBlobWithGrantDigest(syncID, graph, digest)
	require.NoError(t, err)
	require.NoError(t, src.(sdksync.EntitlementGraphStore).PutEntitlementGraphBlob(ctx, blob))
	require.NoError(t, src.Close(ctx))

	source := openEngineStoreRO(t, ctx, srcPath)
	destination := newEngineStore(t, ctx, dstPath, c1zstore.EnginePebble)
	require.NoError(t, Sanitize(ctx, source, destination, Options{Secret: bytes32("graph-sidecar"), TimestampAnchor: fixedAnchor}))
	require.NoError(t, destination.Close(ctx))
	require.NoError(t, source.Close(ctx))

	ro := openEngineStoreRO(t, ctx, dstPath)
	dstRuns, _, err := ro.(syncRunMetadataReader).ListSyncRuns(ctx, "", 100)
	require.NoError(t, err)
	require.Len(t, dstRuns, 1)
	got, err := sdksync.GraphFromStore(ctx, ro, dstRuns[0].ID)
	require.NoError(t, err)
	require.Nil(t, got, "sanitize changes identities, so it must not carry the source graph")
	require.NoError(t, ro.Close(ctx))
}

// TestSanitizeMultiSyncIntoPebbleIsRejected proves the live-dst guard: a
// source whose ListSyncs reports more than one sync (a v1 c1z read through
// pkg/dotc1z/sqlite, for example) cannot be sanitized into a single-sync
// Pebble destination. The Pebble reader scopes ListSyncs to the current
// sync, so the multi-sync shape is a reader double over a real store.
func TestSanitizeMultiSyncIntoPebbleIsRejected(t *testing.T) {
	ctx := context.Background()
	secret := bytes32("multisync-guard")
	tmp := t.TempDir()

	srcPath := filepath.Join(tmp, "src.c1z")
	buildSyncFixture(t, ctx, srcPath, 1)

	src := openEngineStoreRO(t, ctx, srcPath)
	dst := newEngineStore(t, ctx, filepath.Join(tmp, "dst-pebble.c1z"), c1zstore.EnginePebble)
	err := Sanitize(ctx, multiSyncReader{Store: src}, dst, Options{Secret: secret, TimestampAnchor: fixedAnchor})
	require.Error(t, err, "multi-sync source into a pebble destination must be rejected")
	require.Contains(t, err.Error(), "exactly one source sync")
	_ = dst.Close(ctx)
	require.NoError(t, src.Close(ctx))
}

// multiSyncReader adds a second, distinct sync to ListSyncs.
type multiSyncReader struct {
	c1zstore.Store
}

func (r multiSyncReader) ListSyncs(ctx context.Context, req *reader_v2.SyncsReaderServiceListSyncsRequest) (*reader_v2.SyncsReaderServiceListSyncsResponse, error) {
	resp, err := r.Store.ListSyncs(ctx, req)
	if err != nil {
		return nil, err
	}
	syncs := resp.GetSyncs()
	extra := proto.Clone(syncs[0]).(*reader_v2.SyncRun)
	extra.SetId(syncs[0].GetId() + "-second")
	resp.SetSyncs(append(append([]*reader_v2.SyncRun{}, syncs...), extra))
	return resp, nil
}

func TestSanitizeRejectsPebbleDestinationWithoutPageLedgerBeforeMutation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.c1z")
	buildSyncFixture(t, ctx, srcPath, 1)

	src := mustOpen(t, ctx, srcPath, true)
	dst := newEngineStore(t, ctx, filepath.Join(dir, "destination.c1z"), c1zstore.EnginePebble)
	err := Sanitize(ctx, src, writerWithoutPageLedger{Writer: dst}, Options{
		Secret: bytes32("non-ledger-destination"), TimestampAnchor: fixedAnchor,
	})
	require.ErrorContains(t, err, "page-ledger store")
	runs, _, err := dst.(syncRunMetadataReader).ListSyncRuns(ctx, "", 2)
	require.NoError(t, err)
	require.Empty(t, runs)
	require.NoError(t, dst.Close(ctx))
	require.NoError(t, src.Close(ctx))
}
