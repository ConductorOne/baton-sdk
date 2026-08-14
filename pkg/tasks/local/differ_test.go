package local

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	pebbleengine "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

// TestDifferRejectsPebbleArtifactWithGuidance pins the behavior change
// from flipping the default engine to pebble: a default-engine (v3)
// artifact holds a single sync, so the diff-syncs workflow is
// structurally impossible against it. The differ must surface
// ErrDiffUnsupported wrapped with remediation (use --storage-engine
// sqlite) rather than a bare engine error.
func TestDifferRejectsPebbleArtifactWithGuidance(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "pebble.c1z")

	// Engine-less NewStore is the exact path connector syncs take; with
	// the pebble default this produces a v3 artifact.
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))

	mgr := NewDiffer(ctx, path, syncID, syncID)
	err = mgr.Process(ctx, nil, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, pebbleengine.ErrDiffUnsupported)
	require.Contains(t, err.Error(), "--storage-engine sqlite",
		"differ error must tell the operator how to get a diffable artifact")
}

// TestDifferSucceedsOnSQLiteArtifact guards the surviving diff-syncs
// path: a v1 (sqlite) c1z with two ended syncs still diffs after the
// default-engine flip, because the differ's engine-less open dispatches
// on the on-disk magic byte rather than the new pebble default.
func TestDifferSucceedsOnSQLiteArtifact(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "sqlite.c1z")

	store, err := dotc1z.NewC1ZFile(ctx, path)
	require.NoError(t, err)

	putResource := func(id string) {
		t.Helper()
		require.NoError(t, store.PutResources(ctx, v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: id}.Build(),
		}.Build()))
	}

	baseSyncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, store.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user"}.Build()))
	putResource("alice")
	require.NoError(t, store.EndSync(ctx))

	appliedSyncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, store.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user"}.Build()))
	putResource("alice")
	putResource("bob")
	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))

	mgr := NewDiffer(ctx, path, baseSyncID, appliedSyncID)
	require.NoError(t, mgr.Process(ctx, nil, nil))

	// The diff sync must have landed next to the two originals.
	reopened, err := dotc1z.NewC1ZFile(ctx, path)
	require.NoError(t, err)
	defer func() { _ = reopened.Close(ctx) }()
	runs, _, err := reopened.ListSyncRuns(ctx, "", 100)
	require.NoError(t, err)
	require.Len(t, runs, 3, "expected base + applied + generated diff sync")
}
