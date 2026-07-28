package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/stretchr/testify/require"
)

// TestSyncResumesAfterDirectContextCancellation covers the SIGTERM/SIGINT
// shutdown path (connectorrunner.ErrSigTerm), which cancels the *top-level*
// ctx directly -- unlike WithRunDuration, whose deadline lives on a runCtx
// derived from a still-live parent. TestCleanupContextDeadlineExceeded only
// exercises the latter, so it can't catch a regression in this path: a
// forced checkpoint that runs on the already-cancelled ctx instead of a
// context.WithoutCancel copy of it.
//
// This must still (a) force a checkpoint before returning and (b) return
// ErrSyncNotComplete, so a second Sync() on the same store resumes instead
// of restarting from scratch.
func TestSyncResumesAfterDirectContextCancellation(t *testing.T) {
	const resourceTypeCount = 10

	newConnector := func(ctx context.Context) *mockConnector {
		mc := newMockConnector()
		for i := range resourceTypeCount {
			rt := v2.ResourceType_builder{
				Id:          fmt.Sprintf("rt%d", i),
				DisplayName: fmt.Sprintf("Type %d", i),
				Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER},
				Annotations: annotations.New(&v2.SkipEntitlementsAndGrants{}),
			}.Build()
			mc.AddResourceType(ctx, rt)
			user, err := rs.NewUserResource(fmt.Sprintf("u%d", i), rt, fmt.Sprintf("u%d", i), nil,
				rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}))
			require.NoError(t, err)
			mc.AddResource(ctx, user)
		}
		return mc
	}

	tmpDir := t.TempDir()
	c1zPath := filepath.Join(tmpDir, "cancel-resume.c1z")
	bgCtx := context.Background()

	cancelCtx, cancel := context.WithCancel(bgCtx)
	seen := 0
	syncer, err := NewSyncer(cancelCtx, newConnector(bgCtx),
		WithC1ZPath(c1zPath),
		WithTmpDir(tmpDir),
		WithStorageEngine(c1zstore.EnginePebble),
		WithProgressHandler(func(p *Progress) {
			seen++
			// Cancel partway through the resource-type sweep: some
			// resource types synced, some not -- a real SIGTERM landing
			// mid-sync rather than at a clean action boundary.
			if seen == 3 {
				cancel()
			}
		}),
	)
	require.NoError(t, err)

	err = syncer.Sync(cancelCtx)
	require.ErrorIs(t, err, ErrSyncNotComplete, "a direct ctx cancellation must be treated as resumable, not a hard failure")
	require.NoError(t, syncer.Close(bgCtx))

	// Fresh syncer, same store, uncancelled context: must resume the
	// interrupted sync rather than silently starting a new one.
	resumed, err := NewSyncer(bgCtx, newConnector(bgCtx),
		WithC1ZPath(c1zPath),
		WithTmpDir(tmpDir),
		WithStorageEngine(c1zstore.EnginePebble),
	)
	require.NoError(t, err)
	require.NoError(t, resumed.Sync(bgCtx))
	require.NoError(t, resumed.Close(bgCtx))

	store, err := dotc1z.NewStore(bgCtx, c1zPath, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(tmpDir))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(bgCtx)) }()

	lister, ok := store.(interface {
		ListSyncRuns(ctx context.Context, pageToken string, pageSize uint32) ([]*c1zstore.SyncRun, string, error)
	})
	require.True(t, ok)
	runs, _, err := lister.ListSyncRuns(bgCtx, "", 100)
	require.NoError(t, err)
	require.Len(t, runs, 1, "resume must seal the interrupted sync run, not start a second one")

	rresp, err := store.ListResources(bgCtx, v2.ResourcesServiceListResourcesRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, rresp.GetList(), resourceTypeCount, "every resource type's resource must be present after resume")
}
