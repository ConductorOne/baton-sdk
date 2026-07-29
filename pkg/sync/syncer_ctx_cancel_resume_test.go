package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/stretchr/testify/require"
)

func newCancelResumeConnector(ctx context.Context, resourceTypeCount int) *mockConnector {
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
		if err != nil {
			panic(err)
		}
		mc.AddResource(ctx, user)
	}
	return mc
}

func assertResumedSingleRun(t *testing.T, bgCtx context.Context, c1zPath, tmpDir string, resourceTypeCount int) {
	t.Helper()

	resumed, err := NewSyncer(bgCtx, newCancelResumeConnector(bgCtx, resourceTypeCount),
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

// TestSyncResumesAfterLoopBoundaryCancellation covers a cancellation observed
// between actions, at the top of parallelSync's loop -- e.g. a SIGTERM
// landing during the brief window after one action's connector calls have
// all returned but before the next has started. This is the shape
// WithRunDuration's own deadline always took (still covered separately by
// TestCleanupContextDeadlineExceeded), but here the cancellation is a direct
// cancel() on the top-level ctx, not a runCtx-only deadline -- so a forced
// checkpoint that ran on the already-cancelled ctx instead of a
// context.WithoutCancel copy of it would fail silently and this test would
// catch that.
//
// It does NOT cover a cancellation observed mid-action, inside a connector
// call -- see TestSyncResumesAfterMidActionCancellation for that.
func TestSyncResumesAfterLoopBoundaryCancellation(t *testing.T) {
	const resourceTypeCount = 10

	tmpDir := t.TempDir()
	c1zPath := filepath.Join(tmpDir, "cancel-resume-boundary.c1z")
	bgCtx := context.Background()

	cancelCtx, cancel := context.WithCancel(bgCtx)
	var seen atomic.Int64
	syncer, err := NewSyncer(cancelCtx, newCancelResumeConnector(bgCtx, resourceTypeCount),
		WithC1ZPath(c1zPath),
		WithTmpDir(tmpDir),
		WithStorageEngine(c1zstore.EnginePebble),
		WithProgressHandler(func(p *Progress) {
			// mockConnector.ListResources/ListResourceTypes ignore ctx, so
			// this cancellation is only ever observed at the next
			// top-of-loop select, between actions -- not mid-call.
			if seen.Add(1) == 3 {
				cancel()
			}
		}),
	)
	require.NoError(t, err)

	err = syncer.Sync(cancelCtx)
	require.ErrorIs(t, err, ErrSyncNotComplete, "a direct ctx cancellation must be treated as resumable, not a hard failure")
	require.NoError(t, syncer.Close(bgCtx))

	assertResumedSingleRun(t, bgCtx, c1zPath, tmpDir, resourceTypeCount)
}

// cancelingListResourcesConnector wraps mockConnector so that ListResources
// -- the per-resource-type connector call that SyncResourcesOp's fan-out
// (syncParallel) makes once per queued action -- observes cancellation
// exactly like a real connector RPC would: it fails with the ctx's own
// error once the context is cancelled mid-batch, instead of the base mock's
// ctx-blind behavior. This is what lets the test below land the
// cancellation *inside* an in-flight action rather than only ever at a
// clean action boundary.
type cancelingListResourcesConnector struct {
	*mockConnector
	callsBeforeCancel int
	calls             atomic.Int64
	cancel            context.CancelFunc
}

func (c *cancelingListResourcesConnector) ListResources(
	ctx context.Context,
	in *v2.ResourcesServiceListResourcesRequest,
	opts ...grpc.CallOption,
) (*v2.ResourcesServiceListResourcesResponse, error) {
	if int(c.calls.Add(1)) > c.callsBeforeCancel {
		c.cancel()
		return nil, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.mockConnector.ListResources(ctx, in, opts...)
}

// TestSyncResumesAfterMidActionCancellation covers a cancellation that lands
// *inside* an in-flight action's connector call -- the shape a SIGTERM
// actually takes for a long fanned-out batch (e.g. a grants sync spanning
// hours), rather than the narrow window between actions. Before the
// finishOnCancellation routing in parallelSync's per-op return points, this
// error propagated raw (return warnings, err) with no forced checkpoint,
// and IsSyncPreservable did not recognize it as resumable.
func TestSyncResumesAfterMidActionCancellation(t *testing.T) {
	const resourceTypeCount = 10

	tmpDir := t.TempDir()
	c1zPath := filepath.Join(tmpDir, "cancel-resume-mid-action.c1z")
	bgCtx := context.Background()

	cancelCtx, cancel := context.WithCancel(bgCtx)
	connector := &cancelingListResourcesConnector{
		mockConnector:     newCancelResumeConnector(bgCtx, resourceTypeCount),
		callsBeforeCancel: 3,
		cancel:            cancel,
	}
	syncer, err := NewSyncer(cancelCtx, connector,
		WithC1ZPath(c1zPath),
		WithTmpDir(tmpDir),
		WithStorageEngine(c1zstore.EnginePebble),
	)
	require.NoError(t, err)

	err = syncer.Sync(cancelCtx)
	require.ErrorIs(t, err, ErrSyncNotComplete, "a cancellation surfaced mid-action must still be treated as resumable, not a hard failure")
	require.NoError(t, syncer.Close(bgCtx))

	assertResumedSingleRun(t, bgCtx, c1zPath, tmpDir, resourceTypeCount)
}
