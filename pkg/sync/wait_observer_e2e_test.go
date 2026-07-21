package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/ratelimit"
	"github.com/conductorone/baton-sdk/pkg/retry"
)

// gateSimConnector simulates the two connector-side rate-limit wait sources:
//   - the hosted connector manager's gate, which reports a sleep via
//     ratelimit.ObserveWait from *inside* a connector call using only the
//     context the syncer passed in (proving the observer installed at the top
//     of Syncer.Sync survives the syncer's action loop and call plumbing,
//     mirroring prod where c1's ConnectorWrapper.ratelimitDo receives the
//     syncer's context in-process), and
//   - a connector that slept in its own process (client-side prevention /
//     in-SDK backoff) and reports it via a RateLimitWaitReport response
//     annotation, which works across process/lambda boundaries.
type gateSimConnector struct {
	*mockConnector
	gateWait     time.Duration
	annotationMs int64
	gateCalls    int
}

func (g *gateSimConnector) ListGrants(ctx context.Context, in *v2.GrantsServiceListGrantsRequest, opts ...grpc.CallOption) (*v2.GrantsServiceListGrantsResponse, error) {
	resourceType := in.GetResource().GetId().GetResourceType()
	ratelimit.ObserveWait(retry.WithWaitLabel(ctx, resourceType), g.gateWait)
	g.gateCalls++
	resp, err := g.mockConnector.ListGrants(ctx, in, opts...)
	if err != nil {
		return nil, err
	}
	var annos annotations.Annotations = resp.GetAnnotations()
	annos.WithRateLimitWaitReport(g.annotationMs)
	resp.SetAnnotations(annos)
	return resp, nil
}

func TestRateLimitGateWaitsReachSyncStats(t *testing.T) {
	ctx := t.Context()
	tempDir := t.TempDir()

	store, err := dotc1z.NewStore(ctx, filepath.Join(tempDir, "gate-sim.c1z"),
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tempDir),
	)
	require.NoError(t, err)

	mc := newMockConnector()
	mc.rtDB = append(mc.rtDB, groupResourceType, userResourceType)
	group1, _, err := mc.AddGroup(ctx, "g1")
	require.NoError(t, err)
	u1, err := mc.AddUser(ctx, "u1")
	require.NoError(t, err)
	_ = mc.AddGroupMember(ctx, group1, u1)

	gc := &gateSimConnector{mockConnector: mc, gateWait: 250 * time.Millisecond, annotationMs: 100}

	syncer, err := NewSyncer(ctx, gc,
		WithConnectorStore(store),
		WithTmpDir(tempDir),
	)
	require.NoError(t, err)
	require.NoError(t, syncer.Sync(ctx))

	latest, ok := store.(connectorstore.LatestFinishedSyncIDFetcher)
	require.True(t, ok)
	syncID, err := latest.LatestFinishedSyncID(ctx, connectorstore.SyncTypeFull)
	require.NoError(t, err)
	require.NoError(t, store.SetCurrentSync(ctx, syncID))
	token, err := store.CurrentSyncStep(ctx)
	require.NoError(t, err)
	completedState := newState()
	require.NoError(t, completedState.Unmarshal(token))

	durations := completedState.StepDurations()
	require.Positive(t, gc.gateCalls, "sync should have listed grants")
	// Both sources must land: observer-reported gate sleeps plus
	// annotation-reported in-connector sleeps.
	expectedMs := int64(gc.gateCalls) * (gc.gateWait.Milliseconds() + gc.annotationMs)
	require.EqualValues(t, expectedMs, durations["rate_limit_wait"],
		"every gate-reported and annotation-reported wait must land in rate_limit_wait")

	// Attribution: labeled sub-buckets must decompose the flat total.
	var labeledMs int64
	for bucket, ms := range durations {
		if bucket != "rate_limit_wait" && len(bucket) > len("rate_limit_wait:") && bucket[:len("rate_limit_wait:")] == "rate_limit_wait:" {
			labeledMs += ms
		}
	}
	require.EqualValues(t, expectedMs, labeledMs)

	require.NoError(t, syncer.Close(ctx))
}
