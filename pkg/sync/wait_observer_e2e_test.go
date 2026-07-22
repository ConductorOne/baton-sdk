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
	rtCalls      int
}

// ListResourceTypes attaches a RateLimitWaitReport with no resource-type
// attribution, covering the syncer's wiring on the list-resource-types path
// (which has no action resource type, so the report lands unlabeled).
func (g *gateSimConnector) ListResourceTypes(
	ctx context.Context,
	in *v2.ResourceTypesServiceListResourceTypesRequest,
	opts ...grpc.CallOption,
) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	resp, err := g.mockConnector.ListResourceTypes(ctx, in, opts...)
	if err != nil {
		return nil, err
	}
	g.rtCalls++
	var annos annotations.Annotations = resp.GetAnnotations()
	annos.WithRateLimitWaitReport(g.annotationMs)
	resp.SetAnnotations(annos)
	return resp, nil
}

func (g *gateSimConnector) ListGrants(ctx context.Context, in *v2.GrantsServiceListGrantsRequest, opts ...grpc.CallOption) (*v2.GrantsServiceListGrantsResponse, error) {
	resourceType := in.GetResource().GetId().GetResourceType()
	// Really sleep, then report — like the prod gate. The wall bucket only
	// counts real elapsed time past its watermark, so a claimed-but-not-slept
	// wait would leave rate_limit_wait_wall at whatever the test's incidental
	// runtime happens to be.
	time.Sleep(g.gateWait)
	ratelimit.ObserveWait(ratelimit.WithWaitLabel(ctx, resourceType), ratelimit.WaitEvent{Duration: g.gateWait})
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

	gc := &gateSimConnector{mockConnector: mc, gateWait: 25 * time.Millisecond, annotationMs: 100}

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
	require.Positive(t, gc.rtCalls, "sync should have listed resource types")
	// All three sources must land: observer-reported gate sleeps,
	// annotation-reported in-connector sleeps on grants, and unlabeled
	// annotation reports on list-resource-types.
	grantsMs := int64(gc.gateCalls) * (gc.gateWait.Milliseconds() + gc.annotationMs)
	rtMs := int64(gc.rtCalls) * gc.annotationMs
	require.EqualValues(t, grantsMs+rtMs, durations["rate_limit_wait"],
		"every gate-reported and annotation-reported wait must land in rate_limit_wait")

	// Attribution: labeled sub-buckets must decompose the labeled portion of
	// the flat total; list-resource-types reports carry no resource type and
	// stay unlabeled.
	var labeledMs int64
	for bucket, ms := range durations {
		if bucket != "rate_limit_wait" && len(bucket) > len("rate_limit_wait:") && bucket[:len("rate_limit_wait:")] == "rate_limit_wait:" {
			labeledMs += ms
		}
	}
	require.EqualValues(t, grantsMs, labeledMs)

	// The wall-clock bucket merges overlapping end-anchored intervals. The
	// gate sleeps were real and sequential, so each one's interval lies fully
	// past the watermark and the bucket must cover at least their sum; it can
	// never exceed the cumulative worker-seconds.
	wallMs := durations["rate_limit_wait_wall"]
	require.GreaterOrEqual(t, wallMs, int64(gc.gateCalls)*gc.gateWait.Milliseconds())
	require.LessOrEqual(t, wallMs, grantsMs+rtMs)

	require.NoError(t, syncer.Close(ctx))
}
