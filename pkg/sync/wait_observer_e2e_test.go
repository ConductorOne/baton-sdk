package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/ratelimit"
	"github.com/conductorone/baton-sdk/pkg/retry"
)

// gateSimConnector simulates the hosted connector manager's rate-limit gate:
// it reports a sleep via ratelimit.ObserveWait from *inside* a connector call,
// using only the context the syncer passed in. This proves end-to-end that the
// observer installed at the top of Syncer.Sync survives the syncer's action
// loop and call plumbing to reach gate code, mirroring the prod topology where
// c1's ConnectorWrapper.ratelimitDo receives the syncer's context in-process.
type gateSimConnector struct {
	*mockConnector
	gateWait  time.Duration
	gateCalls int
}

func (g *gateSimConnector) ListGrants(ctx context.Context, in *v2.GrantsServiceListGrantsRequest, opts ...grpc.CallOption) (*v2.GrantsServiceListGrantsResponse, error) {
	resourceType := in.GetResource().GetId().GetResourceType()
	ratelimit.ObserveWait(retry.WithWaitLabel(ctx, resourceType), g.gateWait)
	g.gateCalls++
	return g.mockConnector.ListGrants(ctx, in, opts...)
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

	gc := &gateSimConnector{mockConnector: mc, gateWait: 250 * time.Millisecond}

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
	expectedMs := int64(gc.gateCalls) * gc.gateWait.Milliseconds()
	require.EqualValues(t, expectedMs, durations["rate_limit_wait"],
		"every gate-reported wait must land in rate_limit_wait")

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
