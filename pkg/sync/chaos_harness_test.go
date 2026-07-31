package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/types"
)

type chaosTransport int

const (
	chaosTransportDirect chaosTransport = iota
	chaosTransportGRPC
)

func (t chaosTransport) String() string {
	switch t {
	case chaosTransportDirect:
		return "direct"
	case chaosTransportGRPC:
		return "grpc"
	default:
		return "unknown"
	}
}

// chaosHarness owns one run's scenario-to-transport-to-syncer stack. Tests
// customize sync behavior with options without repeating adapter wiring or
// forgetting transport cleanup.
type chaosHarness struct {
	Syncer Syncer
	Run    *chaosconnector.Run

	closeAdapter func() error
}

func newChaosHarness(
	t *testing.T,
	ctx context.Context,
	run *chaosconnector.Run,
	c1zPath string,
	tmpDir string,
	transport chaosTransport,
	opts ...SyncOpt,
) *chaosHarness {
	t.Helper()
	builder, err := chaosconnector.NewBuilder(run)
	require.NoError(t, err)
	server, err := builder.Server(ctx)
	require.NoError(t, err)

	var client types.ConnectorClient
	closeAdapter := func() error { return nil }
	switch transport {
	case chaosTransportDirect:
		client = chaosconnector.NewDirectClient(ctx, server, run)
	case chaosTransportGRPC:
		grpcClient, grpcErr := chaosconnector.NewGRPCClient(ctx, server, run, false, false)
		require.NoError(t, grpcErr)
		client = grpcClient
		closeAdapter = grpcClient.Close
	default:
		t.Fatalf("unknown chaos transport %d", transport)
	}

	baseOpts := []SyncOpt{
		WithC1ZPath(c1zPath),
		WithTmpDir(tmpDir),
		WithStorageEngine(c1zstore.EnginePebble),
		WithDontExpandGrants(),
	}
	sdkSyncer, err := NewSyncer(ctx, client, append(baseOpts, opts...)...)
	require.NoError(t, err)
	return &chaosHarness{
		Syncer:       sdkSyncer,
		Run:          run,
		closeAdapter: closeAdapter,
	}
}

func (h *chaosHarness) SyncAndClose(t *testing.T, ctx context.Context) {
	t.Helper()
	require.NoError(t, h.Syncer.Sync(ctx))
	require.NoError(t, h.Close(ctx))
	require.NoError(t, h.Run.Runtime().VerifyRequired())
}

func (h *chaosHarness) Close(ctx context.Context) error {
	return errors.Join(h.Syncer.Close(ctx), h.closeAdapter())
}
