package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
	chaosoracle "github.com/conductorone/baton-sdk/internal/chaosconnector/oracle"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/types"
)

type chaosTransport int

const (
	chaosTransportDirect chaosTransport = iota
	chaosTransportGRPC
	chaosTransportGRPCServerFault
)

func (t chaosTransport) String() string {
	switch t {
	case chaosTransportDirect:
		return "direct"
	case chaosTransportGRPC:
		return "grpc"
	case chaosTransportGRPCServerFault:
		return "grpc-server-fault"
	default:
		return "unknown"
	}
}

func chaosFaultTransports() []chaosTransport {
	return []chaosTransport{
		chaosTransportDirect,
		chaosTransportGRPC,
		chaosTransportGRPCServerFault,
	}
}

// skipChaosInShort skips the chaos connector suite under -short. Windows CI
// runs with -short because its filesystem is too slow for the full suite.
func skipChaosInShort(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping chaos connector suite in short mode")
	}
}

// chaosHarness owns one run's scenario-to-transport-to-syncer stack. Tests
// customize sync behavior with options without repeating adapter wiring or
// forgetting transport cleanup.
type chaosHarness struct {
	Syncer Syncer
	Run    *chaosconnector.Run
	// Client is the transport-wrapped connector client the syncer holds.
	// Source-cache teardown probes invoke it directly after a sync to
	// verify the connector no longer observes the sync's lookup.
	Client types.ConnectorClient

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
	skipChaosInShort(t)
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
	case chaosTransportGRPCServerFault:
		grpcClient, grpcErr := chaosconnector.NewGRPCServerFaultClient(ctx, server, run, false, false)
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
		Client:       client,
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

func readChaosLogicalContent(
	t *testing.T,
	ctx context.Context,
	path string,
	tmpDir string,
) chaosoracle.LogicalContentSnapshot {
	t.Helper()
	store, err := dotc1z.NewStore(
		ctx,
		path,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()
	content, err := chaosoracle.ReadLogicalContent(ctx, store)
	require.NoError(t, err)
	return content
}

func readChaosSyncRuns(
	t *testing.T,
	ctx context.Context,
	path string,
	tmpDir string,
) []*c1zstore.SyncRun {
	t.Helper()
	store, err := dotc1z.NewStore(
		ctx,
		path,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()
	lister, ok := store.(interface {
		ListSyncRuns(context.Context, string, uint32) ([]*c1zstore.SyncRun, string, error)
	})
	require.True(t, ok)
	runs, _, err := lister.ListSyncRuns(ctx, "", 100)
	require.NoError(t, err)
	return runs
}
