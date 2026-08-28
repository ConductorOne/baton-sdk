package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"context"
	"errors"
	native_sync "sync"
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
	// Held-lock ride-along (CO-6b-007): a source-cache scope lock is
	// acquired in beforeUpserts and released in afterUpserts or by the
	// handlers' deferred release(); a lock still held once the sync is
	// over means a release path was lost — and because syncOneAction
	// retries a failed action in the SAME goroutine and sync.Mutex is not
	// reentrant, that is a permanently hung sync, not a slow one. Bound
	// here so EVERY chaos suite — present and future, success or failure
	// path — evaluates it at test end; any scenario that errors a scoped
	// page between the lock's acquire and release trips it if a handler
	// loses its backstop.
	if concrete, ok := sdkSyncer.(*syncer); ok {
		t.Cleanup(func() {
			require.Empty(t, heldSourceCacheScopeLocks(concrete),
				"source-cache scope locks still held at sync end: a page errored between beforeUpserts and afterUpserts and its release path was lost — this sync would hang its own retry")
		})
	}
	return &chaosHarness{
		Syncer:       sdkSyncer,
		Run:          run,
		Client:       client,
		closeAdapter: closeAdapter,
	}
}

// heldSourceCacheScopeLocks reports which per-scope replay locks are still
// held. Only safe once the sync has finished (no workers contend the
// locks); TryLock on a free mutex briefly holds it, so a concurrent worker
// could see spurious contention.
func heldSourceCacheScopeLocks(s *syncer) []string {
	var held []string
	s.sourceCacheScopeLocks.Range(func(k, v any) bool {
		mu, ok := v.(*native_sync.Mutex)
		if !ok {
			held = append(held, k.(string)+" (non-mutex entry)")
			return true
		}
		if mu.TryLock() {
			mu.Unlock()
		} else {
			held = append(held, k.(string))
		}
		return true
	})
	return held
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
