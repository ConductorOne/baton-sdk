package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	native_sync "sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
	chaosoracle "github.com/conductorone/baton-sdk/internal/chaosconnector/oracle"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

// jumpClock stands still, then jumps an hour on its jumpAt-th reading and
// stays there. The expiry model reads it once when a token is issued and
// once when a known token is presented, so with jumpAt=2 the first token
// issued is the one that has aged out when it comes back.
type jumpClock struct {
	mu     native_sync.Mutex
	reads  int
	jumpAt int
}

func (c *jumpClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reads++
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if c.reads >= c.jumpAt {
		return base.Add(time.Hour)
	}
	return base
}

// The full scenario with the user type's resources spread over three pages,
// so a continuation token is issued and presented.
func newChaosPagedResourcesScenario(t *testing.T) *chaosconnector.Scenario {
	t.Helper()
	scenario, err := chaosconnector.NewFullScenario()
	require.NoError(t, err)
	dataset := scenario.Epochs[scenario.InitialEpoch]
	pages := dataset.Resources[chaosconnector.FullCapabilityResourceTypeID]
	first := pages[""]
	userType := dataset.ResourceTypes[0]
	require.Equal(t, chaosconnector.FullCapabilityResourceTypeID, userType.GetId())
	user2, err := rs.NewUserResource("Chaos User 2", userType, "user-2", nil)
	require.NoError(t, err)
	user3, err := rs.NewUserResource("Chaos User 3", userType, "user-3", nil)
	require.NoError(t, err)
	first.Next = "users-2"
	pages[""] = first
	pages["users-2"] = chaosconnector.Page[*v2.Resource]{List: []*v2.Resource{user2}, Next: "users-3"}
	pages["users-3"] = chaosconnector.Page[*v2.Resource]{List: []*v2.Resource{user3}}
	return scenario
}

// C32, the reference case: a provider in Graph mode expires one page token
// mid-pagination. A connector that translates the rejection into
// RestartAction finishes the sync with the manifest's records and one
// restart page per rejection, on every transport, including the one where
// the error crosses a real gRPC boundary. The same provider behind a
// connector that does not translate fails the sync with the provider's
// error, as before this change.
func TestChaosGraphTokenExpiry(t *testing.T) {
	skipChaosInShort(t)
	transports := map[string]chaosTransport{"direct": chaosTransportDirect, "grpc": chaosTransportGRPC, "grpc-server-fault": chaosTransportGRPCServerFault}
	for name, transport := range transports {
		t.Run("translated/"+name, func(t *testing.T) {
			ctx := t.Context()
			tmpDir := t.TempDir()
			c1zPath := filepath.Join(tmpDir, "graph-expiry.c1z")
			scenario := newChaosPagedResourcesScenario(t)
			run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
			require.NoError(t, err)
			run.SetTokenExpiry(chaosconnector.TokenExpiry{Lifetime: time.Minute, Translate: true, Now: (&jumpClock{jumpAt: 2}).Now})
			harness := newChaosHarness(t, ctx, run, c1zPath, tmpDir, transport, WithWorkerCount(1))
			harness.SyncAndClose(t, ctx)

			rejections := run.TokenRejections()
			require.Len(t, rejections, 1, "exactly one token aged out: %v", rejections)
			var rejected int
			for _, n := range rejections {
				rejected += n
			}
			require.Equal(t, 1, rejected)

			manifest, err := scenario.Manifest(scenario.InitialEpoch)
			require.NoError(t, err)
			assertChaosStoreMatches(t, c1zPath, tmpDir, chaosoracle.ExpectedIdentities(manifest))
			require.EqualValues(t, rejected, chaosRestartPages(t, c1zPath, tmpDir))
		})
		t.Run("untranslated/"+name, func(t *testing.T) {
			ctx := t.Context()
			tmpDir := t.TempDir()
			c1zPath := filepath.Join(tmpDir, "graph-expiry-raw.c1z")
			scenario := newChaosPagedResourcesScenario(t)
			run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
			require.NoError(t, err)
			run.SetTokenExpiry(chaosconnector.TokenExpiry{Lifetime: time.Minute, Now: (&jumpClock{jumpAt: 2}).Now})
			harness := newChaosHarness(t, ctx, run, c1zPath, tmpDir, transport, WithWorkerCount(1))
			err = harness.Syncer.Sync(ctx)
			require.Error(t, err)
			require.Equal(t, codes.InvalidArgument, status.Code(err))
			require.Contains(t, err.Error(), chaosconnector.GraphExpiredTokenMessage)
			require.NotErrorIs(t, err, ErrRestartRejected)
			require.NoError(t, harness.Close(ctx))
			require.Len(t, run.TokenRejections(), 1)
			require.Zero(t, chaosRestartPages(t, c1zPath, tmpDir))
		})
	}
}

func chaosRestartPages(t *testing.T, c1zPath, tmpDir string) uint64 {
	t.Helper()
	ctx := t.Context()
	store, err := dotc1z.NewStore(ctx, c1zPath, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(tmpDir), dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()
	raw, ok := pebble.AsEngine(store)
	require.True(t, ok)
	report, err := raw.GetArchivedLedgerReport(ctx)
	require.NoError(t, err)
	if len(report) == 0 {
		counters, err := raw.Ledger().Counters(ctx)
		require.NoError(t, err)
		return counters.Counters[c1zstore.LedgerActionRestarts]
	}
	var decoded struct {
		Latest struct {
			RestartPages uint64 `json:"restart_pages"`
		} `json:"latest"`
	}
	require.NoError(t, json.Unmarshal(report, &decoded), fmt.Sprintf("%.200s", report))
	return decoded.Latest.RestartPages
}
