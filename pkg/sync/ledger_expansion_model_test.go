package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	batonGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type ledgerExpansionLayerObserver struct {
	expandedGrantLayerStorer
	begins     int
	finishes   int
	failFinish bool
	adds       int
}

func (o *ledgerExpansionLayerObserver) BeginExpandedGrantLayer(ctx context.Context) (bool, error) {
	o.begins++
	return o.expandedGrantLayerStorer.BeginExpandedGrantLayer(ctx)
}

func (o *ledgerExpansionLayerObserver) FinishExpandedGrantLayer(ctx context.Context) error {
	if err := o.expandedGrantLayerStorer.FinishExpandedGrantLayer(ctx); err != nil {
		return err
	}
	o.finishes++
	if o.failFinish && o.adds > 0 {
		return errLedgerInjectedPage
	}
	return nil
}

func TestLedgerExpansionUsesMainModel(t *testing.T) {
	s, f := ledgerExpansionFixture(t)
	observer := &ledgerExpansionLayerObserver{expandedGrantLayerStorer: s.caps.expandedGrantLayer}
	require.NotNil(t, observer.expandedGrantLayerStorer)
	s.caps.expandedGrantLayer = observer
	_, err := s.parallelSync(t.Context(), t.Context(), nil)
	require.NoError(t, err)
	require.Positive(t, observer.begins, "expansion must reach Pebble's layer capability")
	require.Positive(t, observer.finishes)
	_, found, err := f.ledger.GetLedgerRow(t.Context(), c1zstore.LedgerActionIdentity{Op: SyncGrantExpansionOp.String()})
	require.NoError(t, err)
	require.False(t, found, "derived expansion work must not create page rows")
}

type ledgerExpansionSealFailure struct{ c1zstore.PageLedgerStore }

func (s ledgerExpansionSealFailure) EndSyncWithStats(context.Context, c1zstore.SyncStats) error {
	return errLedgerInjectedPage
}

func TestLedgerExpansionPublicReplay(t *testing.T) {
	for _, cut := range []string{"layer", "before-terminal", "after-terminal"} {
		t.Run(cut, func(t *testing.T) {
			t.Setenv("BATON_PEBBLE_SYNTH_LAYER_SEGMENT_ROWS", "1")
			_, reference := ledgerExpansionFixture(t)
			clean := newLedgerExpansionPublicSyncer(t, reference)
			require.NoError(t, clean.Sync(t.Context()))
			want, err := reference.store.ListGrants(t.Context(), &v2.GrantsServiceListGrantsRequest{})
			require.NoError(t, err)
			_, f := ledgerExpansionFixture(t)
			before, err := f.store.ListGrants(t.Context(), &v2.GrantsServiceListGrantsRequest{})
			require.NoError(t, err)
			first := newLedgerExpansionPublicSyncer(t, f)
			observer := &ledgerExpansionLayerObserver{expandedGrantLayerStorer: first.caps.expandedGrantLayer, failFinish: cut == "layer"}
			first.caps.expandedGrantLayer = observer
			if cut == "before-terminal" {
				first.testHooks.ingestHaltHook = func(stage string) error {
					if stage == haltStageInvariantsComplete {
						return errLedgerInjectedPage
					}
					return nil
				}
			}
			if cut == "after-terminal" {
				first.caps.pageLedger = ledgerExpansionSealFailure{PageLedgerStore: f.ledger}
			}
			require.ErrorIs(t, first.Sync(t.Context()), errLedgerInjectedPage)
			require.Positive(t, observer.begins)
			partial, err := f.store.ListGrants(t.Context(), &v2.GrantsServiceListGrantsRequest{})
			require.NoError(t, err)
			require.Greater(t, len(partial.GetList()), len(before.GetList()), "interruption must leave actual expansion output")
			counters, err := f.ledger.LedgerCounters(t.Context())
			require.NoError(t, err)
			expected := uint64(0)
			if cut == "after-terminal" {
				expected = 1
			}
			require.Equal(t, expected, counters.Counters[ledgerCompletedPrefix+SyncGrantExpansionOp.String()])
			syncID := first.syncID
			require.NoError(t, f.store.Close(t.Context()))
			f = openLedgerFixtureAt(t, f.path, false)
			require.NoError(t, f.store.SetCurrentSync(t.Context(), syncID))
			resumed := newLedgerExpansionPublicSyncer(t, f)
			healthy := &ledgerExpansionLayerObserver{expandedGrantLayerStorer: resumed.caps.expandedGrantLayer}
			resumed.caps.expandedGrantLayer = healthy
			require.NoError(t, resumed.Sync(t.Context()))
			if cut == "after-terminal" {
				require.Zero(t, healthy.begins)
			} else {
				require.Positive(t, healthy.begins)
			}
			got, err := f.store.ListGrants(t.Context(), &v2.GrantsServiceListGrantsRequest{})
			require.NoError(t, err)
			equalProtoLists(t, want.GetList(), got.GetList())
			counters, err = f.ledger.LedgerCounters(t.Context())
			require.NoError(t, err)
			wantCounters, err := reference.ledger.LedgerCounters(t.Context())
			require.NoError(t, err)
			require.Equal(t, wantCounters.Counters, counters.Counters)
			require.EqualValues(t, 1, counters.Counters[ledgerCompletedPrefix+SyncGrantExpansionOp.String()])
			_, found, err := f.ledger.GetLedgerRow(t.Context(), c1zstore.LedgerActionIdentity{Op: SyncGrantExpansionOp.String()})
			require.NoError(t, err)
			require.False(t, found)
			report, err := f.ledger.GetArchivedLedgerReport(t.Context())
			require.NoError(t, err)
			var decoded struct {
				Latest struct {
					Performed bool `json:"reference_validation_performed"`
					Checks    struct {
						MissingChildren uint64 `json:"missing_child_references"`
					} `json:"reference_checks"`
				} `json:"latest"`
			}
			require.NoError(t, json.Unmarshal(report, &decoded))
			require.True(t, decoded.Latest.Performed)
			require.Zero(t, decoded.Latest.Checks.MissingChildren, "expansion is a phase, not a missing collection page")
			graph, err := GraphFromStore(t.Context(), f.store, resumed.syncID)
			require.NoError(t, err)
			require.NotNil(t, graph, "preserved graph must survive terminal recovery")
		})
	}
}

func newLedgerExpansionPublicSyncer(t *testing.T, f *ledgerFixture) *syncer {
	t.Helper()
	created, err := NewSyncer(t.Context(), ledgerExpansionConnector{mockConnector: newMockConnector()},
		WithConnectorStore(f.store), WithSyncID(f.engine.CurrentSyncID()), WithOnlyExpandGrants(), WithLedgerDebug(true), WithPreserveEntitlementGraph())
	require.NoError(t, err)
	return created.(*syncer)
}

type ledgerExpansionConnector struct{ *mockConnector }

func (ledgerExpansionConnector) ListResourceTypes(context.Context, *v2.ResourceTypesServiceListResourceTypesRequest, ...grpc.CallOption) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	return nil, errors.New("expansion replay must not recollect resource types")
}
func (ledgerExpansionConnector) ListResources(context.Context, *v2.ResourcesServiceListResourcesRequest, ...grpc.CallOption) (*v2.ResourcesServiceListResourcesResponse, error) {
	return nil, errors.New("expansion replay must not recollect resources")
}
func (ledgerExpansionConnector) ListEntitlements(context.Context, *v2.EntitlementsServiceListEntitlementsRequest, ...grpc.CallOption) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	return nil, errors.New("expansion replay must not recollect entitlements")
}
func (ledgerExpansionConnector) ListGrants(context.Context, *v2.GrantsServiceListGrantsRequest, ...grpc.CallOption) (*v2.GrantsServiceListGrantsResponse, error) {
	return nil, errors.New("expansion replay must not recollect grants")
}

func (o *ledgerExpansionLayerObserver) AddExpandedGrantLayerContributions(ctx context.Context, dest *v2.Entitlement, principals []*v3.PrincipalRef, sources []batonGrant.Sources) error {
	o.adds++
	return o.expandedGrantLayerStorer.AddExpandedGrantLayerContributions(ctx, dest, principals, sources)
}

func TestLedgerExpansionSkipHasNoPage(t *testing.T) {
	for _, reason := range []string{"disabled", "no-expansion"} {
		t.Run(reason, func(t *testing.T) {
			s, f := ledgerExpansionFixture(t)
			if reason == "disabled" {
				s.cfg.dontExpandGrants = true
			} else {
				s.run = newRunState()
				s.run.pushAction(t.Context(), Action{Op: SyncGrantExpansionOp})
			}
			observer := &ledgerExpansionLayerObserver{expandedGrantLayerStorer: s.caps.expandedGrantLayer}
			s.caps.expandedGrantLayer = observer
			_, err := s.parallelSync(t.Context(), t.Context(), nil)
			require.NoError(t, err)
			require.Zero(t, observer.begins)
			_, found, err := f.ledger.GetLedgerRow(t.Context(), c1zstore.LedgerActionIdentity{Op: SyncGrantExpansionOp.String()})
			require.NoError(t, err)
			require.False(t, found)
			require.EqualValues(t, 1, s.terminalLedgerCounters().Counters[ledgerCompletedActions])
			require.Zero(t, s.ledger.runCounterSnapshot().Counters[ledgerCompletedActions], "stop accounting must not persist replayable completion")
		})
	}
}

type ledgerExpansionReadFailure struct{ c1zstore.Store }

func (s ledgerExpansionReadFailure) GetEntitlement(context.Context, *reader_v2.EntitlementsReaderServiceGetEntitlementRequest) (*reader_v2.EntitlementsReaderServiceGetEntitlementResponse, error) {
	return nil, errLedgerInjectedPage
}

func TestLedgerExpansionReadFailureHasNoPage(t *testing.T) {
	s, f := ledgerExpansionFixture(t)
	s.store = ledgerExpansionReadFailure{Store: s.store}
	before := ledgerRawSnapshot(t, f.engine)
	require.ErrorIs(t, s.SyncGrantExpansion(t.Context(), s.run.current()), errLedgerInjectedPage)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
	require.False(t, s.graph.get(t.Context()).Loaded)
}

func TestLedgerExpansionFinishedReplay(t *testing.T) {
	for _, retained := range []bool{false, true} {
		t.Run(map[bool]string{false: "disposed", true: "retained"}[retained], func(t *testing.T) {
			_, f := ledgerExpansionFixture(t)
			first := newLedgerExpansionPublicSyncer(t, f)
			first.cfg.ledgerDebug = retained
			require.NoError(t, first.Sync(t.Context()))
			facts, err := f.ledger.LedgerFacts(t.Context())
			require.NoError(t, err)
			if retained {
				require.NotEmpty(t, facts)
			} else {
				require.Empty(t, facts)
			}
			syncID := first.syncID
			before, err := f.engine.GetSyncRunRecord(t.Context(), syncID)
			require.NoError(t, err)
			want, err := f.store.ListGrants(t.Context(), &v2.GrantsServiceListGrantsRequest{})
			require.NoError(t, err)
			require.NoError(t, f.store.Close(t.Context()))
			f = openLedgerFixtureAt(t, f.path, false)
			require.NoError(t, f.store.SetCurrentSync(t.Context(), syncID))
			next := newLedgerExpansionPublicSyncer(t, f)
			next.testHooks.ledgerWalk = func(entering bool) {
				if entering {
					bound, err := f.engine.GetSyncRunRecord(t.Context(), syncID)
					require.NoError(t, err)
					require.Equal(t, before.GetStartedAt(), bound.GetStartedAt())
					require.Equal(t, before.GetEndedAt(), bound.GetEndedAt())
				}
			}
			observer := &ledgerExpansionLayerObserver{expandedGrantLayerStorer: next.caps.expandedGrantLayer}
			next.caps.expandedGrantLayer = observer
			require.NoError(t, next.Sync(t.Context()))
			require.Positive(t, observer.begins)
			got, err := f.store.ListGrants(t.Context(), &v2.GrantsServiceListGrantsRequest{})
			require.NoError(t, err)
			equalProtoLists(t, want.GetList(), got.GetList())
			counters, err := f.ledger.LedgerCounters(t.Context())
			require.NoError(t, err)
			require.EqualValues(t, 2, counters.Counters[ledgerCompletedPrefix+SyncGrantExpansionOp.String()])
		})
	}
}
