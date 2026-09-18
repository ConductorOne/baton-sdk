package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

func TestLedgerScheduleDrainsDiamondAndCycle(t *testing.T) {
	for _, workers := range []uint32{1, 4} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			f := newLedgerFixture(t)
			runtime, err := newLedgerRuntime(t.Context(), f.ledger, "attempt")
			require.NoError(t, err)
			var calls atomic.Int64
			id := func(name string) c1zstore.LedgerActionIdentity {
				return c1zstore.LedgerActionIdentity{Op: "list-resources", ResourceID: name}
			}
			graph := map[string][]string{"root": {"left", "right"}, "left": {"shared"}, "right": {"shared"}, "shared": {"root"}}
			handler := func(ctx context.Context, s *syncer, action *Action, page *ledgerPage) error {
				calls.Add(1)
				if err := page.writer.PutResourceTypes(ctx, v2.ResourceType_builder{Id: action.ResourceID}.Build()); err != nil {
					return err
				}
				var children []c1zstore.LedgerChild
				for _, name := range graph[action.ResourceID] {
					children = append(children, c1zstore.LedgerChild{Identity: id(name), Spawned: true})
				}
				return ledgerFixtureTransition(ctx, s, action, "", children...)
			}
			f.audit.enter(ledgerHandler)
			require.NoError(t, runLedgerSchedulerFixture(t, runtime, []ledgerAction{{identity: id("root"), spawned: true}}, workers, handler))
			f.audit.enter(ledgerLifecycle)
			require.Equal(t, int64(4), calls.Load())
			counters, err := f.ledger.LedgerCounters(t.Context())
			require.NoError(t, err)
			require.Equal(t, uint64(4), counters.Counters[ledgerCompletedActions])
			before := ledgerRawSnapshot(t, f.engine)
			resumed, err := newLedgerRuntime(t.Context(), f.ledger, "resume")
			require.NoError(t, err)
			f.audit.enter(ledgerWalk)
			require.NoError(t, runLedgerSchedulerFixture(t, resumed, []ledgerAction{{identity: id("root"), spawned: true}}, workers, handler))
			f.audit.enter(ledgerLifecycle)
			require.Equal(t, int64(4), calls.Load())
			require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
		})
	}
}

func TestLedgerScheduleStopsAndJoinsOnError(t *testing.T) {
	f := newLedgerFixture(t)
	runtime, err := newLedgerRuntime(t.Context(), f.ledger, "attempt")
	require.NoError(t, err)
	injected := errors.New("connector error")
	var active atomic.Int64
	roots := []ledgerAction{}
	for i := range 8 {
		roots = append(roots, ledgerAction{identity: c1zstore.LedgerActionIdentity{Op: "list-resources", ResourceID: fmt.Sprint(i)}})
	}
	before := ledgerRawSnapshot(t, f.engine)
	f.audit.enter(ledgerHandler)
	err = runLedgerSchedulerFixture(t, runtime, roots, 4, func(ctx context.Context, s *syncer, action *Action, page *ledgerPage) error {
		active.Add(1)
		defer active.Add(-1)
		if err := page.writer.PutResourceTypes(ctx, v2.ResourceType_builder{Id: action.ResourceID}.Build()); err != nil {
			return err
		}
		return injected
	})
	f.audit.enter(ledgerLifecycle)
	require.ErrorIs(t, err, injected)
	require.Zero(t, active.Load())
	require.Empty(t, runtime.active)
	require.Zero(t, f.audit.writers)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
	require.NoError(t, runtime.flushRunCounters(t.Context(), c1zstore.LedgerCounters{SessionCalls: map[string]c1zstore.CallStat{"get": {Count: 2, Errors: 1}}}))
	require.NoError(t, runtime.flushRunCounters(t.Context(), c1zstore.LedgerCounters{SessionCalls: map[string]c1zstore.CallStat{"get": {Count: 2, Errors: 1}}}))
	counters, err := f.ledger.LedgerCounters(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(2), counters.SessionCalls["get"].Count)
}

func TestLedgerScheduleWalksNewlyDiscoveredChild(t *testing.T) {
	f := newLedgerFixture(t)
	runtime, err := newLedgerRuntime(t.Context(), f.ledger, "first")
	require.NoError(t, err)
	child := c1zstore.LedgerActionIdentity{Op: "list-resources", ResourceTypeID: "type"}
	_, err = runtime.runPage(t.Context(), 0, child, func(_ context.Context, page *ledgerPage) error { return page.transition("") })
	require.NoError(t, err)
	runtime, err = newLedgerRuntime(t.Context(), f.ledger, "resumed")
	require.NoError(t, err)
	calls := 0
	err = runLedgerSchedulerFixture(t, runtime, ledgerListingFixtureRoots(), 1, func(ctx context.Context, s *syncer, action *Action, page *ledgerPage) error {
		calls++
		if ledgerIdentity(action) == child {
			return errors.New("committed child ran again")
		}
		return ledgerFixtureTransition(ctx, s, action, "", c1zstore.LedgerChild{Identity: child})
	})
	require.NoError(t, err)
	require.Equal(t, 1, calls)
}
