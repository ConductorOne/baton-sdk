package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"fmt"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

func TestLedgerPageRequiresTransition(t *testing.T) {
	for _, op := range []string{"init", "list-resources"} {
		t.Run(op, func(t *testing.T) {
			f := newLedgerFixture(t)
			runtime, err := newTestLedgerRuntime(t.Context(), f.ledger, "attempt")
			require.NoError(t, err)
			before := ledgerRawSnapshot(t, f.engine)
			f.audit.enter(ledgerHandler)
			_, err = runtime.runPage(t.Context(), 0, c1zstore.LedgerActionIdentity{Op: op}, func(ctx context.Context, page *ledgerPage) error {
				return page.writer.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "orphan"}.Build())
			})
			f.audit.enter(ledgerLifecycle)
			require.ErrorIs(t, err, errLedgerPageTransition)
			require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
		})
	}
}

func TestLedgerPageFailureDoesNotPublish(t *testing.T) {
	f := newLedgerFixture(t)
	runtime, err := newTestLedgerRuntime(t.Context(), f.ledger, "attempt")
	require.NoError(t, err)
	injected := errors.New("handler failed")
	id := c1zstore.LedgerActionIdentity{Op: "list-resource-types"}
	before := ledgerRawSnapshot(t, f.engine)
	f.audit.enter(ledgerHandler)
	_, err = runtime.runPage(t.Context(), 0, id, func(ctx context.Context, page *ledgerPage) error {
		require.NoError(t, page.writer.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "discard"}.Build()))
		require.NoError(t, page.setFact("uncommitted"))
		page.observations.Counters = map[string]uint64{"resources": 100}
		require.NoError(t, page.transition("later"))
		return injected
	})
	require.ErrorIs(t, err, injected)
	require.Empty(t, runtime.facts)
	require.Empty(t, runtime.workers)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
	_, err = runtime.runPage(t.Context(), 0, id, func(ctx context.Context, page *ledgerPage) error {
		require.False(t, page.hasFact("uncommitted"))
		require.NoError(t, page.writer.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "committed"}.Build()))
		page.observations.Counters = map[string]uint64{"resources": 1}
		require.NoError(t, page.setFact("committed"))
		return page.transition("")
	})
	f.audit.enter(ledgerLifecycle)
	require.NoError(t, err)
	facts, err := f.ledger.LedgerFacts(t.Context())
	require.NoError(t, err)
	require.Equal(t, map[string]string{"committed": ""}, facts)
	counters, err := f.ledger.LedgerCounters(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 1, counters.Counters["resources"])
}

func TestLedgerPageRejectsDuplicateTransition(t *testing.T) {
	f := newLedgerFixture(t)
	runtime, err := newTestLedgerRuntime(t.Context(), f.ledger, "attempt")
	require.NoError(t, err)
	before := ledgerRawSnapshot(t, f.engine)
	_, err = runtime.runPage(t.Context(), 0, c1zstore.LedgerActionIdentity{Op: "init"}, func(_ context.Context, page *ledgerPage) error {
		require.NoError(t, page.transition("first"))
		require.ErrorIs(t, page.transition("second"), errLedgerPageTransition)
		return nil
	})
	require.ErrorIs(t, err, errLedgerPageTransition)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
}

func TestLedgerPageCancellationDoesNotCommit(t *testing.T) {
	f := newLedgerFixture(t)
	runtime, err := newTestLedgerRuntime(t.Context(), f.ledger, "attempt")
	require.NoError(t, err)
	before := ledgerRawSnapshot(t, f.engine)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	_, err = runtime.runPage(ctx, 0, c1zstore.LedgerActionIdentity{Op: "init"}, func(_ context.Context, page *ledgerPage) error {
		cancel()
		return page.transition("")
	})
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
}

func TestLedgerPageCumulativeWorkersAndAttempts(t *testing.T) {
	f := newLedgerFixture(t)
	for attemptIndex, attempt := range []string{"first", "second"} {
		runtime, err := newTestLedgerRuntime(t.Context(), f.ledger, attempt)
		require.NoError(t, err)
		for worker := range uint32(2) {
			for pageIndex := range 2 {
				id := c1zstore.LedgerActionIdentity{Op: "list-resources", ResourceID: fmt.Sprintf("%d-%d-%d", attemptIndex, worker, pageIndex)}
				_, err := runtime.runPage(t.Context(), worker, id, func(_ context.Context, page *ledgerPage) error {
					page.observations = c1zstore.LedgerCounters{
						Counters: map[string]uint64{"records": 3}, Flags: 1 << uint(worker),
						ConnectorCalls: map[string]c1zstore.CallStat{"list": {Count: 1, TotalMs: 5, MaxMs: 5}},
					}
					return page.transition("")
				})
				require.NoError(t, err)
			}
		}
	}
	actual, err := f.ledger.LedgerCounters(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 24, actual.Counters["records"])
	require.EqualValues(t, 3, actual.Flags)
	require.Equal(t, c1zstore.CallStat{Count: 8, TotalMs: 40, MaxMs: 5}, actual.ConnectorCalls["list"])
}

func TestLedgerPageFactValueReadYourWrites(t *testing.T) {
	f := newLedgerFixture(t)
	runtime, err := newTestLedgerRuntime(t.Context(), f.ledger, "attempt")
	require.NoError(t, err)
	f.audit.enter(ledgerHandler)
	_, err = runtime.runPage(t.Context(), 0, c1zstore.LedgerActionIdentity{Op: "init"}, func(_ context.Context, page *ledgerPage) error {
		require.NoError(t, page.setFactValue("value", "first"))
		require.True(t, page.hasFact("value"))
		require.NotContains(t, runtime.facts, "value")
		require.NoError(t, page.setFactValue("value", "last"))
		return page.transition("")
	})
	f.audit.enter(ledgerLifecycle)
	require.NoError(t, err)
	facts, err := f.ledger.LedgerFacts(t.Context())
	require.NoError(t, err)
	require.Equal(t, "last", facts["value"])
	require.Equal(t, facts, runtime.facts)
}
