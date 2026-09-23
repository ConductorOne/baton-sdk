package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

var errLedgerInjectedPage = errors.New("injected page failure")

type ledgerFailingPageStore struct {
	c1zstore.PageLedgerStore
	stage string
}

func (s ledgerFailingPageStore) BeginPage() c1zstore.PageWriter {
	return ledgerFailingPageWriter{PageWriter: s.PageLedgerStore.BeginPage(), stage: s.stage}
}

type ledgerFailingPageWriter struct {
	c1zstore.PageWriter
	stage string
}

func (w ledgerFailingPageWriter) SetFact(name string) error {
	if w.stage == "fact" {
		return errLedgerInjectedPage
	}
	return w.PageWriter.SetFact(name)
}

func (w ledgerFailingPageWriter) SetCounterBucket(runID string, worker uint32, counters c1zstore.LedgerCounters) error {
	if w.stage == "counter" {
		return errLedgerInjectedPage
	}
	return w.PageWriter.SetCounterBucket(runID, worker, counters)
}

func (w ledgerFailingPageWriter) Commit(ctx context.Context, id c1zstore.LedgerActionIdentity, row *c1zstore.LedgerRow) error {
	if w.stage == "commit" {
		return errLedgerInjectedPage
	}
	return w.PageWriter.Commit(ctx, id, row)
}

func TestLedgerPageFailureDiscardsStagedObservations(t *testing.T) {
	for _, stage := range []string{"fact", "counter", "commit"} {
		t.Run(stage, func(t *testing.T) {
			f := newLedgerFixture(t)
			source := ledgerFailingPageStore{PageLedgerStore: f.ledger, stage: stage}
			runtime, err := newLedgerRuntime(t.Context(), source, "attempt")
			require.NoError(t, err)
			before := ledgerRawSnapshot(t, f.engine)
			f.audit.enter(ledgerHandler)
			_, err = runtime.runPage(t.Context(), 0, c1zstore.LedgerActionIdentity{Op: "init"}, func(ctx context.Context, page *ledgerPage) error {
				if err := page.writer.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "discarded"}.Build()); err != nil {
					return err
				}
				if err := page.setFact("failed-fact"); err != nil {
					return err
				}
				page.observations.Counters = map[string]uint64{"failed": 9}
				return page.transition("next", c1zstore.LedgerChild{Identity: c1zstore.LedgerActionIdentity{Op: "child"}})
			})
			f.audit.enter(ledgerLifecycle)
			require.ErrorIs(t, err, errLedgerInjectedPage)
			require.Zero(t, f.audit.writers)
			require.Empty(t, runtime.facts)
			require.Empty(t, runtime.workers)
			require.Empty(t, runtime.active)
			require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
		})
	}
}

func TestLedgerTerminalFailureDoesNotPublishProof(t *testing.T) {
	for _, stage := range []string{"fact", "counter", "commit"} {
		t.Run(stage, func(t *testing.T) {
			f := newLedgerFixture(t)
			require.NoError(t, f.ledger.InitializePendingWork(t.Context(), nil))
			source := ledgerFailingPageStore{PageLedgerStore: f.ledger, stage: stage}
			runtime, err := newLedgerRuntime(t.Context(), source, "attempt")
			require.NoError(t, err)
			before := ledgerRawSnapshot(t, f.engine)
			require.ErrorIs(t, runtime.prepareSeal(t.Context(), c1zstore.LedgerCounters{StepDurationsMs: map[string]int64{"run": 7}}), errLedgerInjectedPage)
			require.NotContains(t, runtime.facts, ledgerFactSealReady)
			require.Zero(t, f.audit.writers)
			require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
			recovered, err := newLedgerRuntime(t.Context(), f.ledger, "retry")
			require.NoError(t, err)
			require.NoError(t, recovered.prepareSeal(t.Context(), c1zstore.LedgerCounters{StepDurationsMs: map[string]int64{"run": 7}}))
			counters, err := f.ledger.LedgerCounters(t.Context())
			require.NoError(t, err)
			require.Equal(t, int64(7), counters.StepDurationsMs["run"])
		})
	}
}
