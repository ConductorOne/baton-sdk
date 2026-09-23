package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/stretchr/testify/require"
)

type ledgerUnstartedFailure struct{ c1zstore.PageLedgerStore }

func (ledgerUnstartedFailure) BoundSyncUnstarted(context.Context) (bool, error) {
	return false, errLedgerInjectedPage
}

func TestLedgerEmptyStartQuality(t *testing.T) {
	for _, kind := range []string{"empty", "session", "resource", "grant", "legacy-token", "legacy-frontier", "known-clean", "known-blocked", "finished", "read-error"} {
		t.Run(kind, func(t *testing.T) {
			f := newLedgerFixture(t)
			ctx := t.Context()
			id := f.engine.CurrentSyncID()
			switch kind {
			case "session":
				require.NoError(t, f.store.SessionStore().Set(ctx, "session", []byte("value"), sessions.WithSyncID(id)))
			case "resource":
				require.NoError(t, f.store.PutResources(ctx, v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "type", Resource: "saved"}.Build()}.Build()))
			case "grant":
				require.NoError(t, f.store.PutGrants(ctx, ledgerGrant("saved", "type", "resource", "user")))
			case "legacy-token", "legacy-frontier":
				require.NoError(t, f.store.CheckpointSync(ctx, `{"version":1,"actions":[]}`))
				if kind == "legacy-frontier" {
					_, err := loadLedgerResume(ctx, f.store, f.ledger, "takeover")
					require.NoError(t, err)
				}
			case "known-clean", "known-blocked":
				facts := []string{ledgerFactIngestKnown}
				if kind == "known-blocked" {
					facts = append(facts, ledgerFactIngestBlocked)
					require.NoError(t, f.ledger.PutCounterBucket(ctx, "prior", 0, c1zstore.LedgerCounters{Flags: ingestQualityReasonGrantDropped}))
				}
				require.NoError(t, f.ledger.InitializePendingWork(ctx, pendingSeeds([]ledgerAction{{identity: c1zstore.LedgerActionIdentity{Op: InitOp.String()}}}), facts...))
			case "finished":
				require.NoError(t, f.store.EndSync(ctx))
				require.NoError(t, f.store.SetCurrentSync(ctx, id))
			}
			s := ledgerContinuationSyncer(f)
			if kind == "read-error" {
				s.caps.pageLedger = ledgerUnstartedFailure{PageLedgerStore: f.ledger}
			}
			before := ledgerRawSnapshot(t, f.engine)
			var walkBefore []ledgerKV
			s.testHooks.ledgerWalk = func(entering bool) {
				if entering {
					walkBefore = ledgerRawSnapshot(t, f.engine)
					f.audit.enter(ledgerWalk)
				} else {
					f.audit.enter(ledgerLifecycle)
					require.True(t, equalLedgerSnapshot(walkBefore, ledgerRawSnapshot(t, f.engine)))
				}
			}
			_, err := s.prepareLedgerState(ctx, "resume", false)
			f.audit.enter(ledgerLifecycle)
			if kind == "read-error" {
				require.ErrorIs(t, err, errLedgerInjectedPage)
				require.Nil(t, s.stats)
				require.Equal(t, before, ledgerRawSnapshot(t, f.engine))
				return
			}
			require.NoError(t, err)
			clean := kind == "empty" || kind == "session" || kind == "known-clean"
			require.Equal(t, !clean, s.ingestFilterStats.snapshot().SourceCacheReplayBlocked)
			switch {
			case clean:
				require.Equal(t, &IngestQualityCheckpoint{}, s.stats.ingestQuality())
			case kind == "known-blocked":
				require.Equal(t, ingestQualityReasonGrantDropped, s.ingestFilterStats.snapshot().ReasonFlags)
			default:
				require.Equal(t, ingestQualityReasonUnknownPriorCheckpoint, s.ingestFilterStats.snapshot().ReasonFlags)
			}
			if kind == "read-error" || kind == "legacy-frontier" || kind == "known-clean" || kind == "known-blocked" {
				require.Equal(t, before, ledgerRawSnapshot(t, f.engine))
			}
			if kind == "empty" || kind == "session" {
				factsBeforePage, err := f.ledger.LedgerFacts(ctx)
				require.NoError(t, err)
				require.Contains(t, factsBeforePage, ledgerFactIngestKnown)
				require.Equal(t, factsBeforePage, s.ledger.facts)
				action := s.run.current()
				require.NoError(t, invokeLedgerTestPage(t, s, ctx, action, func(ctx context.Context, a *Action) error { return s.initializeAction(ctx, a, nil) }, false))
				facts, err := f.ledger.LedgerFacts(ctx)
				require.NoError(t, err)
				require.Contains(t, facts, ledgerFactIngestKnown)
				require.NoError(t, f.store.Close(ctx))
				f = openLedgerFixtureAt(t, f.path, false)
				require.NoError(t, f.store.SetCurrentSync(ctx, id))
				restored := ledgerContinuationSyncer(f)
				before = ledgerRawSnapshot(t, f.engine)
				f.audit.enter(ledgerWalk)
				_, err = restored.prepareLedgerState(ctx, "after-init", false)
				f.audit.enter(ledgerLifecycle)
				require.NoError(t, err)
				require.Equal(t, before, ledgerRawSnapshot(t, f.engine))
				require.Equal(t, &IngestQualityCheckpoint{}, restored.stats.ingestQuality())
			}
		})
	}
}
