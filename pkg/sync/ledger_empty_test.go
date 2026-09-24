package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	engine "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/stretchr/testify/require"
)

type ledgerUnstartedFailure struct{ c1zstore.PageLedgerStore }

func (ledgerUnstartedFailure) BoundSyncUnstarted(context.Context) (bool, error) {
	return false, errLedgerInjectedPage
}

func TestLedgerEmptyStartQuality(t *testing.T) {
	for _, kind := range []string{"empty", "session", "resource", "grant", "legacy-token", "legacy-frontier", "known-clean", "known-blocked", "finished", "read-error",
		"counter-only", "counter-flags", "uninitialized-fact", "uninitialized-blocked"} {
		t.Run(kind, func(t *testing.T) {
			f := newLedgerFixture(t)
			ctx := t.Context()
			id := f.engine.CurrentSyncID()
			uninitializedQuality := kind == "counter-only" || kind == "counter-flags" || kind == "uninitialized-fact" || kind == "uninitialized-blocked"
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
					_, err := loadTestLedgerResume(ctx, f.store, f.ledger, "takeover")
					require.NoError(t, err)
				}
			case "known-clean", "known-blocked":
				facts := []string{ledgerFactIngestKnown}
				if kind == "known-blocked" {
					facts = append(facts, ledgerFactIngestBlocked)
					require.NoError(t, f.ledger.PutCounterBucket(ctx, "prior", 0, c1zstore.LedgerCounters{Flags: ingestQualityReasonGrantDropped}))
				}
				require.NoError(t, f.ledger.InitializePendingWork(ctx, pendingSeeds([]ledgerAction{{identity: c1zstore.LedgerActionIdentity{Op: InitOp.String()}}}), facts...))
			case "counter-only", "counter-flags":
				prior := c1zstore.LedgerCounters{Counters: map[string]uint64{"ingest.grants_dropped": 3}}
				if kind == "counter-flags" {
					prior.Flags = ingestQualityReasonGrantDropped
				}
				require.NoError(t, f.ledger.PutCounterBucket(ctx, "prior", 0, prior))
			case "uninitialized-fact", "uninitialized-blocked":
				require.NoError(t, f.store.CheckpointSync(ctx, `{"version":1,"actions":[]}`))
				facts := []string{ledgerFactIngestBlocked}
				prior := c1zstore.LedgerCounters{}
				if kind == "uninitialized-blocked" {
					facts = append(facts, ledgerFactIngestKnown)
					prior = c1zstore.LedgerCounters{Counters: map[string]uint64{"ingest.grants_dropped": 3}, Flags: ingestQualityReasonGrantDropped}
				}
				_, err := f.ledger.TakeoverToken(ctx, "prior", facts, prior)
				require.NoError(t, err)
			case "finished":
				require.NoError(t, f.store.EndSync(ctx))
				require.NoError(t, f.store.SetCurrentSync(ctx, id))
			}
			if uninitializedQuality {
				_, initialized, err := f.ledger.PendingWork(ctx, 0, 1)
				require.NoError(t, err)
				require.False(t, initialized)
				require.NoError(t, f.store.Close(ctx))
				f = openLedgerFixtureAt(t, f.path, false)
				require.NoError(t, f.store.SetCurrentSync(ctx, id))
			}
			priorCounters, err := f.ledger.LedgerCounters(ctx)
			require.NoError(t, err)
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
			err = s.prepareLedgerState(ctx, "resume", false)
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
			case kind == "known-blocked" || kind == "uninitialized-blocked":
				require.Equal(t, ingestQualityReasonGrantDropped, s.ingestFilterStats.snapshot().ReasonFlags)
			case kind == "uninitialized-fact":
				require.Zero(t, s.ingestFilterStats.snapshot().ReasonFlags)
			default:
				require.Equal(t, ingestQualityReasonUnknownPriorCheckpoint, s.ingestFilterStats.snapshot().ReasonFlags)
			}
			if kind == "read-error" || kind == "legacy-frontier" || kind == "known-clean" || kind == "known-blocked" {
				require.Equal(t, before, ledgerRawSnapshot(t, f.engine))
			}
			if uninitializedQuality {
				storedCounters, err := f.ledger.LedgerCounters(ctx)
				require.NoError(t, err)
				require.Equal(t, priorCounters, storedCounters)
				if kind != "uninitialized-blocked" {
					require.False(t, s.run.hasFact(ledgerFactIngestKnown))
				}
				require.NoError(t, f.store.Close(ctx))
				f = openLedgerFixtureAt(t, f.path, false)
				resumed, err := NewSyncer(ctx, newMockConnector(), WithConnectorStore(f.store), WithSyncID(id), WithSkipEntitlementsAndGrants(true))
				require.NoError(t, err)
				require.NoError(t, resumed.Sync(ctx))
				require.NoError(t, f.store.Close(ctx))
				f = openLedgerFixtureAt(t, f.path, false)
				stats, err := engine.ReadSyncStatsRecord(ctx, f.engine, id)
				require.NoError(t, err)
				require.True(t, stats.GetIngestQuality().GetSourceCacheReplayBlocked())
				require.Equal(t, priorCounters.Flags, stats.GetIngestQuality().GetReasonFlags()&priorCounters.Flags)
				require.Equal(t, priorCounters.Counters["ingest.grants_dropped"], stats.GetIngestQuality().GetGrantsDropped())
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
				err = restored.prepareLedgerState(ctx, "after-init", false)
				f.audit.enter(ledgerLifecycle)
				require.NoError(t, err)
				require.Equal(t, before, ledgerRawSnapshot(t, f.engine))
				require.Equal(t, &IngestQualityCheckpoint{}, restored.stats.ingestQuality())
			}
		})
	}
}
