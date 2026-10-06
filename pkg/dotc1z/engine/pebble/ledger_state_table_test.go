package pebble

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// The pass as a state machine (plan, CO-039 §1–§3). One row per (state,
// event); the expected column is the plan's table. A row's verdict is the
// declaration's phase and ended_at after the event, or that the event was
// refused and left both as they were.

type ledgerState string

const (
	stateUnstarted  ledgerState = "unstarted"
	stateCollecting ledgerState = "collecting"
	stateExpanding  ledgerState = "expanding"
	stateSealing    ledgerState = "sealing"
	stateSealed     ledgerState = "sealed"
)

type ledgerEvent string

const (
	eventSeed           ledgerEvent = "seed"
	eventPage           ledgerEvent = "page"
	eventBeginExpanding ledgerEvent = "begin-expanding"
	eventCompleteEntry  ledgerEvent = "complete-entry"
	eventTerminal       ledgerEvent = "terminal"
	eventStamp          ledgerEvent = "stamp"
	eventBeginPass      ledgerEvent = "begin-pass"
	eventCounterBucket  ledgerEvent = "counter-bucket"
	eventPutFacts       ledgerEvent = "put-facts"
)

// refused means the event returned an error and the state is unchanged.
const refused ledgerState = "refused"

var ledgerStateTable = map[ledgerState]map[ledgerEvent]ledgerState{
	stateUnstarted: {
		eventSeed: stateCollecting, eventPage: stateUnstarted, eventBeginExpanding: refused, eventCompleteEntry: refused,
		// The stamp on a sync with no ledger family is the token path's seal.
		eventTerminal: refused, eventStamp: stateSealed, eventBeginPass: refused, eventCounterBucket: stateUnstarted, eventPutFacts: stateUnstarted,
	},
	stateCollecting: {
		eventSeed: stateCollecting, eventPage: stateCollecting, eventBeginExpanding: stateExpanding, eventCompleteEntry: stateCollecting,
		eventTerminal: refused, eventStamp: refused, eventBeginPass: refused, eventCounterBucket: stateCollecting, eventPutFacts: stateCollecting,
	},
	stateExpanding: {
		eventSeed: stateExpanding, eventPage: refused, eventBeginExpanding: refused, eventCompleteEntry: stateExpanding,
		eventTerminal: refused, eventStamp: refused, eventBeginPass: refused, eventCounterBucket: stateExpanding, eventPutFacts: stateExpanding,
	},
	stateSealing: {
		eventSeed: stateSealing, eventPage: refused, eventBeginExpanding: refused, eventCompleteEntry: refused,
		eventTerminal: refused, eventStamp: stateSealed, eventBeginPass: refused, eventCounterBucket: stateSealing, eventPutFacts: stateSealing,
	},
	stateSealed: {
		eventSeed: refused, eventPage: stateSealed, eventBeginExpanding: refused, eventCompleteEntry: refused,
		eventTerminal: refused, eventStamp: stateSealed, eventBeginPass: stateCollecting, eventCounterBucket: stateSealed, eventPutFacts: stateSealed,
	},
}

// Rows the table cannot express as a plain transition: in Collecting and
// Expanding the terminal page succeeds once the expansion entry is gone, and
// in Collecting the seed is a no-op on an initialized queue. Those are
// covered in ledger_phase_test.go and pending_work_test.go; here the queue
// still holds the expansion entry, so the terminal page is refused for want
// of a drained queue, and the seed leaves the declaration as it was.

func expansionSeed() c1zstore.LedgerWork {
	return c1zstore.LedgerWork{Action: c1zstore.LedgerChild{Identity: c1zstore.LedgerActionIdentity{Op: ledgerExpansionOp}}}
}

func buildLedgerState(t *testing.T, e *Engine, state ledgerState) (string, c1zstore.LedgerWork) {
	t.Helper()
	ctx := t.Context()
	syncID, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	if state == stateUnstarted {
		return syncID, c1zstore.LedgerWork{}
	}
	require.NoError(t, e.Ledger().BeginCollecting(ctx, []c1zstore.LedgerWork{expansionSeed()}, nil))
	pending, _, err := e.Ledger().PendingWork(ctx, 0, 1)
	require.NoError(t, err)
	entry := pending[0]
	writer := e.Ledger().BeginPage()
	require.NoError(t, writer.SetFact("collected"))
	require.NoError(t, writer.Commit(ctx, grantsPageIdentity("group", "cursor"), &c1zstore.LedgerRow{}))
	if state == stateCollecting {
		return syncID, entry
	}
	require.NoError(t, e.Ledger().BeginExpanding(ctx))
	if state == stateExpanding {
		return syncID, entry
	}
	require.NoError(t, e.Ledger().CompletePendingWork(ctx, entry, "attempt", c1zstore.LedgerCounters{}))
	require.NoError(t, commitTerminalPage(t, e, ctx))
	if state == stateSealing {
		return syncID, entry
	}
	require.NoError(t, e.EndSyncWithStats(ctx, c1zstore.SyncStats{}))
	require.NoError(t, e.SetCurrentSync(ctx, syncID))
	return syncID, entry
}

func observeLedgerState(t *testing.T, e *Engine) ledgerState {
	t.Helper()
	ctx := t.Context()
	_, phase, err := e.Ledger().PendingWork(ctx, 0, 1)
	require.NoError(t, err)
	finished, err := e.BoundSyncFinished(ctx)
	require.NoError(t, err)
	switch phase {
	case c1zstore.LedgerQueueCollecting:
		return stateCollecting
	case c1zstore.LedgerQueueExpanding:
		return stateExpanding
	case c1zstore.LedgerQueueSealing:
		return stateSealing
	case c1zstore.LedgerQueueAbsent:
	}
	if finished {
		return stateSealed
	}
	return stateUnstarted
}

func fireLedgerEvent(t *testing.T, e *Engine, event ledgerEvent, entry c1zstore.LedgerWork) error {
	t.Helper()
	ctx := t.Context()
	switch event {
	case eventSeed:
		return e.Ledger().BeginCollecting(ctx, []c1zstore.LedgerWork{expansionSeed()}, nil)
	case eventPage:
		writer := e.Ledger().BeginPage()
		defer writer.Discard()
		require.NoError(t, writer.SetFact("event-page"))
		return writer.Commit(ctx, grantsPageIdentity("group", "event"), &c1zstore.LedgerRow{})
	case eventBeginExpanding:
		return e.Ledger().BeginExpanding(ctx)
	case eventCompleteEntry:
		return e.Ledger().CompletePendingWork(ctx, entry, "attempt", c1zstore.LedgerCounters{})
	case eventTerminal:
		writer := e.Ledger().BeginPage()
		defer writer.Discard()
		require.NoError(t, writer.SetTerminal())
		return writer.Commit(ctx, c1zstore.LedgerActionIdentity{Op: "sync-terminal-v1"}, &c1zstore.LedgerRow{})
	case eventStamp:
		return e.EndSyncWithStats(ctx, c1zstore.SyncStats{})
	case eventBeginPass:
		return e.Ledger().BeginPass(ctx, nil, nil)
	case eventCounterBucket:
		return e.Ledger().PutCounterBucket(ctx, "attempt", c1zstore.RunBucketWorker, c1zstore.LedgerCounters{Counters: map[string]uint64{"x": 1}})
	case eventPutFacts:
		return e.Ledger().PutFacts(ctx, map[string]string{"event-fact": ""})
	}
	t.Fatalf("unknown event %q", event)
	return nil
}

func TestLedgerStateTable(t *testing.T) {
	for state, events := range ledgerStateTable {
		for event, want := range events {
			t.Run(string(state)+"/"+string(event), func(t *testing.T) {
				e, _ := newTestEngine(t)
				syncID, entry := buildLedgerState(t, e, state)
				require.Equal(t, state, observeLedgerState(t, e), "fixture reached its state")
				err := fireLedgerEvent(t, e, event, entry)
				if event == eventStamp && err == nil {
					require.NoError(t, e.SetCurrentSync(t.Context(), syncID))
				}
				got := observeLedgerState(t, e)
				if want == refused {
					require.Error(t, err, "the table says this event is refused in %s", state)
					require.Equal(t, state, got, "a refused event leaves the state as it was")
					return
				}
				require.NoError(t, err)
				require.Equal(t, want, got)
			})
		}
	}
}

// The transition's batch is one unit: a failed commit leaves Collecting.
func TestLedgerBeginExpandingIsOneUnit(t *testing.T) {
	e, _ := newTestEngine(t)
	_, _ = buildLedgerState(t, e, stateCollecting)
	injected := errors.New("expanding commit failure")
	e.test.ledgerBeginExpandingHook = func() error { return injected }
	require.ErrorIs(t, e.Ledger().BeginExpanding(t.Context()), injected)
	e.test.ledgerBeginExpandingHook = nil
	require.Equal(t, stateCollecting, observeLedgerState(t, e))
	e.db.SetRecordCommitTestHook(func() error { return injected })
	require.ErrorIs(t, e.Ledger().BeginExpanding(t.Context()), injected)
	e.db.SetRecordCommitTestHook(nil)
	require.Equal(t, stateCollecting, observeLedgerState(t, e))
	require.NoError(t, e.Ledger().BeginExpanding(t.Context()))
	require.Equal(t, stateExpanding, observeLedgerState(t, e))

	// Guard: more than the expansion entry pending.
	e2, _ := newTestEngine(t)
	_, entry2 := buildLedgerState(t, e2, stateCollecting)
	writer := e2.Ledger().BeginPage()
	require.NoError(t, writer.SetPendingWork(entry2))
	child := c1zstore.LedgerChild{Identity: c1zstore.LedgerActionIdentity{Op: "list-resources", ResourceTypeID: "late"}}
	require.NoError(t, writer.Commit(t.Context(), entry2.Action.Identity, &c1zstore.LedgerRow{NextPageToken: "more", Children: []c1zstore.LedgerChild{child}}))
	require.ErrorContains(t, e2.Ledger().BeginExpanding(t.Context()), "exactly the expansion entry", "collection work still pending beside the expansion entry")
}
