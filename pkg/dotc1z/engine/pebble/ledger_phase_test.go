package pebble

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

func phaseTestTerminal(t *testing.T, e *Engine, row *c1zstore.LedgerRow) error {
	t.Helper()
	writer := e.Ledger().BeginPage()
	defer writer.Discard()
	require.NoError(t, writer.SetTerminal())
	require.NoError(t, writer.SetFact("terminal"))
	require.NoError(t, writer.SetCounterBucket("attempt", c1zstore.RunBucketWorker, c1zstore.LedgerCounters{Counters: map[string]uint64{"run": 1}}))
	if row == nil {
		row = &c1zstore.LedgerRow{}
	}
	return writer.Commit(t.Context(), c1zstore.LedgerActionIdentity{Op: "terminal"}, row)
}

func phaseOf(t *testing.T, e *Engine) c1zstore.LedgerQueuePhase {
	t.Helper()
	_, phase, err := e.Ledger().PendingWork(t.Context(), 0, 1)
	require.NoError(t, err)
	return phase
}

func TestLedgerTerminalTransitionRefusesUntilDrained(t *testing.T) {
	e, _ := newTestEngine(t)
	work := pendingTestSeed(t, e)
	require.Equal(t, c1zstore.LedgerQueueCollecting, phaseOf(t, e))

	err := phaseTestTerminal(t, e, nil)
	require.ErrorContains(t, err, "empty pending-work queue", "the seed's entry blocks the terminal page")
	require.Equal(t, c1zstore.LedgerQueueCollecting, phaseOf(t, e))
	facts, err := e.Ledger().Facts(t.Context())
	require.NoError(t, err)
	require.NotContains(t, facts, "terminal", "a refused terminal page lands nothing")

	require.NoError(t, pendingTestCommit(t, e, work, "B"))
	require.ErrorContains(t, phaseTestTerminal(t, e, nil), "empty pending-work queue", "a continuation keeps the queue nonempty")
	items, phase, err := e.Ledger().PendingWork(t.Context(), 0, 1)
	require.NoError(t, err)
	require.Equal(t, c1zstore.LedgerQueueCollecting, phase)
	require.Len(t, items, 1)
	require.NoError(t, pendingTestCommit(t, e, items[0], ""))

	for _, row := range []*c1zstore.LedgerRow{
		{NextPageToken: "more"},
		{Children: []c1zstore.LedgerChild{{Identity: c1zstore.LedgerActionIdentity{Op: "child"}}}},
	} {
		require.ErrorContains(t, phaseTestTerminal(t, e, row), "must not carry", "%+v", row)
		require.Equal(t, c1zstore.LedgerQueueCollecting, phaseOf(t, e))
	}
	writer := e.Ledger().BeginPage()
	require.NoError(t, writer.SetTerminal())
	require.NoError(t, writer.SetPendingWork(work))
	require.ErrorContains(t, writer.Commit(t.Context(), work.Action.Identity, &c1zstore.LedgerRow{}), "must not carry")
	writer.Discard()

	require.NoError(t, phaseTestTerminal(t, e, nil))
	require.Equal(t, c1zstore.LedgerQueueSealing, phaseOf(t, e))
	facts, err = e.Ledger().Facts(t.Context())
	require.NoError(t, err)
	require.Contains(t, facts, "terminal")
}

func TestLedgerSealingRefusesLatePages(t *testing.T) {
	e, _ := newTestEngine(t)
	work := pendingTestSeed(t, e)
	require.NoError(t, pendingTestCommit(t, e, work, ""))
	require.NoError(t, phaseTestTerminal(t, e, nil))

	writer := e.Ledger().BeginPage()
	require.NoError(t, writer.SetFact("late"))
	require.ErrorIs(t, writer.Commit(t.Context(), c1zstore.LedgerActionIdentity{Op: "late"}, &c1zstore.LedgerRow{}), ErrLedgerQueuePhase)
	writer.Discard()
	require.ErrorIs(t, phaseTestTerminal(t, e, nil), ErrLedgerQueuePhase, "a second terminal page is a late page")
	require.Error(t, e.Ledger().CompletePendingWork(t.Context(), work, "attempt", c1zstore.LedgerCounters{}))
	facts, err := e.Ledger().Facts(t.Context())
	require.NoError(t, err)
	require.NotContains(t, facts, "late")
	require.Equal(t, c1zstore.LedgerQueueSealing, phaseOf(t, e))

	// Lifecycle writes that do not commit a page stay open to the sealing attempt.
	require.NoError(t, e.Ledger().PutCounterBucket(t.Context(), "attempt2", c1zstore.RunBucketWorker, c1zstore.LedgerCounters{Counters: map[string]uint64{"run": 2}}))
	require.NoError(t, e.Ledger().PutFacts(t.Context(), map[string]string{"options": "v"}))
}

func TestLedgerTerminalTransitionIsOneBatch(t *testing.T) {
	e, _ := newTestEngine(t)
	work := pendingTestSeed(t, e)
	require.NoError(t, pendingTestCommit(t, e, work, ""))

	injected := errors.New("terminal commit failure")
	e.db.SetRecordCommitTestHook(func() error { return injected })
	err := phaseTestTerminal(t, e, nil)
	e.db.SetRecordCommitTestHook(nil)
	require.ErrorIs(t, err, injected)
	require.Equal(t, c1zstore.LedgerQueueCollecting, phaseOf(t, e))
	facts, err := e.Ledger().Facts(t.Context())
	require.NoError(t, err)
	require.NotContains(t, facts, "terminal")

	require.NoError(t, phaseTestTerminal(t, e, nil), "the retry succeeds from collecting")
	require.Equal(t, c1zstore.LedgerQueueSealing, phaseOf(t, e))
}

func TestLedgerTerminalTransitionRequiresDeclaration(t *testing.T) {
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.Equal(t, c1zstore.LedgerQueueAbsent, phaseOf(t, e))
	require.ErrorContains(t, phaseTestTerminal(t, e, nil), "requires a pending-work declaration")
	require.Equal(t, c1zstore.LedgerQueueAbsent, phaseOf(t, e))

	// A page without the transition is unaffected by the absent declaration.
	writer := e.Ledger().BeginPage()
	require.NoError(t, writer.SetFact("plain"))
	require.NoError(t, writer.Commit(t.Context(), c1zstore.LedgerActionIdentity{Op: "plain"}, &c1zstore.LedgerRow{}))
}

func TestLedgerWorkStateRejectsForeignEncodings(t *testing.T) {
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	for name, value := range map[string][]byte{
		"version-1":     binary.BigEndian.AppendUint64([]byte{1}, 7),
		"phase-absent":  append(binary.BigEndian.AppendUint64([]byte{workStateVersion}, 7), 0),
		"phase-unknown": append(binary.BigEndian.AppendUint64([]byte{workStateVersion}, 7), 9),
		"short":         {workStateVersion},
	} {
		t.Run(name, func(t *testing.T) {
			batch := e.db.NewRecordBatch()
			require.NoError(t, batch.StageLedgerWorkState(value))
			require.NoError(t, batch.Commit(pebble.Sync))
			batch.Close()
			_, _, err := e.Ledger().PendingWork(t.Context(), 0, 1)
			require.Error(t, err)
			_, _, err = e.Ledger().workState()
			require.Error(t, err)
		})
	}
	batch := e.db.NewRecordBatch()
	require.NoError(t, stageWorkState(batch, 7, c1zstore.LedgerQueueSealing))
	require.NoError(t, batch.Commit(pebble.Sync))
	batch.Close()
	last, phase, err := e.Ledger().workState()
	require.NoError(t, err)
	require.EqualValues(t, 7, last)
	require.Equal(t, c1zstore.LedgerQueueSealing, phase)
}
