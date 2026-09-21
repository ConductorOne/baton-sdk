package pebble

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
)

func TestLedgerArchivePreservesFinishedState(t *testing.T) {
	e, _ := newTestEngine(t)
	id, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	_, err = e.ArchiveLedgerReport(t.Context())
	require.Error(t, err)
	writer := e.Ledger().BeginPage()
	options := c1zstore.LedgerReportOptions{Attempt: "attempt", EffectiveSkipGrants: true, Requested: c1zstore.LedgerRequestedOptions{WorkerCount: 4}}
	encoded, err := json.Marshal(options)
	require.NoError(t, err)
	require.NoError(t, writer.SetFactValue(c1zstore.LedgerFactReportOptions, string(encoded)))
	require.NoError(t, writer.SetFactValue(c1zstore.LedgerFactReportOptionsPrefix+"attempt", string(encoded)))
	require.NoError(t, writer.SetFact("skip-grants"))
	counters := c1zstore.LedgerCounters{Counters: map[string]uint64{"completed": 7}, Flags: 2}
	require.NoError(t, writer.SetCounterBucket("attempt", 0, counters))
	page := grantsPageIdentity("group", "secret-token")
	require.NoError(t, writer.Commit(t.Context(), page, &c1zstore.LedgerRow{GrantsWritten: 3}))
	require.NoError(t, e.EndSyncWithStats(t.Context(), c1zstore.SyncStats{}))
	report, err := e.ArchiveLedgerReport(t.Context())
	require.NoError(t, err)
	require.NotContains(t, string(report), "secret-token")
	require.NoError(t, e.Ledger().Drop(t.Context()))
	saved, err := e.GetArchivedLedgerReport(t.Context())
	require.NoError(t, err)
	require.JSONEq(t, string(report), string(saved))
	for _, attempt := range []string{"", "attempt"} {
		savedOptions, err := e.GetArchivedLedgerOptions(t.Context(), attempt)
		require.NoError(t, err)
		require.Equal(t, options, *savedOptions)
	}
	require.NoError(t, e.SetCurrentSync(t.Context(), id))
	before, err := e.GetSyncRunRecord(t.Context(), id)
	require.NoError(t, err)
	require.NoError(t, e.RestoreLedgerArchive(t.Context()))
	require.NoError(t, e.RestoreLedgerArchive(t.Context()))
	folded, err := e.Ledger().Counters(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 7, folded.Counters["completed"])
	facts, err := e.Ledger().Facts(t.Context())
	require.NoError(t, err)
	require.Contains(t, facts, "skip-grants")
	after, err := e.GetSyncRunRecord(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, before.GetStartedAt(), after.GetStartedAt())
	require.Equal(t, before.GetEndedAt(), after.GetEndedAt())
	require.NoError(t, e.Ledger().PutCounterBucket(t.Context(), "later", 0, c1zstore.LedgerCounters{Counters: map[string]uint64{"completed": 5}}))
	require.NoError(t, e.RestoreLedgerArchive(t.Context()))
	folded, err = e.Ledger().Counters(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 12, folded.Counters["completed"])
	_, err = e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	saved, err = e.GetArchivedLedgerReport(t.Context())
	require.NoError(t, err)
	require.Nil(t, saved)
}

func TestLedgerArchiveUnreadableDoesNotRestore(t *testing.T) {
	e, _ := newTestEngine(t)
	id, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, e.EndSync(t.Context()))
	require.NoError(t, e.SetCurrentSync(t.Context(), id))
	require.NoError(t, e.db.MetaSet(ledgerArchiveKey(), []byte("broken"), pebble.Sync))
	require.Error(t, e.RestoreLedgerArchive(t.Context()))
	facts, err := e.Ledger().Facts(t.Context())
	require.NoError(t, err)
	require.Empty(t, facts)
	active, err := e.Ledger().active()
	require.NoError(t, err)
	require.False(t, active)
}

func TestLedgerArchiveFailureCuts(t *testing.T) {
	for _, cut := range []string{"before-write", "after-write"} {
		t.Run(cut, func(t *testing.T) {
			e, _ := newTestEngine(t)
			id, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			page := grantsPageIdentity("group", "cursor")
			writer := e.Ledger().BeginPage()
			require.NoError(t, writer.SetFact("known"))
			require.NoError(t, writer.Commit(t.Context(), page, &c1zstore.LedgerRow{}))
			require.NoError(t, e.EndSyncWithStats(t.Context(), c1zstore.SyncStats{}))
			injected := errors.New("archive cut")
			e.test.ledgerArchiveHook = func(stage string) error {
				if stage == cut {
					return injected
				}
				return nil
			}
			_, err = e.ArchiveLedgerReport(t.Context())
			require.ErrorIs(t, err, injected)
			_, found, err := e.Ledger().GetRow(t.Context(), page)
			require.NoError(t, err)
			require.True(t, found)
			e.test.ledgerArchiveHook = nil
			_, err = e.ArchiveLedgerReport(t.Context())
			require.NoError(t, err)
			require.NoError(t, e.Ledger().Drop(t.Context()))
			require.NoError(t, e.SetCurrentSync(t.Context(), id))
			e.db.SetRecordCommitTestHook(func() error { return injected })
			require.ErrorIs(t, e.RestoreLedgerArchive(t.Context()), injected)
			e.db.SetRecordCommitTestHook(nil)
			facts, err := e.Ledger().Facts(t.Context())
			require.NoError(t, err)
			require.Empty(t, facts)
			require.NoError(t, e.RestoreLedgerArchive(t.Context()))
			facts, err = e.Ledger().Facts(t.Context())
			require.NoError(t, err)
			require.Contains(t, facts, "known")
		})
	}
}

func TestLedgerArchiveFollowsCompactedBaseRename(t *testing.T) {
	e, _ := newTestEngine(t)
	id, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	writer := e.Ledger().BeginPage()
	require.NoError(t, writer.SetFact("base_skip_grants"))
	require.NoError(t, writer.Commit(t.Context(), grantsPageIdentity("group", ""), &c1zstore.LedgerRow{}))
	require.NoError(t, e.EndSyncWithStats(t.Context(), c1zstore.SyncStats{}))
	_, err = e.ArchiveLedgerReport(t.Context())
	require.NoError(t, err)
	require.NoError(t, e.Ledger().Drop(t.Context()))
	record, err := e.GetSyncRunRecord(t.Context(), id)
	require.NoError(t, err)
	compactedID := ksuid.New().String()
	record.SetSyncId(compactedID)
	require.NoError(t, e.PutSyncRunRecord(t.Context(), record))
	require.NoError(t, e.SetCurrentSync(t.Context(), compactedID))
	require.Error(t, e.RestoreLedgerArchive(t.Context()))
	record.SetCompacted(true)
	require.NoError(t, e.PutSyncRunRecord(t.Context(), record))
	require.NoError(t, e.RestoreLedgerArchive(t.Context()))
	facts, err := e.Ledger().Facts(t.Context())
	require.NoError(t, err)
	require.Contains(t, facts, "base_skip_grants")
}

func TestLedgerArchiveKeepsCollectionAcrossProcessing(t *testing.T) {
	e, _ := newTestEngine(t)
	id, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	for pass := range 3 {
		if pass > 0 {
			require.NoError(t, e.SetCurrentSync(t.Context(), id))
			require.NoError(t, e.RestoreLedgerArchive(t.Context()))
			require.NoError(t, e.Ledger().ClearRows(t.Context(), nil))
		}
		writer := e.Ledger().BeginPage()
		options, err := json.Marshal(c1zstore.LedgerReportOptions{Requested: c1zstore.LedgerRequestedOptions{OnlyExpandGrants: pass > 0}})
		require.NoError(t, err)
		require.NoError(t, writer.SetFactValue(c1zstore.LedgerFactReportOptions, string(options)))
		require.NoError(t, writer.Commit(t.Context(), grantsPageIdentity("group", ""), &c1zstore.LedgerRow{}))
		require.NoError(t, e.EndSyncWithStats(t.Context(), c1zstore.SyncStats{}))
		report, err := e.ArchiveLedgerReport(t.Context())
		require.NoError(t, err)
		var envelope struct {
			Collection json.RawMessage `json:"preceding_collection"`
			Latest     json.RawMessage `json:"latest"`
		}
		require.NoError(t, json.Unmarshal(report, &envelope))
		if pass > 0 {
			require.NotEmpty(t, envelope.Collection)
			require.Contains(t, string(envelope.Collection), `"only_expand_grants":false`)
			require.Contains(t, string(envelope.Latest), `"only_expand_grants":true`)
			require.NotContains(t, string(envelope.Collection), "preceding_collection")
		} else {
			require.Empty(t, envelope.Collection)
		}
		require.NoError(t, e.Ledger().Drop(t.Context()))
	}
}
