package pebble

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
	"github.com/stretchr/testify/require"
)

func TestLedgerDebugReferenceChecksSurviveScrub(t *testing.T) {
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	valid := c1zstore.LedgerActionIdentity{Op: "child", PageToken: "secret-child-token"}
	require.NoError(t, e.Ledger().BeginPage().Commit(t.Context(), valid, &c1zstore.LedgerRow{}))
	children := []c1zstore.LedgerChild{{Identity: valid}}
	for n := range 32 {
		children = append(children, c1zstore.LedgerChild{Identity: c1zstore.LedgerActionIdentity{Op: "missing", ResourceID: fmt.Sprint(n), PageToken: "secret-missing-token"}})
	}
	writer := e.Ledger().BeginPage()
	options, err := json.Marshal(c1zstore.LedgerReportOptions{Requested: c1zstore.LedgerRequestedOptions{LedgerDebug: true}})
	require.NoError(t, err)
	require.NoError(t, writer.SetFactValue(c1zstore.LedgerFactReportOptions, string(options)))
	id := c1zstore.LedgerActionIdentity{Op: "parent", PageToken: "secret-parent-token"}
	require.NoError(t, writer.Commit(t.Context(), id, &c1zstore.LedgerRow{NextPageToken: "secret-next-token", Children: children}))
	for _, scrub := range []bool{false, true} {
		if scrub {
			require.NoError(t, e.Ledger().scrubTokens(t.Context()))
		}
		report, err := e.GenerateLedgerReport(t.Context())
		require.NoError(t, err)
		require.NotContains(t, string(report), "secret-")
		var decoded struct {
			Performed bool                 `json:"reference_validation_performed"`
			Checks    ledgerReferenceStats `json:"reference_checks"`
		}
		require.NoError(t, json.Unmarshal(report, &decoded))
		require.True(t, decoded.Performed)
		require.EqualValues(t, 32, decoded.Checks.MissingChildren)
		require.EqualValues(t, 1, decoded.Checks.MissingContinuations)
		require.EqualValues(t, 34, decoded.Checks.Lookups)
		require.Zero(t, decoded.Checks.IdentityMismatches)
		require.Zero(t, decoded.Checks.Uncheckable)
		require.Len(t, decoded.Checks.Examples, 16)
	}
}

func TestLedgerReferenceFanInReadsEachRowOnce(t *testing.T) {
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	root := c1zstore.LedgerActionIdentity{Op: "root"}
	children := make([]c1zstore.LedgerChild, 0, 1000)
	for n := range 1000 {
		child := c1zstore.LedgerActionIdentity{Op: "child", ResourceID: fmt.Sprint(n)}
		children = append(children, c1zstore.LedgerChild{Identity: child})
		require.NoError(t, e.Ledger().BeginPage().Commit(t.Context(), child, &c1zstore.LedgerRow{Children: []c1zstore.LedgerChild{{Identity: root}}}))
	}
	require.NoError(t, e.Ledger().BeginPage().Commit(t.Context(), root, &c1zstore.LedgerRow{Children: children}))
	lo := rawdb.LedgerKeyPrefix()
	bounds := &pebble.IterOptions{LowerBound: lo, UpperBound: upperBoundOf(lo)}
	rows, err := e.db.NewIter(bounds)
	require.NoError(t, err)
	defer rows.Close()
	keys, err := e.db.NewIter(bounds)
	require.NoError(t, err)
	defer keys.Close()
	source := &countedLedgerReferenceSource{ledgerReportIterator: rows}
	lookup := &countedLedgerReferenceLookup{ledgerReferenceLookup: keys}
	stats, err := scanLedgerReferences(t.Context(), source, lookup)
	require.NoError(t, err)
	require.EqualValues(t, 2000, stats.Lookups)
	require.EqualValues(t, 1001, stats.RowsChecked)
	require.Equal(t, stats.RowsChecked, source.values)
	require.Equal(t, stats.Lookups, lookup.seeks)
	require.Zero(t, stats.MissingChildren)
	require.Zero(t, stats.IdentityMismatches)
}

func TestLedgerDefaultReportDoesNotCheckReferences(t *testing.T) {
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	root := c1zstore.LedgerActionIdentity{Op: "root"}
	require.NoError(t, e.Ledger().BeginPage().Commit(t.Context(), root, &c1zstore.LedgerRow{
		Children: []c1zstore.LedgerChild{{Identity: c1zstore.LedgerActionIdentity{Op: "missing"}}},
	}))
	report, err := e.GenerateLedgerReport(t.Context())
	require.NoError(t, err)
	var decoded map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(report, &decoded))
	require.JSONEq(t, `false`, string(decoded["reference_validation_performed"]))
	require.JSONEq(t, `null`, string(decoded["reference_checks"]))
}

type countedLedgerReferenceSource struct {
	ledgerReportIterator
	values uint64
}

func (s *countedLedgerReferenceSource) Value() []byte {
	s.values++
	return s.ledgerReportIterator.Value()
}

type countedLedgerReferenceLookup struct {
	ledgerReferenceLookup
	seeks uint64
}

func (s *countedLedgerReferenceLookup) SeekGE(key []byte) bool {
	s.seeks++
	return s.ledgerReferenceLookup.SeekGE(key)
}

func TestLedgerReferenceIdentityCheckedOncePerStoredRow(t *testing.T) {
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	target := c1zstore.LedgerActionIdentity{Op: "target"}
	for n := range 10 {
		id := c1zstore.LedgerActionIdentity{Op: "source", ResourceID: fmt.Sprint(n)}
		require.NoError(t, e.Ledger().BeginPage().Commit(t.Context(), id, &c1zstore.LedgerRow{Children: []c1zstore.LedgerChild{{Identity: target}}}))
	}
	value, err := marshalRecord(ledgerRowToProto(&c1zstore.LedgerRow{Identity: c1zstore.LedgerActionIdentity{Op: "wrong-target"}}))
	require.NoError(t, err)
	batch := e.db.NewRecordBatch()
	defer batch.Close()
	require.NoError(t, batch.StageLedgerRow(encodeLedgerKey(target), value))
	require.NoError(t, batch.Commit(pebble.Sync))
	stats, err := e.validateLedgerReferences(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 11, stats.RowsChecked)
	require.EqualValues(t, 10, stats.Lookups)
	require.EqualValues(t, 1, stats.IdentityMismatches)
	require.Zero(t, stats.MissingChildren)
}
