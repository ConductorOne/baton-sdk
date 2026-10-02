package pebble

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

func TestLedgerPutFactsIsOneUnit(t *testing.T) {
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	require.Error(t, e.Ledger().PutFacts(t.Context(), nil))
	require.Error(t, e.Ledger().PutFacts(t.Context(), map[string]string{"": "x"}))

	injected := errors.New("fact commit failure")
	e.db.SetRecordCommitTestHook(func() error { return injected })
	err = e.Ledger().PutFacts(t.Context(), map[string]string{"first": "a", "latest": "a"})
	e.db.SetRecordCommitTestHook(nil)
	require.ErrorIs(t, err, injected)
	facts, err := e.Ledger().Facts(t.Context())
	require.NoError(t, err)
	require.Empty(t, facts, "a failed batch lands neither fact")

	require.NoError(t, e.Ledger().PutFacts(t.Context(), map[string]string{"first": "a", "latest": "a", "bare": ""}))
	facts, err = e.Ledger().Facts(t.Context())
	require.NoError(t, err)
	require.Equal(t, map[string]string{"first": "a", "latest": "a", "bare": ""}, facts)

	require.NoError(t, e.Ledger().PutFacts(t.Context(), map[string]string{"latest": "b"}))
	facts, err = e.Ledger().Facts(t.Context())
	require.NoError(t, err)
	require.Equal(t, map[string]string{"first": "a", "latest": "b", "bare": ""}, facts, "a later write supersedes only the keys it names")

	active, err := e.Ledger().active()
	require.NoError(t, err)
	require.True(t, active, "a fact write stamps the ledger in flight")
}

func TestLedgerPutFactsRequiresBoundSync(t *testing.T) {
	e, _ := newTestEngine(t)
	require.Error(t, e.Ledger().PutFacts(t.Context(), map[string]string{"x": "y"}))
}
