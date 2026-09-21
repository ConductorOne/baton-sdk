package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

func TestLedgerReportOptionsCommitWithPage(t *testing.T) {
	s, f := newLedgerSchedulerFixture(t, 1)
	s.cfg.previousSyncC1ZPath = "private-path-must-not-appear"
	s.cfg.skipGrants = false
	s.run.setFact(factShouldSkipGrants)
	action := s.run.pushAction(t.Context(), Action{Op: SyncResourcesOp, ResourceTypeID: "type"})
	s.testHooks.ledgerHandler = func(context.Context, *Action, *ledgerPage) error { return errors.New("failed page") }
	_, err := runLedgerSchedulerBatch(t, s, SyncResourcesOp)
	require.Error(t, err)
	facts, err := f.ledger.LedgerFacts(t.Context())
	require.NoError(t, err)
	require.NotContains(t, facts, c1zstore.LedgerFactReportOptions)
	s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, _ *ledgerPage) error {
		return s.nextPageOrFinishAction(ctx, action, "")
	}
	_, err = runLedgerSchedulerBatch(t, s, SyncResourcesOp)
	require.NoError(t, err)
	facts, err = f.ledger.LedgerFacts(t.Context())
	require.NoError(t, err)
	encoded := facts[c1zstore.LedgerFactReportOptions]
	require.NotEmpty(t, encoded)
	require.NotContains(t, encoded, "private-path")
	var options c1zstore.LedgerReportOptions
	require.NoError(t, json.Unmarshal([]byte(encoded), &options))
	require.True(t, options.EffectiveSkipGrants)
	require.False(t, options.Requested.SkipGrants)
	require.True(t, options.Requested.PreviousSourceConfigured)
	require.Equal(t, encoded, facts[c1zstore.LedgerFactReportOptionsPrefix+options.Attempt])
	_, found, err := f.ledger.GetLedgerRow(t.Context(), ledgerIdentity(action))
	require.NoError(t, err)
	require.True(t, found)
}

func TestLedgerCanonicalOptionsPreserveFlags(t *testing.T) {
	row := func(attempt, flag string) ledgerKV {
		return ledgerKV{
			key:   append([]byte{3, 12, 1}, c1zstore.LedgerFactReportOptionsPrefix+attempt...),
			value: append([]byte{2}, []byte(`{"attempt":"`+attempt+`","requested":{"skip_grants":`+flag+`}}`)...)}
	}
	baseline, err := canonicalLedgerSnapshot([]ledgerKV{row("one", "false")})
	require.NoError(t, err)
	repeated, err := canonicalLedgerSnapshot([]ledgerKV{row("one", "false"), row("two", "false")})
	require.NoError(t, err)
	require.Equal(t, baseline, repeated)
	changed, err := canonicalLedgerSnapshot([]ledgerKV{row("two", "true")})
	require.NoError(t, err)
	require.NotEqual(t, baseline, changed)
	bad := row("bad", "false")
	bad.value = []byte{2, '{'}
	_, err = canonicalLedgerSnapshot([]ledgerKV{bad})
	require.Error(t, err)
}
