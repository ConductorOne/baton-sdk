package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

func TestLedgerAttemptMetadataBoundedAcrossResumes(t *testing.T) {
	s, f := newLedgerSchedulerFixture(t, 1)
	s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, page *ledgerPage) error {
		page.observations.Counters = map[string]uint64{"pages": 1}
		next := "more"
		if s.ledger.runID == "attempt-127" {
			next = ""
		}
		return s.nextPageOrFinishAction(ctx, action, next)
	}
	for i := 0; i < 128; i++ {
		s.cfg.workerCount = i%4 + 1
		f.audit.enter(ledgerLifecycle)
		require.NoError(t, s.prepareLedgerState(t.Context(), fmt.Sprintf("attempt-%d", i), false))
		require.NoError(t, s.putLedgerReportOptions(t.Context()))
		f.audit.enter(ledgerHandler)
		require.NoError(t, s.invokeActionPage(t.Context(), s.run.current(), nil, false))
		f.audit.enter(ledgerLifecycle)
		facts, err := f.ledger.LedgerFacts(t.Context())
		require.NoError(t, err)
		require.Len(t, facts, 3)
		var first, latest c1zstore.LedgerReportOptions
		require.NoError(t, json.Unmarshal([]byte(facts[c1zstore.LedgerFactFirstReportOptions]), &first))
		require.NoError(t, json.Unmarshal([]byte(facts[c1zstore.LedgerFactReportOptions]), &latest))
		require.Equal(t, "attempt-0", first.Attempt)
		require.Equal(t, 1, first.Requested.WorkerCount)
		require.Equal(t, fmt.Sprintf("attempt-%d", i), latest.Attempt)
		require.Equal(t, s.cfg.workerCount, latest.Requested.WorkerCount)
	}
	total, err := f.ledger.LedgerCounters(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 128, total.Counters["pages"])
	buckets := 0
	for _, row := range ledgerRawSnapshot(t, f.engine) {
		if len(row.key) > 2 && row.key[0] == 3 && row.key[1] == 12 && row.key[2] == 2 {
			buckets++
		}
	}
	require.Equal(t, 2, buckets)
	require.NoError(t, s.ledger.prepareSeal(t.Context(), c1zstore.LedgerCounters{}, c1zstore.LedgerFactDiscardOnSeal))
	require.NoError(t, s.ledger.seal(t.Context()))
	require.NoError(t, f.store.Close(t.Context()))
	f = openLedgerFixtureAt(t, f.path, false)
	for _, attempt := range []string{"attempt-0", "attempt-127", ""} {
		options, err := f.engine.GetArchivedLedgerOptions(t.Context(), attempt)
		require.NoError(t, err)
		require.NotNil(t, options)
		if attempt != "" {
			require.Equal(t, attempt, options.Attempt)
		}
	}
	options, err := f.engine.GetArchivedLedgerOptions(t.Context(), "attempt-64")
	require.NoError(t, err)
	require.Nil(t, options)
	report, err := f.ledger.GetArchivedLedgerReport(t.Context())
	require.NoError(t, err)
	var saved struct {
		Latest struct {
			First struct {
				Attempt string `json:"attempt"`
			} `json:"first_attempt_options"`
			Latest struct {
				Attempt string `json:"attempt"`
			} `json:"latest_attempt_options"`
		} `json:"latest"`
	}
	require.NoError(t, json.Unmarshal(report, &saved))
	require.Equal(t, "attempt-0", saved.Latest.First.Attempt)
	require.Equal(t, "attempt-127", saved.Latest.Latest.Attempt)
}

// CO-038: the snapshot precedes the first page and reflects the request, so a
// fresh sync that disables grants records that before Init has turned the
// option into a fact; an attempt that commits no page still names itself as
// latest; first survives.
func TestLedgerOptionsSnapshotBeforeAnyPage(t *testing.T) {
	s, f := newLedgerSchedulerFixture(t, 1)
	s.cfg.skipGrants = true
	f.audit.enter(ledgerLifecycle)
	require.NoError(t, s.prepareLedgerState(t.Context(), "attempt-a", true))
	require.False(t, s.run.hasFact(factShouldSkipGrants), "premise: Init has not run")
	require.NoError(t, s.putLedgerReportOptions(t.Context()))
	facts, err := f.ledger.LedgerFacts(t.Context())
	require.NoError(t, err)
	var first, latest c1zstore.LedgerReportOptions
	require.NoError(t, json.Unmarshal([]byte(facts[c1zstore.LedgerFactFirstReportOptions]), &first))
	require.NoError(t, json.Unmarshal([]byte(facts[c1zstore.LedgerFactReportOptions]), &latest))
	require.True(t, first.EffectiveSkipGrants)
	require.True(t, latest.EffectiveSkipGrants)
	require.Equal(t, "attempt-a", latest.Attempt)

	s.cfg.skipGrants = false
	require.NoError(t, s.prepareLedgerState(t.Context(), "attempt-b", false))
	require.NoError(t, s.putLedgerReportOptions(t.Context()))
	facts, err = f.ledger.LedgerFacts(t.Context())
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal([]byte(facts[c1zstore.LedgerFactFirstReportOptions]), &first))
	require.NoError(t, json.Unmarshal([]byte(facts[c1zstore.LedgerFactReportOptions]), &latest))
	require.Equal(t, "attempt-a", first.Attempt, "first survives a later attempt")
	require.Equal(t, "attempt-b", latest.Attempt, "an attempt with no page is still the latest")
	require.False(t, latest.EffectiveSkipGrants)
}

type ledgerFailingFactsStore struct {
	c1zstore.PageLedgerStore
}

var errLedgerInjectedFacts = errors.New("injected fact write failure")

func (ledgerFailingFactsStore) PutLedgerFacts(context.Context, map[string]string) error {
	return errLedgerInjectedFacts
}

func TestLedgerOptionsSnapshotFailureWritesNothing(t *testing.T) {
	s, f := newLedgerSchedulerFixture(t, 1)
	f.audit.enter(ledgerLifecycle)
	require.NoError(t, s.prepareLedgerState(t.Context(), "attempt-a", true))
	actual := s.caps.pageLedger
	s.caps.pageLedger = ledgerFailingFactsStore{PageLedgerStore: actual}
	require.ErrorIs(t, s.putLedgerReportOptions(t.Context()), errLedgerInjectedFacts)
	facts, err := f.ledger.LedgerFacts(t.Context())
	require.NoError(t, err)
	require.NotContains(t, facts, c1zstore.LedgerFactFirstReportOptions)
	require.NotContains(t, facts, c1zstore.LedgerFactReportOptions)
	s.caps.pageLedger = actual
	require.NoError(t, s.putLedgerReportOptions(t.Context()))
	facts, err = f.ledger.LedgerFacts(t.Context())
	require.NoError(t, err)
	require.NotEmpty(t, facts[c1zstore.LedgerFactFirstReportOptions])
	require.Equal(t, facts[c1zstore.LedgerFactFirstReportOptions], facts[c1zstore.LedgerFactReportOptions])
}
