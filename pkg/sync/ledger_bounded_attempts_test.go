package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"encoding/json"
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
	require.NoError(t, s.prepareLedgerSeal(t.Context(), c1zstore.LedgerCounters{}, c1zstore.LedgerFactDiscardOnSeal))
	require.NoError(t, s.ledger.seal(t.Context()))
	require.NoError(t, f.store.Close(t.Context()))
	f = openLedgerFixtureAt(t, f.path, false)
	for _, attempt := range []string{"attempt-0", "attempt-127", ""} {
		options, err := f.ledger.GetArchivedLedgerOptions(t.Context(), attempt)
		require.NoError(t, err)
		require.NotNil(t, options)
		if attempt != "" {
			require.Equal(t, attempt, options.Attempt)
		}
	}
	options, err := f.ledger.GetArchivedLedgerOptions(t.Context(), "attempt-64")
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

func TestLedgerFirstOptionsFollowCommitOrder(t *testing.T) {
	s, f := newLedgerSchedulerFixture(t, 2)
	staged, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	f.audit.enter(ledgerHandler)
	go func() {
		_, err := s.ledger.runPage(t.Context(), 0, c1zstore.LedgerActionIdentity{Op: "later"}, func(_ context.Context, page *ledgerPage) error {
			page.reportOptions = s.stageLedgerReportOptions
			if err := page.setFact(factShouldSkipGrants); err != nil {
				return err
			}
			close(staged)
			<-release
			return page.transition("")
		})
		done <- err
	}()
	select {
	case <-staged:
	case err := <-done:
		t.Fatalf("page failed before staging: %v", err)
	}
	_, err := s.ledger.runPage(t.Context(), 1, c1zstore.LedgerActionIdentity{Op: "first"}, func(_ context.Context, page *ledgerPage) error {
		page.reportOptions = s.stageLedgerReportOptions
		return page.transition("")
	})
	require.NoError(t, err)
	close(release)
	require.NoError(t, <-done)
	f.audit.enter(ledgerLifecycle)
	facts, err := f.ledger.LedgerFacts(t.Context())
	require.NoError(t, err)
	for _, key := range []string{c1zstore.LedgerFactFirstReportOptions, c1zstore.LedgerFactReportOptions} {
		var options c1zstore.LedgerReportOptions
		require.NoError(t, json.Unmarshal([]byte(facts[key]), &options))
		require.False(t, options.EffectiveSkipGrants)
	}
	require.Contains(t, facts, factShouldSkipGrants)
}

func TestLedgerFailedOptionsCommitRetries(t *testing.T) {
	s, f := newLedgerSchedulerFixture(t, 1)
	s.ledger.store = ledgerFailingPageStore{PageLedgerStore: f.ledger, stage: "commit"}
	write := func() error {
		_, err := s.ledger.runPage(t.Context(), 0, c1zstore.LedgerActionIdentity{Op: "first"}, func(_ context.Context, page *ledgerPage) error {
			page.reportOptions = s.stageLedgerReportOptions
			return page.transition("")
		})
		return err
	}
	f.audit.enter(ledgerHandler)
	require.ErrorIs(t, write(), errLedgerInjectedPage)
	require.False(t, s.ledger.optionsRecorded)
	facts, err := f.ledger.LedgerFacts(t.Context())
	require.NoError(t, err)
	require.Empty(t, facts)
	s.ledger.store = f.ledger
	require.NoError(t, write())
	f.audit.enter(ledgerLifecycle)
	require.True(t, s.ledger.optionsRecorded)
	facts, err = f.ledger.LedgerFacts(t.Context())
	require.NoError(t, err)
	require.NotEmpty(t, facts[c1zstore.LedgerFactFirstReportOptions])
	require.Equal(t, facts[c1zstore.LedgerFactFirstReportOptions], facts[c1zstore.LedgerFactReportOptions])
}
