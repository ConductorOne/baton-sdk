package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
	chaosoracle "github.com/conductorone/baton-sdk/internal/chaosconnector/oracle"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

func TestChaosConnectorSemanticCorpus(t *testing.T) {
	for _, corpusCase := range chaosconnector.SemanticCorpus() {
		t.Run(corpusCase.Name, func(t *testing.T) {
			runSemanticCorpusCase(t, corpusCase)
		})
	}
}

func TestChaosConnectorRetryDriftCorpus(t *testing.T) {
	for _, corpusCase := range chaosconnector.TemporalCorpus() {
		t.Run(corpusCase.Name, func(t *testing.T) {
			runTemporalCorpusCase(t, corpusCase)
		})
	}
}

func TestChaosConnectorConcurrentDuplicateCompletionOrder(t *testing.T) {
	for _, corpusCase := range chaosconnector.ConcurrentDuplicateCorpus() {
		t.Run(corpusCase.Name, func(t *testing.T) {
			runConcurrentDuplicateCase(t, corpusCase)
		})
	}
}

func TestChaosConnectorConcurrentDuplicateResumeOrder(t *testing.T) {
	for _, corpusCase := range chaosconnector.ConcurrentDuplicateCorpus() {
		t.Run(corpusCase.Name, func(t *testing.T) {
			runConcurrentDuplicateResumeCase(t, corpusCase)
		})
	}
}

func runSemanticCorpusCase(t *testing.T, corpusCase chaosconnector.SemanticCase) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	tmpDir := t.TempDir()
	c1zPath := filepath.Join(tmpDir, "semantic.c1z")

	scenario, err := chaosconnector.NewFullScenario()
	require.NoError(t, err)
	require.NoError(t, corpusCase.Apply(scenario))
	run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	harness := newChaosHarness(t, ctx, run, c1zPath, tmpDir, chaosTransportDirect)
	require.NoError(t, harness.Syncer.Sync(ctx))
	require.NoError(t, harness.Close(t.Context()))
	require.NoError(t, run.Runtime().VerifyRequired())

	store, err := dotc1z.NewStore(
		t.Context(),
		c1zPath,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(t.Context())) }()

	latest, err := store.SyncMeta().LatestFullSync(t.Context())
	require.NoError(t, err)
	require.NotNil(t, latest)
	assertSemanticExpectation(t, t.Context(), store, corpusCase.Expectation)
}

func runTemporalCorpusCase(t *testing.T, corpusCase chaosconnector.TemporalCase) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	tmpDir := t.TempDir()
	c1zPath := filepath.Join(tmpDir, "temporal.c1z")

	scenario, err := corpusCase.Build()
	require.NoError(t, err)
	run, err := chaosconnector.NewRun(scenario, corpusCase.Schedule)
	require.NoError(t, err)
	harness := newChaosHarness(t, ctx, run, c1zPath, tmpDir, chaosTransportDirect)
	require.NoError(t, harness.Syncer.Sync(ctx))
	require.NoError(t, harness.Close(t.Context()))
	require.NoError(t, run.Runtime().VerifyRequired())

	store, err := dotc1z.NewStore(
		t.Context(),
		c1zPath,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(t.Context())) }()

	assertSemanticExpectation(t, t.Context(), store, corpusCase.Expectation)
	if corpusCase.AbsentCanonicalIdentity != "" {
		assertCanonicalIdentityAbsent(
			t,
			t.Context(),
			store,
			corpusCase.Expectation.Entity,
			corpusCase.AbsentCanonicalIdentity,
		)
	}
}

func runConcurrentDuplicateCase(t *testing.T, corpusCase chaosconnector.ConcurrentDuplicateCase) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	tmpDir := t.TempDir()
	c1zPath := filepath.Join(tmpDir, "concurrent-duplicate.c1z")

	scenario, err := chaosconnector.NewConcurrentDuplicateScenario(corpusCase.Entity)
	require.NoError(t, err)
	run, err := chaosconnector.NewRun(scenario, corpusCase.Schedule)
	require.NoError(t, err)
	defer func() {
		if t.Failed() {
			t.Logf("chaos trace: %+v", run.Trace().Events())
		}
	}()
	harness := newChaosHarness(
		t, ctx, run, c1zPath, tmpDir, chaosTransportDirect, WithWorkerCount(2),
	)
	concreteSyncer, ok := harness.Syncer.(*syncer)
	require.True(t, ok)

	done := make(chan error, 1)
	go func() {
		done <- harness.Syncer.Sync(ctx)
	}()

	waitForConcurrentObservation(t, ctx, concreteSyncer, run, corpusCase, corpusCase.FirstToken)

	run.Runtime().ReleaseBarrier("release-" + corpusCase.BlockedToken)
	require.NoError(t, <-done)
	require.NoError(t, harness.Close(t.Context()))
	require.NoError(t, run.Runtime().VerifyRequired())

	store, err := dotc1z.NewStore(
		t.Context(),
		c1zPath,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(t.Context())) }()
	assertSemanticExpectation(t, t.Context(), store, corpusCase.Expectation(corpusCase.BlockedToken))
}

func runConcurrentDuplicateResumeCase(t *testing.T, corpusCase chaosconnector.ConcurrentDuplicateCase) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	tmpDir := t.TempDir()
	c1zPath := filepath.Join(tmpDir, "concurrent-duplicate-resume.c1z")

	scenario, err := chaosconnector.NewConcurrentDuplicateScenario(corpusCase.Entity)
	require.NoError(t, err)
	firstRun, err := chaosconnector.NewRun(scenario, corpusCase.CrashSchedule)
	require.NoError(t, err)
	firstHarness := newChaosHarness(
		t, ctx, firstRun, c1zPath, tmpDir, chaosTransportDirect, WithWorkerCount(2),
	)
	concreteFirst, ok := firstHarness.Syncer.(*syncer)
	require.True(t, ok)

	done := make(chan error, 1)
	go func() {
		done <- firstHarness.Syncer.Sync(ctx)
	}()
	waitForConcurrentObservation(t, ctx, concreteFirst, firstRun, corpusCase, corpusCase.FirstToken)
	firstRun.Runtime().ReleaseBarrier("release-" + corpusCase.BlockedToken)
	require.ErrorIs(t, <-done, chaosconnector.ErrCrashRequested)
	require.NoError(t, firstHarness.Close(t.Context()))
	require.NoError(t, firstRun.Runtime().VerifyRequired())

	resumeScenario, err := chaosconnector.NewConcurrentDuplicateScenario(corpusCase.Entity)
	require.NoError(t, err)
	resumeRun, err := chaosconnector.NewRun(resumeScenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	resumeHarness := newChaosHarness(
		t, ctx, resumeRun, c1zPath, tmpDir, chaosTransportDirect, WithWorkerCount(1),
	)
	require.NoError(t, resumeHarness.Syncer.Sync(ctx))
	require.NoError(t, resumeHarness.Close(t.Context()))
	resumedTokens := make(map[string]bool)
	lastSibling := ""
	for _, event := range resumeRun.Trace().Events() {
		if event.Operation.Method == corpusCase.Method() && event.Outcome == chaosconnector.OutcomeReturned {
			for _, token := range []string{"left", "right"} {
				if corpusCase.OperationMatchesToken(event.Operation, token) {
					resumedTokens[token] = true
					lastSibling = token
				}
			}
		}
	}
	require.True(t, resumedTokens["left"], "resume did not replay left sibling")
	require.True(t, resumedTokens["right"], "resume did not replay right sibling")
	require.NotEmpty(t, lastSibling)

	store, err := dotc1z.NewStore(
		t.Context(),
		c1zPath,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(t.Context())) }()
	assertSemanticExpectation(t, t.Context(), store, corpusCase.Expectation(lastSibling))
}

func waitForConcurrentObservation(
	t *testing.T,
	ctx context.Context,
	concreteSyncer *syncer,
	run *chaosconnector.Run,
	corpusCase chaosconnector.ConcurrentDuplicateCase,
	token string,
) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, event := range run.Trace().Events() {
			if corpusCase.OperationMatchesToken(event.Operation, token) &&
				event.Outcome == chaosconnector.OutcomeReturned {
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond, "unblocked sibling did not return")

	expected := corpusCase.Expectation(token)
	require.Eventually(t, func() bool {
		observation, err := chaosoracle.ReadSemantic(ctx, concreteSyncer.store, chaosoracle.SemanticTarget{
			Entity:            oracleSemanticEntity(t, expected.Entity),
			CanonicalIdentity: expected.CanonicalIdentity,
		})
		if err != nil {
			return false
		}
		return chaosoracle.CompareSemantic(
			chaosoracle.SemanticExpectation{
				Multiplicity: expected.Multiplicity,
				DisplayName:  optionalExpectation(expected.DisplayName),
				ExternalID:   optionalExpectation(expected.ExternalID),
			},
			observation,
		) == nil
	}, 5*time.Second, 10*time.Millisecond, "unblocked sibling was not persisted")
}

func assertSemanticExpectation(
	t *testing.T,
	ctx context.Context,
	store c1zstore.Store,
	expectation chaosconnector.SemanticExpectation,
) {
	t.Helper()
	target := chaosoracle.SemanticTarget{
		Entity:            oracleSemanticEntity(t, expectation.Entity),
		CanonicalIdentity: expectation.CanonicalIdentity,
	}
	observation, err := chaosoracle.ReadSemantic(ctx, store, target)
	require.NoError(t, err)
	require.NoError(t, chaosoracle.CompareSemantic(
		chaosoracle.SemanticExpectation{
			Multiplicity:   expectation.Multiplicity,
			DisplayName:    optionalExpectation(expectation.DisplayName),
			ExternalID:     optionalExpectation(expectation.ExternalID),
			ParentIdentity: optionalExpectation(expectation.ParentIdentity),
		},
		observation,
	))
}

func assertCanonicalIdentityAbsent(
	t *testing.T,
	ctx context.Context,
	store c1zstore.Store,
	entity chaosconnector.ReferentialEntity,
	identity string,
) {
	t.Helper()
	observation, err := chaosoracle.ReadSemantic(ctx, store, chaosoracle.SemanticTarget{
		Entity:            oracleSemanticEntity(t, entity),
		CanonicalIdentity: identity,
	})
	require.NoError(t, err)
	require.Zero(t, observation.Multiplicity)
}

func oracleSemanticEntity(t *testing.T, entity chaosconnector.ReferentialEntity) chaosoracle.SemanticEntity {
	t.Helper()
	switch entity {
	case chaosconnector.ReferentialResource:
		return chaosoracle.SemanticResource
	case chaosconnector.ReferentialEntitlement:
		return chaosoracle.SemanticEntitlement
	case chaosconnector.ReferentialGrant:
		return chaosoracle.SemanticGrant
	default:
		t.Fatalf("unknown referential entity %q", entity)
		return ""
	}
}

func optionalExpectation(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
