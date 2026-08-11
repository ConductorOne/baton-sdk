package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
	chaosoracle "github.com/conductorone/baton-sdk/internal/chaosconnector/oracle"
	storagev3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
)

func TestChaosConnectorDataPolicyLifecycleCorpus(t *testing.T) {
	skipChaosInShort(t)
	for _, transport := range []chaosTransport{chaosTransportDirect, chaosTransportGRPC} {
		t.Run(transport.String(), func(t *testing.T) {
			for _, corpusCase := range chaosconnector.LifecycleCorpus() {
				t.Run(corpusCase.Name, func(t *testing.T) {
					runDataPolicyLifecycleCase(t, transport, corpusCase)
				})
			}
		})
	}
}

func runDataPolicyLifecycleCase(t *testing.T, transport chaosTransport, corpusCase chaosconnector.LifecycleCase) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	tmpDir := t.TempDir()
	c1zPath := filepath.Join(tmpDir, "lifecycle.c1z")
	var baselineContent chaosoracle.LogicalContentSnapshot
	if !corpusCase.Resume.MustFail {
		baselineScenario, err := corpusCase.BuildResume()
		require.NoError(t, err)
		baselinePath := filepath.Join(tmpDir, "lifecycle-baseline.c1z")
		baselineRun, err := chaosconnector.NewRun(baselineScenario, chaosconnector.NewSchedule())
		require.NoError(t, err)
		baselineHarness := newChaosHarness(
			t, ctx, baselineRun, baselinePath, tmpDir, transport, WithWorkerCount(1),
		)
		baselineHarness.SyncAndClose(t, ctx)
		baselineContent = readChaosLogicalContent(t, ctx, baselinePath, tmpDir)
	}

	initialScenario, err := corpusCase.BuildInitial()
	require.NoError(t, err)
	initialRun, err := chaosconnector.NewRun(initialScenario, corpusCase.InterruptSchedule)
	require.NoError(t, err)
	initialHarness := newChaosHarness(
		t, ctx, initialRun, c1zPath, tmpDir, transport, WithWorkerCount(1),
	)
	initialConcrete, ok := initialHarness.Syncer.(*syncer)
	require.True(t, ok)
	// Persist every completed page so the drop case proves counters restore
	// from the checkpoint instead of merely being reconstructed by replay.
	initialConcrete.checkpointInterval = 0
	require.ErrorIs(t, initialHarness.Syncer.Sync(ctx), chaosconnector.ErrInterruptRequested)
	require.NoError(t, initialHarness.Close(t.Context()))
	require.NoError(t, initialRun.Runtime().VerifyRequired())
	require.True(t, lifecycleTraceHas(
		initialRun,
		corpusCase.InterruptedPageToken,
		chaosconnector.OutcomeErrored,
	), "initial attempt did not trace the interrupted page as errored")
	interruptedRuns := readChaosSyncRuns(t, t.Context(), c1zPath, tmpDir)
	require.Len(t, interruptedRuns, 1)
	require.Nil(t, interruptedRuns[0].EndedAt)
	interruptedSyncID := interruptedRuns[0].ID

	firstObservation := readLifecycleObservation(t, c1zPath, tmpDir, corpusCase.Identity)
	firstObservation.Dropped = initialConcrete.ingestFilterStats.entitlementsDropped.Load()
	require.NoError(t, chaosoracle.CompareLifecycle(
		chaosoracle.LifecycleExpectation{
			Sealed:      corpusCase.Initial.Sealed,
			Present:     corpusCase.Initial.Present,
			DisplayName: optionalExpectation(corpusCase.Initial.DisplayName),
			Dropped:     corpusCase.Initial.EntitlementsDropped,
		},
		firstObservation,
	))

	resumeScenario, err := corpusCase.BuildResume()
	require.NoError(t, err)
	resumeRun, err := chaosconnector.NewRun(resumeScenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	resumeHarness := newChaosHarness(
		t, ctx, resumeRun, c1zPath, tmpDir, transport, WithWorkerCount(1),
	)
	resumeConcrete, ok := resumeHarness.Syncer.(*syncer)
	require.True(t, ok)
	resumeErr := resumeHarness.Syncer.Sync(ctx)
	if corpusCase.Resume.MustFail {
		require.Error(t, resumeErr)
		require.ErrorContains(t, resumeErr, corpusCase.Resume.ErrorContains)
	} else {
		require.NoError(t, resumeErr)
	}
	require.NoError(t, resumeHarness.Close(t.Context()))

	finalObservation := readLifecycleObservation(t, c1zPath, tmpDir, corpusCase.Identity)
	finalObservation.Dropped = resumeConcrete.ingestFilterStats.entitlementsDropped.Load()
	require.NoError(t, chaosoracle.CompareLifecycle(
		chaosoracle.LifecycleExpectation{
			Sealed:      corpusCase.Resume.Sealed,
			Present:     corpusCase.Resume.Present,
			DisplayName: optionalExpectation(corpusCase.Resume.DisplayName),
			Dropped:     corpusCase.Resume.EntitlementsDropped,
		},
		finalObservation,
	))
	quality := readLifecycleIngestQuality(t, c1zPath)
	switch corpusCase.Policy {
	case chaosconnector.DataPolicyFail:
		require.Nil(t, quality, "failed sync must not publish ingest quality")
	case chaosconnector.DataPolicyAccept:
		require.NotNil(t, quality)
		require.False(t, quality.GetSourceCacheReplayBlocked())
	case chaosconnector.DataPolicySkipReport:
		require.NotNil(t, quality)
		require.True(t, quality.GetSourceCacheReplayBlocked())
		require.Equal(t, corpusCase.Resume.EntitlementsDropped, quality.GetEntitlementsDropped())
	case chaosconnector.DataPolicyWarnRetain:
		require.NotNil(t, quality)
		require.True(t, quality.GetSourceCacheReplayBlocked())
		require.NotZero(t, quality.GetReasonFlags())
	default:
		require.Failf(t, "unexpected lifecycle policy", "policy %q has no quality expectation", corpusCase.Policy)
	}
	require.True(t, lifecycleTraceHas(
		resumeRun,
		corpusCase.InterruptedPageToken,
		chaosconnector.OutcomeReturned,
	),
		"resume did not execute the interrupted page")
	finalRuns := readChaosSyncRuns(t, t.Context(), c1zPath, tmpDir)
	require.Len(t, finalRuns, 1, "resume must not create a replacement sync")
	require.Equal(t, interruptedSyncID, finalRuns[0].ID)
	if corpusCase.Resume.MustFail {
		require.Nil(t, finalRuns[0].EndedAt, "failed resume must remain unfinished")
	} else {
		require.NotNil(t, finalRuns[0].EndedAt, "successful resume must finish the interrupted sync")
		finalContent := readChaosLogicalContent(t, t.Context(), c1zPath, tmpDir)
		require.NoError(t, chaosoracle.CompareLogicalContent(baselineContent, finalContent),
			"resumed lifecycle store must equal the uninterrupted current-answer run")
	}
}

func readLifecycleIngestQuality(t *testing.T, c1zPath string) *storagev3.IngestQualityStats {
	t.Helper()
	f, err := os.Open(c1zPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()
	manifest, err := formatv3.ReadManifestHeader(f)
	require.NoError(t, err)
	if len(manifest.GetSyncRuns()) == 0 || manifest.GetSyncRuns()[0].GetStats() == nil {
		return nil
	}
	return manifest.GetSyncRuns()[0].GetStats().GetIngestQuality()
}

func readLifecycleObservation(
	t *testing.T,
	c1zPath string,
	tmpDir string,
	identity string,
) chaosoracle.LifecycleObservation {
	t.Helper()
	store, err := dotc1z.NewStore(
		t.Context(),
		c1zPath,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(t.Context())) }()
	observation, err := chaosoracle.ReadLifecycle(t.Context(), store, identity)
	require.NoError(t, err)
	return observation
}

func lifecycleTraceHas(run *chaosconnector.Run, pageToken string, outcome chaosconnector.Outcome) bool {
	for _, event := range run.Trace().Events() {
		if event.Operation.Method == "ListEntitlements" &&
			event.Operation.PageToken == pageToken &&
			event.Outcome == outcome {
			return true
		}
	}
	return false
}
