package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

func TestChaosConnectorDataPolicyLifecycleCorpus(t *testing.T) {
	for _, corpusCase := range chaosconnector.LifecycleCorpus() {
		t.Run(corpusCase.Name, func(t *testing.T) {
			runDataPolicyLifecycleCase(t, corpusCase)
		})
	}
}

func runDataPolicyLifecycleCase(t *testing.T, corpusCase chaosconnector.LifecycleCase) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	tmpDir := t.TempDir()
	c1zPath := filepath.Join(tmpDir, "lifecycle.c1z")

	initialScenario, err := corpusCase.BuildInitial()
	require.NoError(t, err)
	initialRun, err := chaosconnector.NewRun(initialScenario, corpusCase.InterruptSchedule)
	require.NoError(t, err)
	initialSyncer := newLifecycleSyncer(t, ctx, initialRun, c1zPath, tmpDir)
	initialConcrete, ok := initialSyncer.(*syncer)
	require.True(t, ok)
	require.ErrorIs(t, initialSyncer.Sync(ctx), chaosconnector.ErrCrashRequested)
	require.Equal(t, corpusCase.FirstEntitlementsDropped, initialConcrete.ingestFilterStats.entitlementsDropped.Load())
	require.NoError(t, initialSyncer.Close(t.Context()))
	require.NoError(t, initialRun.Runtime().VerifyRequired())
	require.True(t, lifecycleTraceHas(
		initialRun,
		corpusCase.InterruptedPageToken,
		chaosconnector.OutcomeErrored,
	), "initial attempt did not trace the interrupted page as errored")

	firstSnapshot := readLifecycleSnapshot(t, c1zPath, tmpDir, corpusCase.Identity)
	require.False(t, firstSnapshot.sealed, "interrupted attempt sealed")
	require.Equal(t, corpusCase.FirstAttemptPresent, firstSnapshot.present)
	if corpusCase.FirstAttemptPresent {
		require.Equal(t, corpusCase.FinalDisplayName, firstSnapshot.displayName)
	}

	resumeScenario, err := corpusCase.BuildResume()
	require.NoError(t, err)
	resumeRun, err := chaosconnector.NewRun(resumeScenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	resumeSyncer := newLifecycleSyncer(t, ctx, resumeRun, c1zPath, tmpDir)
	resumeConcrete, ok := resumeSyncer.(*syncer)
	require.True(t, ok)
	resumeErr := resumeSyncer.Sync(ctx)
	if corpusCase.ResumeMustFail {
		require.Error(t, resumeErr)
		require.ErrorContains(t, resumeErr, corpusCase.ResumeErrorContains)
	} else {
		require.NoError(t, resumeErr)
	}
	require.Equal(t, corpusCase.ResumeEntitlementsDropped,
		resumeConcrete.ingestFilterStats.entitlementsDropped.Load())
	require.NoError(t, resumeSyncer.Close(t.Context()))

	finalSnapshot := readLifecycleSnapshot(t, c1zPath, tmpDir, corpusCase.Identity)
	require.Equal(t, !corpusCase.ResumeMustFail, finalSnapshot.sealed)
	require.Equal(t, corpusCase.FinalPresent, finalSnapshot.present)
	if corpusCase.FinalPresent {
		require.Equal(t, corpusCase.FinalDisplayName, finalSnapshot.displayName)
	}
	require.True(t, lifecycleTraceHas(
		resumeRun,
		corpusCase.InterruptedPageToken,
		chaosconnector.OutcomeReturned,
	),
		"resume did not execute the interrupted page")
}

func newLifecycleSyncer(
	t *testing.T,
	ctx context.Context,
	run *chaosconnector.Run,
	c1zPath string,
	tmpDir string,
) Syncer {
	t.Helper()
	builder, err := chaosconnector.NewBuilder(run)
	require.NoError(t, err)
	server, err := builder.Server(ctx)
	require.NoError(t, err)
	sdkSyncer, err := NewSyncer(
		ctx,
		chaosconnector.NewDirectClient(ctx, server, run),
		WithC1ZPath(c1zPath),
		WithTmpDir(tmpDir),
		WithStorageEngine(c1zstore.EnginePebble),
		WithDontExpandGrants(),
		WithWorkerCount(1),
	)
	require.NoError(t, err)
	return sdkSyncer
}

type lifecycleSnapshot struct {
	sealed      bool
	present     bool
	displayName string
}

func readLifecycleSnapshot(t *testing.T, c1zPath, tmpDir, identity string) lifecycleSnapshot {
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

	latest, err := store.SyncMeta().LatestFullSync(t.Context())
	require.NoError(t, err)
	out := lifecycleSnapshot{sealed: latest != nil}
	if identity == "" {
		return out
	}

	pageToken := ""
	for {
		response, listErr := store.ListEntitlements(t.Context(), v2.EntitlementsServiceListEntitlementsRequest_builder{
			PageToken: pageToken,
		}.Build())
		require.NoError(t, listErr)
		for _, entitlement := range response.GetList() {
			if entitlement.GetId() == identity {
				out.present = true
				out.displayName = entitlement.GetDisplayName()
				return out
			}
		}
		pageToken = response.GetNextPageToken()
		if pageToken == "" {
			return out
		}
	}
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
