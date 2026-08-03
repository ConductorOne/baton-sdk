package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
)

func TestChaosConnectorPreviousSyncEligibilityUsesArtifactProvenance(t *testing.T) {
	t.Run("finished-full-is-accepted", func(t *testing.T) {
		ctx := t.Context()
		tmpDir := t.TempDir()
		previousPath := filepath.Join(tmpDir, "previous-full.c1z")
		runChaosArtifact(t, ctx, previousPath, tmpDir)

		currentPath := filepath.Join(tmpDir, "current.c1z")
		scenario, err := chaosconnector.NewFullScenario()
		require.NoError(t, err)
		run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
		require.NoError(t, err)
		harness := newChaosHarness(
			t,
			ctx,
			run,
			currentPath,
			tmpDir,
			chaosTransportDirect,
			WithPreviousSyncC1ZPath(previousPath),
		)
		concrete, ok := harness.Syncer.(*syncer)
		require.True(t, ok)
		require.NotNil(t, concrete.previousSyncReader)
		harness.SyncAndClose(t, ctx)
	})

	t.Run("resources-only-degrades-cold", func(t *testing.T) {
		ctx := t.Context()
		tmpDir := t.TempDir()
		previousPath := filepath.Join(tmpDir, "previous-resources-only.c1z")
		runChaosArtifact(
			t,
			ctx,
			previousPath,
			tmpDir,
			WithSkipEntitlementsAndGrants(true),
		)
		runChaosWithIneligiblePrevious(t, ctx, previousPath, tmpDir)
	})

	t.Run("unfinished-degrades-cold", func(t *testing.T) {
		ctx := t.Context()
		tmpDir := t.TempDir()
		previousPath := filepath.Join(tmpDir, "previous-unfinished.c1z")
		scenario, err := chaosconnector.NewFullScenario()
		require.NoError(t, err)
		run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule(chaosconnector.Rule{
			ID: "fail-previous-resource-collection",
			Match: chaosconnector.Matcher{
				Service: chaosconnector.ExactString("ResourcesService"),
				Method:  chaosconnector.ExactString("ListResources"),
				Phase:   chaosconnector.PhaseBeforeCall,
			},
			Effects: []chaosconnector.Effect{{
				Kind:    chaosconnector.EffectError,
				Code:    codes.InvalidArgument,
				Message: "leave previous sync unfinished",
			}},
			MinFires: 1,
			MaxFires: 1,
		}))
		require.NoError(t, err)
		harness := newChaosHarness(t, ctx, run, previousPath, tmpDir, chaosTransportDirect)
		require.Error(t, harness.Syncer.Sync(ctx))
		require.NoError(t, harness.Close(ctx))
		require.NoError(t, run.Runtime().VerifyRequired())

		runChaosWithIneligiblePrevious(t, ctx, previousPath, tmpDir)
	})
}

func runChaosArtifact(
	t *testing.T,
	ctx context.Context,
	path string,
	tmpDir string,
	opts ...SyncOpt,
) {
	t.Helper()
	scenario, err := chaosconnector.NewFullScenario()
	require.NoError(t, err)
	run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	harness := newChaosHarness(t, ctx, run, path, tmpDir, chaosTransportDirect, opts...)
	harness.SyncAndClose(t, ctx)
}

func runChaosWithIneligiblePrevious(
	t *testing.T,
	ctx context.Context,
	previousPath string,
	tmpDir string,
) {
	t.Helper()
	scenario, err := chaosconnector.NewFullScenario()
	require.NoError(t, err)
	run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	harness := newChaosHarness(
		t,
		ctx,
		run,
		filepath.Join(t.TempDir(), "current.c1z"),
		tmpDir,
		chaosTransportDirect,
		WithPreviousSyncC1ZPath(previousPath),
	)
	concrete, ok := harness.Syncer.(*syncer)
	require.True(t, ok)
	require.Nil(t, concrete.previousSyncReader)
	harness.SyncAndClose(t, ctx)
}
