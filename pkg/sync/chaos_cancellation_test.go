package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
	chaosoracle "github.com/conductorone/baton-sdk/internal/chaosconnector/oracle"
)

func TestChaosConnectorCancellationTerminatesAndColdResumes(t *testing.T) {
	skipChaosInShort(t)
	cases := []struct {
		name    string
		effect  chaosconnector.Effect
		timeout time.Duration
		wantErr error
	}{
		{
			name: "deadline-releases-blocked-call",
			effect: chaosconnector.Effect{
				Kind: chaosconnector.EffectBlock, Barrier: "never-released",
			},
			// Leave enough setup budget for -race and expire only after the
			// connector call has reached the deterministic block.
			timeout: 3 * time.Second,
			wantErr: ErrSyncNotComplete,
		},
		{
			name:    "connector-cancellation",
			effect:  chaosconnector.Effect{Kind: chaosconnector.EffectCancel},
			timeout: 2 * time.Second,
			wantErr: context.Canceled,
		},
	}
	for _, testCase := range cases {
		for _, transport := range []chaosTransport{chaosTransportDirect, chaosTransportGRPC} {
			t.Run(testCase.name+"/"+transport.String(), func(t *testing.T) {
				ctx := t.Context()
				tmpDir := t.TempDir()
				path := filepath.Join(tmpDir, "cancel-resume.c1z")
				scenario, err := chaosconnector.NewFullScenario()
				require.NoError(t, err)
				manifest, err := scenario.Manifest(scenario.InitialEpoch)
				require.NoError(t, err)
				expected := chaosoracle.ExpectedIdentities(manifest)

				baselinePath := filepath.Join(tmpDir, "baseline.c1z")
				baselineRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
				require.NoError(t, err)
				newChaosHarness(t, ctx, baselineRun, baselinePath, tmpDir, transport, WithWorkerCount(1)).
					SyncAndClose(t, ctx)
				baseline := readChaosLogicalContent(t, ctx, baselinePath, tmpDir)

				run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule(chaosconnector.Rule{
					ID: "cancel-entitlement-call",
					Match: chaosconnector.Matcher{
						Service: chaosconnector.ExactString("EntitlementsService"),
						Method:  chaosconnector.ExactString("ListEntitlements"),
						Attempt: 1,
						Phase:   chaosconnector.PhaseBeforeCall,
					},
					Effects: []chaosconnector.Effect{testCase.effect}, MinFires: 1, MaxFires: 1,
				}))
				require.NoError(t, err)
				firstCtx, cancel := context.WithTimeout(ctx, testCase.timeout)
				defer cancel()
				harness := newChaosHarness(t, firstCtx, run, path, tmpDir, transport, WithWorkerCount(1))
				started := time.Now()
				var firstErr error
				if testCase.name == "deadline-releases-blocked-call" {
					done := make(chan error, 1)
					go func() { done <- harness.Syncer.Sync(firstCtx) }()
					require.Eventually(t, func() bool {
						return run.Runtime().ActiveOperations() > 0
					}, 2*time.Second, 10*time.Millisecond,
						"deadline premise requires an actively blocked connector call")
					firstErr = <-done
				} else {
					firstErr = harness.Syncer.Sync(firstCtx)
				}
				require.Less(t, time.Since(started), 6*time.Second, "cancellation must terminate boundedly")
				require.True(t, errors.Is(firstErr, testCase.wantErr),
					"expected %v, got %v", testCase.wantErr, firstErr)
				if testCase.name == "deadline-releases-blocked-call" {
					require.ErrorIs(t, firstCtx.Err(), context.DeadlineExceeded)
				}
				require.Zero(t, run.Runtime().ActiveOperations(),
					"cancellation must release the blocked connector call")
				require.NoError(t, harness.Close(ctx))
				require.NoError(t, run.Runtime().VerifyRequired())

				interruptedRuns := readChaosSyncRuns(t, ctx, path, tmpDir)
				require.Len(t, interruptedRuns, 1)
				require.Nil(t, interruptedRuns[0].EndedAt)
				syncID := interruptedRuns[0].ID

				resumeRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
				require.NoError(t, err)
				resumeHarness := newChaosHarness(
					t, ctx, resumeRun, path, tmpDir, transport, WithWorkerCount(1),
				)
				resumeHarness.SyncAndClose(t, ctx)
				finalRuns := readChaosSyncRuns(t, ctx, path, tmpDir)
				require.Len(t, finalRuns, 1)
				require.Equal(t, syncID, finalRuns[0].ID)
				require.NotNil(t, finalRuns[0].EndedAt)
				actual := readChaosLogicalContent(t, ctx, path, tmpDir)
				require.NoError(t, chaosoracle.CompareLogicalContent(baseline, actual))
				assertChaosStoreMatches(t, path, tmpDir, expected)
			})
		}
	}
}
