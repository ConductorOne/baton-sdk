package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
	chaosoracle "github.com/conductorone/baton-sdk/internal/chaosconnector/oracle"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

type grantFaultCase struct {
	name           string
	phase          chaosconnector.Phase
	effect         chaosconnector.Effect
	pageToken      string
	prepare        func(*testing.T, *chaosconnector.Scenario)
	wantCode       codes.Code
	wantComplete   bool
	wantGrants     bool
	wantRetry      bool
	wantColdResume bool
}

func TestChaosConnectorListGrantsFaultMatrix(t *testing.T) {
	skipChaosInShort(t)
	cases := []grantFaultCase{
		{
			name:         "retryable",
			phase:        chaosconnector.PhaseBeforeCall,
			effect:       chaosconnector.Effect{Kind: chaosconnector.EffectError, Code: codes.Unavailable, Message: "injected grant retry"},
			wantComplete: true,
			wantGrants:   true,
			wantRetry:    true,
		},
		{
			name:         "lost-response",
			phase:        chaosconnector.PhaseAfterDelegate,
			effect:       chaosconnector.Effect{Kind: chaosconnector.EffectLoseResponse, Code: codes.Unavailable},
			wantComplete: true,
			wantGrants:   true,
			wantRetry:    true,
		},
		{
			name:      "warn-and-drop",
			phase:     chaosconnector.PhaseBeforeCall,
			pageToken: "dropped",
			effect:    chaosconnector.Effect{Kind: chaosconnector.EffectError, Code: codes.NotFound, Message: "grant page disappeared"},
			prepare: func(t *testing.T, scenario *chaosconnector.Scenario) {
				t.Helper()
				dataset := scenario.Epochs[scenario.InitialEpoch]
				root := dataset.Grants[chaosconnector.FullCapabilityResourceTypeID][""]
				require.Len(t, root.List, 1)
				grant := root.List[0]
				root.List = nil
				root.Spawn = []string{"dropped"}
				dataset.Grants[chaosconnector.FullCapabilityResourceTypeID][""] = root
				dataset.Grants[chaosconnector.FullCapabilityResourceTypeID]["dropped"] =
					chaosconnector.Page[*v2.Grant]{List: []*v2.Grant{grant}}
			},
			wantComplete: true,
			wantGrants:   false,
		},
		{
			name:           "fatal",
			phase:          chaosconnector.PhaseBeforeCall,
			effect:         chaosconnector.Effect{Kind: chaosconnector.EffectError, Code: codes.InvalidArgument, Message: "injected fatal grant error"},
			wantCode:       codes.InvalidArgument,
			wantComplete:   false,
			wantGrants:     true,
			wantColdResume: true,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			for _, transport := range chaosFaultTransports() {
				t.Run(transport.String(), func(t *testing.T) {
					runGrantFaultCase(t, testCase, transport)
				})
			}
		})
	}
}

func runGrantFaultCase(t *testing.T, testCase grantFaultCase, transport chaosTransport) {
	t.Helper()
	ctx := t.Context()
	tmpDir := t.TempDir()
	c1zPath := filepath.Join(tmpDir, "chaos-grants-fault.c1z")

	scenario, err := chaosconnector.NewFullScenario()
	require.NoError(t, err)
	if testCase.prepare != nil {
		testCase.prepare(t, scenario)
	}
	manifest, err := scenario.Manifest(scenario.InitialEpoch)
	require.NoError(t, err)
	expected := chaosoracle.ExpectedIdentities(manifest)
	if !testCase.wantGrants {
		expected.Grants = nil
	}
	var baseline chaosoracle.LogicalContentSnapshot
	if testCase.wantColdResume {
		baselinePath := filepath.Join(tmpDir, "chaos-grants-baseline.c1z")
		baselineRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
		require.NoError(t, err)
		baselineHarness := newChaosHarness(t, ctx, baselineRun, baselinePath, tmpDir, transport)
		baselineHarness.SyncAndClose(t, ctx)
		baseline = readChaosLogicalContent(t, ctx, baselinePath, tmpDir)
	}

	const ruleID = "list-grants-fault"
	run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule(chaosconnector.Rule{
		ID: ruleID,
		Match: chaosconnector.Matcher{
			Service:      chaosconnector.ExactString("GrantsService"),
			Method:       chaosconnector.ExactString("ListGrants"),
			ResourceType: chaosconnector.ExactString(chaosconnector.FullCapabilityResourceTypeID),
			PageToken:    chaosconnector.ExactString(testCase.pageToken),
			Attempt:      1,
			Phase:        testCase.phase,
		},
		Effects:  []chaosconnector.Effect{testCase.effect},
		MinFires: 1,
		MaxFires: 1,
	}))
	require.NoError(t, err)
	harness := newChaosHarness(t, ctx, run, c1zPath, tmpDir, transport)

	syncErr := harness.Syncer.Sync(ctx)
	if testCase.wantComplete {
		require.NoError(t, syncErr)
	} else {
		require.Error(t, syncErr)
		require.Equal(t, testCase.wantCode, status.Code(syncErr))
	}
	require.NoError(t, harness.Close(ctx))
	require.NoError(t, run.Runtime().VerifyRequired())

	expectedCalls := 1
	expectations := []chaosoracle.TraceExpectation{
		{
			Name: "ListGrants first attempt failed",
			Match: chaosconnector.Matcher{
				Service:   chaosconnector.ExactString("GrantsService"),
				Method:    chaosconnector.ExactString("ListGrants"),
				PageToken: chaosconnector.ExactString(testCase.pageToken),
				Attempt:   1,
			},
			Outcomes: []chaosconnector.Outcome{chaosconnector.OutcomeErrored},
			Min:      1,
			Max:      1,
		},
	}
	if testCase.wantRetry {
		expectedCalls = 2
		expectations = append(expectations, chaosoracle.TraceExpectation{
			Name: "ListGrants retry succeeded",
			Match: chaosconnector.Matcher{
				Service:   chaosconnector.ExactString("GrantsService"),
				Method:    chaosconnector.ExactString("ListGrants"),
				PageToken: chaosconnector.ExactString(testCase.pageToken),
				Attempt:   2,
			},
			Outcomes: []chaosconnector.Outcome{chaosconnector.OutcomeReturned},
			Min:      1,
			Max:      1,
		})
	}
	expectations = append(expectations, chaosoracle.TraceExpectation{
		Name: "ListGrants exact call budget",
		Match: chaosconnector.Matcher{
			Service:   chaosconnector.ExactString("GrantsService"),
			Method:    chaosconnector.ExactString("ListGrants"),
			PageToken: chaosconnector.ExactString(testCase.pageToken),
		},
		Outcomes: []chaosconnector.Outcome{
			chaosconnector.OutcomeErrored,
			chaosconnector.OutcomeReturned,
		},
		Min: expectedCalls,
		Max: expectedCalls,
	})
	require.NoError(t, chaosoracle.VerifyTrace(run.Trace().Events(), expectations...))

	if testCase.wantColdResume {
		interruptedRuns := readChaosSyncRuns(t, ctx, c1zPath, tmpDir)
		require.Len(t, interruptedRuns, 1)
		require.Nil(t, interruptedRuns[0].EndedAt)
		syncID := interruptedRuns[0].ID
		resumeRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
		require.NoError(t, err)
		resumeHarness := newChaosHarness(t, ctx, resumeRun, c1zPath, tmpDir, transport)
		resumeHarness.SyncAndClose(t, ctx)
		finalRuns := readChaosSyncRuns(t, ctx, c1zPath, tmpDir)
		require.Len(t, finalRuns, 1)
		require.Equal(t, syncID, finalRuns[0].ID)
		require.NotNil(t, finalRuns[0].EndedAt)
		actual := readChaosLogicalContent(t, ctx, c1zPath, tmpDir)
		require.NoError(t, chaosoracle.CompareLogicalContent(baseline, actual))
		assertChaosStoreMatches(t, c1zPath, tmpDir, expected)
		return
	}

	store, err := dotc1z.NewStore(
		ctx,
		c1zPath,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(context.Background())) }()

	latest, err := store.SyncMeta().LatestFullSync(ctx)
	require.NoError(t, err)
	if !testCase.wantComplete {
		require.Nil(t, latest, "fatal ListGrants failure must not seal")
		return
	}
	require.NotNil(t, latest)
	actual, err := chaosoracle.ReadIdentities(ctx, store)
	require.NoError(t, err)
	require.NoError(t, chaosoracle.CompareIdentities(expected, actual))
}
