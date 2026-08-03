package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
	chaosoracle "github.com/conductorone/baton-sdk/internal/chaosconnector/oracle"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

type collectionFaultTarget struct {
	name        string
	service     string
	method      string
	subject     string
	prepareDrop func(*testing.T, *chaosconnector.Scenario, *chaosoracle.IdentitySnapshot)
}

func TestChaosConnectorResourcesAndEntitlementsFaultMatrix(t *testing.T) {
	targets := []collectionFaultTarget{
		{
			name:    "resources",
			service: "ResourcesService",
			method:  "ListResources",
			prepareDrop: func(t *testing.T, scenario *chaosconnector.Scenario, expected *chaosoracle.IdentitySnapshot) {
				t.Helper()
				dataset := scenario.Epochs[scenario.InitialEpoch]
				pages := dataset.Resources[chaosconnector.FullCapabilityResourceTypeID]
				root := pages[""]
				require.Len(t, root.List, 1)
				item := root.List[0]
				root.List = nil
				root.Next = "dropped"
				pages[""] = root
				pages["dropped"] = chaosconnector.Page[*v2.Resource]{List: []*v2.Resource{item}}
				expected.Resources = nil
			},
		},
		{
			name:    "entitlements",
			service: "EntitlementsService",
			method:  "ListEntitlements",
			subject: chaosconnector.FullCapabilityResourceTypeID,
			prepareDrop: func(t *testing.T, scenario *chaosconnector.Scenario, expected *chaosoracle.IdentitySnapshot) {
				t.Helper()
				dataset := scenario.Epochs[scenario.InitialEpoch]
				pages := dataset.Entitlements[chaosconnector.FullCapabilityResourceTypeID]
				root := pages[""]
				require.Len(t, root.List, 1)
				item := root.List[0]
				root.List = nil
				root.Spawn = []string{"dropped"}
				pages[""] = root
				pages["dropped"] = chaosconnector.Page[*v2.Entitlement]{List: []*v2.Entitlement{item}}
				expected.Entitlements = nil
			},
		},
	}
	faults := []grantFaultCase{
		{
			name: "retryable", phase: chaosconnector.PhaseBeforeCall,
			effect:       chaosconnector.Effect{Kind: chaosconnector.EffectError, Code: codes.Unavailable, Message: "injected collection retry"},
			wantComplete: true, wantRetry: true,
		},
		{
			name: "lost-response", phase: chaosconnector.PhaseAfterDelegate,
			effect:       chaosconnector.Effect{Kind: chaosconnector.EffectLoseResponse, Code: codes.Unavailable},
			wantComplete: true, wantRetry: true,
		},
		{
			name: "warn-and-drop", phase: chaosconnector.PhaseBeforeCall, pageToken: "dropped",
			effect:       chaosconnector.Effect{Kind: chaosconnector.EffectError, Code: codes.NotFound, Message: "collection page disappeared"},
			wantComplete: true,
		},
		{
			name: "fatal-cold-resume", phase: chaosconnector.PhaseBeforeCall,
			effect:   chaosconnector.Effect{Kind: chaosconnector.EffectError, Code: codes.InvalidArgument, Message: "injected collection fatal"},
			wantCode: codes.InvalidArgument,
		},
	}

	for _, target := range targets {
		for _, fault := range faults {
			for _, transport := range chaosFaultTransports() {
				t.Run(target.name+"/"+fault.name+"/"+transport.String(), func(t *testing.T) {
					runCollectionFaultCase(t, target, fault, transport)
				})
			}
		}
	}
}

func runCollectionFaultCase(
	t *testing.T,
	target collectionFaultTarget,
	fault grantFaultCase,
	transport chaosTransport,
) {
	t.Helper()
	ctx := t.Context()
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "collection-fault.c1z")
	scenario, err := chaosconnector.NewFullScenario()
	require.NoError(t, err)
	manifest, err := scenario.Manifest(scenario.InitialEpoch)
	require.NoError(t, err)
	expected := chaosoracle.ExpectedIdentities(manifest)
	expected.Grants = nil
	if fault.name == "warn-and-drop" {
		target.prepareDrop(t, scenario, &expected)
	}

	var baseline chaosoracle.LogicalContentSnapshot
	if fault.name != "warn-and-drop" {
		baselinePath := filepath.Join(tmpDir, "baseline.c1z")
		baselineRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
		require.NoError(t, err)
		baselineHarness := newChaosHarness(
			t, ctx, baselineRun, baselinePath, tmpDir, transport, WithWorkerCount(1), WithSkipGrants(true),
		)
		baselineHarness.SyncAndClose(t, ctx)
		baseline = readChaosLogicalContent(t, ctx, baselinePath, tmpDir)
	}

	run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule(chaosconnector.Rule{
		ID: "collection-fault",
		Match: chaosconnector.Matcher{
			Service: targetExact(target.service), Method: targetExact(target.method),
			ResourceType: targetExact(chaosconnector.FullCapabilityResourceTypeID),
			Subject:      targetExact(target.subject),
			PageToken:    targetExact(fault.pageToken), Attempt: 1, Phase: fault.phase,
		},
		Effects: []chaosconnector.Effect{fault.effect}, MinFires: 1, MaxFires: 1,
	}))
	require.NoError(t, err)
	harness := newChaosHarness(
		t, ctx, run, path, tmpDir, transport, WithWorkerCount(1), WithSkipGrants(true),
	)
	firstErr := harness.Syncer.Sync(ctx)
	if fault.wantComplete {
		require.NoError(t, firstErr)
	} else {
		require.Equal(t, fault.wantCode, status.Code(firstErr))
	}
	require.NoError(t, harness.Close(ctx))
	require.NoError(t, run.Runtime().VerifyRequired())

	expectedCalls := 1
	if fault.wantRetry {
		expectedCalls = 2
	}
	require.NoError(t, chaosoracle.VerifyTrace(run.Trace().Events(),
		chaosoracle.TraceExpectation{
			Name: target.method + " failed first attempt",
			Match: chaosconnector.Matcher{
				Service: targetExact(target.service), Method: targetExact(target.method),
				ResourceType: targetExact(chaosconnector.FullCapabilityResourceTypeID),
				Subject:      targetExact(target.subject), PageToken: targetExact(fault.pageToken), Attempt: 1,
			},
			Outcomes: []chaosconnector.Outcome{chaosconnector.OutcomeErrored}, Min: 1, Max: 1,
		},
		chaosoracle.TraceExpectation{
			Name: target.method + " exact call budget",
			Match: chaosconnector.Matcher{
				Service: targetExact(target.service), Method: targetExact(target.method),
				ResourceType: targetExact(chaosconnector.FullCapabilityResourceTypeID),
				Subject:      targetExact(target.subject), PageToken: targetExact(fault.pageToken),
			},
			Outcomes: []chaosconnector.Outcome{chaosconnector.OutcomeErrored, chaosconnector.OutcomeReturned},
			Min:      expectedCalls, Max: expectedCalls,
		},
	))

	if !fault.wantComplete {
		interruptedRuns := readChaosSyncRuns(t, ctx, path, tmpDir)
		require.Len(t, interruptedRuns, 1)
		require.Nil(t, interruptedRuns[0].EndedAt)
		syncID := interruptedRuns[0].ID
		resumeRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
		require.NoError(t, err)
		resumeHarness := newChaosHarness(
			t, ctx, resumeRun, path, tmpDir, transport, WithWorkerCount(1), WithSkipGrants(true),
		)
		resumeHarness.SyncAndClose(t, ctx)
		finalRuns := readChaosSyncRuns(t, ctx, path, tmpDir)
		require.Len(t, finalRuns, 1)
		require.Equal(t, syncID, finalRuns[0].ID)
		require.NotNil(t, finalRuns[0].EndedAt)
	}

	assertChaosStoreMatches(t, path, tmpDir, expected)
	if fault.name != "warn-and-drop" {
		actual := readChaosLogicalContent(t, ctx, path, tmpDir)
		require.NoError(t, chaosoracle.CompareLogicalContent(baseline, actual))
	}
}

func targetExact(value string) *string {
	return chaosconnector.ExactString(value)
}
