package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
	chaosoracle "github.com/conductorone/baton-sdk/internal/chaosconnector/oracle"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func TestChaosConnectorReservedBatonIDOwnershipIsRejected(t *testing.T) {
	for _, transport := range []chaosTransport{chaosTransportDirect, chaosTransportGRPC} {
		t.Run(transport.String(), func(t *testing.T) {
			ctx := t.Context()
			tmpDir := t.TempDir()
			path := filepath.Join(tmpDir, "reserved-baton-id.c1z")
			scenario, err := chaosconnector.NewFullScenario()
			require.NoError(t, err)
			root := scenario.Epochs[scenario.InitialEpoch].
				Resources[chaosconnector.FullCapabilityResourceTypeID][""]
			require.Len(t, root.List, 1)
			marker, err := anypb.New(&v2.BatonID{})
			require.NoError(t, err)
			root.List[0].SetAnnotations(append(root.List[0].GetAnnotations(), marker))
			scenario.Epochs[scenario.InitialEpoch].
				Resources[chaosconnector.FullCapabilityResourceTypeID][""] = root

			run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
			require.NoError(t, err)
			harness := newChaosHarness(t, ctx, run, path, tmpDir, transport, WithWorkerCount(1))
			syncErr := harness.Syncer.Sync(ctx)
			require.ErrorContains(t, syncErr, "SDK-reserved BatonID ownership annotation")
			require.NoError(t, harness.Close(ctx))
			runs := readChaosSyncRuns(t, ctx, path, tmpDir)
			require.Len(t, runs, 1)
			require.Nil(t, runs[0].EndedAt)
			content := readChaosLogicalContent(t, ctx, path, tmpDir)
			require.Empty(t, content.Resources,
				"reserved ownership marker must be rejected before resource persistence")
		})
	}
}

func TestChaosConnectorMalformedKnownAnnotationFailsWithoutSealing(t *testing.T) {
	for _, transport := range chaosFaultTransports() {
		t.Run(transport.String(), func(t *testing.T) {
			ctx := t.Context()
			tmpDir := t.TempDir()
			path := filepath.Join(tmpDir, "malformed-annotation.c1z")
			scenario, err := chaosconnector.NewFullScenario()
			require.NoError(t, err)
			run := newMutationRun(t, scenario, "EntitlementsService", "ListEntitlements",
				chaosconnector.MutationMalformedAnnotation)
			harness := newChaosHarness(t, ctx, run, path, tmpDir, transport, WithWorkerCount(1))
			syncErr := harness.Syncer.Sync(ctx)
			require.ErrorContains(t, syncErr, "error parsing enqueue-page-tokens annotation")
			require.NoError(t, harness.Close(ctx))
			require.NoError(t, run.Runtime().VerifyRequired())
			runs := readChaosSyncRuns(t, ctx, path, tmpDir)
			require.Len(t, runs, 1)
			require.Nil(t, runs[0].EndedAt)
		})
	}
}

func TestChaosConnectorClearedNextPageTokenSealsOnlyVisiblePrefix(t *testing.T) {
	for _, transport := range chaosFaultTransports() {
		t.Run(transport.String(), func(t *testing.T) {
			ctx := t.Context()
			tmpDir := t.TempDir()
			path := filepath.Join(tmpDir, "clear-next-page.c1z")
			scenario, err := chaosconnector.NewFullScenario()
			require.NoError(t, err)
			pages := scenario.Epochs[scenario.InitialEpoch].Resources[chaosconnector.FullCapabilityResourceTypeID]
			root := pages[""]
			require.Len(t, root.List, 1)
			hidden := proto.Clone(root.List[0]).(*v2.Resource)
			hidden.GetId().SetResource("hidden-by-cleared-token")
			hidden.SetDisplayName("Hidden by cleared token")
			root.Next = "hidden"
			pages[""] = root
			pages["hidden"] = chaosconnector.Page[*v2.Resource]{List: []*v2.Resource{hidden}}

			manifest, err := scenario.Manifest(scenario.InitialEpoch)
			require.NoError(t, err)
			expected := chaosoracle.ExpectedIdentities(manifest)
			expected.Resources = removeString(expected.Resources,
				chaosconnector.FullCapabilityResourceTypeID+"\x00hidden-by-cleared-token")
			run := newMutationRun(t, scenario, "ResourcesService", "ListResources",
				chaosconnector.MutationClearNextPageToken)
			harness := newChaosHarness(t, ctx, run, path, tmpDir, transport, WithWorkerCount(1))
			harness.SyncAndClose(t, ctx)
			require.NoError(t, run.Runtime().VerifyRequired())
			for _, event := range run.Trace().Events() {
				require.False(t, event.Operation.Method == "ListResources" &&
					event.Operation.PageToken == "hidden", "cleared continuation must not be requested")
			}
			assertChaosStoreMatches(t, path, tmpDir, expected)
		})
	}
}

func TestChaosConnectorReversedResponseOrderPreservesLogicalContent(t *testing.T) {
	for _, transport := range chaosFaultTransports() {
		t.Run(transport.String(), func(t *testing.T) {
			ctx := t.Context()
			tmpDir := t.TempDir()
			scenario, err := chaosconnector.NewFullScenario()
			require.NoError(t, err)
			root := scenario.Epochs[scenario.InitialEpoch].
				Resources[chaosconnector.FullCapabilityResourceTypeID][""]
			require.Len(t, root.List, 1)
			second := proto.Clone(root.List[0]).(*v2.Resource)
			second.GetId().SetResource("reverse-order-second")
			second.SetDisplayName("Reverse order second")
			root.List = append(root.List, second)
			scenario.Epochs[scenario.InitialEpoch].
				Resources[chaosconnector.FullCapabilityResourceTypeID][""] = root

			baselinePath := filepath.Join(tmpDir, "baseline.c1z")
			baselineRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
			require.NoError(t, err)
			newChaosHarness(t, ctx, baselineRun, baselinePath, tmpDir, transport, WithWorkerCount(1)).
				SyncAndClose(t, ctx)
			baseline := readChaosLogicalContent(t, ctx, baselinePath, tmpDir)

			path := filepath.Join(tmpDir, "reversed.c1z")
			run := newMutationRun(t, scenario, "ResourcesService", "ListResources",
				chaosconnector.MutationReverseFirstList)
			newChaosHarness(t, ctx, run, path, tmpDir, transport, WithWorkerCount(1)).
				SyncAndClose(t, ctx)
			require.NoError(t, run.Runtime().VerifyRequired())
			actual := readChaosLogicalContent(t, ctx, path, tmpDir)
			require.NoError(t, chaosoracle.CompareLogicalContent(baseline, actual))
		})
	}
}

func newMutationRun(
	t *testing.T,
	scenario *chaosconnector.Scenario,
	service string,
	method string,
	mutation string,
) *chaosconnector.Run {
	t.Helper()
	subject := ""
	if method == "ListEntitlements" {
		subject = chaosconnector.FullCapabilityResourceTypeID
	}
	run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule(chaosconnector.Rule{
		ID: "sync-policy-" + mutation,
		Match: chaosconnector.Matcher{
			Service:      chaosconnector.ExactString(service),
			Method:       chaosconnector.ExactString(method),
			ResourceType: chaosconnector.ExactString(chaosconnector.FullCapabilityResourceTypeID),
			Subject:      chaosconnector.ExactString(subject),
			PageToken:    chaosconnector.ExactString(""),
			Attempt:      1,
			Phase:        chaosconnector.PhaseBeforeResponse,
		},
		Effects:  []chaosconnector.Effect{{Kind: chaosconnector.EffectMutate, Mutation: mutation}},
		MinFires: 1,
		MaxFires: 1,
	}))
	require.NoError(t, err)
	return run
}
