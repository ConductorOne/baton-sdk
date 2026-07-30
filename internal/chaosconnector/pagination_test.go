package chaosconnector

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func TestConnectorBuilderRejectsUnchangedPageToken(t *testing.T) {
	scenario, err := NewFullScenario()
	require.NoError(t, err)
	pages := scenario.Epochs[scenario.InitialEpoch].Resources[FullCapabilityResourceTypeID]
	root := pages[""]
	root.Next = "loop"
	pages[""] = root
	pages["loop"] = Page[*v2.Resource]{Next: "loop"}

	run, err := NewRun(scenario, NewSchedule())
	require.NoError(t, err)
	builder, err := NewBuilder(run)
	require.NoError(t, err)
	server, err := builder.Server(t.Context())
	require.NoError(t, err)
	client := NewDirectClient(t.Context(), server, run)

	first, err := client.ListResources(t.Context(), v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: FullCapabilityResourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Equal(t, "loop", first.GetNextPageToken())

	_, err = client.ListResources(t.Context(), v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: FullCapabilityResourceTypeID,
		PageToken:      first.GetNextPageToken(),
	}.Build())
	require.Equal(t, codes.Internal, status.Code(err))
}

func TestEmptyPageWithContinuationMakesProgress(t *testing.T) {
	scenario, err := NewFullScenario()
	require.NoError(t, err)
	pages := scenario.Epochs[scenario.InitialEpoch].Resources[FullCapabilityResourceTypeID]
	resource := pages[""].List[0]
	pages[""] = Page[*v2.Resource]{Next: "payload"}
	pages["payload"] = Page[*v2.Resource]{List: []*v2.Resource{resource}}

	run, err := NewRun(scenario, NewSchedule())
	require.NoError(t, err)
	builder, err := NewBuilder(run)
	require.NoError(t, err)
	server, err := builder.Server(t.Context())
	require.NoError(t, err)
	client := NewDirectClient(t.Context(), server, run)

	first, err := client.ListResources(t.Context(), v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: FullCapabilityResourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Empty(t, first.GetList())
	require.Equal(t, "payload", first.GetNextPageToken())

	second, err := client.ListResources(t.Context(), v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: FullCapabilityResourceTypeID,
		PageToken:      first.GetNextPageToken(),
	}.Build())
	require.NoError(t, err)
	require.Len(t, second.GetList(), 1)
	require.Empty(t, second.GetNextPageToken())
}

func TestEpochTransitionChangesRetryAnswerDeterministically(t *testing.T) {
	scenario, err := NewFullScenario()
	require.NoError(t, err)
	initial := scenario.Epochs[scenario.InitialEpoch]
	scenario.Epochs["changed"] = &Dataset{
		ResourceTypes:      cloneMessages(initial.ResourceTypes),
		Resources:          initial.Resources,
		StaticEntitlements: initial.StaticEntitlements,
		Entitlements:       initial.Entitlements,
		Grants:             initial.Grants,
	}

	run, err := NewRun(scenario, NewSchedule(Rule{
		ID: "advance-epoch-after-first-call",
		Match: Matcher{
			Service: "ResourcesService",
			Method:  "ListResources",
			Attempt: 1,
			Phase:   PhaseAfterDelegate,
		},
		Effects:  []Effect{{Kind: EffectSetEpoch, Epoch: "changed"}},
		MinFires: 1,
		MaxFires: 1,
	}))
	require.NoError(t, err)
	builder, err := NewBuilder(run)
	require.NoError(t, err)
	server, err := builder.Server(t.Context())
	require.NoError(t, err)
	client := NewDirectClient(t.Context(), server, run)

	_, err = client.ListResources(t.Context(), v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: FullCapabilityResourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Equal(t, "changed", run.Epoch())
	require.NoError(t, run.Runtime().VerifyRequired())
}
