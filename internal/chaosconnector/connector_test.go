package chaosconnector

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/types"
)

func newFullConnector(t *testing.T) (*Run, types.ConnectorServer) {
	t.Helper()
	scenario, err := NewFullScenario()
	require.NoError(t, err)
	run, err := NewRun(scenario, NewSchedule())
	require.NoError(t, err)
	builder, err := NewBuilder(run, WithFullCapabilities())
	require.NoError(t, err)
	server, err := builder.Server(t.Context())
	require.NoError(t, err)
	return run, server
}

func TestFullCapabilityConnectorThroughBothAdapters(t *testing.T) {
	tests := []struct {
		name string
		open func(*testing.T, *Run, types.ConnectorServer) types.ConnectorClient
	}{
		{
			name: "direct",
			open: func(t *testing.T, run *Run, server types.ConnectorServer) types.ConnectorClient {
				return NewDirectClient(t.Context(), server, run)
			},
		},
		{
			name: "grpc",
			open: func(t *testing.T, run *Run, server types.ConnectorServer) types.ConnectorClient {
				client, err := NewGRPCClient(t.Context(), server, run, true, true)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, client.Close()) })
				return client
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			run, server := newFullConnector(t)
			client := tc.open(t, run, server)
			exerciseCleanSurface(t, client)
			require.NoError(t, run.Runtime().VerifyRequired())
			require.NotEmpty(t, run.Trace().Events())
		})
	}
}

func exerciseCleanSurface(t *testing.T, client types.ConnectorClient) {
	t.Helper()
	ctx := t.Context()

	metadata, err := client.GetMetadata(ctx, v2.ConnectorServiceGetMetadataRequest_builder{}.Build())
	require.NoError(t, err)
	require.Equal(t, "SDK Internal Chaos Connector", metadata.GetMetadata().GetDisplayName())
	require.Contains(t, metadata.GetMetadata().GetCapabilities().GetConnectorCapabilities(), v2.Capability_CAPABILITY_SYNC)
	require.Contains(t, metadata.GetMetadata().GetCapabilities().GetConnectorCapabilities(), v2.Capability_CAPABILITY_PROVISION)
	require.Contains(t, metadata.GetMetadata().GetCapabilities().GetConnectorCapabilities(), v2.Capability_CAPABILITY_TICKETING)
	require.Contains(t, metadata.GetMetadata().GetCapabilities().GetConnectorCapabilities(), v2.Capability_CAPABILITY_ACTIONS)
	require.Contains(t, metadata.GetMetadata().GetCapabilities().GetConnectorCapabilities(), v2.Capability_CAPABILITY_EVENT_FEED_V2)

	_, err = client.Validate(ctx, v2.ConnectorServiceValidateRequest_builder{}.Build())
	require.NoError(t, err)

	resourceTypes, err := client.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, resourceTypes.GetList(), 2)

	resources, err := client.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: FullCapabilityResourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resources.GetList(), 1)
	user := resources.GetList()[0]

	gotResource, err := client.GetResource(ctx, v2.ResourceGetterServiceGetResourceRequest_builder{
		ResourceId: user.GetId(),
	}.Build())
	require.NoError(t, err)
	require.True(t, proto.Equal(user.GetId(), gotResource.GetResource().GetId()))

	typeResource := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: FullCapabilityResourceTypeID}.Build(),
	}.Build()
	typeAnnotations := annotations.New(&v2.TypeScopedEntitlements{})
	entitlements, err := client.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
		Resource:    typeResource,
		Annotations: typeAnnotations,
	}.Build())
	require.NoError(t, err)
	require.Len(t, entitlements.GetList(), 1)

	grants, err := client.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
		Resource:    typeResource,
		Annotations: annotations.New(&v2.TypeScopedGrants{}),
	}.Build())
	require.NoError(t, err)
	require.Len(t, grants.GetList(), 1)

	_, err = client.Grant(ctx, v2.GrantManagerServiceGrantRequest_builder{
		Entitlement: entitlements.GetList()[0],
		Principal:   user,
	}.Build())
	require.NoError(t, err)
	_, err = client.Revoke(ctx, v2.GrantManagerServiceRevokeRequest_builder{
		Grant: grants.GetList()[0],
	}.Build())
	require.NoError(t, err)

	created, err := client.CreateResource(ctx, v2.CreateResourceRequest_builder{Resource: user}.Build())
	require.NoError(t, err)
	require.True(t, proto.Equal(user.GetId(), created.GetCreated().GetId()))
	_, err = client.DeleteResource(ctx, v2.DeleteResourceRequest_builder{ResourceId: user.GetId()}.Build())
	require.NoError(t, err)
	_, err = client.DeleteResourceV2(ctx, v2.DeleteResourceV2Request_builder{ResourceId: user.GetId()}.Build())
	require.NoError(t, err)

	feeds, err := client.ListEventFeeds(ctx, v2.ListEventFeedsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, feeds.GetList(), 1)
	_, err = client.ListEvents(ctx, v2.ListEventsRequest_builder{
		EventFeedId: feeds.GetList()[0].GetId(),
	}.Build())
	require.NoError(t, err)

	schemas, err := client.ListTicketSchemas(ctx, v2.TicketsServiceListTicketSchemasRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, schemas.GetList(), 1)
	_, err = client.GetTicketSchema(ctx, v2.TicketsServiceGetTicketSchemaRequest_builder{
		Id: schemas.GetList()[0].GetId(),
	}.Build())
	require.NoError(t, err)
	_, err = client.GetTicket(ctx, v2.TicketsServiceGetTicketRequest_builder{Id: "ticket-1"}.Build())
	require.NoError(t, err)

	actionSchemas, err := client.ListActionSchemas(ctx, v2.ListActionSchemasRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, actionSchemas.GetSchemas(), 1)
	actionResponse, err := client.InvokeAction(ctx, v2.InvokeActionRequest_builder{
		Name: actionSchemas.GetSchemas()[0].GetName(),
		Args: &structpb.Struct{},
	}.Build())
	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, actionResponse.GetStatus())

	_, err = client.Cleanup(ctx, v2.ConnectorServiceCleanupRequest_builder{}.Build())
	require.NoError(t, err)
}

func TestFaultRuntimeIsSharedByBothAdapters(t *testing.T) {
	for _, grpcAdapter := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "grpc"}[grpcAdapter], func(t *testing.T) {
			scenario, err := NewFullScenario()
			require.NoError(t, err)
			run, err := NewRun(scenario, NewSchedule(Rule{
				ID: "first-resources-call",
				Match: Matcher{
					Service: ExactString("ResourcesService"),
					Method:  ExactString("ListResources"),
					Attempt: 1,
					Phase:   PhaseBeforeCall,
				},
				Effects:  []Effect{{Kind: EffectError, Code: 14, Message: "injected unavailable"}},
				MinFires: 1,
				MaxFires: 1,
			}))
			require.NoError(t, err)
			builder, err := NewBuilder(run, WithFullCapabilities())
			require.NoError(t, err)
			server, err := builder.Server(context.Background())
			require.NoError(t, err)

			var client types.ConnectorClient
			if grpcAdapter {
				grpcClient, grpcErr := NewGRPCClient(t.Context(), server, run, true, true)
				require.NoError(t, grpcErr)
				t.Cleanup(func() { require.NoError(t, grpcClient.Close()) })
				client = grpcClient
			} else {
				client = NewDirectClient(t.Context(), server, run)
			}

			_, err = client.ListResources(t.Context(), v2.ResourcesServiceListResourcesRequest_builder{
				ResourceTypeId: FullCapabilityResourceTypeID,
			}.Build())
			require.Error(t, err)
			_, err = client.ListResources(t.Context(), v2.ResourcesServiceListResourcesRequest_builder{
				ResourceTypeId: FullCapabilityResourceTypeID,
			}.Build())
			require.NoError(t, err)
			require.NoError(t, run.Runtime().VerifyRequired())
		})
	}
}

func TestFaultEffectsExecuteInDeclaredOrder(t *testing.T) {
	for _, test := range []struct {
		name      string
		effects   []Effect
		wantEpoch string
	}{
		{
			name: "terminal error prevents later epoch change",
			effects: []Effect{
				{Kind: EffectError, Code: codes.Unavailable, Message: "stop"},
				{Kind: EffectSetEpoch, Epoch: retryDriftEpoch},
			},
			wantEpoch: "initial",
		},
		{
			name: "epoch change before terminal error is retained",
			effects: []Effect{
				{Kind: EffectSetEpoch, Epoch: retryDriftEpoch},
				{Kind: EffectError, Code: codes.Unavailable, Message: "stop"},
			},
			wantEpoch: retryDriftEpoch,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			scenario, err := resourceContentDriftScenario()
			require.NoError(t, err)
			run, err := NewRun(scenario, NewSchedule(Rule{
				ID: "ordered-effects",
				Match: Matcher{
					Method: ExactString("Validate"),
					Phase:  PhaseBeforeCall,
				},
				Effects:  test.effects,
				MinFires: 1,
				MaxFires: 1,
			}))
			require.NoError(t, err)
			builder, err := NewBuilder(run)
			require.NoError(t, err)
			server, err := builder.Server(ctx)
			require.NoError(t, err)
			client := NewDirectClient(ctx, server, run)

			_, err = client.Validate(ctx, v2.ConnectorServiceValidateRequest_builder{}.Build())
			require.Equal(t, codes.Unavailable, status.Code(err))
			require.Equal(t, test.wantEpoch, run.Epoch())
			require.NoError(t, run.Runtime().VerifyRequired())
		})
	}
}

func TestAfterDelegateEffectsDoNotMaskDelegateErrors(t *testing.T) {
	ctx := t.Context()
	scenario, err := NewFullScenario()
	require.NoError(t, err)
	run, err := NewRun(scenario, NewSchedule(Rule{
		ID: "must-not-mask-delegate",
		Match: Matcher{
			Method: ExactString("GetResource"),
			Phase:  PhaseAfterDelegate,
		},
		Effects:  []Effect{{Kind: EffectError, Code: codes.Internal, Message: "masked"}},
		MinFires: 0,
		MaxFires: 1,
	}))
	require.NoError(t, err)
	builder, err := NewBuilder(run)
	require.NoError(t, err)
	server, err := builder.Server(ctx)
	require.NoError(t, err)
	client := NewDirectClient(ctx, server, run)

	_, err = client.GetResource(ctx, v2.ResourceGetterServiceGetResourceRequest_builder{
		ResourceId: v2.ResourceId_builder{
			ResourceType: FullCapabilityResourceTypeID,
			Resource:     "missing",
		}.Build(),
	}.Build())
	require.Equal(t, codes.NotFound, status.Code(err))
	require.Zero(t, run.Runtime().FireCounts()["must-not-mask-delegate"])
}

func TestConnectorSurfaceRegistryMatchesAggregateInterface(t *testing.T) {
	clientType := reflect.TypeOf((*types.ConnectorClient)(nil)).Elem()
	coverage := ConnectorSurfaceCoverage()
	require.Equal(t, clientType.NumMethod(), len(coverage),
		"every aggregate client method must be supported or explicitly excluded")

	for i := range clientType.NumMethod() {
		method := clientType.Method(i)
		entry, ok := coverage[method.Name]
		require.True(t, ok, "missing chaos connector surface entry for %s", method.Name)
		require.Contains(t, []SurfaceStatus{SurfaceSupported, SurfaceExcluded}, entry.Status)
		if entry.Status == SurfaceExcluded {
			require.NotEmpty(t, entry.Reason)
		}
	}
}
