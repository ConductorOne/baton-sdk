package chaosconnector

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

func TestResponseMutationSchedule(t *testing.T) {
	scenario, err := NewFullScenario()
	require.NoError(t, err)
	run, err := NewRun(scenario, NewSchedule(Rule{
		ID: "unknown-response-annotation",
		Match: Matcher{
			Service: ExactString("ResourcesService"),
			Method:  ExactString("ListResources"),
			Phase:   PhaseBeforeResponse,
		},
		Effects: []Effect{{
			Kind:     EffectMutate,
			Mutation: MutationUnknownAnnotation,
		}},
		MinFires: 1,
		MaxFires: 1,
	}))
	require.NoError(t, err)
	builder, err := NewBuilder(run)
	require.NoError(t, err)
	server, err := builder.Server(t.Context())
	require.NoError(t, err)
	client := NewDirectClient(t.Context(), server, run)

	response, err := client.ListResources(t.Context(), v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: FullCapabilityResourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, response.GetAnnotations(), 1)
	require.Equal(t, "type.googleapis.com/chaosconnector.UnknownAnnotation", response.GetAnnotations()[0].GetTypeUrl())
	require.NoError(t, run.Runtime().VerifyRequired())
}

func TestBuiltInAnnotationMutations(t *testing.T) {
	base := v2.ResourcesServiceListResourcesResponse_builder{
		Annotations: annotations.New(&v2.RateLimitWaitReport{}),
	}.Build()
	registry := NewMutationRegistry()

	t.Run("duplicate", func(t *testing.T) {
		response := v2.ResourcesServiceListResourcesResponse_builder{
			Annotations: append([]*anypb.Any(nil), base.GetAnnotations()...),
		}.Build()
		require.NoError(t, registry.Apply(MutationDuplicateAnnotation, response))
		require.Len(t, response.GetAnnotations(), 2)
		require.Equal(t, response.GetAnnotations()[0].GetTypeUrl(), response.GetAnnotations()[1].GetTypeUrl())
	})

	t.Run("malformed known", func(t *testing.T) {
		response := v2.ResourcesServiceListResourcesResponse_builder{}.Build()
		require.NoError(t, registry.Apply(MutationMalformedAnnotation, response))
		require.Len(t, response.GetAnnotations(), 1)
		var enqueue v2.EnqueuePageTokens
		require.Error(t, response.GetAnnotations()[0].UnmarshalTo(&enqueue))
	})

	t.Run("unknown protobuf field", func(t *testing.T) {
		response := v2.ResourcesServiceListResourcesResponse_builder{}.Build()
		require.NoError(t, registry.Apply(MutationUnknownProtoField, response))
		require.NotEmpty(t, response.ProtoReflect().GetUnknown())
	})
}

func TestBuiltInRepeatedMessageMutations(t *testing.T) {
	registry := NewMutationRegistry()
	newResponse := func() *v2.ResourcesServiceListResourcesResponse {
		return v2.ResourcesServiceListResourcesResponse_builder{
			List: []*v2.Resource{
				v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "a"}.Build(), DisplayName: "A"}.Build(),
				v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "b"}.Build(), DisplayName: "B"}.Build(),
			},
		}.Build()
	}

	t.Run("duplicate first", func(t *testing.T) {
		response := newResponse()
		require.NoError(t, registry.Apply(MutationDuplicateFirstItem, response))
		require.Len(t, response.GetList(), 3)
		require.Equal(t, "a", response.GetList()[2].GetId().GetResource())
	})

	t.Run("reverse", func(t *testing.T) {
		response := newResponse()
		require.NoError(t, registry.Apply(MutationReverseFirstList, response))
		require.Equal(t, "b", response.GetList()[0].GetId().GetResource())
		require.Equal(t, "a", response.GetList()[1].GetId().GetResource())
	})

	t.Run("clear first", func(t *testing.T) {
		response := newResponse()
		require.NoError(t, registry.Apply(MutationClearFirstItem, response))
		require.Nil(t, response.GetList()[0].GetId())
		require.Empty(t, response.GetList()[0].GetDisplayName())
	})

	t.Run("reject no-op reverse", func(t *testing.T) {
		response := v2.ResourcesServiceListResourcesResponse_builder{
			List: newResponse().GetList()[:1],
		}.Build()
		require.ErrorContains(t, registry.Apply(MutationReverseFirstList, response), "did not change")
	})

	t.Run("reject no-op clear continuation", func(t *testing.T) {
		response := v2.ResourcesServiceListResourcesResponse_builder{}.Build()
		require.ErrorContains(t, registry.Apply(MutationClearNextPageToken, response), "did not change")
	})
}

func TestAnnotationRegistryEntriesResolve(t *testing.T) {
	for name, policy := range KnownAnnotationPolicies() {
		t.Run(string(name), func(t *testing.T) {
			_, err := protoregistry.GlobalTypes.FindMessageByName(name)
			require.NoError(t, err, "known policy must name a compiled protobuf message")
			require.NotEmpty(t, policy.Category)
			require.NotEmpty(t, policy.Obligation)
			require.NotEmpty(t, policy.Scopes)
		})
	}
}

func TestConnectorAnnotationFieldInventory(t *testing.T) {
	fields := ConnectorAnnotationFields()
	require.Greater(t, len(fields), 40, "descriptor walk must cover the protocol, not a hand-picked response")

	got := make(map[protoreflect.FullName]struct{}, len(fields))
	for _, field := range fields {
		require.NotEmpty(t, field.Message)
		require.NotEmpty(t, field.Field)
		_, duplicate := got[field.Field]
		require.False(t, duplicate, "annotation field inventory must not contain duplicates")
		got[field.Field] = struct{}{}
	}
	require.Contains(t, got, protoreflect.FullName("c1.connector.v2.Grant.annotations"))
	require.Contains(t, got, protoreflect.FullName("c1.connector.v2.GrantsServiceListGrantsResponse.annotations"))
	require.Contains(t, got, protoreflect.FullName("c1.connector.v2.ListEventsResponse.annotations"))
}
