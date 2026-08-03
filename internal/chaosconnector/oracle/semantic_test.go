package oracle

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func TestSemanticOracleRejectsEveryPlantedViolation(t *testing.T) {
	expected := SemanticExpectation{
		Multiplicity:   1,
		DisplayName:    stringPointer("expected display"),
		ExternalID:     stringPointer("expected external"),
		ParentIdentity: stringPointer("expected parent"),
	}
	control := SemanticObservation{
		Multiplicity:   1,
		DisplayName:    "expected display",
		ExternalID:     "expected external",
		ParentIdentity: "expected parent",
	}
	require.NoError(t, CompareSemantic(expected, control))

	tests := []struct {
		name     string
		mutate   func(*SemanticObservation)
		contains string
	}{
		{
			name:     "loss",
			mutate:   func(actual *SemanticObservation) { actual.Multiplicity = 0 },
			contains: "multiplicity mismatch",
		},
		{
			name:     "duplication",
			mutate:   func(actual *SemanticObservation) { actual.Multiplicity = 2 },
			contains: "multiplicity mismatch",
		},
		{
			name:     "wrong content",
			mutate:   func(actual *SemanticObservation) { actual.DisplayName = "wrong" },
			contains: "display name mismatch",
		},
		{
			name:     "wrong external id",
			mutate:   func(actual *SemanticObservation) { actual.ExternalID = "wrong" },
			contains: "external id mismatch",
		},
		{
			name:     "wrong parent",
			mutate:   func(actual *SemanticObservation) { actual.ParentIdentity = "wrong" },
			contains: "parent mismatch",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := control
			test.mutate(&actual)
			require.ErrorContains(t, CompareSemantic(expected, actual), test.contains)
		})
	}
}

func TestLifecycleOracleRejectsEveryPlantedViolation(t *testing.T) {
	expected := LifecycleExpectation{
		Sealed:      true,
		Present:     true,
		DisplayName: stringPointer("expected"),
		Dropped:     3,
	}
	control := LifecycleObservation{
		Sealed:      true,
		Present:     true,
		DisplayName: "expected",
		Dropped:     3,
	}
	require.NoError(t, CompareLifecycle(expected, control))

	tests := []struct {
		name     string
		mutate   func(*LifecycleObservation)
		contains string
	}{
		{
			name:     "wrong sealing",
			mutate:   func(actual *LifecycleObservation) { actual.Sealed = false },
			contains: "sealing mismatch",
		},
		{
			name:     "row resurrected or lost",
			mutate:   func(actual *LifecycleObservation) { actual.Present = false },
			contains: "presence mismatch",
		},
		{
			name:     "stale content",
			mutate:   func(actual *LifecycleObservation) { actual.DisplayName = "stale" },
			contains: "display name mismatch",
		},
		{
			name:     "wrong evidence count",
			mutate:   func(actual *LifecycleObservation) { actual.Dropped++ },
			contains: "drop count mismatch",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := control
			test.mutate(&actual)
			require.ErrorContains(t, CompareLifecycle(expected, actual), test.contains)
		})
	}
}

func TestReadSemanticPagesExhaustivelyAndRejectsUnknownEntity(t *testing.T) {
	reader := &semanticReaderStub{}
	observation, err := ReadSemantic(t.Context(), reader, SemanticTarget{
		Entity:            SemanticResource,
		CanonicalIdentity: "user\x00target",
	})
	require.NoError(t, err)
	require.Equal(t, SemanticObservation{
		Multiplicity:   1,
		DisplayName:    "Target on second page",
		ParentIdentity: "user\x00parent",
	}, observation)
	require.Equal(t, []string{"", "second"}, reader.resourceTokens)

	_, err = ReadSemantic(t.Context(), reader, SemanticTarget{
		Entity:            SemanticEntity("unknown"),
		CanonicalIdentity: "anything",
	})
	require.ErrorContains(t, err, "unknown semantic entity")
}

func stringPointer(value string) *string {
	return &value
}

type semanticReaderStub struct {
	resourceTokens []string
}

func (s *semanticReaderStub) ListResourceTypes(
	context.Context,
	*v2.ResourceTypesServiceListResourceTypesRequest,
) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	return v2.ResourceTypesServiceListResourceTypesResponse_builder{}.Build(), nil
}

func (s *semanticReaderStub) ListResources(
	_ context.Context,
	request *v2.ResourcesServiceListResourcesRequest,
) (*v2.ResourcesServiceListResourcesResponse, error) {
	s.resourceTokens = append(s.resourceTokens, request.GetPageToken())
	if request.GetPageToken() == "" {
		return v2.ResourcesServiceListResourcesResponse_builder{
			List: []*v2.Resource{v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: "user",
					Resource:     "other",
				}.Build(),
			}.Build()},
			NextPageToken: "second",
		}.Build(), nil
	}
	return v2.ResourcesServiceListResourcesResponse_builder{
		List: []*v2.Resource{v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "user",
				Resource:     "target",
			}.Build(),
			DisplayName: "Target on second page",
			ParentResourceId: v2.ResourceId_builder{
				ResourceType: "user",
				Resource:     "parent",
			}.Build(),
		}.Build()},
	}.Build(), nil
}

func (s *semanticReaderStub) ListEntitlements(
	context.Context,
	*v2.EntitlementsServiceListEntitlementsRequest,
) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	return v2.EntitlementsServiceListEntitlementsResponse_builder{}.Build(), nil
}

func (s *semanticReaderStub) ListGrants(
	context.Context,
	*v2.GrantsServiceListGrantsRequest,
) (*v2.GrantsServiceListGrantsResponse, error) {
	return v2.GrantsServiceListGrantsResponse_builder{}.Build(), nil
}
