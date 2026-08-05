package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func TestValidateConnectorResourceIdentity(t *testing.T) {
	valid := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(),
	}.Build()

	tests := []struct {
		name     string
		resource *v2.Resource
		reason   string
	}{
		{name: "valid identity", resource: valid},
		{name: "nil resource", reason: connectorDataNil},
		{
			name:     "missing identity",
			resource: v2.Resource_builder{}.Build(),
			reason:   connectorDataMissingIdentity,
		},
		{
			name: "missing resource type",
			resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{Resource: "u1"}.Build(),
			}.Build(),
			reason: connectorDataMissingIdentity,
		},
		{
			name: "missing resource id",
			resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{ResourceType: "user"}.Build(),
			}.Build(),
			reason: connectorDataMissingIdentity,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reason, err := validateConnectorResource(test.resource)
			require.NoError(t, err)
			require.Equal(t, test.reason, reason)
		})
	}
}

func TestValidateConnectorEntitlementIdentity(t *testing.T) {
	validResource := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(),
	}.Build()
	tests := []struct {
		name        string
		entitlement *v2.Entitlement
		reason      string
	}{
		{
			name: "valid identity",
			entitlement: v2.Entitlement_builder{
				Id:       "member",
				Resource: validResource,
			}.Build(),
		},
		{name: "nil entitlement", reason: connectorDataNil},
		{
			name:        "missing entitlement id",
			entitlement: v2.Entitlement_builder{Resource: validResource}.Build(),
			reason:      connectorDataMissingIdentity,
		},
		{
			name:        "missing resource",
			entitlement: v2.Entitlement_builder{Id: "member"}.Build(),
			reason:      connectorDataMissingResourceIdentity,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reason, err := validateConnectorEntitlement(test.entitlement)
			require.NoError(t, err)
			require.Equal(t, test.reason, reason)
		})
	}
}

func TestValidateConnectorResourceTypeIdentity(t *testing.T) {
	tests := []struct {
		name         string
		resourceType *v2.ResourceType
		reason       string
	}{
		{
			name:         "valid identity",
			resourceType: v2.ResourceType_builder{Id: "user"}.Build(),
		},
		{name: "nil resource type", reason: connectorDataNil},
		{
			name:         "missing identity",
			resourceType: v2.ResourceType_builder{}.Build(),
			reason:       connectorDataMissingIdentity,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reason, err := validateConnectorResourceType(test.resourceType)
			require.NoError(t, err)
			require.Equal(t, test.reason, reason)
		})
	}
}

func TestInvalidConnectorDataSummaryReportsObservedCounts(t *testing.T) {
	core, entries := newCaptureCore()
	logger := zap.New(core)
	s := &syncer{}

	filtered, err := filterConnectorData(s, connectorDataResource,
		[]*v2.Resource{nil, v2.Resource_builder{}.Build()},
		validateConnectorResource,
	)
	require.NoError(t, err)
	require.Empty(t, filtered)
	s.logInvalidConnectorDataSummary(logger)

	warning := findEntry(entries(), zapcore.WarnLevel, "connector returned invalid data; skipped records")
	require.NotNil(t, warning)
	require.EqualValues(t, 2, fieldInt(t, warning, "invalid_records_observed"))
	require.EqualValues(t, 2, fieldInt(t, warning, "invalid_resources_observed"))
}

func BenchmarkFilterConnectorDataValidPage(b *testing.B) {
	resource := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(),
	}.Build()
	resources := make([]*v2.Resource, 100)
	for i := range resources {
		resources[i] = resource
	}
	s := &syncer{}
	b.Run("validation-only-baseline", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			for _, value := range resources {
				reason, err := validateConnectorResource(value)
				if err != nil || reason != "" {
					b.Fatalf("unexpected validation result: reason=%q err=%v", reason, err)
				}
			}
		}
	})
	b.Run("filter", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			filtered, err := filterConnectorData(s, connectorDataResource, resources, validateConnectorResource)
			if err != nil || len(filtered) != len(resources) {
				b.Fatalf("unexpected filter result: len=%d err=%v", len(filtered), err)
			}
		}
	})
}
