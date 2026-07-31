package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func TestValidateConnectorResourceIdentity(t *testing.T) {
	valid := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(),
	}.Build()

	tests := []struct {
		name        string
		resource    *v2.Resource
		errContains string
	}{
		{name: "valid identity", resource: valid},
		{name: "nil resource", errContains: "nil resource"},
		{
			name:        "missing identity",
			resource:    v2.Resource_builder{}.Build(),
			errContains: "missing identity",
		},
		{
			name: "missing resource type",
			resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{Resource: "u1"}.Build(),
			}.Build(),
			errContains: "missing identity",
		},
		{
			name: "missing resource id",
			resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{ResourceType: "user"}.Build(),
			}.Build(),
			errContains: "missing identity",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateConnectorResource(test.resource)
			if test.errContains == "" {
				require.NoError(t, err)
				return
			}
			require.Equal(t, codes.Internal, status.Code(err))
			require.ErrorContains(t, err, test.errContains)
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
		errContains string
	}{
		{
			name: "valid identity",
			entitlement: v2.Entitlement_builder{
				Id:       "member",
				Resource: validResource,
			}.Build(),
		},
		{name: "nil entitlement", errContains: "nil entitlement"},
		{
			name:        "missing entitlement id",
			entitlement: v2.Entitlement_builder{Resource: validResource}.Build(),
			errContains: "missing identity",
		},
		{
			name:        "missing resource",
			entitlement: v2.Entitlement_builder{Id: "member"}.Build(),
			errContains: "missing resource identity",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateConnectorEntitlement(test.entitlement)
			if test.errContains == "" {
				require.NoError(t, err)
				return
			}
			require.Equal(t, codes.Internal, status.Code(err))
			require.ErrorContains(t, err, test.errContains)
		})
	}
}

func TestValidateConnectorResourceTypeIdentity(t *testing.T) {
	tests := []struct {
		name         string
		resourceType *v2.ResourceType
		errContains  string
	}{
		{
			name:         "valid identity",
			resourceType: v2.ResourceType_builder{Id: "user"}.Build(),
		},
		{name: "nil resource type", errContains: "nil resource type"},
		{
			name:         "missing identity",
			resourceType: v2.ResourceType_builder{}.Build(),
			errContains:  "missing identity",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateConnectorResourceType(test.resourceType)
			if test.errContains == "" {
				require.NoError(t, err)
				return
			}
			require.Equal(t, codes.Internal, status.Code(err))
			require.ErrorContains(t, err, test.errContains)
		})
	}
}
