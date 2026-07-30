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
