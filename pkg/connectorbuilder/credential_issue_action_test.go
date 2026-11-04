package connectorbuilder_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/actions"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
)

// actionResponse builds the public Struct a credential-issue handler returns,
// so the round trip exercises the same shape a connector produces.
func actionResponse(t *testing.T, identityID *v2.ResourceId, expiresAt string, mode v2.CredentialResourceMode) *structpb.Struct {
	t.Helper()
	fields := map[string]any{
		actions.ReturnSecretResourceTypeID: "organization-api-key",
		actions.ReturnSecretResourceID:     "key-1",
		actions.ReturnIdentityID: map[string]any{
			"resource_type_id": identityID.GetResourceType(),
			"resource_id":      identityID.GetResource(),
		},
		actions.ReturnResourceMode: mode.String(),
		actions.ReturnRequestID:    "req-1",
		actions.ReturnExpiresAt:    expiresAt,
	}
	if expiresAt == "" {
		delete(fields, actions.ReturnExpiresAt)
	}
	response, err := structpb.NewStruct(fields)
	require.NoError(t, err)
	return response
}

func apiKeyDescriptorForTest() *v2.CredentialIssueOptionDescriptor {
	return v2.CredentialIssueOptionDescriptor_builder{
		Option:               v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY,
		ResourceMode:         v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE,
		SecretResourceTypeId: "organization-api-key",
	}.Build()
}

func issuedOutput(t *testing.T, response *structpb.Struct) *connectorbuilder.CredentialIssueOutput {
	t.Helper()
	secret, mode, err := actions.CredentialIssueResource(response)
	require.NoError(t, err)
	return &connectorbuilder.CredentialIssueOutput{
		Secret: secret,
		PlaintextData: []*v2.PlaintextData{
			v2.PlaintextData_builder{Name: "api_key", Bytes: []byte(`{"key_value":"k"}`)}.Build(),
		},
		ResourceMode: mode,
	}
}

func TestCredentialIssueActionOutputPassesTheValidator(t *testing.T) {
	identityID := v2.ResourceId_builder{ResourceType: "user", Resource: "u-1"}.Build()
	requested := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)

	response := actionResponse(t, identityID, requested.Format(time.RFC3339), v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE)
	output := issuedOutput(t, response)

	require.NoError(t, connectorbuilder.ValidateIssuedCredential(
		identityID, timestamppb.New(requested), output, apiKeyDescriptorForTest()))
}

func TestCredentialIssueActionOutputRoundTripsSubSecondExpiry(t *testing.T) {
	identityID := v2.ResourceId_builder{ResourceType: "user", Resource: "u-1"}.Build()
	// A sub-second expiry must survive the string round trip, or the validator
	// sees a trait expiry that appears to exceed the request.
	requested := time.Date(2027, 1, 1, 0, 0, 0, 123456789, time.UTC)

	response := actionResponse(t, identityID, requested.Format(time.RFC3339Nano), v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE)
	output := issuedOutput(t, response)

	require.NoError(t, connectorbuilder.ValidateIssuedCredential(
		identityID, timestamppb.New(requested), output, apiKeyDescriptorForTest()))

	// The validator only rejects a later expiry, so it would accept a truncated
	// one; assert the sub-second value survived the response string.
	trait := &v2.SecretTrait{}
	annos := annotations.Annotations(output.Secret.GetAnnotations())
	found, err := annos.Pick(trait)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, requested.UnixNano(), trait.GetExpiresAt().AsTime().UnixNano(),
		"the sub-second expiry must survive the RFC 3339 round trip")
}

func TestCredentialIssueActionOutputRejections(t *testing.T) {
	identityID := v2.ResourceId_builder{ResourceType: "user", Resource: "u-1"}.Build()
	otherIdentity := v2.ResourceId_builder{ResourceType: "user", Resource: "u-2"}.Build()
	requested := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		name     string
		response *structpb.Struct
		caller   *v2.ResourceId
		// requestedAt is the expiry the caller asked for; unset means it asked
		// for none, so the response is not required to carry one.
		requestedAt *time.Time
		want        string
	}{
		{
			name:     "wrong identity",
			response: actionResponse(t, otherIdentity, "", v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE),
			caller:   identityID,
			want:     "identity_id does not match",
		},
		{
			name: "expiry later than requested",
			response: actionResponse(t, identityID, requested.Add(time.Hour).Format(time.RFC3339),
				v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE),
			caller:      identityID,
			requestedAt: &requested,
			want:        "exceeds requested expiry",
		},
		{
			name:     "mode mismatch",
			response: actionResponse(t, identityID, "", v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_VIRTUAL),
			caller:   identityID,
			want:     "resource mode does not match",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output := issuedOutput(t, tc.response)
			var requestedExpiry *timestamppb.Timestamp
			if tc.requestedAt != nil {
				requestedExpiry = timestamppb.New(*tc.requestedAt)
			}
			err := connectorbuilder.ValidateIssuedCredential(tc.caller, requestedExpiry, output, apiKeyDescriptorForTest())
			require.Error(t, err)
			require.ErrorContains(t, err, tc.want)
		})
	}
}
