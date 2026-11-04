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

var testIdentityID = v2.ResourceId_builder{ResourceType: "user", Resource: "u-1"}.Build()

func testDescriptor() *v2.CredentialIssueOptionDescriptor {
	return v2.CredentialIssueOptionDescriptor_builder{
		Option:               v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY,
		ResourceMode:         v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE,
		SecretResourceTypeId: "organization-api-key",
	}.Build()
}

// actionResponse builds the public Struct a credential-issue handler returns, so
// the round trip exercises the shape a connector produces.
func actionResponse(t *testing.T, secretID, expiresAt string) *structpb.Struct {
	t.Helper()
	fields := map[string]any{}
	if secretID != "" {
		fields[actions.ReturnSecretResourceID] = secretID
	}
	if expiresAt != "" {
		fields[actions.ReturnExpiresAt] = expiresAt
	}
	response, err := structpb.NewStruct(fields)
	require.NoError(t, err)
	return response
}

func issuedOutput(t *testing.T, response *structpb.Struct) *connectorbuilder.CredentialIssueOutput {
	t.Helper()
	descriptor := testDescriptor()
	secret, err := actions.CredentialIssueResource(response, testIdentityID, descriptor)
	require.NoError(t, err)
	return &connectorbuilder.CredentialIssueOutput{
		Secret: secret,
		PlaintextData: []*v2.PlaintextData{
			v2.PlaintextData_builder{Name: "api_key", Bytes: []byte(`{"key_value":"k"}`)}.Build(),
		},
		ResourceMode: descriptor.GetResourceMode(),
	}
}

func TestCredentialIssueActionOutputPassesTheValidator(t *testing.T) {
	requested := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	output := issuedOutput(t, actionResponse(t, "key-1", requested.Format(time.RFC3339)))

	require.NoError(t, connectorbuilder.ValidateIssuedCredential(
		testIdentityID, timestamppb.New(requested), output, testDescriptor()))
}

func TestCredentialIssueActionOutputRoundTripsSubSecondExpiry(t *testing.T) {
	// The validator only rejects a later expiry, so a truncated one would pass;
	// assert the sub-second value survived the response string.
	requested := time.Date(2027, 1, 1, 0, 0, 0, 123456789, time.UTC)
	output := issuedOutput(t, actionResponse(t, "key-1", requested.Format(time.RFC3339Nano)))

	require.NoError(t, connectorbuilder.ValidateIssuedCredential(
		testIdentityID, timestamppb.New(requested), output, testDescriptor()))

	trait := &v2.SecretTrait{}
	annos := annotations.Annotations(output.Secret.GetAnnotations())
	found, err := annos.Pick(trait)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, requested.UnixNano(), trait.GetExpiresAt().AsTime().UnixNano(),
		"the sub-second expiry must survive the RFC 3339 round trip")
}

func TestCredentialIssueActionOutputExpiresLaterThanRequested(t *testing.T) {
	requested := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	output := issuedOutput(t, actionResponse(t, "key-1", requested.Add(time.Hour).Format(time.RFC3339)))

	err := connectorbuilder.ValidateIssuedCredential(
		testIdentityID, timestamppb.New(requested), output, testDescriptor())
	require.Error(t, err)
	require.ErrorContains(t, err, "exceeds requested expiry")
}

func TestCredentialIssueActionResponseWithoutSecretResourceID(t *testing.T) {
	_, err := actions.CredentialIssueResource(actionResponse(t, "", ""), testIdentityID, testDescriptor())
	require.Error(t, err)
	require.ErrorContains(t, err, actions.ReturnSecretResourceID)
}
