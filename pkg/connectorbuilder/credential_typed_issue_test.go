package connectorbuilder

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

const typedAPIKeyV2 = "api_key_v2"

func typedDescriptor(secretResourceTypeID, outputContentType string) *v2.CredentialIssueOptionDescriptor {
	descriptor := apiKeyDescriptor(secretResourceTypeID)
	descriptor.SetOutputContentType(outputContentType)
	return descriptor
}

func typedDetails(secretResourceTypeID, outputContentType string) *v2.CredentialDetailsCredentialIssue {
	return v2.CredentialDetailsCredentialIssue_builder{
		Options:         []*v2.CredentialIssueOptionDescriptor{typedDescriptor(secretResourceTypeID, outputContentType)},
		PreferredOption: v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY,
	}.Build()
}

// typedIssuer wires one option declaring an output content type (possibly
// empty) to a builder, and reports whether Issue was reached.
func typedIssuer(t *testing.T, declared string) (*builder, *multiTypeIssuer) {
	t.Helper()
	issuer := newMultiTypeIssuer(typedDetails(serviceAccountKey, declared))
	connector, err := NewConnector(context.Background(), newTestConnector([]ResourceSyncer{
		issuer,
		newNamedSecretDeleter(serviceAccountKey),
	}))
	require.NoError(t, err)
	return connector.(*builder), issuer
}

func typedRequest(t *testing.T, outputContentType string) *v2.IssueCredentialRequest {
	t.Helper()
	return v2.IssueCredentialRequest_builder{
		IdentityId:        v2.ResourceId_builder{ResourceType: "user", Resource: "u-1"}.Build(),
		CredentialOptions: apiKeyOptions(serviceAccountKey),
		EncryptionConfigs: []*v2.EncryptionConfig{newIssueEncryptionConfig(t)},
		RequestId:         "request-1",
		OutputContentType: outputContentType,
	}.Build()
}

// TestIssueCredentialV2RequiresTheDeclaredContract pins the pre-mint agreement:
// a typed request reaches Issue only when it names exactly the type the
// executing descriptor declares. Every other combination is refused before the
// provider is touched, which is why the issuer records lastInput only inside
// Issue.
func TestIssueCredentialV2RequiresTheDeclaredContract(t *testing.T) {
	ctx := context.Background()

	t.Run("a matching type reaches Issue and is carried to the connector", func(t *testing.T) {
		connector, issuer := typedIssuer(t, typedAPIKeyV2)
		_, err := connector.IssueCredentialV2(ctx, typedRequest(t, typedAPIKeyV2))
		require.NoError(t, err)
		require.NotNil(t, issuer.lastInput)
		require.Equal(t, typedAPIKeyV2, issuer.lastInput.OutputContentType)
	})

	t.Run("a mismatched type is refused before mint", func(t *testing.T) {
		connector, issuer := typedIssuer(t, typedAPIKeyV2)
		_, err := connector.IssueCredentialV2(ctx, typedRequest(t, "certificate"))
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
		require.Contains(t, err.Error(), typedAPIKeyV2, "the refusal names the executing contract")
		require.Nil(t, issuer.lastInput, "the provider must not be mutated on contract drift")
	})

	t.Run("an option that declares nothing cannot serve a typed request", func(t *testing.T) {
		connector, issuer := typedIssuer(t, "")
		_, err := connector.IssueCredentialV2(ctx, typedRequest(t, typedAPIKeyV2))
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
		require.Nil(t, issuer.lastInput)
	})

	t.Run("an empty requested type is malformed on the typed path", func(t *testing.T) {
		connector, issuer := typedIssuer(t, typedAPIKeyV2)
		_, err := connector.IssueCredentialV2(ctx, typedRequest(t, ""))
		require.Equal(t, codes.InvalidArgument, status.Code(err))
		require.Nil(t, issuer.lastInput)
	})
}

// TestIssueCredentialLegacyPathIsUnchanged pins the other direction: the legacy
// method neither serves a typed request nor changes its own behaviour.
func TestIssueCredentialLegacyPathIsUnchanged(t *testing.T) {
	ctx := context.Background()

	t.Run("a typed field on the legacy method is refused", func(t *testing.T) {
		connector, issuer := typedIssuer(t, typedAPIKeyV2)
		_, err := connector.IssueCredential(ctx, typedRequest(t, typedAPIKeyV2))
		require.Equal(t, codes.InvalidArgument, status.Code(err))
		require.Contains(t, err.Error(), "IssueCredentialV2")
		require.Nil(t, issuer.lastInput)
	})

	t.Run("an untyped legacy request still reaches Issue", func(t *testing.T) {
		connector, issuer := typedIssuer(t, typedAPIKeyV2)
		_, err := connector.IssueCredential(ctx, typedRequest(t, ""))
		require.NoError(t, err)
		require.NotNil(t, issuer.lastInput)
		require.Empty(t, issuer.lastInput.OutputContentType,
			"a legacy request must not carry a typed contract to the connector")
	})
}
