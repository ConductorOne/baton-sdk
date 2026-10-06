package connectorbuilder

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
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

// undeliverableIssuer mints a real provider object and then returns output the
// builder rejects, which is the shape that used to lose the handle.
type undeliverableIssuer struct {
	ResourceSyncer
	details *v2.CredentialDetailsCredentialIssue
}

func (m *undeliverableIssuer) IssueCapabilityDetails(context.Context) (*v2.CredentialDetailsCredentialIssue, annotations.Annotations, error) {
	return m.details, annotations.Annotations{}, nil
}

func (m *undeliverableIssuer) Issue(_ context.Context, input *CredentialIssueInput) (*CredentialIssueOutput, error) {
	secret, err := resource.NewSecretResource(
		"Issued key",
		v2.ResourceType_builder{Id: serviceAccountKey}.Build(),
		"minted-handle-1",
		[]resource.SecretTraitOption{resource.WithSecretIdentityID(input.IdentityID)},
	)
	if err != nil {
		return nil, err
	}
	// A live object with no deliverable material: the value was disclosed once
	// and did not come back.
	return &CredentialIssueOutput{
		Secret:        secret,
		PlaintextData: nil,
		ResourceMode:  v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE,
	}, nil
}

// TestPostIssueFailureKeepsTheMintedHandle pins the ambiguity contract: a
// failure after Issue means the provider outcome is unresolved, so the error
// must carry a machine-readable handle and must not read as a safe retry.
// The assertion is on the structured detail, not on the message text: a caller
// must never have to parse English to find the object it has to clean up.
func TestPostIssueFailureKeepsTheMintedHandle(t *testing.T) {
	issuer := &undeliverableIssuer{ResourceSyncer: newTestResourceSyncer("user"), details: typedDetails(serviceAccountKey, "")}
	connector, err := NewConnector(context.Background(), newTestConnector([]ResourceSyncer{
		issuer,
		newNamedSecretDeleter(serviceAccountKey),
	}))
	require.NoError(t, err)

	_, err = connector.(*builder).IssueCredential(context.Background(), typedRequest(t, ""))
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Internal, st.Code())
	require.Contains(t, st.Message(), "must not be retried")
	require.Contains(t, st.Message(), "unresolved",
		"the message must not claim a live object, only an unresolved outcome")

	var handle *v2.ResourceId
	var info *errdetails.ErrorInfo
	for _, detail := range st.Details() {
		switch d := detail.(type) {
		case *v2.ResourceId:
			handle = d
		case *errdetails.ErrorInfo:
			info = d
		}
	}
	require.NotNil(t, handle, "the minted identity must be a structured status detail")
	require.Equal(t, "minted-handle-1", handle.GetResource())
	require.Equal(t, serviceAccountKey, handle.GetResourceType())
	require.NotNil(t, info)
	require.Equal(t, "CREDENTIAL_ISSUED_BUT_UNDELIVERABLE", info.GetReason())
	require.Equal(t, "forbidden", info.GetMetadata()["retry"])
	require.Equal(t, "required", info.GetMetadata()["cleanup"])
	require.NotContains(t, err.Error(), "material",
		"no credential material may appear in the error")
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
		resp, err := connector.IssueCredential(ctx, typedRequest(t, ""))
		require.NoError(t, err)
		require.NotNil(t, issuer.lastInput)
		require.Empty(t, issuer.lastInput.OutputContentType,
			"a legacy request must not carry a typed contract to the connector")
		// An operation prepared under the legacy contract keeps its old raw
		// material and the connector's own name for it. Upgrading the SDK must
		// not relabel or re-encode it.
		require.Len(t, resp.GetEncryptedData(), 1)
		require.Equal(t, "api_key", resp.GetEncryptedData()[0].GetName())
		require.NotEmpty(t, resp.GetEncryptedData()[0].GetEncryptedBytes())
	})
}
