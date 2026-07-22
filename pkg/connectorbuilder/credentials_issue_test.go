package connectorbuilder

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"testing"

	"github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/jwk"
	resource "github.com/conductorone/baton-sdk/pkg/types/resource"
)

// testCredentialIssuer implements CredentialIssuer. Issue mints a new secret
// resource for the identity and returns two plaintext blobs (e.g. an API key
// and its id), exercising the builder's PlaintextData -> Encrypt fan-out.
type testCredentialIssuer struct {
	ResourceSyncer
	lastIdentityID    *v2.ResourceId
	lastOptions       *v2.LocalCredentialOptions
	lastInput         *CredentialIssueInput
	capabilityDetails *v2.CredentialDetailsCredentialIssue
	capabilityError   error
}

func newTestCredentialIssuer(resourceType string) *testCredentialIssuer {
	return &testCredentialIssuer{
		ResourceSyncer: newTestResourceSyncer(resourceType),
	}
}

func (t *testCredentialIssuer) Issue(
	ctx context.Context,
	input *CredentialIssueInput,
) (*CredentialIssueOutput, error) {
	t.lastInput = input
	t.lastIdentityID = input.IdentityID
	t.lastOptions = input.CredentialOptions

	secret, err := resource.NewSecretResource(
		"Issued key for "+input.IdentityID.GetResource(),
		v2.ResourceType_builder{Id: "secret"}.Build(),
		"issued-key-1",
		[]resource.SecretTraitOption{resource.WithSecretIdentityID(input.IdentityID)},
	)
	if err != nil {
		return nil, err
	}

	plaintexts := []*v2.PlaintextData{
		v2.PlaintextData_builder{
			Name:        "api_key",
			Description: "Issued API key",
			Bytes:       []byte("super-secret-key-material"),
		}.Build(),
		v2.PlaintextData_builder{
			Name:        "api_key_id",
			Description: "Issued API key id",
			Bytes:       []byte("key-id-123"),
		}.Build(),
	}

	return &CredentialIssueOutput{Secret: secret, PlaintextData: plaintexts}, nil
}

func (t *testCredentialIssuer) IssueCapabilityDetails(ctx context.Context) (*v2.CredentialDetailsCredentialIssue, annotations.Annotations, error) {
	if t.capabilityError != nil {
		return nil, nil, t.capabilityError
	}
	if t.capabilityDetails != nil {
		return t.capabilityDetails, nil, nil
	}
	return v2.CredentialDetailsCredentialIssue_builder{
		Options: []*v2.CredentialIssueOptionDescriptor{
			v2.CredentialIssueOptionDescriptor_builder{
				Option: v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY,
				Scopes: []string{"read", "write"},
			}.Build(),
			v2.CredentialIssueOptionDescriptor_builder{Option: v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_TOKEN}.Build(),
		},
		PreferredOption: v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY,
	}.Build(), annotations.Annotations{}, nil
}

func TestIssueCredentialFailsBeforeProviderMutation(t *testing.T) {
	ctx := context.Background()
	identityID := v2.ResourceId_builder{
		ResourceType: "service_account",
		Resource:     "sa-1",
	}.Build()
	options := v2.CredentialOptions_builder{
		ApiKey: &v2.CredentialOptions_ApiKey{},
	}.Build()

	t.Run("missing encryption configuration", func(t *testing.T) {
		issuer := newTestCredentialIssuer("service_account")
		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{issuer}))
		require.NoError(t, err)

		_, err = connector.IssueCredential(ctx, v2.IssueCredentialRequest_builder{
			IdentityId:        identityID,
			CredentialOptions: options,
		}.Build())
		require.ErrorContains(t, err, "at least one encryption config is required")
		require.Nil(t, issuer.lastInput, "connector must not mint a credential without a delivery path")
	})

	t.Run("capability lookup failure", func(t *testing.T) {
		issuer := newTestCredentialIssuer("service_account")
		issuer.capabilityError = errors.New("capability unavailable")
		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{issuer}))
		require.NoError(t, err)

		_, err = connector.IssueCredential(ctx, v2.IssueCredentialRequest_builder{
			IdentityId:        identityID,
			CredentialOptions: options,
			EncryptionConfigs: []*v2.EncryptionConfig{newIssueEncryptionConfig(t)},
		}.Build())
		require.ErrorContains(t, err, "capability unavailable")
		require.Nil(t, issuer.lastInput, "connector must not mint a credential when preflight fails")
	})
}

// newIssueEncryptionConfig builds a JWK EncryptionConfig from a fresh RSA key so
// the builder's encryption fan-out has a real public key to encrypt against.
func newIssueEncryptionConfig(t *testing.T) *v2.EncryptionConfig {
	t.Helper()
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	pubJWK := (&jose.JSONWebKey{Key: privKey}).Public()
	pubJWKBytes, err := pubJWK.MarshalJSON()
	require.NoError(t, err)

	return v2.EncryptionConfig_builder{
		Provider: jwk.EncryptionProviderJwk,
		JwkPublicKeyConfig: v2.EncryptionConfig_JWKPublicKeyConfig_builder{
			PubKey: pubJWKBytes,
		}.Build(),
	}.Build()
}

func TestIssueCredential(t *testing.T) {
	ctx := context.Background()

	// Error case: ResourceSyncer without a credential issuer configured.
	rsSyncer := &testResourceSyncer{v2.ResourceType_builder{Id: "service_account"}.Build()}
	connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{rsSyncer}))
	require.NoError(t, err)

	_, err = connector.IssueCredential(ctx, v2.IssueCredentialRequest_builder{
		IdentityId: v2.ResourceId_builder{
			ResourceType: "service_account",
			Resource:     "sa-1",
		}.Build(),
		CredentialOptions: v2.CredentialOptions_builder{
			ApiKey: &v2.CredentialOptions_ApiKey{},
		}.Build(),
	}.Build())
	require.ErrorContains(t, err, "resource type does not have credential issuer configured")

	// Success case: credential issuer implemented, with a real encryption config.
	issuer := newTestCredentialIssuer("service_account")
	connector, err = NewConnector(ctx, newTestConnector([]ResourceSyncer{issuer}))
	require.NoError(t, err)

	encConfig := newIssueEncryptionConfig(t)
	resp, err := connector.IssueCredential(ctx, v2.IssueCredentialRequest_builder{
		IdentityId: v2.ResourceId_builder{
			ResourceType: "service_account",
			Resource:     "sa-1",
		}.Build(),
		CredentialOptions: v2.CredentialOptions_builder{
			ApiKey: v2.CredentialOptions_ApiKey_builder{
				Scopes: []string{"read", "write"},
			}.Build(),
		}.Build(),
		EncryptionConfigs: []*v2.EncryptionConfig{encConfig},
	}.Build())
	require.NoError(t, err)
	require.NotNil(t, resp)

	// The new secret resource is returned as a first-class object.
	require.NotNil(t, resp.GetSecret())
	require.Equal(t, "secret", resp.GetSecret().GetId().GetResourceType())
	require.Equal(t, "issued-key-1", resp.GetSecret().GetId().GetResource())

	// The issuer received the converted options and identity.
	require.Equal(t, "sa-1", issuer.lastIdentityID.GetResource())
	require.Equal(t, []string{"read", "write"}, issuer.lastOptions.GetApiKey().GetScopes())

	// Encryption fan-out: one EncryptedData per (plaintext x config). Two
	// plaintexts, one config => two encrypted entries, none in plaintext.
	require.Len(t, resp.GetEncryptedData(), 2)
	for _, ed := range resp.GetEncryptedData() {
		require.NotEmpty(t, ed.GetEncryptedBytes())
		require.NotEqual(t, []byte("super-secret-key-material"), ed.GetEncryptedBytes())
		require.NotEqual(t, []byte("key-id-123"), ed.GetEncryptedBytes())
	}
}

func TestIssueCredentialCapabilityAdvertisement(t *testing.T) {
	ctx := context.Background()

	connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{
		newTestCredentialIssuer("service_account"),
	}))
	require.NoError(t, err)

	b, ok := connector.(*builder)
	require.True(t, ok)

	caps, err := b.GetCapabilities(ctx)
	require.NoError(t, err)

	// CAPABILITY_CREDENTIAL_ISSUE advertised both connector-wide and per-resource-type.
	require.Contains(t, caps.GetConnectorCapabilities(), v2.Capability_CAPABILITY_CREDENTIAL_ISSUE)
	require.Contains(t, caps.GetResourceTypeCapabilities()[0].GetCapabilities(), v2.Capability_CAPABILITY_CREDENTIAL_ISSUE)

	// Credential issue details surface the supported/preferred options.
	issueDetails := caps.GetCredentialDetails().GetCapabilityCredentialIssue()
	require.NotNil(t, issueDetails)
	require.Equal(t,
		v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY,
		issueDetails.GetPreferredOption(),
	)
	require.Equal(t, v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_TOKEN, issueDetails.GetOptions()[1].GetOption())
}
