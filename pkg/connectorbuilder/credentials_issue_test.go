package connectorbuilder

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

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
	lastOptions       *v2.CredentialIssueOptions
	lastInput         *CredentialIssueInput
	capabilityDetails *v2.CredentialDetailsCredentialIssue
	capabilityError   error
}

type testCredentialSecretDeleter struct{ ResourceSyncer }

func newTestCredentialSecretDeleter() *testCredentialSecretDeleter {
	return &testCredentialSecretDeleter{ResourceSyncer: newTestResourceSyncer("secret")}
}

func (d *testCredentialSecretDeleter) Delete(context.Context, *v2.ResourceId) (annotations.Annotations, error) {
	return nil, nil
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

	return &CredentialIssueOutput{
		Secret:        secret,
		PlaintextData: plaintexts,
		ResourceMode:  v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE,
	}, nil
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
				Option:               v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY,
				Scopes:               []string{"read", "write"},
				ResourceMode:         v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE,
				SecretResourceTypeId: "secret",
			}.Build(),
			v2.CredentialIssueOptionDescriptor_builder{
				Option:               v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_TOKEN,
				ResourceMode:         v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_VIRTUAL,
				SecretResourceTypeId: "secret",
			}.Build(),
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
	options := v2.CredentialIssueOptions_builder{
		ApiKey: &v2.CredentialIssueOptions_ApiKey{},
	}.Build()

	t.Run("missing encryption configuration", func(t *testing.T) {
		issuer := newTestCredentialIssuer("service_account")
		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{issuer, newTestCredentialSecretDeleter()}))
		require.NoError(t, err)

		_, err = connector.IssueCredential(ctx, v2.IssueCredentialRequest_builder{
			IdentityId:        identityID,
			CredentialOptions: options,
			RequestId:         "request-1",
		}.Build())
		require.ErrorContains(t, err, "at least one encryption config is required")
		require.Nil(t, issuer.lastInput, "connector must not mint a credential without a delivery path")
	})

	t.Run("capability lookup failure", func(t *testing.T) {
		issuer := newTestCredentialIssuer("service_account")
		issuer.capabilityError = errors.New("capability unavailable")
		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{issuer, newTestCredentialSecretDeleter()}))
		require.NoError(t, err)

		_, err = connector.IssueCredential(ctx, v2.IssueCredentialRequest_builder{
			IdentityId:        identityID,
			CredentialOptions: options,
			EncryptionConfigs: []*v2.EncryptionConfig{newIssueEncryptionConfig(t)},
			RequestId:         "request-2",
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
		CredentialOptions: v2.CredentialIssueOptions_builder{
			ApiKey: &v2.CredentialIssueOptions_ApiKey{},
		}.Build(),
		RequestId: "request-3",
	}.Build())
	require.ErrorContains(t, err, "resource type does not have credential issuer configured")

	// Success case: credential issuer implemented, with a real encryption config.
	issuer := newTestCredentialIssuer("service_account")
	connector, err = NewConnector(ctx, newTestConnector([]ResourceSyncer{issuer, newTestCredentialSecretDeleter()}))
	require.NoError(t, err)

	encConfig := newIssueEncryptionConfig(t)
	resp, err := connector.IssueCredential(ctx, v2.IssueCredentialRequest_builder{
		IdentityId: v2.ResourceId_builder{
			ResourceType: "service_account",
			Resource:     "sa-1",
		}.Build(),
		CredentialOptions: v2.CredentialIssueOptions_builder{
			ApiKey: v2.CredentialIssueOptions_ApiKey_builder{
				Scopes: []string{"read", "write"},
			}.Build(),
		}.Build(),
		EncryptionConfigs: []*v2.EncryptionConfig{encConfig},
		RequestId:         "request-4",
	}.Build())
	require.NoError(t, err)
	require.NotNil(t, resp)

	// The new secret resource is returned as a first-class object.
	require.NotNil(t, resp.GetSecret())
	require.Equal(t, "secret", resp.GetSecret().GetId().GetResourceType())
	require.Equal(t, "issued-key-1", resp.GetSecret().GetId().GetResource())
	require.Equal(t, v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE, resp.GetResourceMode())
	require.Equal(t, "request-4", resp.GetRequestId())

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
		newTestCredentialSecretDeleter(),
	}))
	require.NoError(t, err)

	b, ok := connector.(*builder)
	require.True(t, ok)

	caps, err := b.GetCapabilities(ctx)
	require.NoError(t, err)

	// CAPABILITY_CREDENTIAL_ISSUE advertised both connector-wide and per-resource-type.
	require.Contains(t, caps.GetConnectorCapabilities(), v2.Capability_CAPABILITY_CREDENTIAL_ISSUE)
	var serviceAccountCapability *v2.ResourceTypeCapability
	for _, capability := range caps.GetResourceTypeCapabilities() {
		if capability.GetResourceType().GetId() == "service_account" {
			serviceAccountCapability = capability
			break
		}
	}
	require.NotNil(t, serviceAccountCapability)
	require.Contains(t, serviceAccountCapability.GetCapabilities(), v2.Capability_CAPABILITY_CREDENTIAL_ISSUE)

	// Credential issue details surface the supported/preferred options.
	issueDetails := serviceAccountCapability.GetCredentialIssue()
	require.NotNil(t, issueDetails)
	require.Equal(t,
		v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY,
		issueDetails.GetPreferredOption(),
	)
	require.Equal(t, v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_TOKEN, issueDetails.GetOptions()[1].GetOption())
}

func TestIssueCredentialCapabilityRequiresSecretDeleter(t *testing.T) {
	ctx := context.Background()
	connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{newTestCredentialIssuer("service_account")}))
	require.NoError(t, err)

	_, err = connector.(*builder).GetCapabilities(ctx)
	require.ErrorContains(t, err, "without ResourceDeleterV2")
}

func TestIssueCredentialCapabilitiesArePerResourceType(t *testing.T) {
	ctx := context.Background()
	apiIssuer := newTestCredentialIssuer("api_identity")
	secretIssuer := newTestCredentialIssuer("client_identity")
	secretIssuer.capabilityDetails = v2.CredentialDetailsCredentialIssue_builder{
		Options: []*v2.CredentialIssueOptionDescriptor{
			v2.CredentialIssueOptionDescriptor_builder{
				Option:               v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_CLIENT_SECRET,
				ResourceMode:         v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE,
				SecretResourceTypeId: "secret",
			}.Build(),
		},
		PreferredOption: v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_CLIENT_SECRET,
	}.Build()

	connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{secretIssuer, apiIssuer, newTestCredentialSecretDeleter()}))
	require.NoError(t, err)
	caps, err := connector.(*builder).GetCapabilities(ctx)
	require.NoError(t, err)

	byType := make(map[string]*v2.CredentialDetailsCredentialIssue)
	for _, capability := range caps.GetResourceTypeCapabilities() {
		byType[capability.GetResourceType().GetId()] = capability.GetCredentialIssue()
	}
	require.Equal(t, v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY, byType["api_identity"].GetPreferredOption())
	require.Equal(t, v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_CLIENT_SECRET, byType["client_identity"].GetPreferredOption())
}

func TestValidateKeyGenerationProfile(t *testing.T) {
	bits := uint32(2048)
	require.NoError(t, validateKeyGenerationProfile(v2.KeyGenerationProfile_builder{Kty: "RSA", RsaModulusBits: &bits}.Build()))
	require.Error(t, validateKeyGenerationProfile(v2.KeyGenerationProfile_builder{Kty: "RSA"}.Build()))
	require.Error(t, validateKeyGenerationProfile(v2.KeyGenerationProfile_builder{Kty: "oct", RsaModulusBits: &bits}.Build()))
	curve := "P-256"
	require.NoError(t, validateKeyGenerationProfile(v2.KeyGenerationProfile_builder{Kty: "EC", Crv: &curve}.Build()))
}

func TestValidateCredentialIssueInputExpiryAndAudiences(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	details := v2.CredentialDetailsCredentialIssue_builder{
		Options: []*v2.CredentialIssueOptionDescriptor{
			v2.CredentialIssueOptionDescriptor_builder{
				Option:                 v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_TOKEN,
				Audiences:              []string{"api://one", "api://two"},
				CustomAudiencesAllowed: false,
				Expiry: v2.IssuanceExpiryCapability_builder{
					Min: durationpb.New(5 * time.Minute),
					Max: durationpb.New(time.Hour),
				}.Build(),
				ResourceMode:         v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_VIRTUAL,
				SecretResourceTypeId: "secret",
			}.Build(),
		},
		PreferredOption: v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_TOKEN,
	}.Build()
	input := &CredentialIssueInput{
		IdentityID: v2.ResourceId_builder{ResourceType: "service_account", Resource: "sa-1"}.Build(),
		CredentialOptions: v2.CredentialIssueOptions_builder{
			Token: v2.CredentialIssueOptions_Token_builder{Audiences: []string{"api://one", "api://two"}}.Build(),
		}.Build(),
		ExpiresAt: timestamppb.New(now.Add(30 * time.Minute)),
		RequestID: "request-1",
	}

	descriptor, err := validateCredentialIssueInput(input, details, now)
	require.NoError(t, err)
	require.Equal(t, v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_VIRTUAL, descriptor.GetResourceMode())

	input.ExpiresAt = timestamppb.New(now.Add(-time.Second))
	_, err = validateCredentialIssueInput(input, details, now)
	require.ErrorContains(t, err, "future")

	input.ExpiresAt = timestamppb.New(now.Add(30 * time.Minute))
	input.CredentialOptions.GetToken().SetAudiences([]string{"api://unknown"})
	_, err = validateCredentialIssueInput(input, details, now)
	require.ErrorContains(t, err, "not advertised")

	input.RequestID = "not valid!"
	_, err = validateCredentialIssueInput(input, details, now)
	require.ErrorContains(t, err, "request id")
}

func TestValidateCredentialIssueCapabilityDetails(t *testing.T) {
	t.Run("rejects fields that do not apply to option", func(t *testing.T) {
		details := v2.CredentialDetailsCredentialIssue_builder{
			Options: []*v2.CredentialIssueOptionDescriptor{
				v2.CredentialIssueOptionDescriptor_builder{
					Option:               v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_CLIENT_SECRET,
					Scopes:               []string{"read"},
					ResourceMode:         v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_VIRTUAL,
					SecretResourceTypeId: "secret",
				}.Build(),
			},
			PreferredOption: v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_CLIENT_SECRET,
		}.Build()
		require.ErrorContains(t, validateCredentialIssueCapabilityDetails(details), "may only advertise expiry")
	})

	t.Run("rejects inverted expiry range", func(t *testing.T) {
		details := v2.CredentialDetailsCredentialIssue_builder{
			Options: []*v2.CredentialIssueOptionDescriptor{
				v2.CredentialIssueOptionDescriptor_builder{
					Option: v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_TOKEN,
					Expiry: v2.IssuanceExpiryCapability_builder{
						Min: durationpb.New(time.Hour),
						Max: durationpb.New(time.Minute),
					}.Build(),
					ResourceMode:         v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_VIRTUAL,
					SecretResourceTypeId: "secret",
				}.Build(),
			},
			PreferredOption: v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_TOKEN,
		}.Build()
		require.ErrorContains(t, validateCredentialIssueCapabilityDetails(details), "minimum must not exceed maximum")
	})
}

func TestValidateCredentialIssueOutputLifecycle(t *testing.T) {
	identityID := v2.ResourceId_builder{ResourceType: "service_account", Resource: "sa-1"}.Build()
	requestedExpiry := time.Date(2026, 7, 22, 13, 0, 0, 0, time.UTC)
	newOutput := func(secretType string, expiry time.Time, mode v2.CredentialResourceMode) *CredentialIssueOutput {
		secret, err := resource.NewSecretResource(
			"issued secret",
			v2.ResourceType_builder{Id: secretType}.Build(),
			"secret-1",
			[]resource.SecretTraitOption{
				resource.WithSecretIdentityID(identityID),
				resource.WithSecretExpiresAt(expiry),
			},
		)
		require.NoError(t, err)
		return &CredentialIssueOutput{
			Secret:        secret,
			PlaintextData: []*v2.PlaintextData{v2.PlaintextData_builder{Name: "token", Bytes: []byte("secret")}.Build()},
			ResourceMode:  mode,
		}
	}
	descriptor := v2.CredentialIssueOptionDescriptor_builder{
		Option:               v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_TOKEN,
		ResourceMode:         v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_VIRTUAL,
		SecretResourceTypeId: "secret",
	}.Build()

	expiresAt := timestamppb.New(requestedExpiry)
	virtual := v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_VIRTUAL
	discoverable := v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE
	require.NoError(t, validateCredentialIssueOutput(identityID, expiresAt, newOutput("secret", requestedExpiry, virtual), descriptor))
	require.ErrorContains(t, validateCredentialIssueOutput(identityID, expiresAt, newOutput("other", requestedExpiry, virtual), descriptor), "resource type")
	require.ErrorContains(t, validateCredentialIssueOutput(identityID, expiresAt, newOutput("secret", requestedExpiry, discoverable), descriptor), "resource mode")
	require.ErrorContains(t, validateCredentialIssueOutput(identityID, expiresAt, newOutput("secret", requestedExpiry.Add(time.Minute), virtual), descriptor), "exceeds requested expiry")
}
