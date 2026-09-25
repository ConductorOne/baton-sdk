package connectorbuilder

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/jwe"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/jwk"
)

// preflightSecretMarker stands in for the credential a connector would mint.
// Any error surfaced to the platform must not carry it.
const preflightSecretMarker = "PREFLIGHT-SECRET-DO-NOT-ECHO"

// markerPlaintexts is the credential material most tests use: one value holding
// preflightSecretMarker, whose bytes must never appear in an error.
func markerPlaintexts() []*v2.PlaintextData {
	return []*v2.PlaintextData{
		v2.PlaintextData_builder{Name: "password", Bytes: []byte(preflightSecretMarker)}.Build(),
	}
}

// countingAccountManager records whether CreateAccount reached the connector, so
// a test can pin the ordering between the connector call and config validation.
type countingAccountManager struct {
	ResourceSyncer
	calls int
	// plaintexts is what this connector mints. Tests that exercise a connector
	// returning no plaintext credentials set it to nil.
	plaintexts []*v2.PlaintextData
}

func newCountingAccountManager(resourceType string) *countingAccountManager {
	return &countingAccountManager{
		ResourceSyncer: newTestResourceSyncer(resourceType),
		plaintexts:     markerPlaintexts(),
	}
}

func (c *countingAccountManager) CreateAccount(ctx context.Context, _ *v2.AccountInfo, _ *v2.LocalCredentialOptions) (CreateAccountResponse, []*v2.PlaintextData, annotations.Annotations, error) {
	c.calls++
	return v2.CreateAccountResponse_SuccessResult_builder{
		IsCreateAccountResult: true,
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: c.ResourceType(ctx).GetId(),
				Resource:     "created-account",
			}.Build(),
		}.Build(),
	}.Build(), c.plaintexts, annotations.Annotations{}, nil
}

func (c *countingAccountManager) CreateAccountCapabilityDetails(context.Context) (*v2.CredentialDetailsAccountProvisioning, annotations.Annotations, error) {
	return nil, annotations.Annotations{}, nil
}

// countingCredentialManager is the RotateCredential counterpart.
type countingCredentialManager struct {
	ResourceSyncer
	calls int
	// plaintexts is what this connector mints. Tests that exercise a connector
	// returning no plaintext credentials set it to nil.
	plaintexts []*v2.PlaintextData
}

func newCountingCredentialManager(resourceType string) *countingCredentialManager {
	return &countingCredentialManager{
		ResourceSyncer: newTestResourceSyncer(resourceType),
		plaintexts:     markerPlaintexts(),
	}
}

func (c *countingCredentialManager) Rotate(context.Context, *v2.ResourceId, *v2.LocalCredentialOptions) ([]*v2.PlaintextData, annotations.Annotations, error) {
	c.calls++
	return c.plaintexts, annotations.Annotations{}, nil
}

func (c *countingCredentialManager) RotateCapabilityDetails(context.Context) (*v2.CredentialDetailsCredentialRotation, annotations.Annotations, error) {
	return nil, annotations.Annotations{}, nil
}

// invalidJWEConfig is rejected by the provider's own config validation: the
// public JWK lacks the algorithm and key material the profile requires.
func invalidJWEConfig() *v2.EncryptionConfig {
	return v2.EncryptionConfig_builder{
		Provider: jwe.EncryptionProvider,
		KeyId:    "recipient-1",
		JwkPublicKeyConfig: v2.EncryptionConfig_JWKPublicKeyConfig_builder{
			PubKey: []byte(`{"kty":"AKP"}`),
		}.Build(),
	}.Build()
}

// jwkConfigWithAAD is a legacy JWK recipient that asked for authenticated data.
// The JWK provider cannot honor it, so it must not be silently dropped.
func jwkConfigWithAAD(t *testing.T) *v2.EncryptionConfig {
	t.Helper()
	config := newIssueEncryptionConfig(t)
	config.GetJwkPublicKeyConfig().SetAdditionalAuthenticatedData([]byte("bound-context"))
	return config
}

// unknownProviderConfig names a provider that is not registered, which the
// shared resolver refuses regardless of the key material.
func unknownProviderConfig() *v2.EncryptionConfig {
	return v2.EncryptionConfig_builder{
		Provider: "baton/not-a-provider/v1",
		KeyId:    "recipient-1",
		JwkPublicKeyConfig: v2.EncryptionConfig_JWKPublicKeyConfig_builder{
			PubKey: []byte(`{"kty":"AKP"}`),
		}.Build(),
	}.Build()
}

// encryptionConfigCase names one encryption-configuration shape a caller can
// supply, with the configs built per test so each case gets fresh key material.
type encryptionConfigCase struct {
	name    string
	configs func(t *testing.T) []*v2.EncryptionConfig
}

// unusableEncryptionConfigs are the shapes the shared resolver or a provider
// refuses. Both the returned-plaintext and the zero-output tests drive these, so
// the two tests differ only in what the connector returns.
func unusableEncryptionConfigs() []encryptionConfigCase {
	return []encryptionConfigCase{
		{"nil entry", func(*testing.T) []*v2.EncryptionConfig {
			return []*v2.EncryptionConfig{nil}
		}},
		{"unknown provider", func(*testing.T) []*v2.EncryptionConfig {
			return []*v2.EncryptionConfig{unknownProviderConfig()}
		}},
		{"provider-rejected recipient", func(t *testing.T) []*v2.EncryptionConfig {
			return []*v2.EncryptionConfig{jwkConfigWithAAD(t)}
		}},
		{"malformed JWE recipient", func(*testing.T) []*v2.EncryptionConfig {
			return []*v2.EncryptionConfig{invalidJWEConfig()}
		}},
		{"JWE recipient fan-out", func(t *testing.T) []*v2.EncryptionConfig {
			return []*v2.EncryptionConfig{validJWEConfig(t), validJWEConfig(t)}
		}},
	}
}

// TestCreateAndRotateRejectUnusableConfigWhenPlaintextReturned covers the case
// that still fails: the connector produced a plaintext value, so the supplied
// config is about to be used and an unusable one must stop the operation.
// Validation runs after the connector call, so the connector has already run by
// the time the error is returned; the caller gets the error and no response.
func TestCreateAndRotateRejectUnusableConfigWhenPlaintextReturned(t *testing.T) {
	ctx := context.Background()

	for _, tc := range unusableEncryptionConfigs() {
		t.Run("create account / "+tc.name, func(t *testing.T) {
			accountManager := newCountingAccountManager("user")
			connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{accountManager}))
			require.NoError(t, err)

			response, err := connector.CreateAccount(ctx, v2.CreateAccountRequest_builder{
				AccountInfo:       v2.AccountInfo_builder{}.Build(),
				CredentialOptions: v2.CredentialOptions_builder{NoPassword: &v2.CredentialOptions_NoPassword{}}.Build(),
				EncryptionConfigs: tc.configs(t),
			}.Build())

			require.Error(t, err)
			require.Equal(t, codes.InvalidArgument, status.Code(err), "want InvalidArgument, got %v", err)
			require.Equal(t, 1, accountManager.calls, "the connector runs before the config is checked")
			require.Nil(t, response, "no response may be returned when the plaintexts could not be encrypted")
			require.NotContains(t, err.Error(), preflightSecretMarker)
		})

		t.Run("rotate credential / "+tc.name, func(t *testing.T) {
			credentialManager := newCountingCredentialManager("user")
			connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{credentialManager}))
			require.NoError(t, err)

			response, err := connector.RotateCredential(ctx, v2.RotateCredentialRequest_builder{
				ResourceId:        v2.ResourceId_builder{ResourceType: "user", Resource: "test-user"}.Build(),
				CredentialOptions: v2.CredentialOptions_builder{NoPassword: &v2.CredentialOptions_NoPassword{}}.Build(),
				EncryptionConfigs: tc.configs(t),
			}.Build())

			require.Error(t, err)
			require.Equal(t, codes.InvalidArgument, status.Code(err), "want InvalidArgument, got %v", err)
			require.Equal(t, 1, credentialManager.calls, "the connector runs before the config is checked")
			require.Nil(t, response, "no response may be returned when the plaintexts could not be encrypted")
			require.NotContains(t, err.Error(), preflightSecretMarker)
		})
	}
}

// TestCreateAndRotateSucceedWithUnusedUnusableConfig covers the zero-output case
// existing connectors depend on: a connector that returns no plaintext never uses
// the supplied configs, so an unusable one must not turn a completed operation
// into a failure. ActionRequired, InProgress, AlreadyExists and NoPassword flows
// reach this, as does a rotation that does not return the new value.
//
// The empty config list is included as the baseline: it succeeds both here and
// when the connector does return plaintexts.
func TestCreateAndRotateSucceedWithUnusedUnusableConfig(t *testing.T) {
	ctx := context.Background()

	cases := append(unusableEncryptionConfigs(), encryptionConfigCase{
		name:    "empty config list",
		configs: func(*testing.T) []*v2.EncryptionConfig { return nil },
	})

	for _, tc := range cases {
		t.Run("create account / "+tc.name, func(t *testing.T) {
			accountManager := newCountingAccountManager("user")
			accountManager.plaintexts = nil
			connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{accountManager}))
			require.NoError(t, err)

			response, err := connector.CreateAccount(ctx, v2.CreateAccountRequest_builder{
				AccountInfo:       v2.AccountInfo_builder{}.Build(),
				CredentialOptions: v2.CredentialOptions_builder{NoPassword: &v2.CredentialOptions_NoPassword{}}.Build(),
				EncryptionConfigs: tc.configs(t),
			}.Build())

			require.NoError(t, err, "a config the operation never uses must not fail it")
			require.Equal(t, 1, accountManager.calls)
			require.NotNil(t, response.GetSuccess())
			require.Empty(t, response.GetEncryptedData())
		})

		t.Run("rotate credential / "+tc.name, func(t *testing.T) {
			credentialManager := newCountingCredentialManager("user")
			credentialManager.plaintexts = nil
			connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{credentialManager}))
			require.NoError(t, err)

			response, err := connector.RotateCredential(ctx, v2.RotateCredentialRequest_builder{
				ResourceId:        v2.ResourceId_builder{ResourceType: "user", Resource: "test-user"}.Build(),
				CredentialOptions: v2.CredentialOptions_builder{NoPassword: &v2.CredentialOptions_NoPassword{}}.Build(),
				EncryptionConfigs: tc.configs(t),
			}.Build())

			require.NoError(t, err, "a config the operation never uses must not fail it")
			require.Equal(t, 1, credentialManager.calls)
			require.Empty(t, response.GetEncryptedData())
		})
	}
}

// TestCreateAndRotateKeepEmptyAndLegacyConfigBehavior pins the compatibility
// half of the preflight change: no encryption configs, and a legacy no-AAD JWK
// recipient, must both still reach the connector.
func TestCreateAndRotateKeepEmptyAndLegacyConfigBehavior(t *testing.T) {
	ctx := context.Background()

	t.Run("create account with no encryption configs", func(t *testing.T) {
		accountManager := newCountingAccountManager("user")
		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{accountManager}))
		require.NoError(t, err)

		resp, err := connector.CreateAccount(ctx, v2.CreateAccountRequest_builder{
			AccountInfo:       v2.AccountInfo_builder{}.Build(),
			CredentialOptions: v2.CredentialOptions_builder{NoPassword: &v2.CredentialOptions_NoPassword{}}.Build(),
		}.Build())
		require.NoError(t, err)
		require.NotNil(t, resp.GetSuccess())
		require.Equal(t, 1, accountManager.calls)
		require.Empty(t, resp.GetEncryptedData())
	})

	t.Run("create account with a legacy no-AAD JWK recipient", func(t *testing.T) {
		accountManager := newCountingAccountManager("user")
		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{accountManager}))
		require.NoError(t, err)

		resp, err := connector.CreateAccount(ctx, v2.CreateAccountRequest_builder{
			AccountInfo:       v2.AccountInfo_builder{}.Build(),
			CredentialOptions: v2.CredentialOptions_builder{NoPassword: &v2.CredentialOptions_NoPassword{}}.Build(),
			EncryptionConfigs: []*v2.EncryptionConfig{newIssueEncryptionConfig(t)},
		}.Build())
		require.NoError(t, err)
		require.Equal(t, 1, accountManager.calls)
		require.Len(t, resp.GetEncryptedData(), 1)
		require.Equal(t, jwk.EncryptionProviderJwk, resp.GetEncryptedData()[0].GetProvider())
	})

	t.Run("rotate credential with no encryption configs", func(t *testing.T) {
		credentialManager := newCountingCredentialManager("user")
		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{credentialManager}))
		require.NoError(t, err)

		resp, err := connector.RotateCredential(ctx, v2.RotateCredentialRequest_builder{
			ResourceId:        v2.ResourceId_builder{ResourceType: "user", Resource: "test-user"}.Build(),
			CredentialOptions: v2.CredentialOptions_builder{NoPassword: &v2.CredentialOptions_NoPassword{}}.Build(),
		}.Build())
		require.NoError(t, err)
		require.Equal(t, 1, credentialManager.calls)
		require.Empty(t, resp.GetEncryptedData())
	})

	t.Run("rotate credential with a legacy no-AAD JWK recipient", func(t *testing.T) {
		credentialManager := newCountingCredentialManager("user")
		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{credentialManager}))
		require.NoError(t, err)

		resp, err := connector.RotateCredential(ctx, v2.RotateCredentialRequest_builder{
			ResourceId:        v2.ResourceId_builder{ResourceType: "user", Resource: "test-user"}.Build(),
			CredentialOptions: v2.CredentialOptions_builder{NoPassword: &v2.CredentialOptions_NoPassword{}}.Build(),
			EncryptionConfigs: []*v2.EncryptionConfig{newIssueEncryptionConfig(t)},
		}.Build())
		require.NoError(t, err)
		require.Equal(t, 1, credentialManager.calls)
		require.Len(t, resp.GetEncryptedData(), 1)
	})
}
