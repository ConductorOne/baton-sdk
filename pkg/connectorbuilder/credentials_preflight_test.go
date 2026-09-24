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

// countingAccountManager records whether CreateAccount reached the connector.
// CreateAccount mints the account in the upstream provider, so an invocation
// that later fails to encrypt has already mutated state that the caller must
// now reconcile. These tests assert the count is zero.
type countingAccountManager struct {
	ResourceSyncer
	calls int
}

func newCountingAccountManager(resourceType string) *countingAccountManager {
	return &countingAccountManager{ResourceSyncer: newTestResourceSyncer(resourceType)}
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
	}.Build(), []*v2.PlaintextData{
		v2.PlaintextData_builder{Name: "password", Bytes: []byte(preflightSecretMarker)}.Build(),
	}, annotations.Annotations{}, nil
}

func (c *countingAccountManager) CreateAccountCapabilityDetails(context.Context) (*v2.CredentialDetailsAccountProvisioning, annotations.Annotations, error) {
	return nil, annotations.Annotations{}, nil
}

// countingCredentialManager is the RotateCredential counterpart: Rotate replaces
// the credential on the upstream resource, so reaching it is a state change.
type countingCredentialManager struct {
	ResourceSyncer
	calls int
}

func newCountingCredentialManager(resourceType string) *countingCredentialManager {
	return &countingCredentialManager{ResourceSyncer: newTestResourceSyncer(resourceType)}
}

func (c *countingCredentialManager) Rotate(context.Context, *v2.ResourceId, *v2.LocalCredentialOptions) ([]*v2.PlaintextData, annotations.Annotations, error) {
	c.calls++
	return []*v2.PlaintextData{
		v2.PlaintextData_builder{Name: "password", Bytes: []byte(preflightSecretMarker)}.Build(),
	}, annotations.Annotations{}, nil
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

func TestCreateAccountValidatesEncryptionConfigBeforeMutation(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name   string
		config func(t *testing.T) *v2.EncryptionConfig
	}{
		{"malformed JWE recipient", func(*testing.T) *v2.EncryptionConfig { return invalidJWEConfig() }},
		{"legacy JWK recipient with authenticated data", jwkConfigWithAAD},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			accountManager := newCountingAccountManager("user")
			connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{accountManager}))
			require.NoError(t, err)

			_, err = connector.CreateAccount(ctx, v2.CreateAccountRequest_builder{
				AccountInfo:       v2.AccountInfo_builder{}.Build(),
				CredentialOptions: v2.CredentialOptions_builder{NoPassword: &v2.CredentialOptions_NoPassword{}}.Build(),
				EncryptionConfigs: []*v2.EncryptionConfig{tc.config(t)},
			}.Build())

			require.Error(t, err)
			require.Equal(t, codes.InvalidArgument, status.Code(err), "want InvalidArgument, got %v", err)
			require.Zero(t, accountManager.calls, "CreateAccount must not mutate the provider when its encryption config is invalid")
			require.NotContains(t, err.Error(), preflightSecretMarker)
		})
	}
}

func TestRotateCredentialValidatesEncryptionConfigBeforeMutation(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name    string
		configs func(t *testing.T) []*v2.EncryptionConfig
	}{
		{"malformed JWE recipient", func(*testing.T) []*v2.EncryptionConfig {
			return []*v2.EncryptionConfig{invalidJWEConfig()}
		}},
		{"legacy JWK recipient with authenticated data", func(t *testing.T) []*v2.EncryptionConfig {
			return []*v2.EncryptionConfig{jwkConfigWithAAD(t)}
		}},
		{"JWE recipient fan-out", func(t *testing.T) []*v2.EncryptionConfig {
			return []*v2.EncryptionConfig{validJWEConfig(t), validJWEConfig(t)}
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			credentialManager := newCountingCredentialManager("user")
			connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{credentialManager}))
			require.NoError(t, err)

			_, err = connector.RotateCredential(ctx, v2.RotateCredentialRequest_builder{
				ResourceId:        v2.ResourceId_builder{ResourceType: "user", Resource: "test-user"}.Build(),
				CredentialOptions: v2.CredentialOptions_builder{NoPassword: &v2.CredentialOptions_NoPassword{}}.Build(),
				EncryptionConfigs: tc.configs(t),
			}.Build())

			require.Error(t, err)
			require.Equal(t, codes.InvalidArgument, status.Code(err), "want InvalidArgument, got %v", err)
			require.Zero(t, credentialManager.calls, "Rotate must not mutate the provider when its encryption config is invalid")
			require.NotContains(t, err.Error(), preflightSecretMarker)
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
