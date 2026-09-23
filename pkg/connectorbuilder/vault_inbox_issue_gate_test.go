package connectorbuilder

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"testing"

	filippoage "filippo.io/age"
	"filippo.io/hpke"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/vaultinbox"
	resource "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/stretchr/testify/require"
)

// vaultInboxIssuer is a fake provider with a counted mint. The count is the
// point: the gate rules below are only meaningful if a refused issuance left the
// count at zero and a post-mint failure left it at exactly one.
type vaultInboxIssuer struct {
	ResourceSyncer
	issueCalls int
	values     []*v2.PlaintextData
	details    *v2.CredentialDetailsCredentialIssue
}

func newVaultInboxIssuer(values []*v2.PlaintextData, profiles []v2.VaultInboxSuite) *vaultInboxIssuer {
	return &vaultInboxIssuer{
		ResourceSyncer: newTestResourceSyncer("service_account"),
		values:         values,
		details: v2.CredentialDetailsCredentialIssue_builder{
			Options: []*v2.CredentialIssueOptionDescriptor{
				v2.CredentialIssueOptionDescriptor_builder{
					Option:               v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY,
					Scopes:               []string{"read", "write"},
					ResourceMode:         v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE,
					SecretResourceTypeId: "secret",
					VaultInboxProfiles:   profiles,
				}.Build(),
			},
			PreferredOption: v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY,
		}.Build(),
	}
}

func (i *vaultInboxIssuer) Issue(_ context.Context, input *CredentialIssueInput) (*CredentialIssueOutput, error) {
	i.issueCalls++
	secret, err := resource.NewSecretResource(
		"Issued key for "+input.IdentityID.GetResource(),
		v2.ResourceType_builder{Id: "secret"}.Build(),
		"issued-key-1",
		[]resource.SecretTraitOption{resource.WithSecretIdentityID(input.IdentityID)},
	)
	if err != nil {
		return nil, err
	}
	return &CredentialIssueOutput{
		Secret:        secret,
		PlaintextData: i.values,
		ResourceMode:  v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE,
	}, nil
}

func (i *vaultInboxIssuer) IssueCapabilityDetails(context.Context) (*v2.CredentialDetailsCredentialIssue, annotations.Annotations, error) {
	return i.details, annotations.Annotations{}, nil
}

const gateScheme = "latchkey.vault_submission.secret.v1"
const gateJWKAlg = "HPKE-Base-X-Wing-Draft06-HKDF-SHA256-ChaCha20Poly1305"

// gateRecipient mints a synthetic recipient the way a reader would: only the
// public JWK and thumbprint cross into the SDK config.
func gateRecipient(t *testing.T, seed byte) (string, string) {
	t.Helper()
	key, err := hpke.MLKEM768X25519().NewPrivateKey(bytes.Repeat([]byte{seed}, 32))
	require.NoError(t, err)
	pub := base64.RawURLEncoding.EncodeToString(key.PublicKey().Bytes())
	jwk := `{"kty":"AKP","alg":"` + gateJWKAlg + `","pub":"` + pub + `"}`
	digest := sha256.Sum256([]byte(`{"alg":"` + gateJWKAlg + `","kty":"AKP","pub":"` + pub + `"}`))
	return jwk, base64.RawURLEncoding.EncodeToString(digest[:])
}

func gateConfig(t *testing.T, mutate func(*v2.VaultInboxRecipientConfig)) *v2.EncryptionConfig {
	t.Helper()
	jwk, thumbprint := gateRecipient(t, 0x42)
	config := v2.VaultInboxRecipientConfig_builder{
		ConfigVersion:       v2.VaultInboxConfigVersion_VAULT_INBOX_CONFIG_VERSION_V1,
		Suite:               v2.VaultInboxSuite_VAULT_INBOX_SUITE_XWING_MLKEM768_X25519_HKDF_SHA256_CHACHA20POLY1305_V1,
		TenantId:            "tenant-1",
		VaultBoundaryId:     "vault-1",
		InboxKeyId:          "inbox-key-1",
		KeyGeneration:       1,
		PayloadScheme:       gateScheme,
		SubmissionId:        "submission-1",
		PublicJwkJson:       jwk,
		PublicKeyThumbprint: thumbprint,
	}.Build()
	if mutate != nil {
		mutate(config)
	}
	return v2.EncryptionConfig_builder{
		Provider:                  vaultinbox.EncryptionProvider,
		VaultInboxRecipientConfig: config,
	}.Build()
}

// validAgeConfig is a *valid* age recipient, so a mixed config is refused by the
// vault-inbox exclusivity gate rather than by the age validator.
func validAgeConfig(t *testing.T) *v2.EncryptionConfig {
	t.Helper()
	identity, err := filippoage.GenerateHybridIdentity()
	require.NoError(t, err)
	return v2.EncryptionConfig_builder{
		AgeRecipientConfig: v2.EncryptionConfig_AgeRecipientConfig_builder{
			Recipient: identity.Recipient().String(),
		}.Build(),
	}.Build()
}

// TestVaultInboxProviderRefusesAForeignProviderName pins the provider's own
// mismatch branch directly. Routing a vault-inbox config through the request
// path with another provider name selects *that* provider, so this branch is
// only reachable at the provider boundary.
func TestVaultInboxProviderRefusesAForeignProviderName(t *testing.T) {
	t.Parallel()
	config := gateConfig(t, nil)
	config.SetProvider("baton/age/v1")
	require.Error(t, vaultinbox.NewProvider().ValidateConfig(context.Background(), config),
		"a config naming another provider must not be sealed by this one")
}

// gateAccountManager lets a test choose the CreateAccount result, so the
// cardinality rule can be exercised against the non-success outcomes that
// legitimately carry no plaintext.
type gateAccountManager struct {
	ResourceSyncer
	result      CreateAccountResponse
	plaintexts  []*v2.PlaintextData
	createCalls int
}

func (m *gateAccountManager) CreateAccount(
	context.Context,
	*v2.AccountInfo,
	*v2.LocalCredentialOptions,
) (CreateAccountResponse, []*v2.PlaintextData, annotations.Annotations, error) {
	m.createCalls++
	return m.result, m.plaintexts, annotations.Annotations{}, nil
}

func (m *gateAccountManager) CreateAccountCapabilityDetails(context.Context) (*v2.CredentialDetailsAccountProvisioning, annotations.Annotations, error) {
	return v2.CredentialDetailsAccountProvisioning_builder{}.Build(), annotations.Annotations{}, nil
}

func gateCreateAccountRequest(t *testing.T) *v2.CreateAccountRequest {
	t.Helper()
	return v2.CreateAccountRequest_builder{
		ResourceTypeId: "service_account",
		AccountInfo:    &v2.AccountInfo{},
		CredentialOptions: v2.CredentialOptions_builder{
			RandomPassword: v2.CredentialOptions_RandomPassword_builder{Length: 12}.Build(),
		}.Build(),
		EncryptionConfigs: []*v2.EncryptionConfig{gateConfig(t, nil)},
	}.Build()
}

// TestVaultInboxCreateAccountKeepsStructuredResults pins the width of the
// cardinality rule on this path. CreateAccount's non-success outcomes carry no
// plaintext by contract, so demanding one would replace "the account already
// exists" with a hard failure; only more than one value is refused.
func TestVaultInboxCreateAccountKeepsStructuredResults(t *testing.T) {
	t.Parallel()

	t.Run("non-success result with no plaintext still returns its structure", func(t *testing.T) {
		t.Parallel()
		manager := &gateAccountManager{
			ResourceSyncer: newTestResourceSyncer("service_account"),
			result:         &v2.CreateAccountResponse_AlreadyExistsResult{IsCreateAccountResult: true},
		}
		connector, err := NewConnector(context.Background(), newTestConnector([]ResourceSyncer{manager}))
		require.NoError(t, err)

		resp, err := connector.CreateAccount(context.Background(), gateCreateAccountRequest(t))
		require.NoError(t, err, "an outcome that carries no credential must not be turned into a failure")
		require.NotNil(t, resp.GetAlreadyExists(), "the structured result must survive")
		require.Empty(t, resp.GetEncryptedData())
		require.Equal(t, 1, manager.createCalls)
	})

	t.Run("a success result with no value is still a failure", func(t *testing.T) {
		t.Parallel()
		manager := &gateAccountManager{
			ResourceSyncer: newTestResourceSyncer("service_account"),
			result:         &v2.CreateAccountResponse_SuccessResult{IsCreateAccountResult: true},
		}
		connector, err := NewConnector(context.Background(), newTestConnector([]ResourceSyncer{manager}))
		require.NoError(t, err)

		_, err = connector.CreateAccount(context.Background(), gateCreateAccountRequest(t))
		require.Error(t, err, "a success with no credential would seal no submission and report success")
		require.Equal(t, 1, manager.createCalls,
			"the option asked for a value, so the connector's empty success is refused after the create")
	})

	t.Run("an option that yields no credential is refused before the create", func(t *testing.T) {
		t.Parallel()
		manager := &gateAccountManager{
			ResourceSyncer: newTestResourceSyncer("service_account"),
			result:         &v2.CreateAccountResponse_SuccessResult{IsCreateAccountResult: true},
		}
		connector, err := NewConnector(context.Background(), newTestConnector([]ResourceSyncer{manager}))
		require.NoError(t, err)

		request := gateCreateAccountRequest(t)
		request.SetCredentialOptions(v2.CredentialOptions_builder{
			NoPassword: &v2.CredentialOptions_NoPassword{},
		}.Build())

		_, err = connector.CreateAccount(context.Background(), request)
		require.Error(t, err, "a vault inbox recipient with nothing to deliver is a misconfiguration")
		require.Zero(t, manager.createCalls, "the account must not be created for a refusal we can make up front")
	})

	t.Run("more than one plaintext is still refused", func(t *testing.T) {
		t.Parallel()
		manager := &gateAccountManager{
			ResourceSyncer: newTestResourceSyncer("service_account"),
			result:         &v2.CreateAccountResponse_SuccessResult{IsCreateAccountResult: true},
			plaintexts: []*v2.PlaintextData{
				gateValue("api_key", []byte("v")),
				gateValue("api_key_id", []byte("id")),
			},
		}
		connector, err := NewConnector(context.Background(), newTestConnector([]ResourceSyncer{manager}))
		require.NoError(t, err)

		_, err = connector.CreateAccount(context.Background(), gateCreateAccountRequest(t))
		require.Error(t, err, "two values would seal two envelopes bound to one submission id")
		require.Equal(t, 1, manager.createCalls, "the account manager must not be re-invoked")
	})
}

// gateCredentialManager lets a test observe whether a rotation reached the
// provider, which is the only way to tell a pre-mint refusal from a post-mint one.
type gateCredentialManager struct {
	ResourceSyncer
	rotateCalls int
	plaintexts  []*v2.PlaintextData
}

func (m *gateCredentialManager) Rotate(
	context.Context,
	*v2.ResourceId,
	*v2.LocalCredentialOptions,
) ([]*v2.PlaintextData, annotations.Annotations, error) {
	m.rotateCalls++
	return m.plaintexts, annotations.Annotations{}, nil
}

func (m *gateCredentialManager) RotateCapabilityDetails(context.Context) (*v2.CredentialDetailsCredentialRotation, annotations.Annotations, error) {
	return v2.CredentialDetailsCredentialRotation_builder{}.Build(), annotations.Annotations{}, nil
}

func gateRotateRequest(t *testing.T, options *v2.CredentialOptions) *v2.RotateCredentialRequest {
	t.Helper()
	return v2.RotateCredentialRequest_builder{
		ResourceId:        v2.ResourceId_builder{ResourceType: "service_account", Resource: "sa-1"}.Build(),
		CredentialOptions: options,
		EncryptionConfigs: []*v2.EncryptionConfig{gateConfig(t, nil)},
	}.Build()
}

// TestVaultInboxRotateRefusesBeforeMinting pins the same pre-mint refusal on the
// rotation path. Rotating first is worse than creating first: the prior
// credential may already be invalidated with nothing delivered in its place.
func TestVaultInboxRotateRefusesBeforeMinting(t *testing.T) {
	t.Parallel()

	randomPassword := v2.CredentialOptions_builder{
		RandomPassword: v2.CredentialOptions_RandomPassword_builder{Length: 12}.Build(),
	}.Build()
	noPassword := v2.CredentialOptions_builder{NoPassword: &v2.CredentialOptions_NoPassword{}}.Build()
	// EncryptedPassword carries material the caller already holds, so it never
	// asks the connector to mint a value either.
	encryptedPassword := v2.CredentialOptions_builder{
		EncryptedPassword: v2.CredentialOptions_EncryptedPassword_builder{}.Build(),
	}.Build()

	cases := map[string]*v2.CredentialOptions{
		"no password":        noPassword,
		"sso":                v2.CredentialOptions_builder{Sso: v2.CredentialOptions_SSO_builder{}.Build()}.Build(),
		"encrypted password": encryptedPassword,
	}
	for name, options := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			manager := &gateCredentialManager{ResourceSyncer: newTestResourceSyncer("service_account")}
			connector, err := NewConnector(context.Background(), newTestConnector([]ResourceSyncer{manager}))
			require.NoError(t, err)

			_, err = connector.RotateCredential(context.Background(), gateRotateRequest(t, options))
			require.Error(t, err)
			require.Zero(t, manager.rotateCalls, "the rotation must be refused before the provider is touched")
		})
	}

	// A rotation with no options at all is a supported shape: ConvertCredentialOptions
	// returns (nil, nil) for a nil options pointer and the connector mints its own
	// replacement, which is exactly one value, so the vault-inbox rule must not
	// refuse it. An *empty* options message is a different thing and is already
	// refused by the option conversion.
	t.Run("unset options still rotate", func(t *testing.T) {
		t.Parallel()
		manager := &gateCredentialManager{
			ResourceSyncer: newTestResourceSyncer("service_account"),
			plaintexts:     []*v2.PlaintextData{gateValue("api_key", []byte("v"))},
		}
		connector, err := NewConnector(context.Background(), newTestConnector([]ResourceSyncer{manager}))
		require.NoError(t, err)

		resp, err := connector.RotateCredential(context.Background(), gateRotateRequest(t, nil))
		require.NoError(t, err)
		require.Equal(t, 1, manager.rotateCalls)
		require.Len(t, resp.GetEncryptedData(), 1)
	})

	t.Run("a password-producing option still rotates", func(t *testing.T) {
		t.Parallel()
		manager := &gateCredentialManager{
			ResourceSyncer: newTestResourceSyncer("service_account"),
			plaintexts:     []*v2.PlaintextData{gateValue("api_key", []byte("v"))},
		}
		connector, err := NewConnector(context.Background(), newTestConnector([]ResourceSyncer{manager}))
		require.NoError(t, err)

		resp, err := connector.RotateCredential(context.Background(), gateRotateRequest(t, randomPassword))
		require.NoError(t, err)
		require.Equal(t, 1, manager.rotateCalls)
		require.Len(t, resp.GetEncryptedData(), 1)
		require.Equal(t, vaultinbox.EncryptionProvider, resp.GetEncryptedData()[0].GetProvider())
	})

	// Post-mint on this path: the rotation has happened, so the refusal must be
	// the cardinality rule rather than anything earlier, and the rotation must not
	// be attempted a second time.
	t.Run("two values are refused after exactly one rotation", func(t *testing.T) {
		t.Parallel()
		manager := &gateCredentialManager{
			ResourceSyncer: newTestResourceSyncer("service_account"),
			plaintexts: []*v2.PlaintextData{
				gateValue("api_key", []byte("v")),
				gateValue("api_key_id", []byte("id")),
			},
		}
		connector, err := NewConnector(context.Background(), newTestConnector([]ResourceSyncer{manager}))
		require.NoError(t, err)

		_, err = connector.RotateCredential(context.Background(), gateRotateRequest(t, randomPassword))
		require.ErrorContains(t, err, "exactly one plaintext value",
			"the cardinality rule must be what refuses this, not an earlier check")
		require.Equal(t, 1, manager.rotateCalls, "the rotation must not be retried")
	})
}

// TestVaultInboxGateMatchesProviderNameOnlyConfig pins the provider-name branch of
// IsVaultInboxConfig. Every other fixture in the suite sets both the provider and
// the inner message, so without this case that branch could be deleted with the
// suite still green — and the config would then reach the provider, fail at
// Encrypt, and do so after the rotation had already invalidated the prior
// credential.
func TestVaultInboxGateMatchesProviderNameOnlyConfig(t *testing.T) {
	t.Parallel()
	providerNameOnly := v2.EncryptionConfig_builder{
		Provider: vaultinbox.EncryptionProvider,
	}.Build()

	t.Run("rotate refuses before the provider is touched", func(t *testing.T) {
		t.Parallel()
		manager := &gateCredentialManager{ResourceSyncer: newTestResourceSyncer("service_account")}
		connector, err := NewConnector(context.Background(), newTestConnector([]ResourceSyncer{manager}))
		require.NoError(t, err)

		request := gateRotateRequest(t, v2.CredentialOptions_builder{
			RandomPassword: v2.CredentialOptions_RandomPassword_builder{Length: 12}.Build(),
		}.Build())
		request.SetEncryptionConfigs([]*v2.EncryptionConfig{providerNameOnly})

		_, err = connector.RotateCredential(context.Background(), request)
		require.Error(t, err)
		require.Zero(t, manager.rotateCalls, "the config must be refused before the rotation reaches the provider")
	})

	t.Run("create refuses before the account is created", func(t *testing.T) {
		t.Parallel()
		manager := &gateAccountManager{ResourceSyncer: newTestResourceSyncer("service_account")}
		connector, err := NewConnector(context.Background(), newTestConnector([]ResourceSyncer{manager}))
		require.NoError(t, err)

		request := gateCreateAccountRequest(t)
		request.SetEncryptionConfigs([]*v2.EncryptionConfig{providerNameOnly})

		_, err = connector.CreateAccount(context.Background(), request)
		require.Error(t, err)
		require.Zero(t, manager.createCalls, "the config must be refused before the account is created")
	})
}

func gateValue(name string, value []byte) *v2.PlaintextData {
	return v2.PlaintextData_builder{Name: name, Bytes: value}.Build()
}

func gateRequest(configs []*v2.EncryptionConfig) *v2.IssueCredentialRequest {
	return v2.IssueCredentialRequest_builder{
		IdentityId: v2.ResourceId_builder{ResourceType: "service_account", Resource: "sa-1"}.Build(),
		CredentialOptions: v2.CredentialIssueOptions_builder{
			SecretResourceTypeId: "secret",
			ApiKey:               &v2.CredentialIssueOptions_ApiKey{},
		}.Build(),
		EncryptionConfigs: configs,
		RequestId:         "request-1",
	}.Build()
}

func gateConnector(t *testing.T, issuer *vaultInboxIssuer) *builder {
	t.Helper()
	connector, err := NewConnector(context.Background(), newTestConnector([]ResourceSyncer{issuer, newTestCredentialSecretDeleter()}))
	require.NoError(t, err)
	return connector.(*builder)
}

// TestVaultInboxIssueCredentialMintsOnceAndSeals is the happy path through the
// real builder: one mint, one sealed result, and no plaintext in the response.
func TestVaultInboxIssueCredentialMintsOnceAndSeals(t *testing.T) {
	t.Parallel()
	value := []byte("super-secret-key-material")
	issuer := newVaultInboxIssuer(
		[]*v2.PlaintextData{gateValue("api_key", value)},
		[]v2.VaultInboxSuite{v2.VaultInboxSuite_VAULT_INBOX_SUITE_XWING_MLKEM768_X25519_HKDF_SHA256_CHACHA20POLY1305_V1},
	)

	resp, err := gateConnector(t, issuer).IssueCredential(context.Background(), gateRequest([]*v2.EncryptionConfig{gateConfig(t, nil)}))
	require.NoError(t, err)
	require.Equal(t, 1, issuer.issueCalls, "the provider must be minted exactly once")
	require.Len(t, resp.GetEncryptedData(), 1)
	require.Equal(t, vaultinbox.EncryptionProvider, resp.GetEncryptedData()[0].GetProvider())
	require.Equal(t, []string{"inbox-key-1"}, resp.GetEncryptedData()[0].GetKeyIds())
	require.NotEmpty(t, resp.GetEncryptedData()[0].GetEncryptedBytes())

	// The response carries ciphertext, never the minted value.
	require.NotContains(t, string(resp.GetEncryptedData()[0].GetEncryptedBytes()), string(value))
	require.NotContains(t, resp.String(), string(value), "no plaintext may appear anywhere in the response")
	require.Equal(t, 1, issuer.issueCalls, "a successful issuance must not mint a second credential")
}

// TestVaultInboxIssueCredentialRefusesBeforeMinting pins the boundary between
// the pre-mint gates and the mint itself: each of these configs must be refused
// with the provider untouched.
//
// Regression sensitivity: with the exclusivity, advertisement, or config gates
// removed, the corresponding case would reach the provider and the call count
// would be 1 instead of 0, so this test fails rather than silently passing.
func TestVaultInboxIssueCredentialRefusesBeforeMinting(t *testing.T) {
	t.Parallel()
	advertised := []v2.VaultInboxSuite{v2.VaultInboxSuite_VAULT_INBOX_SUITE_XWING_MLKEM768_X25519_HKDF_SHA256_CHACHA20POLY1305_V1}
	noProfiles := []v2.VaultInboxSuite{}

	cases := map[string]struct {
		configs  []*v2.EncryptionConfig
		profiles []v2.VaultInboxSuite
	}{
		"unknown config version": {configs: []*v2.EncryptionConfig{gateConfig(t, func(c *v2.VaultInboxRecipientConfig) {
			c.ConfigVersion = v2.VaultInboxConfigVersion_VAULT_INBOX_CONFIG_VERSION_UNSPECIFIED
		})}, profiles: advertised},
		"unknown suite": {configs: []*v2.EncryptionConfig{gateConfig(t, func(c *v2.VaultInboxRecipientConfig) {
			c.Suite = v2.VaultInboxSuite_VAULT_INBOX_SUITE_UNSPECIFIED
		})}, profiles: advertised},
		"unsupported payload scheme": {configs: []*v2.EncryptionConfig{gateConfig(t, func(c *v2.VaultInboxRecipientConfig) {
			c.PayloadScheme = "latchkey.vault_submission.secret.v2"
		})}, profiles: advertised},
		"unknown inner field": {configs: []*v2.EncryptionConfig{gateConfig(t, func(c *v2.VaultInboxRecipientConfig) {
			c.ProtoReflect().SetUnknown([]byte{0x80, 0x7c, 0x01})
		})}, profiles: advertised},
		"mismatched thumbprint": {configs: []*v2.EncryptionConfig{gateConfig(t, func(c *v2.VaultInboxRecipientConfig) {
			c.PublicKeyThumbprint = "not-the-thumbprint"
		})}, profiles: advertised},
		"unadvertised profile": {configs: []*v2.EncryptionConfig{gateConfig(t, nil)}, profiles: noProfiles},
		// A real age recipient, so the age validator accepts it and
		// validateVaultInboxConfigExclusivity is the gate that refuses.
		"mixed recipient configs": {configs: []*v2.EncryptionConfig{gateConfig(t, nil), validAgeConfig(t)}, profiles: advertised},
		"duplicate recipient configs": {configs: []*v2.EncryptionConfig{
			gateConfig(t, nil), gateConfig(t, nil),
		}, profiles: advertised},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			issuer := newVaultInboxIssuer([]*v2.PlaintextData{gateValue("api_key", []byte("v"))}, tc.profiles)
			resp, err := gateConnector(t, issuer).IssueCredential(context.Background(), gateRequest(tc.configs))
			require.Error(t, err)
			require.Nil(t, resp)
			require.Zero(t, issuer.issueCalls, "a refused config must not mint anything")
		})
	}
}

// TestVaultInboxIssueCredentialFailsAfterOneMint pins the post-mint half: the
// provider has already minted, so the failure is a refusal to return a usable
// result, and the SDK must not mint again to recover.
//
// Regression sensitivity: without the cardinality and value gates these cases
// would return a successful response with zero or several envelopes (or an
// oversized one), so the assertions on the error and the call count fail.
func TestVaultInboxIssueCredentialFailsAfterOneMint(t *testing.T) {
	t.Parallel()
	advertised := []v2.VaultInboxSuite{v2.VaultInboxSuite_VAULT_INBOX_SUITE_XWING_MLKEM768_X25519_HKDF_SHA256_CHACHA20POLY1305_V1}

	cases := map[string][]*v2.PlaintextData{
		"zero values": {},
		"multiple values": {
			gateValue("api_key", []byte("v")),
			gateValue("api_key_id", []byte("id")),
		},
		"unusable value": {
			v2.PlaintextData_builder{Bytes: []byte("v")}.Build(),
		},
		"oversized value": {
			gateValue("api_key", bytes.Repeat([]byte("a"), vaultinbox.MaxPlaintextBytes+1)),
		},
	}

	for name, values := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			issuer := newVaultInboxIssuer(values, advertised)
			resp, err := gateConnector(t, issuer).IssueCredential(context.Background(), gateRequest([]*v2.EncryptionConfig{gateConfig(t, nil)}))
			require.Error(t, err, "the issuance must not report success")
			require.Nil(t, resp, "no partial result may be returned")
			require.Equal(t, 1, issuer.issueCalls, "a post-mint failure must not trigger a second mint")
		})
	}
}
