package crypto //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"strconv"
	"testing"

	filippoage "filippo.io/age"
	"filippo.io/hpke"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/vaultinbox"
	"github.com/stretchr/testify/require"
)

// validVaultInboxConfig builds one structurally valid vault-inbox recipient so
// the exclusivity rule is exercised after validation, not instead of it.
func validVaultInboxConfig(t *testing.T) *v2.EncryptionConfig {
	t.Helper()
	key, err := hpke.MLKEM768X25519().NewPrivateKey(bytes.Repeat([]byte{0x11}, 32))
	require.NoError(t, err)
	pub := base64.RawURLEncoding.EncodeToString(key.PublicKey().Bytes())
	const alg = "HPKE-Base-X-Wing-Draft06-HKDF-SHA256-ChaCha20Poly1305"
	digest := sha256.Sum256([]byte(`{"alg":"` + alg + `","kty":"AKP","pub":"` + pub + `"}`))
	thumbprint := base64.RawURLEncoding.EncodeToString(digest[:])
	jwk := `{"kty":"AKP","alg":"` + alg + `","pub":"` + pub + `","` + vaultinbox.JWKExtensionMember + `":{` +
		`"version":` + strconv.Itoa(vaultinbox.JWKExtensionVersion) + `,"suite":"` + alg + `",` +
		`"tenant_id":"tenant-1","vault_boundary_id":"vault-1","key_generation":1,` +
		`"payload_scheme":"` + vaultinbox.PayloadSchemeSecretV1 + `","submission_id":"submission-1",` +
		`"public_key_thumbprint":"` + thumbprint + `"}}`
	return v2.EncryptionConfig_builder{
		Provider: vaultinbox.EncryptionProvider,
		KeyId:    "inbox-key-1",
		JwkPublicKeyConfig: v2.EncryptionConfig_JWKPublicKeyConfig_builder{
			PubKey: []byte(jwk),
		}.Build(),
	}.Build()
}

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

// TestExtendedJWKWithoutTheProviderDoesNotDowngrade pins C5. A recipient JWK that
// carries the vault-inbox extension but does not name the provider is not inbox
// mode, so it is routed to the classical JWK provider, which must refuse the AKP
// X-Wing key rather than seal it under a profile the caller did not select.
func TestExtendedJWKWithoutTheProviderDoesNotDowngrade(t *testing.T) {
	config := validVaultInboxConfig(t)
	config.SetProvider("")

	manager, err := NewEncryptionManager(nil, []*v2.EncryptionConfig{config})
	require.NoError(t, err, "the exclusivity rule does not apply to an unrecognized config")
	encrypted, err := manager.Encrypt(context.Background(), v2.PlaintextData_builder{
		Name:  "api_key",
		Bytes: []byte("v"),
	}.Build())
	require.Error(t, err, "an extended JWK without the provider must not be sealed under another profile")
	require.Empty(t, encrypted)
}

// TestVaultInboxRecipientMustBeTheOnlyRecipient pins the rule that the
// vault-inbox profile cannot be fanned out beside another recipient: the
// ciphertext it produces is the entire submission payload.
func TestVaultInboxRecipientMustBeTheOnlyRecipient(t *testing.T) {
	require.NoError(t, ValidateEncryptionConfigs([]*v2.EncryptionConfig{validVaultInboxConfig(t)}))
	require.Error(t, ValidateEncryptionConfigs([]*v2.EncryptionConfig{
		validVaultInboxConfig(t), validAgeConfig(t),
	}))
	require.Error(t, ValidateEncryptionConfigs([]*v2.EncryptionConfig{
		validVaultInboxConfig(t), validVaultInboxConfig(t),
	}))
	// A list without a vault-inbox recipient keeps its existing behavior.
	require.NoError(t, ValidateEncryptionConfigs([]*v2.EncryptionConfig{validAgeConfig(t)}))
	require.Error(t, ValidateEncryptionConfigs([]*v2.EncryptionConfig{validAgeConfig(t), nil}))

	// The manager constructor enforces the same rule, because RotateCredential
	// and CreateAccount build a manager without ever calling
	// ValidateEncryptionConfigs.
	_, err := NewEncryptionManager(nil, []*v2.EncryptionConfig{validVaultInboxConfig(t), validAgeConfig(t)})
	require.Error(t, err)
	_, err = NewEncryptionManager(nil, []*v2.EncryptionConfig{validVaultInboxConfig(t)})
	require.NoError(t, err)
	_, err = NewEncryptionManager(nil, []*v2.EncryptionConfig{validAgeConfig(t)})
	require.NoError(t, err)
}
