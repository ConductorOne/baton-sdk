package crypto //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
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
	jwk := `{"kty":"AKP","alg":"` + alg + `","pub":"` + pub + `"}`
	canonical := `{"alg":"` + alg + `","kty":"AKP","pub":"` + pub + `"}`
	digest := sha256.Sum256([]byte(canonical))
	return v2.EncryptionConfig_builder{
		Provider: vaultinbox.EncryptionProvider,
		VaultInboxRecipientConfig: v2.VaultInboxRecipientConfig_builder{
			ConfigVersion:       v2.VaultInboxConfigVersion_VAULT_INBOX_CONFIG_VERSION_V1,
			Suite:               v2.VaultInboxSuite_VAULT_INBOX_SUITE_XWING_MLKEM768_X25519_HKDF_SHA256_CHACHA20POLY1305_V1,
			TenantId:            "tenant-1",
			VaultBoundaryId:     "vault-1",
			InboxKeyId:          "inbox-key-1",
			KeyGeneration:       1,
			PayloadScheme:       "latchkey.vault_submission.secret.v1",
			SubmissionId:        "submission-1",
			PublicJwkJson:       jwk,
			PublicKeyThumbprint: base64.RawURLEncoding.EncodeToString(digest[:]),
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
