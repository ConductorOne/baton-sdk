package connectorbuilder

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/crypto"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/vaultinbox"
	"github.com/stretchr/testify/require"
)

func vaultInboxTestConfig() *v2.EncryptionConfig {
	return v2.EncryptionConfig_builder{
		Provider: vaultinbox.EncryptionProvider,
		VaultInboxRecipientConfig: v2.VaultInboxRecipientConfig_builder{
			Suite:        v2.VaultInboxSuite_VAULT_INBOX_SUITE_XWING_MLKEM768_X25519_HKDF_SHA256_CHACHA20POLY1305_V1,
			SubmissionId: "submission-1",
		}.Build(),
	}.Build()
}

func plaintexts(n int) []*v2.PlaintextData {
	out := make([]*v2.PlaintextData, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, v2.PlaintextData_builder{Name: "api_key", Bytes: []byte("v")}.Build())
	}
	return out
}

// TestVaultInboxProfileMustBeAdvertised pins the capability gate: a descriptor
// that does not list the requested profile must not be sealed to, so the
// advertisement is a contract rather than documentation.
func TestVaultInboxProfileMustBeAdvertised(t *testing.T) {
	configs := []*v2.EncryptionConfig{vaultInboxTestConfig()}

	advertised := v2.CredentialIssueOptionDescriptor_builder{
		VaultInboxProfiles: []v2.VaultInboxSuite{
			v2.VaultInboxSuite_VAULT_INBOX_SUITE_XWING_MLKEM768_X25519_HKDF_SHA256_CHACHA20POLY1305_V1,
		},
	}.Build()
	require.NoError(t, validateVaultInboxProfileAdvertised(configs, advertised))
	require.Error(t, validateVaultInboxProfileAdvertised(configs, &v2.CredentialIssueOptionDescriptor{}))
	// A descriptor for another profile does not cover this one.
	require.Error(t, validateVaultInboxProfileAdvertised(configs, v2.CredentialIssueOptionDescriptor_builder{
		VaultInboxProfiles: []v2.VaultInboxSuite{v2.VaultInboxSuite_VAULT_INBOX_SUITE_UNSPECIFIED},
	}.Build()))
	// Non-vault-inbox configs are unaffected.
	require.NoError(t, validateVaultInboxProfileAdvertised(nil, &v2.CredentialIssueOptionDescriptor{}))
}

// TestVaultInboxIssuanceRequiresExactlyOnePlaintext covers the reconciliation
// rule: a vault-inbox recipient carries the whole submission payload, so a
// connector returning zero or several values fails the issuance instead of
// producing a partial or mislabeled submission.
func TestVaultInboxIssuanceRequiresExactlyOnePlaintext(t *testing.T) {
	configs := []*v2.EncryptionConfig{vaultInboxTestConfig()}

	require.NoError(t, crypto.ValidateVaultInboxPlaintextCardinality(configs, plaintexts(1)))
	require.Error(t, crypto.ValidateVaultInboxPlaintextCardinality(configs, plaintexts(0)))
	require.Error(t, crypto.ValidateVaultInboxPlaintextCardinality(configs, plaintexts(2)))
	// Without a vault-inbox recipient any cardinality stays legal.
	require.NoError(t, crypto.ValidateVaultInboxPlaintextCardinality(nil, plaintexts(2)))
}
