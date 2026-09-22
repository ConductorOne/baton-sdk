package connectorbuilder

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/vaultinbox"
	"github.com/stretchr/testify/require"
)

func vaultInboxTestConfig() *v2.EncryptionConfig {
	return v2.EncryptionConfig_builder{
		Provider: vaultinbox.EncryptionProvider,
		VaultInboxRecipientConfig: v2.VaultInboxRecipientConfig_builder{
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

// TestVaultInboxIssuanceRequiresExactlyOnePlaintext covers the reconciliation
// rule: a vault-inbox recipient carries the whole submission payload, so a
// connector returning zero or several values fails the issuance instead of
// producing a partial or mislabeled submission.
func TestVaultInboxIssuanceRequiresExactlyOnePlaintext(t *testing.T) {
	configs := []*v2.EncryptionConfig{vaultInboxTestConfig()}

	require.NoError(t, validateVaultInboxPlaintextCardinality(configs, plaintexts(1)))
	require.Error(t, validateVaultInboxPlaintextCardinality(configs, plaintexts(0)))
	require.Error(t, validateVaultInboxPlaintextCardinality(configs, plaintexts(2)))
	// Without a vault-inbox recipient any cardinality stays legal.
	require.NoError(t, validateVaultInboxPlaintextCardinality(nil, plaintexts(2)))
}
