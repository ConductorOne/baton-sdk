package crypto //nolint:revive,nolintlint // Matches the package under test.

import (
	"context"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/fullknowledge"
	"github.com/stretchr/testify/require"
)

func TestCreateAndRotateRejectFullKnowledgeBeforeOptionsConversion(t *testing.T) {
	for _, conf := range []*v2.EncryptionConfig{
		v2.EncryptionConfig_builder{Provider: fullknowledge.EncryptionProvider}.Build(),
		v2.EncryptionConfig_builder{Provider: " BATON/FULL-KNOWLEDGE-VAULT/V1 "}.Build(),
		v2.EncryptionConfig_builder{FullKnowledgeVaultConfig: &v2.FullKnowledgeVaultConfig{}}.Build(),
	} {
		opts, err := ConvertCredentialOptions(context.Background(), nil, nil, []*v2.EncryptionConfig{conf})
		require.ErrorContains(t, err, "supported only for credential issuance")
		require.Nil(t, opts)
	}
}
