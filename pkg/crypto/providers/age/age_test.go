package age

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	filippoage "filippo.io/age"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
)

func TestRecipientEncryptionProviderInteroperability(t *testing.T) {
	tests := map[string]func(t *testing.T) (filippoage.Identity, filippoage.Recipient){
		"x25519": func(t *testing.T) (filippoage.Identity, filippoage.Recipient) {
			identity, err := filippoage.GenerateX25519Identity()
			require.NoError(t, err)
			return identity, identity.Recipient()
		},
		"hybrid_post_quantum": func(t *testing.T) (filippoage.Identity, filippoage.Recipient) {
			identity, err := filippoage.GenerateHybridIdentity()
			require.NoError(t, err)
			return identity, identity.Recipient()
		},
	}

	for name, makeKeyPair := range tests {
		t.Run(name, func(t *testing.T) {
			identity, recipient := makeKeyPair(t)
			recipientText := recipient.(interface{ String() string }).String()
			plaintext := []byte("connector-created-credential")
			input := v2.PlaintextData_builder{
				Name:        "api-key",
				Description: "created by a connector",
				Schema:      "opaque",
				Bytes:       plaintext,
			}.Build()
			config := v2.EncryptionConfig_builder{
				AgeRecipientConfig: v2.EncryptionConfig_AgeRecipientConfig_builder{
					Recipient: recipientText,
				}.Build(),
			}.Build()

			encrypted, err := (&RecipientEncryptionProvider{}).Encrypt(context.Background(), config, input)
			require.NoError(t, err)
			require.Equal(t, EncryptionProviderAge, encrypted.GetProvider())
			require.Equal(t, input.GetName(), encrypted.GetName())
			require.Equal(t, input.GetDescription(), encrypted.GetDescription())
			require.Equal(t, input.GetSchema(), encrypted.GetSchema())
			require.NotEqual(t, plaintext, encrypted.GetEncryptedBytes())

			keyID := sha256.Sum256([]byte(recipientText))
			require.Equal(t, []string{hex.EncodeToString(keyID[:])}, encrypted.GetKeyIds())

			reader, err := filippoage.Decrypt(bytes.NewReader(encrypted.GetEncryptedBytes()), identity)
			require.NoError(t, err)
			var decrypted bytes.Buffer
			_, err = decrypted.ReadFrom(reader)
			require.NoError(t, err)
			require.Equal(t, plaintext, decrypted.Bytes())
		})
	}
}

func TestRecipientEncryptionProviderRejectsInvalidConfig(t *testing.T) {
	identity, err := filippoage.GenerateX25519Identity()
	require.NoError(t, err)

	tests := map[string]*v2.EncryptionConfig{
		"nil config":         nil,
		"missing age config": v2.EncryptionConfig_builder{}.Build(),
		"empty recipient": v2.EncryptionConfig_builder{
			AgeRecipientConfig: v2.EncryptionConfig_AgeRecipientConfig_builder{}.Build(),
		}.Build(),
		"private identity": v2.EncryptionConfig_builder{
			AgeRecipientConfig: v2.EncryptionConfig_AgeRecipientConfig_builder{Recipient: identity.String()}.Build(),
		}.Build(),
		"surrounding whitespace": v2.EncryptionConfig_builder{
			AgeRecipientConfig: v2.EncryptionConfig_AgeRecipientConfig_builder{Recipient: " " + identity.Recipient().String()}.Build(),
		}.Build(),
		"multiple recipients": v2.EncryptionConfig_builder{
			AgeRecipientConfig: v2.EncryptionConfig_AgeRecipientConfig_builder{
				Recipient: strings.Join([]string{identity.Recipient().String(), identity.Recipient().String()}, "\n"),
			}.Build(),
		}.Build(),
	}

	for name, config := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := (&RecipientEncryptionProvider{}).Encrypt(context.Background(), config, &v2.PlaintextData{})
			require.Error(t, err)
			require.NotContains(t, err.Error(), identity.String())
		})
	}
}
