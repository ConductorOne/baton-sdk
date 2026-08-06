package age

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
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
			require.Empty(t, encrypted.GetKeyId())

			// The provider derives key_ids via the exported helper, so the
			// convention is enforced by shared code rather than prose.
			require.Equal(t, []string{KeyIDForRecipient(recipientText)}, encrypted.GetKeyIds())
			// Independently confirm the helper computes hex(sha256(recipient)).
			keyID := sha256.Sum256([]byte(recipientText))
			require.Equal(t, hex.EncodeToString(keyID[:]), KeyIDForRecipient(recipientText))

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

// TestKeyIDForRecipientGolden pins the deterministic key-ID derivation against a
// fixed vector. Age encryption is randomized, so ciphertext cannot be pinned,
// but the key-ID derivation is deterministic and must not drift.
func TestKeyIDForRecipientGolden(t *testing.T) {
	const (
		recipient = "age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"
		want      = "cf7e5682313150685bf16e42c4ea48e168e16e8206c5c4ad37c72d013cb6a5f4"
	)
	require.Equal(t, want, KeyIDForRecipient(recipient))
}

// ageFixture mirrors testdata/fixture.json: a checked-in ciphertext plus the
// throwaway identity that decrypts it. Consumers can vendor the same artifact to
// exercise their decrypt path against a stable input.
type ageFixture struct {
	Identity  string `json:"identity"`
	Recipient string `json:"recipient"`
	Plaintext string `json:"plaintext"`
	KeyID     string `json:"key_id"`
}

// TestDecryptFixture verifies the checked-in ciphertext decrypts to the expected
// plaintext and that its recorded key ID matches KeyIDForRecipient. This is the
// contract a downstream consumer's decrypt test should mirror.
func TestDecryptFixture(t *testing.T) {
	metaBytes, err := os.ReadFile(filepath.Join("testdata", "fixture.json"))
	require.NoError(t, err)
	var fixture ageFixture
	require.NoError(t, json.Unmarshal(metaBytes, &fixture))

	require.Equal(t, fixture.KeyID, KeyIDForRecipient(fixture.Recipient))

	identity, err := filippoage.ParseX25519Identity(fixture.Identity)
	require.NoError(t, err)
	require.Equal(t, fixture.Recipient, identity.Recipient().String())

	ciphertext, err := os.ReadFile(filepath.Join("testdata", "fixture.age"))
	require.NoError(t, err)
	reader, err := filippoage.Decrypt(bytes.NewReader(ciphertext), identity)
	require.NoError(t, err)
	var decrypted bytes.Buffer
	_, err = decrypted.ReadFrom(reader)
	require.NoError(t, err)
	require.Equal(t, fixture.Plaintext, decrypted.String())
}
