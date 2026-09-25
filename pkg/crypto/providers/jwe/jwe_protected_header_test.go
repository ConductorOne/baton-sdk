package jwe

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// independentProtectedHeader renders the protected header from the published
// wire contract without calling the provider, so the tests measure the exact
// bytes a consuming decoder receives.
func independentProtectedHeader(t *testing.T, keyID string) []byte {
	t.Helper()
	encoded, err := json.Marshal(struct {
		Algorithm string `json:"alg"`
		KeyID     string `json:"kid"`
	}{Algorithm: Algorithm, KeyID: keyID})
	require.NoError(t, err)
	return encoded
}

// keyIDForSerializedSize returns a key id whose protected header serializes to
// exactly size bytes. json.Marshal escapes '<' to \u003c, six bytes on the wire
// per character, so a key id inside the 1024-byte limit reaches serialized
// sizes that a plain key id cannot.
func keyIDForSerializedSize(t *testing.T, size int) string {
	t.Helper()
	need := size - len(independentProtectedHeader(t, ""))
	require.GreaterOrEqual(t, need, 0, "size %d is below the empty-header overhead", size)

	escaped := need / 6
	keyID := strings.Repeat("<", escaped) + strings.Repeat("k", need-escaped*6)
	require.LessOrEqual(t, len(keyID), 1024, "key id must stay inside the key-id limit")
	require.Equal(t, size, len(independentProtectedHeader(t, keyID)))
	return keyID
}

func configWithKeyID(t *testing.T, keyID string) *v2.EncryptionConfig {
	t.Helper()
	return configForRawJWK(keyID, customJWK(t, freshXWingPublicKey(t), nil), nil)
}

func encryptPayload(t *testing.T, config *v2.EncryptionConfig) (*v2.EncryptedData, error) {
	t.Helper()
	return (&Provider{}).Encrypt(context.Background(), config, v2.PlaintextData_builder{Name: "m", Bytes: []byte("payload")}.Build())
}

// TestProtectedHeaderSizeLimit pins the decoded protected-header limit the
// consuming reader enforces. The cap applies to the serialized JSON, not to the
// key id's own length, so the boundary is measured in wire bytes.
func TestProtectedHeaderSizeLimit(t *testing.T) {
	cases := []struct {
		name    string
		size    int
		wantErr bool
	}{
		{"one byte under the limit", MaxProtectedHeaderBytes - 1, false},
		{"exactly at the limit", MaxProtectedHeaderBytes, false},
		{"one byte over the limit", MaxProtectedHeaderBytes + 1, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			keyID := keyIDForSerializedSize(t, tc.size)
			config := configWithKeyID(t, keyID)

			validationErr := (&Provider{}).ValidateConfig(context.Background(), config)
			encrypted, encryptErr := encryptPayload(t, config)

			if tc.wantErr {
				requireInvalidArgument(t, validationErr)
				requireInvalidArgument(t, encryptErr)
				require.Nil(t, encrypted)
				return
			}
			require.NoError(t, validationErr)
			require.NoError(t, encryptErr)
			require.Equal(t,
				independentProtectedHeader(t, keyID),
				decodeRawURL(t, decodeJWE(t, encrypted.GetEncryptedBytes()).Protected),
				"the wire protected header must be the serialized header the limit is measured on")
		})
	}
}

// TestProtectedHeaderSizeLimitKeyIDCases covers the caller-visible pair: an
// ordinary key id at the key-id limit passes, and a key id that escapes past
// the header limit is refused even though its raw length is legal.
func TestProtectedHeaderSizeLimitKeyIDCases(t *testing.T) {
	t.Run("ordinary key id at the key id limit is accepted", func(t *testing.T) {
		keyID := strings.Repeat("k", 1024)
		require.Greater(t, len(keyID), 1024-1)
		require.LessOrEqual(t, len(independentProtectedHeader(t, keyID)), MaxProtectedHeaderBytes)

		config := configWithKeyID(t, keyID)
		require.NoError(t, (&Provider{}).ValidateConfig(context.Background(), config))

		encrypted, err := encryptPayload(t, config)
		require.NoError(t, err)
		require.Equal(t, independentProtectedHeader(t, keyID),
			decodeRawURL(t, decodeJWE(t, encrypted.GetEncryptedBytes()).Protected))
	})

	t.Run("escaped key id within the key id limit is refused", func(t *testing.T) {
		keyID := strings.Repeat("<", 1024)
		require.Len(t, keyID, 1024)
		require.Greater(t, len(independentProtectedHeader(t, keyID)), MaxProtectedHeaderBytes)

		config := configWithKeyID(t, keyID)
		requireInvalidArgument(t, (&Provider{}).ValidateConfig(context.Background(), config))

		encrypted, err := encryptPayload(t, config)
		requireInvalidArgument(t, err)
		require.Nil(t, encrypted)
	})
}

// TestProtectedHeaderLimitErrorsDoNotEchoKeyID checks the failure carries no
// part of the rejected key id, which is caller-supplied input.
func TestProtectedHeaderLimitErrorsDoNotEchoKeyID(t *testing.T) {
	keyID := strings.Repeat("<", 1024)
	config := configWithKeyID(t, keyID)

	validationErr := (&Provider{}).ValidateConfig(context.Background(), config)
	requireInvalidArgument(t, validationErr)
	require.NotContains(t, validationErr.Error(), "<")

	_, encryptErr := encryptPayload(t, config)
	requireInvalidArgument(t, encryptErr)
	require.NotContains(t, encryptErr.Error(), "<")
}
