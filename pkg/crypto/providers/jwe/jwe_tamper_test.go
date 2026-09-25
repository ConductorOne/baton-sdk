package jwe

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"filippo.io/hpke"
	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func flipDecodedByte(t *testing.T, encoded string, index int) string {
	t.Helper()
	decoded := decodeRawURL(t, encoded)
	require.Greater(t, len(decoded), index)
	decoded[index] ^= 0x01
	return base64.RawURLEncoding.EncodeToString(decoded)
}

func retargetProtectedHeader(t *testing.T, encoded string, member string, value string) string {
	t.Helper()
	var header map[string]any
	require.NoError(t, json.Unmarshal(decodeRawURL(t, encoded), &header))
	header[member] = value
	updated, err := json.Marshal(header)
	require.NoError(t, err)
	return base64.RawURLEncoding.EncodeToString(updated)
}

// TestIndependentReaderAuthenticatesEverySealedField drives the tamper matrix
// for J7. The independent reader must reject a message whose encapsulation,
// ciphertext, authentication tag, protected header or external context was
// changed after sealing.
//
// JWE iv and tag are deliberately not mutated here: the profile requires both
// to be empty and the HPKE tag lives inside ciphertext, so neither is an input
// the reader authenticates. A reader that treated them as ciphertext would
// disagree with this wire format, not detect tampering.
func TestIndependentReaderAuthenticatesEverySealedField(t *testing.T) {
	privateKey, err := hpke.MLKEM768X25519().GenerateKey()
	require.NoError(t, err)
	recipient := testRecipient{
		config: v2.EncryptionConfig_builder{
			Provider: EncryptionProvider,
			KeyId:    "recipient-1",
			JwkPublicKeyConfig: v2.EncryptionConfig_JWKPublicKeyConfig_builder{
				PubKey:                      akpJWK(t, xwingJWKMembers(t, privateKey.PublicKey().Bytes())),
				AdditionalAuthenticatedData: []byte("tenant=acme"),
			}.Build(),
		}.Build(),
		privateKey: privateKey,
	}
	plaintext := []byte("super-secret-key-material")
	message := encryptFor(t, recipient, v2.PlaintextData_builder{Name: "m", Bytes: plaintext}.Build()).GetEncryptedBytes()

	// Positive control: the unmutated message reads back. Without this the
	// matrix below would pass on a reader that rejects everything.
	require.Equal(t, plaintext, mustIndependentOpen(t, message, recipient.privateKey))

	mutations := map[string]func(t *testing.T, envelope *FlattenedJWE){
		"encapsulation first byte": func(t *testing.T, envelope *FlattenedJWE) {
			envelope.EncryptedKey = flipDecodedByte(t, envelope.EncryptedKey, 0)
		},
		"encapsulation last byte": func(t *testing.T, envelope *FlattenedJWE) {
			envelope.EncryptedKey = flipDecodedByte(t, envelope.EncryptedKey, xwingEncapsulatedKeyBytes-1)
		},
		"encapsulation truncated": func(t *testing.T, envelope *FlattenedJWE) {
			encapsulatedKey := decodeRawURL(t, envelope.EncryptedKey)
			envelope.EncryptedKey = base64.RawURLEncoding.EncodeToString(encapsulatedKey[:len(encapsulatedKey)-1])
		},
		"ciphertext first byte": func(t *testing.T, envelope *FlattenedJWE) {
			envelope.Ciphertext = flipDecodedByte(t, envelope.Ciphertext, 0)
		},
		"authentication tag byte": func(t *testing.T, envelope *FlattenedJWE) {
			envelope.Ciphertext = flipDecodedByte(t, envelope.Ciphertext, len(decodeRawURL(t, envelope.Ciphertext))-1)
		},
		"ciphertext truncated": func(t *testing.T, envelope *FlattenedJWE) {
			ciphertext := decodeRawURL(t, envelope.Ciphertext)
			envelope.Ciphertext = base64.RawURLEncoding.EncodeToString(ciphertext[:len(ciphertext)-chacha20Poly1305TagBytes])
		},
		"protected header alg": func(t *testing.T, envelope *FlattenedJWE) {
			envelope.Protected = retargetProtectedHeader(t, envelope.Protected, "alg", "RSA-OAEP-256")
		},
		"protected header kid": func(t *testing.T, envelope *FlattenedJWE) {
			envelope.Protected = retargetProtectedHeader(t, envelope.Protected, "kid", "recipient-2")
		},
		"protected header added member": func(t *testing.T, envelope *FlattenedJWE) {
			envelope.Protected = retargetProtectedHeader(t, envelope.Protected, "enc", "A256GCM")
		},
		"external context changed": func(t *testing.T, envelope *FlattenedJWE) {
			envelope.AAD = base64.RawURLEncoding.EncodeToString([]byte("tenant=other"))
		},
		"external context appended": func(t *testing.T, envelope *FlattenedJWE) {
			envelope.AAD = base64.RawURLEncoding.EncodeToString(append(decodeRawURL(t, envelope.AAD), 'x'))
		},
		"external context emptied": func(t *testing.T, envelope *FlattenedJWE) {
			envelope.AAD = ""
		},
		"encrypted_key malformed base64url": func(t *testing.T, envelope *FlattenedJWE) {
			envelope.EncryptedKey = envelope.EncryptedKey[:len(envelope.EncryptedKey)-1] + "+"
		},
		"ciphertext malformed base64url": func(t *testing.T, envelope *FlattenedJWE) {
			envelope.Ciphertext = envelope.Ciphertext[:len(envelope.Ciphertext)-1] + "="
		},
	}

	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			envelope := decodeJWE(t, message)
			mutate(t, &envelope)
			_, err := independentOpen(marshalJWE(t, envelope), recipient.privateKey, nil)
			require.Error(t, err, "reader must reject a message with a changed %s", name)
		})
	}
}

// TestIndependentReaderAcceptsAnyProtectedHeaderMemberOrder shows member order is
// not a wire invariant. The HPKE additional data is the protected string as
// transmitted, so a message sealed with the members in a different order reads
// back correctly. What a reader rejects is a header changed without resealing.
//
// Sealing happens here rather than through the provider, so this does not depend
// on the provider's own member order.
func TestIndependentReaderAcceptsAnyProtectedHeaderMemberOrder(t *testing.T) {
	privateKey, err := hpke.MLKEM768X25519().GenerateKey()
	require.NoError(t, err)
	aad := []byte("tenant=acme")
	plaintext := []byte("super-secret-key-material")

	// kid before alg: the reverse of the order the provider emits.
	encodedHeader := base64.RawURLEncoding.EncodeToString([]byte(`{"kid":"recipient-1","alg":"` + Algorithm + `"}`))
	encodedAAD := base64.RawURLEncoding.EncodeToString(aad)

	encapsulatedKey, sender, err := hpke.NewSender(privateKey.PublicKey(), hpke.HKDFSHA256(), hpke.ChaCha20Poly1305(), nil)
	require.NoError(t, err)
	ciphertext, err := sender.Seal([]byte(encodedHeader+"."+encodedAAD), plaintext)
	require.NoError(t, err)
	message, err := json.Marshal(FlattenedJWE{
		Protected:    encodedHeader,
		EncryptedKey: base64.RawURLEncoding.EncodeToString(encapsulatedKey),
		AAD:          encodedAAD,
		Ciphertext:   base64.RawURLEncoding.EncodeToString(ciphertext),
	})
	require.NoError(t, err)

	recovered, err := independentOpen(message, privateKey, nil)
	require.NoError(t, err)
	require.Equal(t, plaintext, recovered)

	// Relabelling the transmitted header, without resealing, is rejected.
	envelope := decodeJWE(t, message)
	envelope.Protected = retargetProtectedHeader(t, envelope.Protected, "kid", "recipient-2")
	_, err = independentOpen(marshalJWE(t, envelope), privateKey, nil)
	require.Error(t, err)
}

// TestIndependentReaderRejectsWrongPrivateKey covers the recipient-key
// dimension: a well-formed message read with a different X-Wing key must fail.
// ML-KEM decapsulation uses implicit rejection, so the failure may surface at
// context setup or at Open; either is a rejection.
func TestIndependentReaderRejectsWrongPrivateKey(t *testing.T) {
	recipient := newTestRecipient(t, "recipient-1", []byte("tenant=acme"))
	plaintext := []byte("super-secret-key-material")
	message := encryptFor(t, recipient, v2.PlaintextData_builder{Name: "m", Bytes: plaintext}.Build()).GetEncryptedBytes()

	require.Equal(t, plaintext, mustIndependentOpen(t, message, recipient.privateKey))

	wrongPrivateKey, err := hpke.MLKEM768X25519().GenerateKey()
	require.NoError(t, err)
	_, err = independentOpen(message, wrongPrivateKey, nil)
	require.Error(t, err)
}
