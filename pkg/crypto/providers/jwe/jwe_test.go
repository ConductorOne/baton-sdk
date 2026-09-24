package jwe

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"sort"
	"strings"
	"testing"

	"filippo.io/hpke"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

const (
	// X-Wing public keys are ML-KEM-768 encapsulation keys (1184 bytes)
	// followed by an X25519 point (32 bytes).
	xwingPublicKeyBytes = 1184 + 32
	// X-Wing encapsulated keys are an ML-KEM-768 ciphertext (1088 bytes)
	// followed by an X25519 point (32 bytes).
	xwingEncapsulatedKeyBytes = 1088 + 32
	// ChaCha20-Poly1305 appends a 16-byte Poly1305 tag to the ciphertext.
	chacha20Poly1305TagBytes = 16
)

// mlkemEncodingBytes is the prefix of an ML-KEM-768 encapsulation key that
// holds the packed 12-bit polynomial coefficients (3 polynomials x 256
// coefficients x 12 bits).
const mlkemEncodingBytes = 3 * 256 * 12 / 8

// flattenedJWE is the flattened JSON serialization the provider emits. The
// independent reader parses these wire fields and derives the HPKE inputs
// itself; it never calls the provider.
type flattenedJWE struct {
	Protected    string `json:"protected"`
	EncryptedKey string `json:"encrypted_key"`
	AAD          string `json:"aad"`
	IV           string `json:"iv"`
	Ciphertext   string `json:"ciphertext"`
	Tag          string `json:"tag"`
}

// testRecipient pairs the config the provider consumes with the private key an
// independent reader needs.
type testRecipient struct {
	config     *v2.EncryptionConfig
	privateKey hpke.PrivateKey
}

func akpJWK(t *testing.T, members map[string]any) []byte {
	t.Helper()
	encoded, err := json.Marshal(members)
	require.NoError(t, err)
	return encoded
}

// xwingJWKMembers renders the required public JWK members for an X-Wing public
// key. Callers may override or add members; JSON marshalling sorts keys, so the
// output is stable.
func xwingJWKMembers(t *testing.T, publicKey []byte) map[string]any {
	t.Helper()
	return map[string]any{
		"kty": "AKP",
		"alg": Algorithm,
		"pub": base64.RawURLEncoding.EncodeToString(publicKey),
	}
}

func newTestRecipient(t *testing.T, keyID string, aad []byte, mutate ...func(*v2.EncryptionConfig_JWKPublicKeyConfig)) testRecipient {
	t.Helper()
	privateKey, err := hpke.MLKEM768X25519().GenerateKey()
	require.NoError(t, err)

	jwkConfig := v2.EncryptionConfig_JWKPublicKeyConfig_builder{
		PubKey: akpJWK(t, xwingJWKMembers(t, privateKey.PublicKey().Bytes())),
	}.Build()
	if aad != nil {
		jwkConfig.SetAdditionalAuthenticatedData(aad)
	}
	for _, apply := range mutate {
		apply(jwkConfig)
	}

	return testRecipient{
		config: v2.EncryptionConfig_builder{
			Provider:           EncryptionProvider,
			KeyId:              keyID,
			JwkPublicKeyConfig: jwkConfig,
		}.Build(),
		privateKey: privateKey,
	}
}

func decodeRawURL(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(value)
	require.NoError(t, err)
	return decoded
}

func decodeJWE(t *testing.T, raw []byte) flattenedJWE {
	t.Helper()
	var envelope flattenedJWE
	require.NoError(t, json.Unmarshal(raw, &envelope))
	return envelope
}

func marshalJWE(t *testing.T, envelope flattenedJWE) []byte {
	t.Helper()
	encoded, err := json.Marshal(envelope)
	require.NoError(t, err)
	return encoded
}

func jsonMembers(t *testing.T, raw []byte) []string {
	t.Helper()
	var members map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &members))
	names := make([]string, 0, len(members))
	for name := range members {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// independentOpen decrypts a flattened JWE using the HPKE recipient derived
// from the private key directly. It reconstructs the HPKE AAD from the wire
// protected header and aad strings, per draft-ietf-jose-hpke-encrypt-22
// section 5, rather than calling the provider's own framing code.
func independentOpen(message []byte, privateKey hpke.PrivateKey, info []byte) ([]byte, error) {
	var envelope flattenedJWE
	if err := json.Unmarshal(message, &envelope); err != nil {
		return nil, err
	}
	encapsulatedKey, err := base64.RawURLEncoding.Strict().DecodeString(envelope.EncryptedKey)
	if err != nil {
		return nil, err
	}
	ciphertext, err := base64.RawURLEncoding.Strict().DecodeString(envelope.Ciphertext)
	if err != nil {
		return nil, err
	}
	recipient, err := hpke.NewRecipient(encapsulatedKey, privateKey, hpke.HKDFSHA256(), hpke.ChaCha20Poly1305(), info)
	if err != nil {
		return nil, err
	}
	return recipient.Open([]byte(envelope.Protected+"."+envelope.AAD), ciphertext)
}

func mustIndependentOpen(t *testing.T, message []byte, privateKey hpke.PrivateKey) []byte {
	t.Helper()
	plaintext, err := independentOpen(message, privateKey, nil)
	require.NoError(t, err)
	return plaintext
}

func encryptFor(t *testing.T, recipient testRecipient, plaintext *v2.PlaintextData) *v2.EncryptedData {
	t.Helper()
	encrypted, err := (&Provider{}).Encrypt(context.Background(), recipient.config, plaintext)
	require.NoError(t, err)
	return encrypted
}

// TestEncryptEmitsDraft22FlattenedJWE pins the wire shape: the protected header
// carries only alg and kid, the encapsulation and ciphertext are unpadded
// base64url, iv and tag are empty (the HPKE tag stays inside ciphertext), and
// the HPKE AAD is the wire protected header joined to the wire aad by a dot.
func TestEncryptEmitsDraft22FlattenedJWE(t *testing.T) {
	const keyID = "recipient-7"
	aad := []byte("tenant=acme;purpose=api-key")
	recipient := newTestRecipient(t, keyID, aad)
	plaintext := v2.PlaintextData_builder{
		Name:        "api_key",
		Description: "Issued API key",
		Schema:      "opaque",
		Bytes:       []byte("super-secret-key-material"),
	}.Build()

	encrypted := encryptFor(t, recipient, plaintext)

	require.Equal(t, EncryptionProvider, encrypted.GetProvider())
	require.Equal(t, []string{keyID}, encrypted.GetKeyIds())
	require.Empty(t, encrypted.GetKeyId(), "deprecated key_id must stay empty")
	require.Equal(t, plaintext.GetName(), encrypted.GetName())
	require.Equal(t, plaintext.GetDescription(), encrypted.GetDescription())
	require.Equal(t, plaintext.GetSchema(), encrypted.GetSchema())

	// The envelope is the flattened serialization and nothing else.
	require.Equal(t,
		[]string{"aad", "ciphertext", "encrypted_key", "iv", "protected", "tag"},
		jsonMembers(t, encrypted.GetEncryptedBytes()))

	envelope := decodeJWE(t, encrypted.GetEncryptedBytes())
	require.Empty(t, envelope.IV, "JWE iv must be empty; the HPKE tag lives in ciphertext")
	require.Empty(t, envelope.Tag, "JWE tag must be empty; the HPKE tag lives in ciphertext")
	require.Equal(t, base64.RawURLEncoding.EncodeToString(aad), envelope.AAD)

	// The protected header holds only alg and kid: no enc, ek, psk_id, zip or
	// crit, and no unprotected header alongside it.
	require.Equal(t, []string{"alg", "kid"}, jsonMembers(t, decodeRawURL(t, envelope.Protected)))
	var header struct {
		Algorithm string `json:"alg"`
		KeyID     string `json:"kid"`
	}
	require.NoError(t, json.Unmarshal(decodeRawURL(t, envelope.Protected), &header))
	require.Equal(t, Algorithm, header.Algorithm)
	require.Equal(t, keyID, header.KeyID)

	require.Len(t, decodeRawURL(t, envelope.EncryptedKey), xwingEncapsulatedKeyBytes)
	ciphertext := decodeRawURL(t, envelope.Ciphertext)
	require.Len(t, ciphertext, len(plaintext.GetBytes())+chacha20Poly1305TagBytes,
		"ciphertext must retain the HPKE authentication tag")

	// Independent read: succeeds only if the HPKE info is empty and the AAD
	// binding matches the wire strings.
	require.Equal(t, plaintext.GetBytes(), mustIndependentOpen(t, encrypted.GetEncryptedBytes(), recipient.privateKey))
	_, err := independentOpen(encrypted.GetEncryptedBytes(), recipient.privateKey, []byte("not-empty"))
	require.Error(t, err, "HPKE info must be empty")
}

// TestEncryptPayloadAndAADMatrix covers the payload and context dimensions the
// profile must carry unchanged: binary, plain-text and empty payloads against
// empty, opaque binary and JSON context.
func TestEncryptPayloadAndAADMatrix(t *testing.T) {
	payloads := map[string][]byte{
		"binary payload": {0x00, 0xff, 0x01, 0x80, 0x7f, 0xfe},
		"plain text":     []byte("hunter2"),
		"empty payload":  {},
	}
	contexts := map[string][]byte{
		"empty context":  nil,
		"opaque binary":  {0x00, 0xa0, 0xff, 0x0a},
		"json context":   []byte(`{"tenant":"acme","purpose":"api-key"}`),
		"single byte":    {0x01},
	}

	for payloadName, payload := range payloads {
		for contextName, aad := range contexts {
			t.Run(payloadName+" / "+contextName, func(t *testing.T) {
				recipient := newTestRecipient(t, "recipient-1", aad)
				plaintext := v2.PlaintextData_builder{Name: "material", Bytes: payload}.Build()

				encrypted := encryptFor(t, recipient, plaintext)

				envelope := decodeJWE(t, encrypted.GetEncryptedBytes())
				// The aad member is always present, including for zero bytes.
				require.Contains(t, jsonMembers(t, encrypted.GetEncryptedBytes()), "aad")
				require.Equal(t, base64.RawURLEncoding.EncodeToString(aad), envelope.AAD)
				require.Len(t, decodeRawURL(t, envelope.Ciphertext), len(payload)+chacha20Poly1305TagBytes)
				// Byte content is the contract; an empty payload reads back as
				// a nil slice, so compare as strings.
				require.Equal(t, string(payload), string(mustIndependentOpen(t, encrypted.GetEncryptedBytes(), recipient.privateKey)))
				require.Equal(t, []string{"recipient-1"}, encrypted.GetKeyIds())
				require.Equal(t, "material", encrypted.GetName())
			})
		}
	}
}

// TestEncryptUsesFreshEncapsulationPerCall covers the stateless contract: one
// HPKE sender per message, so two encryptions of identical input still produce
// distinct encapsulation and ciphertext.
func TestEncryptUsesFreshEncapsulationPerCall(t *testing.T) {
	recipient := newTestRecipient(t, "recipient-1", []byte("context"))
	plaintext := v2.PlaintextData_builder{Name: "material", Bytes: []byte("repeatable input")}.Build()

	first := decodeJWE(t, encryptFor(t, recipient, plaintext).GetEncryptedBytes())
	second := decodeJWE(t, encryptFor(t, recipient, plaintext).GetEncryptedBytes())
	third := decodeJWE(t, encryptFor(t, recipient, plaintext).GetEncryptedBytes())

	require.NotEqual(t, first.EncryptedKey, second.EncryptedKey)
	require.NotEqual(t, second.EncryptedKey, third.EncryptedKey)
	require.NotEqual(t, first.Ciphertext, second.Ciphertext)
	require.NotEqual(t, second.Ciphertext, third.Ciphertext)

	for _, envelope := range []flattenedJWE{first, second, third} {
		require.Equal(t, plaintext.GetBytes(), mustIndependentOpen(t, marshalJWE(t, envelope), recipient.privateKey))
	}
}

// TestEncryptDoesNotMutateInputs checks that encryption leaves the caller's
// config and plaintext untouched, including the context bytes the config owns.
func TestEncryptDoesNotMutateInputs(t *testing.T) {
	recipient := newTestRecipient(t, "recipient-1", []byte("context"))
	plaintext := v2.PlaintextData_builder{
		Name:        "api_key",
		Description: "Issued API key",
		Bytes:       []byte("super-secret-key-material"),
	}.Build()

	configBefore, err := proto.Marshal(recipient.config)
	require.NoError(t, err)
	plaintextBefore, err := proto.Marshal(plaintext)
	require.NoError(t, err)
	contextBefore := bytes.Clone(recipient.config.GetJwkPublicKeyConfig().GetAdditionalAuthenticatedData())

	_ = encryptFor(t, recipient, plaintext)

	configAfter, err := proto.Marshal(recipient.config)
	require.NoError(t, err)
	plaintextAfter, err := proto.Marshal(plaintext)
	require.NoError(t, err)

	require.Equal(t, configBefore, configAfter, "config must not be mutated by encryption")
	require.Equal(t, plaintextBefore, plaintextAfter, "plaintext must not be mutated by encryption")
	require.Equal(t, contextBefore, recipient.config.GetJwkPublicKeyConfig().GetAdditionalAuthenticatedData())
}

// TestEncryptRejectsPlaintextOutsideSizeLimit covers the plaintext bound at the
// boundary: exactly at the limit encrypts, one byte over is refused without
// producing a response, and a missing message is refused.
func TestEncryptRejectsPlaintextOutsideSizeLimit(t *testing.T) {
	recipient := newTestRecipient(t, "recipient-1", nil)

	t.Run("nil message", func(t *testing.T) {
		encrypted, err := (&Provider{}).Encrypt(context.Background(), recipient.config, nil)
		requireInvalidArgument(t, err)
		require.Nil(t, encrypted)
	})

	t.Run("empty bytes", func(t *testing.T) {
		encrypted := encryptFor(t, recipient, v2.PlaintextData_builder{Name: "empty"}.Build())
		require.Len(t, decodeRawURL(t, decodeJWE(t, encrypted.GetEncryptedBytes()).Ciphertext), chacha20Poly1305TagBytes)
	})

	t.Run("exactly at limit", func(t *testing.T) {
		payload := bytes.Repeat([]byte{0x5a}, MaxPlaintextBytes)
		encrypted := encryptFor(t, recipient, v2.PlaintextData_builder{Name: "max", Bytes: payload}.Build())
		require.Equal(t, payload, mustIndependentOpen(t, encrypted.GetEncryptedBytes(), recipient.privateKey))
	})

	t.Run("one byte over limit", func(t *testing.T) {
		payload := bytes.Repeat([]byte{0x5a}, MaxPlaintextBytes+1)
		encrypted, err := (&Provider{}).Encrypt(context.Background(), recipient.config, v2.PlaintextData_builder{Name: "over", Bytes: payload}.Build())
		requireInvalidArgument(t, err)
		require.Nil(t, encrypted)
	})
}

// TestEncryptRejectsNonCanonicalPublicKeyEncoding pins the base64url rule: the
// encoding must be unpadded and canonical, so a padded, standard-alphabet or
// non-canonical spelling of the key is refused rather than silently accepted.
func TestEncryptRejectsNonCanonicalPublicKeyEncoding(t *testing.T) {
	privateKey, err := hpke.MLKEM768X25519().GenerateKey()
	require.NoError(t, err)
	canonical := base64.RawURLEncoding.EncodeToString(privateKey.PublicKey().Bytes())

	nonCanonical := nonCanonicalRawURL(t, canonical)
	// The mutation sets only a padding bit, so the decoded key is unchanged and
	// the rejection is purely about canonical form.
	require.Equal(t, decodeRawURL(t, canonical), rawURLLenientDecode(t, nonCanonical))

	cases := map[string]string{
		"padded with equals":   canonical + "=",
		"standard base64 char": canonical[:len(canonical)-1] + "+",
		"non-canonical suffix": nonCanonical,
	}
	for name, encoding := range cases {
		t.Run(name, func(t *testing.T) {
			members := xwingJWKMembers(t, privateKey.PublicKey().Bytes())
			members["pub"] = encoding
			config := v2.EncryptionConfig_builder{
				Provider: EncryptionProvider,
				KeyId:    "recipient-1",
				JwkPublicKeyConfig: v2.EncryptionConfig_JWKPublicKeyConfig_builder{
					PubKey: akpJWK(t, members),
				}.Build(),
			}.Build()

			requireInvalidArgument(t, (&Provider{}).ValidateConfig(context.Background(), config))
			_, err := (&Provider{}).Encrypt(context.Background(), config, v2.PlaintextData_builder{Name: "m", Bytes: []byte("x")}.Build())
			requireInvalidArgument(t, err)
		})
	}
}

const rawURLAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

// nonCanonicalRawURL replaces the final character of an unpadded base64url
// string with the next alphabet entry. For a 1216-byte key the final character
// carries four unused zero bits, so this sets a padding bit without changing
// the decoded bytes.
func nonCanonicalRawURL(t *testing.T, canonical string) string {
	t.Helper()
	last := canonical[len(canonical)-1]
	index := strings.IndexByte(rawURLAlphabet, last)
	require.GreaterOrEqual(t, index, 0)
	require.Less(t, index+1, len(rawURLAlphabet))
	return canonical[:len(canonical)-1] + string(rawURLAlphabet[index+1])
}

func rawURLLenientDecode(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	require.NoError(t, err)
	return decoded
}
