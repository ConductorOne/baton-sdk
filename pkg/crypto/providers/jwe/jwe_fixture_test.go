package jwe

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"testing"
	"time"

	"filippo.io/hpke"
	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

const fixturePath = "testdata/fixture.json"

// fixtureSeed is the fixed 32-byte seed the fixture's X-Wing key is derived
// from. It is synthetic: the private key below is public and must never be used
// outside tests. Keeping the seed (rather than only the derived key) lets a
// second implementation reproduce the keypair from its own KEM.
var fixtureSeed = []byte{
	0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
	0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
	0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
	0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
}

const fixturePlaintext = "synthetic-jwe-fixture-plaintext"

var fixtureAAD = []byte("tenant=synthetic;record=1")

// xwingJWEFixture is the committed test artifact. A consuming implementation
// ingests expected.jwe_json, reconstructs the recipient from private_key_seed,
// and must recover plaintext. It is a synthetic key and payload: no real
// credential, tenant, or infrastructure value appears here.
type xwingJWEFixture struct {
	Description string `json:"description"`
	Profile     struct {
		Provider    string `json:"provider"`
		Algorithm   string `json:"alg"`
		KEM         string `json:"kem"`
		KEMID       uint16 `json:"kem_id"`
		KDF         string `json:"kdf"`
		KDFID       uint16 `json:"kdf_id"`
		AEAD        string `json:"aead"`
		AEADID      uint16 `json:"aead_id"`
		Framing     string `json:"framing"`
		HPKEInfo    string `json:"hpke_info"`
		HPKEAAD     string `json:"hpke_aad"`
		JWEIVAndTag string `json:"jwe_iv_and_tag"`
	} `json:"profile"`
	Provenance struct {
		Producer     string `json:"producer"`
		HPKELibrary  string `json:"hpke_library"`
		HPKEVersion  string `json:"hpke_version"`
		GoVersion    string `json:"go_version"`
		Generator    string `json:"generator"`
		GeneratedUTC string `json:"generated_utc"`
	} `json:"provenance"`
	KeyID       string `json:"key_id"`
	PrivateSeed string `json:"private_key_seed"`
	PublicKey   string `json:"public_key"`
	Plaintext   string `json:"plaintext"`
	AAD         string `json:"additional_authenticated_data"`
	Expected    struct {
		EncryptedKeyBytes int    `json:"encrypted_key_bytes"`
		CiphertextBytes   int    `json:"ciphertext_bytes"`
		JWEJSON           string `json:"jwe_json"`
	} `json:"expected"`
}

func readFixture(t *testing.T) xwingJWEFixture {
	t.Helper()
	raw, err := os.ReadFile(fixturePath)
	require.NoError(t, err)
	var fixture xwingJWEFixture
	require.NoError(t, json.Unmarshal(raw, &fixture))
	return fixture
}

// hpkeModuleVersion reports the filippo.io/hpke version linked into the test
// binary, so the fixture records which primitives produced it.
func hpkeModuleVersion(t *testing.T) string {
	t.Helper()
	info, ok := debug.ReadBuildInfo()
	require.True(t, ok, "build info unavailable")
	for _, dependency := range info.Deps {
		if dependency.Path == "filippo.io/hpke" {
			return dependency.Version
		}
	}
	t.Fatal("filippo.io/hpke is not a linked dependency")
	return ""
}

func buildFixture(t *testing.T) xwingJWEFixture {
	t.Helper()
	privateKey, err := hpke.MLKEM768X25519().NewPrivateKey(fixtureSeed)
	require.NoError(t, err)
	publicKey := privateKey.PublicKey().Bytes()

	config := v2.EncryptionConfig_builder{
		Provider: EncryptionProvider,
		KeyId:    "fixture-recipient-1",
		JwkPublicKeyConfig: v2.EncryptionConfig_JWKPublicKeyConfig_builder{
			PubKey:                      akpJWK(t, xwingJWKMembers(t, publicKey)),
			AdditionalAuthenticatedData: fixtureAAD,
		}.Build(),
	}.Build()
	encrypted, err := (&Provider{}).Encrypt(context.Background(), config, v2.PlaintextData_builder{
		Name:  "fixture-plaintext",
		Bytes: []byte(fixturePlaintext),
	}.Build())
	require.NoError(t, err)

	var fixture xwingJWEFixture
	fixture.Description = "Synthetic X-Wing JWE produced by the baton-sdk JWE credential provider. The private key is a public throwaway and must never be used outside tests."
	fixture.Profile.Provider = EncryptionProvider
	fixture.Profile.Algorithm = Algorithm
	fixture.Profile.KEM = "MLKEM768-X25519 (X-Wing)"
	fixture.Profile.KEMID = 0x647a
	fixture.Profile.KDF = "HKDF-SHA256"
	fixture.Profile.KDFID = 0x0001
	fixture.Profile.AEAD = "ChaCha20-Poly1305"
	fixture.Profile.AEADID = 0x0003
	fixture.Profile.Framing = "flattened JWE JSON, integrated base mode (draft-ietf-jose-hpke-encrypt-22 section 5)"
	fixture.Profile.HPKEInfo = "empty"
	fixture.Profile.HPKEAAD = `ASCII(protected + "." + aad), always including an empty context`
	fixture.Profile.JWEIVAndTag = "both empty; the HPKE authentication tag is the last 16 bytes of ciphertext"
	fixture.Provenance.Producer = "baton-sdk pkg/crypto/providers/jwe"
	fixture.Provenance.HPKELibrary = "filippo.io/hpke"
	fixture.Provenance.HPKEVersion = hpkeModuleVersion(t)
	fixture.Provenance.GoVersion = runtime.Version()
	fixture.Provenance.Generator = "TestRegenerateXWingJWEFixture (internal test, JWE_FIXTURE_REGENERATE=1)"
	fixture.Provenance.GeneratedUTC = time.Now().UTC().Format(time.RFC3339)
	fixture.KeyID = "fixture-recipient-1"
	fixture.PrivateSeed = base64.RawURLEncoding.EncodeToString(fixtureSeed)
	fixture.PublicKey = base64.RawURLEncoding.EncodeToString(publicKey)
	fixture.Plaintext = base64.RawURLEncoding.EncodeToString([]byte(fixturePlaintext))
	fixture.AAD = base64.RawURLEncoding.EncodeToString(fixtureAAD)
	envelope := decodeJWE(t, encrypted.GetEncryptedBytes())
	fixture.Expected.EncryptedKeyBytes = len(decodeRawURL(t, envelope.EncryptedKey))
	fixture.Expected.CiphertextBytes = len(decodeRawURL(t, envelope.Ciphertext))
	fixture.Expected.JWEJSON = string(encrypted.GetEncryptedBytes())
	return fixture
}

// TestXWingJWEFixture verifies the committed artifact: the seed reproduces the
// recorded public key, the recorded profile matches the provider's constants
// and linked HPKE version, the framing fields have the pinned shape, and an
// independent reader recovers the plaintext from the raw JWE JSON.
//
// This is SDK-side evidence for a second implementation to consume, not proof
// that any particular consumer interoperates.
func TestXWingJWEFixture(t *testing.T) {
	fixture := readFixture(t)

	require.Equal(t, EncryptionProvider, fixture.Profile.Provider)
	require.Equal(t, Algorithm, fixture.Profile.Algorithm)
	require.Equal(t, uint16(0x647a), fixture.Profile.KEMID)
	require.Equal(t, uint16(0x0001), fixture.Profile.KDFID)
	require.Equal(t, uint16(0x0003), fixture.Profile.AEADID)
	require.Equal(t, hpkeModuleVersion(t), fixture.Provenance.HPKEVersion,
		"fixture was produced with a different filippo.io/hpke version")

	seed, err := base64.RawURLEncoding.Strict().DecodeString(fixture.PrivateSeed)
	require.NoError(t, err)
	require.Len(t, seed, 32)
	privateKey, err := hpke.MLKEM768X25519().NewPrivateKey(seed)
	require.NoError(t, err)
	require.Equal(t,
		base64.RawURLEncoding.EncodeToString(privateKey.PublicKey().Bytes()),
		fixture.PublicKey,
		"recorded public key does not match the recording seed")

	aad, err := base64.RawURLEncoding.Strict().DecodeString(fixture.AAD)
	require.NoError(t, err)
	plaintext, err := base64.RawURLEncoding.Strict().DecodeString(fixture.Plaintext)
	require.NoError(t, err)

	envelope := decodeJWE(t, []byte(fixture.Expected.JWEJSON))
	require.Equal(t,
		[]string{"aad", "ciphertext", "encrypted_key", "iv", "protected", "tag"},
		jsonMembers(t, []byte(fixture.Expected.JWEJSON)))
	require.Equal(t, []string{"alg", "kid"}, jsonMembers(t, decodeRawURL(t, envelope.Protected)))
	require.Empty(t, envelope.IV)
	require.Empty(t, envelope.Tag)
	require.Equal(t, fixture.AAD, envelope.AAD)
	require.Equal(t, fixture.Expected.EncryptedKeyBytes, len(decodeRawURL(t, envelope.EncryptedKey)))
	require.Equal(t, xwingEncapsulatedKeyBytes, fixture.Expected.EncryptedKeyBytes)
	require.Equal(t, fixture.Expected.CiphertextBytes, len(decodeRawURL(t, envelope.Ciphertext)))
	require.Equal(t, len(plaintext)+chacha20Poly1305TagBytes, fixture.Expected.CiphertextBytes)

	recovered, err := independentOpen([]byte(fixture.Expected.JWEJSON), privateKey, nil)
	require.NoError(t, err)
	require.Equal(t, plaintext, recovered)
	require.Equal(t, fixtureAAD, aad)
}

// TestRegenerateXWingJWEFixture rewrites the committed fixture. It is skipped
// by default because re-freezing is deliberate: HPKE encapsulation is
// randomized, so every regeneration invalidates a consumer that pinned the
// previous bytes.
func TestRegenerateXWingJWEFixture(t *testing.T) {
	if os.Getenv("JWE_FIXTURE_REGENERATE") != "1" {
		t.Skip("set JWE_FIXTURE_REGENERATE=1 to re-freeze the fixture")
	}
	fixture := buildFixture(t)
	encoded, err := json.MarshalIndent(fixture, "", "  ")
	require.NoError(t, err)
	encoded = append(encoded, '\n')
	require.NoError(t, os.MkdirAll(filepath.Dir(fixturePath), 0o755))
	require.NoError(t, os.WriteFile(fixturePath, encoded, 0o600))
	t.Logf("wrote %s", fixturePath)
}
