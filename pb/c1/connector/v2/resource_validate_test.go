package v2

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"testing"

	"filippo.io/hpke"
	"github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/require"
)

// jwkPublicKeyMaxLen mirrors the max_len declared on
// EncryptionConfig.JWKPublicKeyConfig.pub_key and
// .additional_authenticated_data in proto/c1/connector/v2/resource.proto.
const jwkPublicKeyMaxLen = 16384

// buildJWKPublicKeyConfig builds the nested config the generated validators
// walk, so a boundary case names exactly one deviation from an in-bound config.
func buildJWKPublicKeyConfig(pubKey, aad []byte) *EncryptionConfig {
	return EncryptionConfig_builder{
		Provider: "baton/jwk/v1",
		JwkPublicKeyConfig: EncryptionConfig_JWKPublicKeyConfig_builder{
			PubKey:                      pubKey,
			AdditionalAuthenticatedData: aad,
		}.Build(),
	}.Build()
}

// legacyPublicJWK renders a real public JWK for a key type a legacy recipient
// may carry, so the bound is checked against realistic sizes rather than only
// against padded filler.
func legacyPublicJWK(t *testing.T, key any) []byte {
	t.Helper()
	encoded, err := json.Marshal((&jose.JSONWebKey{Key: key}).Public())
	require.NoError(t, err)
	return encoded
}

// xwingPublicJWK renders the public JWK the JWE provider consumes. The
// algorithm identifier must match the provider's Algorithm constant, which is
// the source of truth; this sample only needs to be byte-realistic for the
// length bound under test.
func xwingPublicJWK(t *testing.T) []byte {
	t.Helper()
	privateKey, err := hpke.MLKEM768X25519().GenerateKey()
	require.NoError(t, err)
	encoded, err := json.Marshal(map[string]string{
		"kty": "AKP",
		"alg": "https://c1.ai/alg/hpke-xwing-hkdf-sha256-chacha20poly1305/v1",
		"pub": base64.RawURLEncoding.EncodeToString(privateKey.PublicKey().Bytes()),
	})
	require.NoError(t, err)
	return encoded
}

func representativePublicJWKs(t *testing.T) map[string][]byte {
	t.Helper()
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	edPublic, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	return map[string][]byte{
		"legacy RSA":     legacyPublicJWK(t, rsaKey),
		"legacy EC":      legacyPublicJWK(t, ecKey),
		"legacy Ed25519": legacyPublicJWK(t, edPublic),
		"X-Wing AKP":     xwingPublicJWK(t),
	}
}

// TestJWKPublicKeyConfigPubKeyBound pins the generated pub_key bound: the
// declared maximum is accepted, one byte over is refused, and absent or empty
// values are unchanged by the rule.
func TestJWKPublicKeyConfigPubKeyBound(t *testing.T) {
	cases := []struct {
		name    string
		pubKey  []byte
		wantErr bool
	}{
		{"nil pub_key", nil, false},
		{"empty pub_key", []byte{}, false},
		{"one byte below the bound", make([]byte, jwkPublicKeyMaxLen-1), false},
		{"at the bound", make([]byte, jwkPublicKeyMaxLen), false},
		{"one byte above the bound", make([]byte, jwkPublicKeyMaxLen+1), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config := buildJWKPublicKeyConfig(tc.pubKey, []byte("in-bound context"))

			err := config.Validate()
			allErr := config.ValidateAll()

			if !tc.wantErr {
				require.NoError(t, err)
				require.NoError(t, allErr)
				return
			}
			require.Error(t, err)
			require.ErrorContains(t, err, "PubKey", "the refusal must name the field it measured")
			require.ErrorContains(t, err, "16384", "the refusal must name the bound it enforced")
			require.Error(t, allErr)
		})
	}
}

// TestJWKPublicKeyConfigAdditionalAuthenticatedDataBound pins the generated
// AAD bound the same way. The pub_key in every case here is in bound, so the
// two fields are measured independently.
func TestJWKPublicKeyConfigAdditionalAuthenticatedDataBound(t *testing.T) {
	inBoundPubKey := []byte(`{"kty":"AKP","pub":"in-bound"}`)

	cases := []struct {
		name    string
		aad     []byte
		wantErr bool
	}{
		{"nil context", nil, false},
		{"empty context", []byte{}, false},
		{"one byte below the bound", make([]byte, jwkPublicKeyMaxLen-1), false},
		{"at the bound", make([]byte, jwkPublicKeyMaxLen), false},
		{"one byte above the bound", make([]byte, jwkPublicKeyMaxLen+1), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config := buildJWKPublicKeyConfig(inBoundPubKey, tc.aad)

			err := config.Validate()
			allErr := config.ValidateAll()

			if !tc.wantErr {
				require.NoError(t, err)
				require.NoError(t, allErr)
				return
			}
			require.Error(t, err)
			require.ErrorContains(t, err, "AdditionalAuthenticatedData")
			require.ErrorContains(t, err, "16384")
			require.Error(t, allErr)
		})
	}
}

// TestJWKPublicKeyConfigBoundsAreIndependent checks that each field is reported
// on its own: with pub_key in bound only the context is named, with the context
// in bound only pub_key is named, and with both over the limit ValidateAll
// reports both violations rather than stopping at the first.
func TestJWKPublicKeyConfigBoundsAreIndependent(t *testing.T) {
	overLimit := make([]byte, jwkPublicKeyMaxLen+1)

	t.Run("only pub_key is over", func(t *testing.T) {
		config := buildJWKPublicKeyConfig(overLimit, []byte("in-bound context"))
		err := config.ValidateAll()
		require.Error(t, err)
		require.ErrorContains(t, err, "PubKey")
		require.NotContains(t, err.Error(), "AdditionalAuthenticatedData")
	})

	t.Run("only the context is over", func(t *testing.T) {
		config := buildJWKPublicKeyConfig([]byte(`{"kty":"AKP"}`), overLimit)
		err := config.ValidateAll()
		require.Error(t, err)
		require.ErrorContains(t, err, "AdditionalAuthenticatedData")
		require.NotContains(t, err.Error(), "PubKey")
	})

	t.Run("both are over and both are reported", func(t *testing.T) {
		config := buildJWKPublicKeyConfig(overLimit, overLimit)
		require.Error(t, config.ValidateAll(), "the outer message must reject too")

		// ValidateAll on the nested message collects every violation; the outer
		// message reports the nested failure as one entry.
		var multiErr EncryptionConfig_JWKPublicKeyConfigMultiError
		require.ErrorAs(t, config.GetJwkPublicKeyConfig().ValidateAll(), &multiErr)
		require.Len(t, multiErr.AllErrors(), 2)
	})
}

// TestJWKPublicKeyConfigAcceptsRepresentativeKeys checks the bound against real
// recipients. The shared pub_key bound deliberately covers legacy JWK key types
// as well as the JWE X-Wing key, so each has to stay inside it.
func TestJWKPublicKeyConfigAcceptsRepresentativeKeys(t *testing.T) {
	for name, publicJWK := range representativePublicJWKs(t) {
		t.Run(name, func(t *testing.T) {
			require.NotEmpty(t, publicJWK)
			require.LessOrEqual(t, len(publicJWK), jwkPublicKeyMaxLen,
				"a representative %s public JWK must fit the declared bound", name)

			config := buildJWKPublicKeyConfig(publicJWK, []byte("tenant=synthetic"))
			require.NoError(t, config.Validate())
			require.NoError(t, config.ValidateAll())
		})
	}
}
