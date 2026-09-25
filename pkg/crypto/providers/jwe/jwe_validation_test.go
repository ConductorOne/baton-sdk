package jwe

import (
	"context"
	"crypto/ecdh"
	"encoding/json"
	"strings"
	"testing"

	"filippo.io/hpke"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// secretMarker stands in for private material. No validation error may echo it.
const secretMarker = "SECRET-MARKER-DO-NOT-ECHO"

func requireInvalidArgument(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "want InvalidArgument, got %v", err)
}

func freshXWingPublicKey(t *testing.T) []byte {
	t.Helper()
	privateKey, err := hpke.MLKEM768X25519().GenerateKey()
	require.NoError(t, err)
	return privateKey.PublicKey().Bytes()
}

// customJWK renders a public JWK from the X-Wing key, applying overrides and
// removals so a case can name exactly one deviation from a valid recipient.
func customJWK(t *testing.T, publicKey []byte, overrides map[string]any, drop ...string) []byte {
	t.Helper()
	members := xwingJWKMembers(t, publicKey)
	for name, value := range overrides {
		members[name] = value
	}
	for _, name := range drop {
		delete(members, name)
	}
	return akpJWK(t, members)
}

func configForRawJWK(keyID string, rawJWK []byte, aad []byte) *v2.EncryptionConfig {
	jwkConfig := v2.EncryptionConfig_JWKPublicKeyConfig_builder{PubKey: rawJWK}.Build()
	if aad != nil {
		jwkConfig.SetAdditionalAuthenticatedData(aad)
	}
	return v2.EncryptionConfig_builder{
		Provider:           EncryptionProvider,
		KeyId:              keyID,
		JwkPublicKeyConfig: jwkConfig,
	}.Build()
}

// padJWKToSize grows a public JWK with an ignored member until it marshals to
// exactly total bytes.
func padJWKToSize(t *testing.T, publicKey []byte, total int) []byte {
	t.Helper()
	members := xwingJWKMembers(t, publicKey)
	padding := ""
	for {
		members["ignored"] = padding
		encoded, err := json.Marshal(members)
		require.NoError(t, err)
		if len(encoded) == total {
			return encoded
		}
		require.Less(t, len(encoded), total, "base JWK already exceeds %d bytes", total)
		padding += strings.Repeat("a", total-len(encoded))
	}
}

// declaredMaxLen mirrors the max_len the proto declares on both
// EncryptionConfig.JWKPublicKeyConfig.pub_key and
// .additional_authenticated_data. The proto is the single source of truth: the
// provider reads the bound through the generated validator instead of repeating
// it, so tests reference the declared number rather than a provider constant.
const declaredMaxLen = 16384

// TestDeclaredFieldBoundsEnforcedByBothEntryPoints covers each declared bound at
// its boundary through both exported entry points. ValidateConfig and Encrypt
// share one recipient preflight, so a direct call to either must refuse the same
// configurations, and an at-limit configuration must still produce a message
// that a recipient can read.
func TestDeclaredFieldBoundsEnforcedByBothEntryPoints(t *testing.T) {
	privateKey, err := hpke.MLKEM768X25519().GenerateKey()
	require.NoError(t, err)
	publicKey := privateKey.PublicKey().Bytes()
	validJWK := customJWK(t, publicKey, nil)

	cases := []struct {
		name    string
		config  *v2.EncryptionConfig
		wantErr bool
	}{
		{
			"pub_key at the declared maximum",
			configForRawJWK("recipient-1", padJWKToSize(t, publicKey, declaredMaxLen), nil),
			false,
		},
		{
			"pub_key one byte over the declared maximum",
			configForRawJWK("recipient-1", padJWKToSize(t, publicKey, declaredMaxLen+1), nil),
			true,
		},
		{
			"authenticated data at the declared maximum",
			configForRawJWK("recipient-1", validJWK, make([]byte, declaredMaxLen)),
			false,
		},
		{
			"authenticated data one byte over the declared maximum",
			configForRawJWK("recipient-1", validJWK, make([]byte, declaredMaxLen+1)),
			true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			validationErr := (&Provider{}).ValidateConfig(context.Background(), tc.config)
			encrypted, encryptErr := encryptPayload(t, tc.config)

			if tc.wantErr {
				requireInvalidArgument(t, validationErr)
				requireInvalidArgument(t, encryptErr)
				require.Nil(t, encrypted)
				return
			}
			require.NoError(t, validationErr)
			require.NoError(t, encryptErr)
			require.Equal(t, "payload",
				string(mustIndependentOpen(t, encrypted.GetEncryptedBytes(), privateKey)),
				"an at-limit configuration must still encrypt to a readable message")
		})
	}
}

// TestPublicJWKFieldsStructuralMalformation drives the byte-level object shapes
// the decoder can reach: trailing commas, mismatched or missing delimiters, and
// a bare separator. Each must be refused rather than parsed into fields.
func TestPublicJWKFieldsStructuralMalformation(t *testing.T) {
	for _, input := range []string{
		`{"kty":"AKP",}`,
		`{"kty":"AKP"]`,
		`{"kty":"AKP"`,
		`{`,
		`{"kty"}`,
		`{"kty":"AKP",,"alg":"x"}`,
		`{"kty":"AKP"} {}`,
		`{"kty":"AKP"} trailing`,
	} {
		t.Run(input, func(t *testing.T) {
			fields, err := publicJWKFields([]byte(input))
			requireInvalidArgument(t, err)
			require.Nil(t, fields)
		})
	}
}

// TestPublicKeyRejectionsUseDistinctMechanisms proves each unusable X-Wing
// public key is refused by the layer that is supposed to refuse it. The
// provider reports every public-key failure as one message, so the error text
// cannot carry the premise; the primitive is probed directly instead.
//
// The three mechanisms are the X-Wing length check, the ML-KEM coefficient
// modulus, and the X25519 low-order ECDH probe. Parsing alone accepts a
// low-order X25519 point, which is why the provider probes ECDH.
func TestPublicKeyRejectionsUseDistinctMechanisms(t *testing.T) {
	publicKey := freshXWingPublicKey(t)
	x25519Offset := len(publicKey) - 32

	// Positive control: the key every mutation is derived from parses and
	// passes the ECDH probe.
	_, err := hpke.MLKEM768X25519().NewPublicKey(publicKey)
	require.NoError(t, err)
	require.NoError(t, x25519Probe(publicKey[x25519Offset:]))

	invalidModulus := append([]byte{}, publicKey...)
	for i := range mlkemEncodingBytes {
		invalidModulus[i] = 0xff
	}
	lowOrder := func(value byte) []byte {
		mutated := append([]byte{}, publicKey...)
		for i := range mutated[x25519Offset:] {
			mutated[x25519Offset+i] = 0
		}
		mutated[x25519Offset] = value
		return mutated
	}

	t.Run("length is rejected by the X-Wing parser", func(t *testing.T) {
		truncated := publicKey[:len(publicKey)-1]
		extended := append(append([]byte{}, publicKey...), 0x00)
		require.Len(t, truncated, xwingPublicKeyBytes-1)

		_, err := hpke.MLKEM768X25519().NewPublicKey(truncated)
		require.Error(t, err)
		_, err = hpke.MLKEM768X25519().NewPublicKey(extended)
		require.Error(t, err)
	})

	t.Run("invalid ML-KEM modulus is rejected by the KEM parser", func(t *testing.T) {
		// Same length as the valid key, so the rejection cannot be the length
		// check: all-ones coefficients decode above the ML-KEM modulus.
		require.Len(t, invalidModulus, xwingPublicKeyBytes)

		_, err := hpke.MLKEM768X25519().NewPublicKey(invalidModulus)
		require.Error(t, err)
	})

	t.Run("low-order X25519 points parse but fail the ECDH probe", func(t *testing.T) {
		for name, point := range map[string][]byte{"zero": lowOrder(0), "one": lowOrder(1)} {
			require.Len(t, point, xwingPublicKeyBytes)
			// The KEM parser accepts a low-order point.
			_, err := hpke.MLKEM768X25519().NewPublicKey(point)
			require.NoError(t, err, "%s point parses", name)

			// The ECDH probe is what rejects it.
			require.Error(t, x25519Probe(point[x25519Offset:]), "%s point must fail the probe", name)
		}
	})
}

// x25519Probe mirrors the provider's fixed validation probe: a public key that
// produces an all-zero shared secret with any fixed scalar is rejected by
// crypto/ecdh.
func x25519Probe(point []byte) error {
	scalar := [32]byte{1}
	privateKey, err := ecdh.X25519().NewPrivateKey(scalar[:])
	if err != nil {
		return err
	}
	publicKey, err := ecdh.X25519().NewPublicKey(point)
	if err != nil {
		return err
	}
	_, err = privateKey.ECDH(publicKey)
	return err
}

// TestRecipientValidationMatrix drives ValidateConfig and Encrypt over the
// parser surface. Every rejected case must be refused by both entry points with
// InvalidArgument, so a caller that validates up front sees the same verdict as
// one that goes straight to encryption.
func TestRecipientValidationMatrix(t *testing.T) {
	publicKey := freshXWingPublicKey(t)
	x25519Offset := len(publicKey) - 32

	rejected := []struct {
		name  string
		build func(t *testing.T) *v2.EncryptionConfig
	}{
		{"nil config", func(*testing.T) *v2.EncryptionConfig { return nil }},
		{"missing provider", func(t *testing.T) *v2.EncryptionConfig {
			config := configForRawJWK("recipient-1", customJWK(t, publicKey, nil), nil)
			config.SetProvider("")
			return config
		}},
		{"wrong provider", func(t *testing.T) *v2.EncryptionConfig {
			config := configForRawJWK("recipient-1", customJWK(t, publicKey, nil), nil)
			config.SetProvider("baton/age/v1")
			return config
		}},
		{"unknown provider", func(t *testing.T) *v2.EncryptionConfig {
			config := configForRawJWK("recipient-1", customJWK(t, publicKey, nil), nil)
			config.SetProvider("baton/not-a-provider/v1")
			return config
		}},
		{"missing JWK section", func(t *testing.T) *v2.EncryptionConfig {
			return v2.EncryptionConfig_builder{Provider: EncryptionProvider, KeyId: "recipient-1"}.Build()
		}},
		{"empty key id", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("", customJWK(t, publicKey, nil), nil)
		}},
		{"key id with surrounding whitespace", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK(" recipient-1 ", customJWK(t, publicKey, nil), nil)
		}},
		{"key id over size limit", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK(strings.Repeat("k", 1025), customJWK(t, publicKey, nil), nil)
		}},
		{"key id with invalid utf-8", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK(string([]byte{0xff, 0xfe}), customJWK(t, publicKey, nil), nil)
		}},
		{"empty public JWK", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", nil, nil)
		}},
		{"public JWK not JSON", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", []byte("not json"), nil)
		}},
		{"public JWK is an array", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", []byte(`["AKP"]`), nil)
		}},
		{"public JWK is a bare string", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", []byte(`"AKP"`), nil)
		}},
		{"public JWK truncated", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", []byte(`{"kty":"AKP"`), nil)
		}},
		{"public JWK duplicate member", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", []byte(
				`{"kty":"AKP","kty":"AKP","alg":"`+Algorithm+`","pub":"`+strings.Repeat("A", 4)+`"}`), nil)
		}},
		{"public JWK duplicate member with identical value", func(t *testing.T) *v2.EncryptionConfig {
			raw := customJWK(t, publicKey, nil)
			duplicated := append([]byte(`{"pub":`), raw...)
			duplicated = append(duplicated, []byte(`}`)...)
			return configForRawJWK("recipient-1", duplicated, nil)
		}},
		{"public JWK trailing data", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", append(customJWK(t, publicKey, nil), []byte(`{}`)...), nil)
		}},
		{"public JWK trailing whitespace and null", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", append(customJWK(t, publicKey, nil), []byte(" null")...), nil)
		}},
		{"missing kty", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, nil, "kty"), nil)
		}},
		{"null kty", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, map[string]any{"kty": nil}), nil)
		}},
		{"numeric kty", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, map[string]any{"kty": 3}), nil)
		}},
		{"classical kty", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, map[string]any{"kty": "RSA"}), nil)
		}},
		{"missing alg", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, nil, "alg"), nil)
		}},
		{"null alg", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, map[string]any{"alg": nil}), nil)
		}},
		{"classical alg", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, map[string]any{"alg": "RSA-OAEP-256"}), nil)
		}},
		{"algorithm without version suffix", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, map[string]any{"alg": strings.TrimSuffix(Algorithm, "/v1")}), nil)
		}},
		{"missing pub", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, nil, "pub"), nil)
		}},
		{"null pub", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, map[string]any{"pub": nil}), nil)
		}},
		{"numeric pub", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, map[string]any{"pub": 42}), nil)
		}},
		{"empty pub string", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, map[string]any{"pub": ""}), nil)
		}},
		{"private member priv", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, map[string]any{"priv": secretMarker}), nil)
		}},
		{"private member d", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, map[string]any{"d": secretMarker}), nil)
		}},
		{"private member k", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, map[string]any{"k": secretMarker}), nil)
		}},
		{"private member priv with null value", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, map[string]any{"priv": nil}), nil)
		}},
		{"private member d with null value", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, map[string]any{"d": nil}), nil)
		}},
		{"private member k with null value", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, map[string]any{"k": nil}), nil)
		}},
		{"mismatched kid", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, map[string]any{"kid": "other"}), nil)
		}},
		{"null kid", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, map[string]any{"kid": nil}), nil)
		}},
		{"mismatched use", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, map[string]any{"use": "sig"}), nil)
		}},
		{"null use", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, map[string]any{"use": nil}), nil)
		}},
		{"key_ops with decrypt", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, map[string]any{"key_ops": []string{"decrypt"}}), nil)
		}},
		{"key_ops with encrypt and decrypt", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, map[string]any{"key_ops": []string{"encrypt", "decrypt"}}), nil)
		}},
		{"key_ops not an array", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, map[string]any{"key_ops": "encrypt"}), nil)
		}},
		{"key_ops with null entry", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, map[string]any{"key_ops": []any{nil}}), nil)
		}},
		{"public key shorter than the profile", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey[:len(publicKey)-1], nil), nil)
		}},
		{"public key longer than the profile", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, append(append([]byte{}, publicKey...), 0x00), nil), nil)
		}},
		{"public key with invalid ML-KEM encoding", func(t *testing.T) *v2.EncryptionConfig {
			// The 12-bit coefficients are packed from the first 1152 bytes.
			// All-ones decodes to 4095, above the ML-KEM modulus (3329), so the
			// polynomial decode must fail.
			corrupted := append([]byte{}, publicKey...)
			for i := range mlkemEncodingBytes {
				corrupted[i] = 0xff
			}
			return configForRawJWK("recipient-1", customJWK(t, corrupted, nil), nil)
		}},
		{"X25519 low-order point zero", func(t *testing.T) *v2.EncryptionConfig {
			corrupted := append([]byte{}, publicKey...)
			for i := x25519Offset; i < len(corrupted); i++ {
				corrupted[i] = 0
			}
			return configForRawJWK("recipient-1", customJWK(t, corrupted, nil), nil)
		}},
		{"X25519 low-order point one", func(t *testing.T) *v2.EncryptionConfig {
			corrupted := append([]byte{}, publicKey...)
			for i := x25519Offset; i < len(corrupted); i++ {
				corrupted[i] = 0
			}
			corrupted[x25519Offset] = 1
			return configForRawJWK("recipient-1", customJWK(t, corrupted, nil), nil)
		}},
		{"public JWK over size limit", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", padJWKToSize(t, publicKey, declaredMaxLen+1), nil)
		}},
	}

	for _, tc := range rejected {
		t.Run(tc.name, func(t *testing.T) {
			config := tc.build(t)
			requireInvalidArgument(t, (&Provider{}).ValidateConfig(context.Background(), config))
			encrypted, err := (&Provider{}).Encrypt(context.Background(), config, v2.PlaintextData_builder{Name: "m", Bytes: []byte("x")}.Build())
			requireInvalidArgument(t, err)
			require.Nil(t, encrypted)
		})
	}

	accepted := []struct {
		name  string
		build func(t *testing.T) *v2.EncryptionConfig
	}{
		{"canonical recipient", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, nil), nil)
		}},
		{"provider name with case and padding", func(t *testing.T) *v2.EncryptionConfig {
			config := configForRawJWK("recipient-1", customJWK(t, publicKey, nil), nil)
			config.SetProvider(" " + strings.ToUpper(EncryptionProvider) + " ")
			return config
		}},
		{"matching kid", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, map[string]any{"kid": "recipient-1"}), nil)
		}},
		{"matching use", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, map[string]any{"use": "enc"}), nil)
		}},
		{"matching key_ops", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, map[string]any{"key_ops": []string{"encrypt"}}), nil)
		}},
		{"ignored extra JWK properties", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, map[string]any{
				"x-custom": "ignored",
				"ext":      true,
				"nested":   map[string]any{"a": 1},
			}), nil)
		}},
		{"key id at size limit", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK(strings.Repeat("k", 1024), customJWK(t, publicKey, nil), nil)
		}},
		{"public JWK at size limit", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", padJWKToSize(t, publicKey, declaredMaxLen), nil)
		}},
		{"authenticated data at size limit", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, nil), make([]byte, declaredMaxLen))
		}},
		{"empty authenticated data", func(t *testing.T) *v2.EncryptionConfig {
			return configForRawJWK("recipient-1", customJWK(t, publicKey, nil), []byte{})
		}},
	}

	for _, tc := range accepted {
		t.Run(tc.name, func(t *testing.T) {
			config := tc.build(t)
			require.NoError(t, (&Provider{}).ValidateConfig(context.Background(), config))
		})
	}

	// Authenticated data above the limit is rejected, and it is a config-level
	// rejection rather than a failure that only appears mid-encryption.
	t.Run("authenticated data over size limit", func(t *testing.T) {
		config := configForRawJWK("recipient-1", customJWK(t, publicKey, nil), make([]byte, declaredMaxLen+1))
		requireInvalidArgument(t, (&Provider{}).ValidateConfig(context.Background(), config))
		_, err := (&Provider{}).Encrypt(context.Background(), config, v2.PlaintextData_builder{Name: "m", Bytes: []byte("x")}.Build())
		requireInvalidArgument(t, err)
	})
}

// TestIgnoredJWKPropertiesNeverBecomeHeaders is the J1/J3 contract for extra
// members: properties the profile does not define are ignored, and in
// particular must not be copied into the JWE protected header or envelope.
func TestIgnoredJWKPropertiesNeverBecomeHeaders(t *testing.T) {
	// A public JWK with header-shaped extra members that a lenient
	// implementation might echo into the wire format.
	publicKey := freshXWingPublicKey(t)
	config := v2.EncryptionConfig_builder{
		Provider: EncryptionProvider,
		KeyId:    "recipient-1",
		JwkPublicKeyConfig: v2.EncryptionConfig_JWKPublicKeyConfig_builder{
			PubKey: customJWK(t, publicKey, map[string]any{
				"enc":     "A256GCM",
				"iv":      "AAAA",
				"zip":     "DEF",
				"crit":    []string{"exp"},
				"x-extra": "ignored",
			}),
		}.Build(),
	}.Build()

	encrypted, err := (&Provider{}).Encrypt(context.Background(), config, v2.PlaintextData_builder{Name: "m", Bytes: []byte("payload")}.Build())
	require.NoError(t, err)

	envelope := decodeJWE(t, encrypted.GetEncryptedBytes())
	require.Equal(t, []string{"alg", "kid"}, jsonMembers(t, decodeRawURL(t, envelope.Protected)))
	require.Equal(t,
		[]string{"aad", "ciphertext", "encrypted_key", "iv", "protected", "tag"},
		jsonMembers(t, encrypted.GetEncryptedBytes()))
}

// TestValidationErrorsDoNotEchoInput checks that parser failures carry no part
// of the rejected input: neither the private-key bytes nor a caller secret
// planted in the JWK.
func TestValidationErrorsDoNotEchoInput(t *testing.T) {
	publicKey := freshXWingPublicKey(t)

	cases := []struct {
		name   string
		config *v2.EncryptionConfig
	}{
		{"marker as the entire JWK", configForRawJWK("recipient-1", []byte(secretMarker), nil)},
		{"marker as private member value", configForRawJWK("recipient-1",
			customJWK(t, publicKey, map[string]any{"priv": secretMarker}), nil)},
		{"marker inside malformed JSON", configForRawJWK("recipient-1",
			[]byte(`{"kty":"AKP","alg":`+secretMarker), nil)},
		{"marker in an over-length key id", configForRawJWK(secretMarker+strings.Repeat("k", 1025), customJWK(t, publicKey, nil), nil)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := (&Provider{}).Encrypt(context.Background(), tc.config, v2.PlaintextData_builder{Name: "m", Bytes: []byte("x")}.Build())
			require.Error(t, err)
			require.NotContains(t, err.Error(), secretMarker, "error must not echo rejected input")
		})
	}
}
