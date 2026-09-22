package vaultinbox

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"filippo.io/hpke"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// vectorSeed is the fixed 0x42 X-Wing seed the existing Rust service-principal
// vector also uses, so one synthetic recipient covers both interop fixtures.
var vectorSeed = bytes.Repeat([]byte{0x42}, 32)

const (
	vectorTenant     = "tenant-vector"
	vectorVault      = "vault-vector"
	vectorInboxKey   = "inbox-key-vector"
	vectorGeneration = 7
	vectorScheme     = "latchkey.vault_submission.secret.v1"
	vectorSubmission = "vault-submission-vector"
	vectorValue      = "datalog-api-key-value"
	vectorName       = "api_key"
	vectorDesc       = "Datadog organization API key"
)

func vectorPrivateKey(t *testing.T) hpke.PrivateKey {
	t.Helper()
	key, err := hpke.MLKEM768X25519().NewPrivateKey(append([]byte(nil), vectorSeed...))
	require.NoError(t, err)
	return key
}

func publicJWKFor(key hpke.PrivateKey) string {
	return `{"kty":"` + jwkKtyAKP + `","alg":"` + jwkAlg + `","pub":"` +
		base64.RawURLEncoding.EncodeToString(key.PublicKey().Bytes()) + `"}`
}

func vectorPublicJWK(t *testing.T) (string, string) {
	t.Helper()
	jwk := publicJWKFor(vectorPrivateKey(t))
	thumbprint, err := publicKeyThumbprint(jwk)
	require.NoError(t, err)
	return jwk, thumbprint
}

// configFor builds a fully valid config, then hands the caller the chance to
// mutate it so each rejection case differs in exactly one field.
func configFor(t *testing.T, mutate func(*v2.VaultInboxRecipientConfig)) *v2.EncryptionConfig {
	t.Helper()
	jwk, thumbprint := vectorPublicJWK(t)
	config := v2.VaultInboxRecipientConfig_builder{
		ConfigVersion:       v2.VaultInboxConfigVersion_VAULT_INBOX_CONFIG_VERSION_V1,
		Suite:               v2.VaultInboxSuite_VAULT_INBOX_SUITE_XWING_MLKEM768_X25519_HKDF_SHA256_CHACHA20POLY1305_V1,
		TenantId:            vectorTenant,
		VaultBoundaryId:     vectorVault,
		InboxKeyId:          vectorInboxKey,
		KeyGeneration:       vectorGeneration,
		PayloadScheme:       vectorScheme,
		SubmissionId:        vectorSubmission,
		PublicJwkJson:       jwk,
		PublicKeyThumbprint: thumbprint,
	}.Build()
	if mutate != nil {
		mutate(config)
	}
	return v2.EncryptionConfig_builder{
		Provider:                  EncryptionProvider,
		VaultInboxRecipientConfig: config,
	}.Build()
}

func vectorConfig(t *testing.T) *v2.EncryptionConfig {
	t.Helper()
	return configFor(t, nil)
}

func vectorPlaintext() *v2.PlaintextData {
	return v2.PlaintextData_builder{
		Name:        vectorName,
		Description: vectorDesc,
		Bytes:       []byte(vectorValue),
	}.Build()
}

// expectedPayloadJSON is the byte-for-byte SecretSubmissionPayloadV3 the
// Latchkey open path parses. Keys are the Rust serde field names in declaration
// order; content_type normalizes to "generic"; annotations is always present.
const expectedPayloadJSON = `{"version":3,"submission_id":"vault-submission-vector",` +
	`"display_name":"api_key","description":"Datadog organization API key",` +
	`"content_type":"generic","annotations":{},` +
	`"value_b64":"ZGF0YWxvZy1hcGkta2V5LXZhbHVl"}`

func TestEncryptProducesTheVaultInboxEnvelope(t *testing.T) {
	ctx := context.Background()
	config := vectorConfig(t)

	encrypted, err := NewProvider().Encrypt(ctx, config, vectorPlaintext())
	require.NoError(t, err)
	require.Equal(t, EncryptionProvider, encrypted.GetProvider())
	require.Equal(t, []string{vectorInboxKey}, encrypted.GetKeyIds())
	require.Empty(t, encrypted.GetKeyId(), "the deprecated single key_id stays unset")
	require.Equal(t, vectorName, encrypted.GetName())
	require.Equal(t, vectorDesc, encrypted.GetDescription())

	var envelope struct {
		Version    uint8  `json:"version"`
		Alg        string `json:"alg"`
		Enc        string `json:"enc"`
		Ciphertext string `json:"ciphertext"`
	}
	require.NoError(t, json.Unmarshal(encrypted.GetEncryptedBytes(), &envelope))
	require.Equal(t, uint8(envelopeVersion), envelope.Version)
	require.Equal(t, jwkAlg, envelope.Alg)

	// The sealed plaintext is exactly the container the Rust decoder accepts.
	opened := openVectorEnvelope(t, config, envelope.Enc, envelope.Ciphertext)
	require.Equal(t, expectedPayloadJSON, string(opened))

	var payload struct {
		Version      uint8             `json:"version"`
		SubmissionID string            `json:"submission_id"`
		DisplayName  string            `json:"display_name"`
		ContentType  string            `json:"content_type"`
		ValueB64     string            `json:"value_b64"`
		Annotations  map[string]string `json:"annotations"`
	}
	require.NoError(t, json.Unmarshal(opened, &payload))
	require.Equal(t, uint8(payloadVersionV3), payload.Version)
	require.Equal(t, vectorSubmission, payload.SubmissionID)
	require.Equal(t, contentTypeGeneric, payload.ContentType)
	require.Empty(t, payload.Annotations)
	require.Equal(t, vectorValue, string(mustDecodeB64(t, payload.ValueB64)))
}

// TestEveryBindingFieldIsAuthenticated flips one AAD field at a time. Each
// mismatch must fail the AEAD open, which is what stops a server from moving a
// ciphertext to another tenant, vault, inbox key, generation, or scheme.
func TestEveryBindingFieldIsAuthenticated(t *testing.T) {
	ctx := context.Background()
	config := vectorConfig(t)
	encrypted, err := NewProvider().Encrypt(ctx, config, vectorPlaintext())
	require.NoError(t, err)
	var envelope struct {
		Enc        string `json:"enc"`
		Ciphertext string `json:"ciphertext"`
	}
	require.NoError(t, json.Unmarshal(encrypted.GetEncryptedBytes(), &envelope))

	mutate := map[string]func(*v2.VaultInboxRecipientConfig){
		"tenant":         func(c *v2.VaultInboxRecipientConfig) { c.TenantId = "tenant-other" },
		"vault":          func(c *v2.VaultInboxRecipientConfig) { c.VaultBoundaryId = "vault-other" },
		"inbox_key_id":   func(c *v2.VaultInboxRecipientConfig) { c.InboxKeyId = "inbox-key-other" },
		"key_generation": func(c *v2.VaultInboxRecipientConfig) { c.KeyGeneration = vectorGeneration + 1 },
		"payload_scheme": func(c *v2.VaultInboxRecipientConfig) { c.PayloadScheme = "latchkey.vault_submission.secret.v2" },
	}
	for name, edit := range mutate {
		t.Run(name, func(t *testing.T) {
			tampered := proto.Clone(config).(*v2.EncryptionConfig)
			edit(tampered.GetVaultInboxRecipientConfig())
			enc, err := base64.RawURLEncoding.DecodeString(envelope.Enc)
			require.NoError(t, err)
			ciphertext, err := base64.RawURLEncoding.DecodeString(envelope.Ciphertext)
			require.NoError(t, err)
			binding := bindingBytes(tampered.GetVaultInboxRecipientConfig())
			recipient, err := hpke.NewRecipient(enc, vectorPrivateKey(t), hpke.HKDFSHA256(), hpke.ChaCha20Poly1305(), binding)
			require.NoError(t, err)
			_, openErr := recipient.Open(binding, ciphertext)
			require.Error(t, openErr, "a rebound ciphertext must not open")
		})
	}
}

func TestValidateConfigRejectsUnsupportedProfiles(t *testing.T) {
	lowOrder := configFor(t, func(c *v2.VaultInboxRecipientConfig) {
		raw := mustDecodeB64(t, jwkPub(t, c.GetPublicJwkJson()))
		copy(raw[len(raw)-32:], make([]byte, 32))
		jwk := `{"kty":"` + jwkKtyAKP + `","alg":"` + jwkAlg + `","pub":"` + base64.RawURLEncoding.EncodeToString(raw) + `"}`
		c.PublicJwkJson = jwk
		thumbprint, err := publicKeyThumbprint(jwk)
		require.NoError(t, err)
		c.PublicKeyThumbprint = thumbprint
	})

	unknownFields := configFor(t, nil)
	unknownFields.ProtoReflect().SetUnknown([]byte{0x80, 0x7c, 0x01})

	cases := map[string]*v2.EncryptionConfig{
		"nil config":     nil,
		"wrong provider": withProvider(t, "baton/age/v1"),
		"unknown fields": unknownFields,
		"config version unset": configFor(t, func(c *v2.VaultInboxRecipientConfig) {
			c.ConfigVersion = v2.VaultInboxConfigVersion_VAULT_INBOX_CONFIG_VERSION_UNSPECIFIED
		}),
		"suite unset":           configFor(t, func(c *v2.VaultInboxRecipientConfig) { c.Suite = v2.VaultInboxSuite_VAULT_INBOX_SUITE_UNSPECIFIED }),
		"tenant empty":          configFor(t, func(c *v2.VaultInboxRecipientConfig) { c.TenantId = "" }),
		"vault empty":           configFor(t, func(c *v2.VaultInboxRecipientConfig) { c.VaultBoundaryId = "" }),
		"inbox key empty":       configFor(t, func(c *v2.VaultInboxRecipientConfig) { c.InboxKeyId = "" }),
		"generation zero":       configFor(t, func(c *v2.VaultInboxRecipientConfig) { c.KeyGeneration = 0 }),
		"scheme empty":          configFor(t, func(c *v2.VaultInboxRecipientConfig) { c.PayloadScheme = "" }),
		"submission id empty":   configFor(t, func(c *v2.VaultInboxRecipientConfig) { c.SubmissionId = "" }),
		"thumbprint empty":      configFor(t, func(c *v2.VaultInboxRecipientConfig) { c.PublicKeyThumbprint = "" }),
		"thumbprint mismatch":   configFor(t, func(c *v2.VaultInboxRecipientConfig) { c.PublicKeyThumbprint = "not-the-thumbprint" }),
		"oversized identifier":  configFor(t, func(c *v2.VaultInboxRecipientConfig) { c.VaultBoundaryId = strings.Repeat("v", maxIDBytes+1) }),
		"content type too long": configFor(t, func(c *v2.VaultInboxRecipientConfig) { c.ContentType = strings.Repeat("a", maxContentBytes+1) }),
		"low-order recipient":   lowOrder,
	}
	for name, config := range cases {
		t.Run(name, func(t *testing.T) {
			require.Error(t, NewProvider().ValidateConfig(context.Background(), config))
		})
	}
	require.NoError(t, NewProvider().ValidateConfig(context.Background(), vectorConfig(t)))
}

func withProvider(t *testing.T, name string) *v2.EncryptionConfig {
	t.Helper()
	config := vectorConfig(t)
	config.SetProvider(name)
	return config
}

func TestValidateConfigRejectsMalformedJWK(t *testing.T) {
	_, thumbprint := vectorPublicJWK(t)
	build := func(jwk string) *v2.EncryptionConfig {
		return v2.EncryptionConfig_builder{
			Provider: EncryptionProvider,
			VaultInboxRecipientConfig: v2.VaultInboxRecipientConfig_builder{
				ConfigVersion:       v2.VaultInboxConfigVersion_VAULT_INBOX_CONFIG_VERSION_V1,
				Suite:               v2.VaultInboxSuite_VAULT_INBOX_SUITE_XWING_MLKEM768_X25519_HKDF_SHA256_CHACHA20POLY1305_V1,
				TenantId:            vectorTenant,
				VaultBoundaryId:     vectorVault,
				InboxKeyId:          vectorInboxKey,
				KeyGeneration:       vectorGeneration,
				PayloadScheme:       vectorScheme,
				SubmissionId:        vectorSubmission,
				PublicJwkJson:       jwk,
				PublicKeyThumbprint: thumbprint,
			}.Build(),
		}.Build()
	}
	cases := map[string]string{
		"not json":          "not-json",
		"wrong kty":         `{"kty":"EC","alg":"` + jwkAlg + `","pub":"AAAA"}`,
		"wrong alg":         `{"kty":"` + jwkKtyAKP + `","alg":"HPKE-Base-X-Wing-Draft06Obsolete","pub":"AAAA"}`,
		"private present":   `{"kty":"` + jwkKtyAKP + `","alg":"` + jwkAlg + `","pub":"AAAA","priv":"AAAA"}`,
		"unknown jwk field": `{"kty":"` + jwkKtyAKP + `","alg":"` + jwkAlg + `","pub":"AAAA","kid":"x"}`,
		"pub empty":         `{"kty":"` + jwkKtyAKP + `","alg":"` + jwkAlg + `","pub":""}`,
		"pub short":         `{"kty":"` + jwkKtyAKP + `","alg":"` + jwkAlg + `","pub":"` + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 100)) + `"}`,
		"pub oversized":     `{"kty":"` + jwkKtyAKP + `","alg":"` + jwkAlg + `","pub":"` + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, PublicKeyBytes+1)) + `"}`,
		"pub not base64":    `{"kty":"` + jwkKtyAKP + `","alg":"` + jwkAlg + `","pub":"!!!!"}`,
	}
	for name, jwk := range cases {
		t.Run(name, func(t *testing.T) {
			require.Error(t, NewProvider().ValidateConfig(context.Background(), build(jwk)))
		})
	}
}

func TestEncryptRejectsUnusablePlaintext(t *testing.T) {
	ctx := context.Background()
	config := vectorConfig(t)
	for name, plaintext := range map[string]*v2.PlaintextData{
		"nil":      nil,
		"no name":  v2.PlaintextData_builder{Bytes: []byte("v")}.Build(),
		"no bytes": v2.PlaintextData_builder{Name: "api_key"}.Build(),
		"too large": v2.PlaintextData_builder{
			Name:  "api_key",
			Bytes: bytes.Repeat([]byte("a"), MaxPlaintextBytes+1),
		}.Build(),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewProvider().Encrypt(ctx, config, plaintext)
			require.Error(t, err)
		})
	}
}

func TestPublicKeyThumbprintMatchesLatchkeyVector(t *testing.T) {
	// The Latchkey proto documents this exact vector: pub "AQID" hashes the
	// canonical {"alg","kty","pub"} object to this digest.
	jwk := `{"kty":"AKP","alg":"` + jwkAlg + `","pub":"AQID"}`
	thumbprint, err := publicKeyThumbprint(jwk)
	require.NoError(t, err)
	require.Equal(t, "YHoF2Vc1mqHO84GuAfiFVAX5UwAFBFJFitEkeGRz-9E", thumbprint)
}

func TestLowOrderX25519PublicKeyFailsTheEcdhProbe(t *testing.T) {
	raw := bytes.Repeat([]byte{1}, PublicKeyBytes)
	copy(raw[len(raw)-32:], make([]byte, 32))
	probe, err := ecdh.X25519().NewPrivateKey(make([]byte, 32))
	require.NoError(t, err)
	x25519, err := ecdh.X25519().NewPublicKey(raw[len(raw)-32:])
	require.NoError(t, err)
	_, err = probe.ECDH(x25519)
	require.Error(t, err)
}

func openVectorEnvelope(t *testing.T, config *v2.EncryptionConfig, enc, ciphertext string) []byte {
	t.Helper()
	rawEnc, err := base64.RawURLEncoding.DecodeString(enc)
	require.NoError(t, err)
	rawCiphertext, err := base64.RawURLEncoding.DecodeString(ciphertext)
	require.NoError(t, err)
	binding := bindingBytes(config.GetVaultInboxRecipientConfig())
	recipient, err := hpke.NewRecipient(rawEnc, vectorPrivateKey(t), hpke.HKDFSHA256(), hpke.ChaCha20Poly1305(), binding)
	require.NoError(t, err)
	plaintext, err := recipient.Open(binding, rawCiphertext)
	require.NoError(t, err)
	return plaintext
}

func mustDecodeB64(t *testing.T, value string) []byte {
	t.Helper()
	raw, err := base64.RawURLEncoding.DecodeString(value)
	require.NoError(t, err)
	return raw
}

func jwkPub(t *testing.T, jwkJSON string) string {
	t.Helper()
	var jwk struct {
		Pub string `json:"pub"`
	}
	require.NoError(t, json.Unmarshal([]byte(jwkJSON), &jwk))
	return jwk.Pub
}
