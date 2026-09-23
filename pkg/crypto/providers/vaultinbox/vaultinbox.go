// Package vaultinbox encrypts credentials using the Latchkey inbox wire format.
// It trusts recipient configuration from the authenticated C1 action transport;
// it does not verify recipient attestation signatures. C1's authorized member
// runtime ingests the ciphertext as a native full-knowledge vault secret.
// See docs/vault-inbox-delivery.md for the wire contract and trust boundaries.
package vaultinbox

import (
	"context"
	"crypto/ecdh"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"filippo.io/hpke"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// EncryptionProvider is the EncryptedData.provider and EncryptionConfig.provider
// value for the vault-inbox profile.
const EncryptionProvider = "baton/vault-inbox/v1"

const (
	// PublicKeyBytes is the encoded X-Wing encapsulation key: the ML-KEM-768
	// encapsulation key (1184) followed by the X25519 public key (32).
	PublicKeyBytes = 1216
	// MaxPlaintextBytes bounds the credential value before any expansion.
	// VAULT_INBOX_SUBMISSION_SIZE_LIMIT_BYTES caps the *sealed envelope* at 2 MiB,
	// and both the value and the ciphertext are base64url-expanded (4/3) on the
	// way, so a value near that ceiling would build an envelope the inbox cannot
	// accept. The post-seal envelope check below is the authoritative bound; this
	// one only rejects absurd inputs before any crypto runs.
	MaxPlaintextBytes = 1 << 20
	// MaxSubmissionEnvelopeBytes mirrors the Latchkey inbox's
	// VAULT_INBOX_SUBMISSION_SIZE_LIMIT_BYTES, the hard cap on the bytes that are
	// actually uploaded.
	MaxSubmissionEnvelopeBytes = 2 * 1024 * 1024
	// maxNameBytes and maxDescriptionBytes mirror the submission row's
	// safe_display_name / safe_description limits, so an oversized connector
	// string cannot mint a credential whose submission is rejected later.
	maxNameBytes        = 255
	maxDescriptionBytes = 1024

	jwkKtyAKP = "AKP"
	// jwkAlg is the exact `alg` the Latchkey inbox JWK header must carry.
	jwkAlg = "HPKE-Base-X-Wing-Draft06-HKDF-SHA256-ChaCha20Poly1305"

	// PayloadSchemeSecretV1 is the only payload scheme this provider emits, and
	// the label the Latchkey client SDK binds into the HPKE AAD. It is a protocol
	// identifier, not a credential.
	PayloadSchemeSecretV1 = "latchkey.vault_submission.secret.v1" //nolint:gosec // G101: a protocol scheme label, not a secret

	// infoPrefix is the HPKE info/AAD domain label.
	infoPrefix = "latchkey/v1/vault-inbox-submission/info"
	// envelopeVersion is the only vault-inbox submission envelope version.
	envelopeVersion = 2
	// payloadVersionV3 is the only secret-submission payload version the open
	// path accepts; v1/v2 carry no submission-id binding and are rejected.
	payloadVersionV3 = 3
	// contentTypeGeneric is what an empty declared content type normalizes to,
	// matching content_type_or_generic in the Rust SDK.
	contentTypeGeneric = "generic"

	maxIDBytes      = 1024
	maxContentBytes = 128
	// maxJWKBytes bounds the one config field handled by raw JSON parsing. A
	// canonical AKP JWK for a 1216-byte key is about 1.7 KB.
	maxJWKBytes = 16 * 1024
)

var base64URL = base64.RawURLEncoding

// Provider seals plaintext credentials to a vault-inbox recipient.
type Provider struct{}

// NewProvider returns the vault-inbox encryption provider.
func NewProvider() *Provider { return &Provider{} }

// ValidateConfig refuses a malformed, unknown, or unsupported config before any
// provider work happens. It is called by crypto.ValidateEncryptionConfigs ahead
// of an irreversible credential issuance.
func (p *Provider) ValidateConfig(_ context.Context, conf *v2.EncryptionConfig) error {
	_, _, err := recipientFromConfig(conf)
	return err
}

// Encrypt builds the vault-inbox secret-submission payload for one plaintext
// value and seals it to the configured inbox key.
func (p *Provider) Encrypt(ctx context.Context, conf *v2.EncryptionConfig, plaintext *v2.PlaintextData) (*v2.EncryptedData, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	config, publicKey, err := recipientFromConfig(conf)
	if err != nil {
		return nil, err
	}
	if plaintext == nil || strings.TrimSpace(plaintext.GetName()) == "" {
		return nil, invalid("plaintext value must have a name")
	}
	if len(plaintext.GetName()) > maxNameBytes {
		return nil, invalid(fmt.Sprintf("plaintext name must be at most %d bytes", maxNameBytes))
	}
	if len(plaintext.GetDescription()) > maxDescriptionBytes {
		return nil, invalid(fmt.Sprintf("plaintext description must be at most %d bytes", maxDescriptionBytes))
	}
	value := plaintext.GetBytes()
	if len(value) == 0 || len(value) > MaxPlaintextBytes {
		return nil, invalid("credential value must contain 1..1048576 bytes")
	}

	payload, err := encodeSecretSubmissionPayloadV3(
		config.GetSubmissionId(),
		plaintext.GetName(),
		plaintext.GetDescription(),
		config.GetContentType(),
		value,
	)
	if err != nil {
		return nil, err
	}

	binding := bindingBytes(config)
	var enc, ciphertext []byte
	func() {
		// Best-effort clearing; JSON and base64 encoding may retain plaintext copies.
		defer clear(payload)
		defer clear(binding)
		kdf := hpke.HKDFSHA256()
		aead := hpke.ChaCha20Poly1305()
		var sender *hpke.Sender
		// Latchkey uses the same binding for HPKE info and AEAD AAD.
		enc, sender, err = hpke.NewSender(publicKey, kdf, aead, binding)
		if err != nil {
			return
		}
		ciphertext, err = sender.Seal(binding, payload)
	}()
	if err != nil {
		return nil, fmt.Errorf("vault inbox: seal submission: %w", err)
	}

	envelope, err := json.Marshal(submissionEnvelope{
		Version:    envelopeVersion,
		Alg:        jwkAlg,
		Enc:        base64URL.EncodeToString(enc),
		Ciphertext: base64URL.EncodeToString(ciphertext),
	})
	clear(enc)
	if err != nil {
		return nil, fmt.Errorf("vault inbox: encode submission envelope: %w", err)
	}
	// Enforce the transport limit on the encoded envelope, not just the value.
	if len(envelope) > MaxSubmissionEnvelopeBytes {
		return nil, invalid(fmt.Sprintf(
			"sealed submission envelope must be at most %d bytes, got %d",
			MaxSubmissionEnvelopeBytes, len(envelope)))
	}

	return v2.EncryptedData_builder{
		Provider:       EncryptionProvider,
		Name:           plaintext.GetName(),
		Description:    plaintext.GetDescription(),
		Schema:         plaintext.GetSchema(),
		EncryptedBytes: envelope,
		// The inbox key ID, not the JWK thumbprint.
		KeyIds: []string{config.GetInboxKeyId()},
	}.Build(), nil
}

// recipientFromConfig validates every field the provider depends on and returns
// both the config and the parsed HPKE recipient.
func recipientFromConfig(conf *v2.EncryptionConfig) (*v2.VaultInboxRecipientConfig, hpke.PublicKey, error) {
	if conf == nil {
		return nil, nil, invalid("encryption config is required")
	}
	config := conf.GetVaultInboxRecipientConfig()
	if config == nil {
		return nil, nil, invalid("vault inbox recipient config is required")
	}
	if name := strings.ToLower(strings.TrimSpace(conf.GetProvider())); name != "" && name != EncryptionProvider {
		return nil, nil, invalid("provider does not match vault inbox config")
	}
	// Unknown profile fields may change authenticated semantics; outer config
	// fields remain additive for compatibility with other providers.
	if len(config.ProtoReflect().GetUnknown()) != 0 {
		return nil, nil, invalid("unknown config fields")
	}
	if config.GetConfigVersion() != v2.VaultInboxConfigVersion_VAULT_INBOX_CONFIG_VERSION_V1 {
		return nil, nil, invalid("unsupported config version")
	}
	if config.GetSuite() != v2.VaultInboxSuite_VAULT_INBOX_SUITE_XWING_MLKEM768_X25519_HKDF_SHA256_CHACHA20POLY1305_V1 {
		return nil, nil, invalid("unsupported vault inbox suite")
	}
	for _, field := range []struct{ name, value string }{
		{"tenant_id", config.GetTenantId()},
		{"vault_boundary_id", config.GetVaultBoundaryId()},
		{"inbox_key_id", config.GetInboxKeyId()},
		{"submission_id", config.GetSubmissionId()},
		{"public_key_thumbprint", config.GetPublicKeyThumbprint()},
	} {
		if err := validateIdentifier(field.name, field.value); err != nil {
			return nil, nil, err
		}
	}
	// The authenticated scheme must match the payload this provider emits.
	if config.GetPayloadScheme() != PayloadSchemeSecretV1 {
		return nil, nil, invalid("unsupported payload_scheme")
	}
	if config.GetKeyGeneration() == 0 {
		return nil, nil, invalid("key_generation must be non-zero")
	}
	if len(config.GetContentType()) > maxContentBytes || strings.ContainsFunc(config.GetContentType(), unicode.IsControl) {
		return nil, nil, invalid("invalid content_type")
	}
	if len(config.GetPublicJwkJson()) > maxJWKBytes {
		return nil, nil, invalid(fmt.Sprintf("public_jwk_json must be at most %d bytes", maxJWKBytes))
	}

	thumbprint, err := publicKeyThumbprint(config.GetPublicJwkJson())
	if err != nil {
		return nil, nil, err
	}
	if config.GetPublicKeyThumbprint() != thumbprint {
		return nil, nil, invalid("public_key_thumbprint does not match public_jwk_json")
	}
	publicKey, err := parsePublicKey(config.GetPublicJwkJson())
	if err != nil {
		return nil, nil, err
	}
	return config, publicKey, nil
}

// parsePublicKey accepts exactly one public AKP JWK holding an X-Wing key.
//
// Extra JOSE members are allowed; the thumbprint covers only alg, kty, and pub.
func parsePublicKey(jwkJSON string) (hpke.PublicKey, error) {
	var jwk struct {
		Kty  string `json:"kty"`
		Alg  string `json:"alg"`
		Pub  string `json:"pub"`
		Priv string `json:"priv"`
	}
	if err := json.Unmarshal([]byte(jwkJSON), &jwk); err != nil {
		return nil, invalid("public_jwk_json is not a public AKP JWK")
	}
	if jwk.Kty != jwkKtyAKP {
		return nil, invalid("public_jwk_json kty is not AKP")
	}
	if jwk.Alg != jwkAlg {
		return nil, invalid("public_jwk_json alg is not the vault inbox suite")
	}
	if jwk.Priv != "" {
		return nil, invalid("public_jwk_json must not carry private material")
	}
	if jwk.Pub == "" {
		return nil, invalid("public_jwk_json pub is required")
	}
	raw, err := base64URL.DecodeString(jwk.Pub)
	if err != nil {
		return nil, invalid("public_jwk_json pub is not base64url")
	}
	if len(raw) != PublicKeyBytes {
		return nil, invalid(fmt.Sprintf("public key must be exactly %d bytes", PublicKeyBytes))
	}
	publicKey, err := hpke.MLKEM768X25519().NewPublicKey(raw)
	if err != nil {
		return nil, invalid("public_jwk_json pub is not an X-Wing key")
	}
	// Parsing alone accepts low-order X25519 points, which would collapse the
	// hybrid secret. Probe the X25519 component before the key is used.
	probe, err := ecdh.X25519().NewPrivateKey(make([]byte, 32))
	if err != nil {
		return nil, invalid("cannot validate public key")
	}
	x25519, err := ecdh.X25519().NewPublicKey(raw[len(raw)-32:])
	if err != nil {
		return nil, invalid("public_jwk_json pub is not an X-Wing key")
	}
	if _, err := probe.ECDH(x25519); err != nil {
		return nil, invalid("public_jwk_json pub is not a valid X-Wing key")
	}
	return publicKey, nil
}

// Field order matches Latchkey's thumbprint encoding.
type canonicalInboxJWK struct {
	Alg string `json:"alg"`
	Kty string `json:"kty"`
	Pub string `json:"pub"`
}

// publicKeyThumbprint re-derives the profile thumbprint exactly as the Latchkey
// core does: base64url(SHA-256(`{"alg":..,"kty":..,"pub":..}`)).
func publicKeyThumbprint(jwkJSON string) (string, error) {
	var jwk struct {
		Alg string `json:"alg"`
		Kty string `json:"kty"`
		Pub string `json:"pub"`
	}
	decoder := json.NewDecoder(strings.NewReader(jwkJSON))
	if err := decoder.Decode(&jwk); err != nil {
		return "", invalid("public_jwk_json is not a JWK")
	}
	if jwk.Alg != jwkAlg || jwk.Kty != jwkKtyAKP || jwk.Pub == "" {
		return "", invalid("public_jwk_json does not describe a vault inbox key")
	}
	canonical, err := json.Marshal(canonicalInboxJWK{Alg: jwk.Alg, Kty: jwk.Kty, Pub: jwk.Pub})
	if err != nil {
		return "", invalid("public_jwk_json cannot be canonicalized")
	}
	digest := sha256.Sum256(canonical)
	return base64URL.EncodeToString(digest[:]), nil
}

// bindingBytes reproduces the Latchkey injective framing used as both the HPKE
// info and the AEAD AAD. Field order is fixed: domain label, envelope
// version (one byte), suite id, tenant, vault, inbox key id, key generation as
// ASCII decimal, payload scheme.
func bindingBytes(config *v2.VaultInboxRecipientConfig) []byte {
	version := []byte{envelopeVersion}
	generation := []byte(fmt.Sprintf("%d", config.GetKeyGeneration()))
	return framed(infoPrefix, [][]byte{
		version,
		[]byte(jwkAlg),
		[]byte(config.GetTenantId()),
		[]byte(config.GetVaultBoundaryId()),
		[]byte(config.GetInboxKeyId()),
		generation,
		[]byte(config.GetPayloadScheme()),
	})
}

// framed writes u32-be(len) || bytes for the domain label and then each field,
// matching crates/latchkey-mls-core/src/framing.rs.
func framed(domain string, fields [][]byte) []byte {
	size := 4 + len(domain)
	for _, field := range fields {
		size += 4 + len(field)
	}
	out := make([]byte, 0, size)
	out = appendField(out, []byte(domain))
	for _, field := range fields {
		out = appendField(out, field)
	}
	return out
}

func appendField(dst, field []byte) []byte {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(field))) //nolint:gosec // G115: bounded identifiers and fixed literals
	dst = append(dst, length[:]...)
	return append(dst, field...)
}

type submissionEnvelope struct {
	Version    uint8  `json:"version"`
	Alg        string `json:"alg"`
	Enc        string `json:"enc"`
	Ciphertext string `json:"ciphertext"`
}

// secretSubmissionPayloadV3 mirrors crate::latchkey_client_sdk::vault_inbox
// `SecretSubmissionPayloadV3`. Field order and names are the serde contract.
type secretSubmissionPayloadV3 struct {
	Version      uint8             `json:"version"`
	SubmissionID string            `json:"submission_id"`
	DisplayName  string            `json:"display_name"`
	Description  string            `json:"description"`
	ContentType  string            `json:"content_type"`
	Annotations  map[string]string `json:"annotations"`
	ValueB64     string            `json:"value_b64"`
}

func encodeSecretSubmissionPayloadV3(
	submissionID, displayName, description, contentType string,
	value []byte,
) ([]byte, error) {
	if submissionID == "" {
		return nil, invalid("submission_id is required")
	}
	normalized := contentType
	if normalized == "" {
		normalized = contentTypeGeneric
	}
	return json.Marshal(secretSubmissionPayloadV3{
		Version:      payloadVersionV3,
		SubmissionID: submissionID,
		DisplayName:  displayName,
		Description:  description,
		ContentType:  normalized,
		Annotations:  map[string]string{},
		ValueB64:     base64URL.EncodeToString(value),
	})
}

func validateIdentifier(name, value string) error {
	if value == "" || len(value) > maxIDBytes || !utf8.ValidString(value) ||
		strings.TrimSpace(value) != value || strings.ContainsFunc(value, unicode.IsControl) {
		return invalid("invalid " + name)
	}
	return nil
}

func invalid(message string) error {
	return status.Error(codes.InvalidArgument, "vault inbox: "+message)
}
