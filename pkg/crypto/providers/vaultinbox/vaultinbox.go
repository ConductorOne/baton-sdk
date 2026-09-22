// Package vaultinbox seals a connector's plaintext credential into an existing
// C1 vault-inbox submission.
//
// This provider is the connector-side half of the full-knowledge vault-inbox
// delivery transport. C1 chooses the destination vault and the binding
// coordinates from the approved ticket and the live inbox profile, hands them
// over in a VaultInboxRecipientConfig, and the connector produces the exact
// ciphertext the shipped Latchkey inbox reader already opens:
//
//   - the plaintext is a SecretSubmissionPayloadV3 JSON container, the only
//     payload shape `decode_secret_submission_payload_for_open` accepts, with
//     the server-allocated submission id bound inside it;
//   - the HPKE instance is Base / X-Wing (ML-KEM-768 + X25519) / HKDF-SHA256 /
//     ChaCha20-Poly1305, the one suite `vault_inbox_hpke` constructs; and
//   - the HPKE info and AEAD AAD are the same bytes: the Latchkey injective
//     `u32-be(len) || field` framing of the domain label followed by the
//     envelope version, suite id, tenant, vault, inbox key id, key generation,
//     and payload scheme (see crates/latchkey-mls-core/src/vault_inbox.rs
//     `binding_bytes` and src/framing.rs `framed`).
//
// EncryptedData carries the JSON VaultInboxSubmissionEnvelope, and key_ids
// carries exactly the inbox key id so C1 can reject a ciphertext sealed to
// anything else. Nothing here mints a key, signs anything, or invents a
// profile: the trust root for the recipient key stays with the owner-device
// attestation the Latchkey client verifies, and C1 remains the authority that
// the vended key is the active one for the destination vault.
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
	// MaxPlaintextBytes mirrors VAULT_INBOX_SUBMISSION_SIZE_LIMIT_BYTES in the
	// Rust client. The sealed envelope must still fit the inbox size limit, so
	// the plaintext bound is strictly below it.
	MaxPlaintextBytes = 1 << 20

	jwkKtyAKP = "AKP"
	// jwkAlg is the exact `alg` the Latchkey inbox JWK header must carry.
	jwkAlg = "HPKE-Base-X-Wing-Draft06-HKDF-SHA256-ChaCha20Poly1305"

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
		defer clear(payload)
		defer clear(binding)
		info := hpke.HKDFSHA256()
		aead := hpke.ChaCha20Poly1305()
		var sender *hpke.Sender
		enc, sender, err = hpke.NewSender(publicKey, info, aead, binding)
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

	return v2.EncryptedData_builder{
		Provider:       EncryptionProvider,
		Name:           plaintext.GetName(),
		Description:    plaintext.GetDescription(),
		Schema:         plaintext.GetSchema(),
		EncryptedBytes: envelope,
		// The inbox key id, not the JWK thumbprint: C1 compares this against the
		// submission's active key id, and the thumbprint is already bound inside
		// the sealed binding and the attestation.
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
	if len(conf.ProtoReflect().GetUnknown()) != 0 || len(config.ProtoReflect().GetUnknown()) != 0 {
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
		{"payload_scheme", config.GetPayloadScheme()},
		{"submission_id", config.GetSubmissionId()},
		{"public_key_thumbprint", config.GetPublicKeyThumbprint()},
	} {
		if err := validateIdentifier(field.name, field.value); err != nil {
			return nil, nil, err
		}
	}
	if config.GetKeyGeneration() == 0 {
		return nil, nil, invalid("key_generation must be non-zero")
	}
	if len(config.GetContentType()) > maxContentBytes || strings.ContainsFunc(config.GetContentType(), unicode.IsControl) {
		return nil, nil, invalid("invalid content_type")
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
func parsePublicKey(jwkJSON string) (hpke.PublicKey, error) {
	var jwk struct {
		Kty  string `json:"kty"`
		Alg  string `json:"alg"`
		Pub  string `json:"pub"`
		Priv string `json:"priv"`
	}
	decoder := json.NewDecoder(strings.NewReader(jwkJSON))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&jwk); err != nil {
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
	canonical := fmt.Sprintf(`{"alg":"%s","kty":"%s","pub":"%s"}`, jwkAlg, jwkKtyAKP, jwk.Pub)
	digest := sha256.Sum256([]byte(canonical))
	return base64URL.EncodeToString(digest[:]), nil
}

// bindingBytes reproduces the Latchkey injective framing used as both the HPKE
// info and the AEAD AAD. Field order is load-bearing: domain label, envelope
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
