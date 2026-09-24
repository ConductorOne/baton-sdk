// Package vaultinbox encrypts credentials using the Latchkey inbox wire format.
// It trusts recipient configuration from the authenticated C1 action transport;
// it does not verify recipient attestation signatures. C1's authorized member
// runtime ingests the ciphertext as a native full-knowledge vault secret.
// See docs/vault-inbox-delivery.md for the wire contract and trust boundaries.
package vaultinbox

import (
	"context"
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
		config.SubmissionID,
		plaintext.GetName(),
		plaintext.GetDescription(),
		config.ContentType,
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
		KeyIds: []string{config.InboxKeyID},
	}.Build(), nil
}

// Field order matches Latchkey's thumbprint encoding.
type canonicalInboxJWK struct {
	Alg string `json:"alg"`
	Kty string `json:"kty"`
	Pub string `json:"pub"`
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
