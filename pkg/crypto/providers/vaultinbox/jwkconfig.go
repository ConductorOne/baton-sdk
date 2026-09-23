package vaultinbox

import (
	"bytes"
	"crypto/ecdh"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode"

	"filippo.io/hpke"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// JWKExtensionMember is the JWK member that carries the vault-inbox binding
// context. It is a namespaced protocol extension rather than a JOSE-registered
// member, so a reader that does not know this profile cannot mistake it for
// standard key metadata.
const JWKExtensionMember = "baton_vault_inbox"

// JWKExtensionVersion is the only extension shape this provider implements. A
// version it does not implement is refused before the provider runs, so an older
// connector never silently seals a newer profile.
const JWKExtensionVersion = 1

// SuiteLabel is the HPKE suite the extension must name. The provider requires it
// to agree with the JWK header's `alg`, so one document cannot describe the
// suite in its key header and a different one in its context.
const SuiteLabel = jwkAlg

// AdvertisedSuite is the capability value this profile corresponds to on
// CredentialIssueOptionDescriptor. The suite enum stays in the proto because
// capability advertisement still needs it; only the config message that used to
// carry a suite field is removed by this alternative.
const AdvertisedSuite = v2.VaultInboxSuite_VAULT_INBOX_SUITE_XWING_MLKEM768_X25519_HKDF_SHA256_CHACHA20POLY1305_V1

// jwkExtension is the typed protocol extension carried on the recipient JWK.
//
// Every field is required and is validated before any provider work. The inbox
// key id is deliberately absent: EncryptedData.key_id / EncryptionConfig.key_id
// is the single authoritative inbox key id, and duplicating it here would allow
// two sources to disagree about which key a ciphertext is bound to.
type jwkExtension struct {
	Version             uint32 `json:"version"`
	Suite               string `json:"suite"`
	TenantID            string `json:"tenant_id"`
	VaultBoundaryID     string `json:"vault_boundary_id"`
	KeyGeneration       uint64 `json:"key_generation"`
	PayloadScheme       string `json:"payload_scheme"`
	SubmissionID        string `json:"submission_id"`
	PublicKeyThumbprint string `json:"public_key_thumbprint"`
	ContentType         string `json:"content_type"`
}

// recipient is the validated binding context the provider seals against. It
// replaces the config message the alternative removes: same fields, same order
// in the binding, but parsed from the JWK extension rather than from protobuf.
type recipient struct {
	TenantID            string
	VaultBoundaryID     string
	InboxKeyID          string
	KeyGeneration       uint64
	PayloadScheme       string
	SubmissionID        string
	PublicKeyThumbprint string
	ContentType         string
}

// jwkDocument is the outer recipient JWK plus its parsed extension.
type jwkDocument struct {
	Alg string
	Kty string
	Pub string
	Kid string
	Ext jwkExtension
}

// recipientFromConfig validates every field the provider depends on and returns
// the binding context plus the parsed HPKE recipient.
//
// The provider is selected explicitly. Inbox mode is never inferred from the key
// type, and a document that carries the extension without naming this provider is
// refused rather than falling through to classical JWK encryption.
func recipientFromConfig(conf *v2.EncryptionConfig) (*recipient, hpke.PublicKey, error) {
	if conf == nil {
		return nil, nil, invalid("encryption config is required")
	}
	if name := strings.ToLower(strings.TrimSpace(conf.GetProvider())); name != EncryptionProvider {
		return nil, nil, invalid("provider does not match vault inbox config")
	}
	jwkConfig := conf.GetJwkPublicKeyConfig()
	if jwkConfig == nil {
		return nil, nil, invalid("vault inbox recipient requires a jwk public key config")
	}
	// Unknown fields on the provider-specific config are refused: its contents
	// are frozen into the HPKE binding. Unknown fields on the shared
	// EncryptionConfig stay tolerated so that message remains additive for every
	// other provider.
	if len(jwkConfig.ProtoReflect().GetUnknown()) != 0 {
		return nil, nil, invalid("unknown config fields")
	}
	if err := validateIdentifier("key_id", conf.GetKeyId()); err != nil {
		return nil, nil, err
	}
	pubKeyJSON := string(jwkConfig.GetPubKey())
	if len(pubKeyJSON) > maxJWKBytes {
		return nil, nil, invalid(fmt.Sprintf("public_jwk_json must be at most %d bytes", maxJWKBytes))
	}

	document, err := parseJWKDocument(pubKeyJSON)
	if err != nil {
		return nil, nil, err
	}
	// key_id is the single authoritative inbox key id. A JWK that names a
	// different key in its `kid` is refused rather than silently overridden,
	// because two sources for that value could disagree about which key a
	// ciphertext is bound to.
	if document.Kid != "" && document.Kid != conf.GetKeyId() {
		return nil, nil, invalid("public_jwk_json kid does not match key_id")
	}
	publicKey, err := publicKeyFromRaw(document.Pub)
	if err != nil {
		return nil, nil, err
	}

	// The thumbprint is re-derived from the JWK and compared, so a document
	// cannot claim a thumbprint that does not belong to the key it names. It
	// covers alg/kty/pub only: the context extension is deliberately outside it,
	// so adding or removing context cannot move a legitimate key's thumbprint.
	thumbprint, err := canonicalThumbprint(document.Alg, document.Kty, document.Pub)
	if err != nil {
		return nil, nil, err
	}
	if document.Ext.PublicKeyThumbprint != thumbprint {
		return nil, nil, invalid("public_key_thumbprint does not match the public JWK")
	}

	ext := document.Ext
	for _, field := range []struct{ name, value string }{
		{"tenant_id", ext.TenantID},
		{"vault_boundary_id", ext.VaultBoundaryID},
		{"submission_id", ext.SubmissionID},
		{"public_key_thumbprint", ext.PublicKeyThumbprint},
	} {
		if err := validateIdentifier(field.name, field.value); err != nil {
			return nil, nil, err
		}
	}
	if ext.PayloadScheme != PayloadSchemeSecretV1 {
		return nil, nil, invalid("unsupported payload_scheme")
	}
	if ext.KeyGeneration == 0 {
		return nil, nil, invalid("key_generation must be non-zero")
	}
	if len(ext.ContentType) > maxContentBytes || strings.ContainsFunc(ext.ContentType, unicode.IsControl) {
		return nil, nil, invalid("invalid content_type")
	}

	return &recipient{
		TenantID:            ext.TenantID,
		VaultBoundaryID:     ext.VaultBoundaryID,
		InboxKeyID:          conf.GetKeyId(),
		KeyGeneration:       ext.KeyGeneration,
		PayloadScheme:       ext.PayloadScheme,
		SubmissionID:        ext.SubmissionID,
		PublicKeyThumbprint: ext.PublicKeyThumbprint,
		ContentType:         ext.ContentType,
	}, publicKey, nil
}

// parseJWKDocument parses exactly one JSON object holding a public AKP JWK and
// the vault-inbox extension.
//
// Ordinary optional JOSE members are accepted, because rejecting unknown
// metadata would break documents that are otherwise valid. What is *not*
// accepted is ambiguity: a member repeated in the same object, a member the
// extension does not define, and any content after the object. Go's decoder
// resolves a repeated member by taking the last one, which would let two readers
// of the same bytes disagree about the key or the binding, so duplicates are
// detected here rather than inherited.
func parseJWKDocument(jwkJSON string) (*jwkDocument, error) {
	outerKeys, err := objectMemberNames([]byte(jwkJSON))
	if err != nil {
		return nil, invalid("public_jwk_json is not a single JSON object")
	}
	if dup := firstDuplicate(outerKeys); dup != "" {
		return nil, invalid("public_jwk_json repeats the member " + dup)
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(jwkJSON), &fields); err != nil {
		return nil, invalid("public_jwk_json is not a public AKP JWK")
	}

	document := &jwkDocument{}
	if err := unmarshalStrictMember(fields, "alg", &document.Alg); err != nil {
		return nil, err
	}
	if err := unmarshalStrictMember(fields, "kty", &document.Kty); err != nil {
		return nil, err
	}
	if err := unmarshalStrictMember(fields, "pub", &document.Pub); err != nil {
		return nil, err
	}
	// `kid` is an ordinary optional JOSE member. It is read only so a later
	// check can reject a value that disagrees with the authoritative key_id;
	// the binding never consumes it.
	if err := unmarshalStrictMember(fields, "kid", &document.Kid); err != nil {
		return nil, err
	}
	// Private material is refused explicitly rather than ignored. A `priv`
	// member could carry material the caller believes is being used.
	if rawPrivate, present := fields["priv"]; present {
		var probe string
		if err := json.Unmarshal(rawPrivate, &probe); err == nil && strings.TrimSpace(probe) != "" {
			return nil, invalid("public_jwk_json must not carry private material")
		}
	}

	if document.Kty != jwkKtyAKP {
		return nil, invalid("public_jwk_json kty is not AKP")
	}
	if document.Alg != jwkAlg {
		return nil, invalid("public_jwk_json alg is not the vault inbox suite")
	}
	if document.Pub == "" {
		return nil, invalid("public_jwk_json pub is required")
	}

	rawExtension, present := fields[JWKExtensionMember]
	if !present {
		return nil, invalid("public_jwk_json is missing the " + JWKExtensionMember + " extension")
	}
	ext, err := parseExtension(rawExtension)
	if err != nil {
		return nil, err
	}
	document.Ext = *ext
	return document, nil
}

// parseExtension decodes the namespaced extension strictly: unknown members,
// repeated members, trailing content, and a wrong version are all refused. The
// extension is a protocol surface, so an unrecognised member means the document
// was written for a shape this provider cannot honour, and accepting it would
// seal against coordinates it did not fully understand.
func parseExtension(raw json.RawMessage) (*jwkExtension, error) {
	names, err := objectMemberNames(raw)
	if err != nil {
		return nil, invalid("the " + JWKExtensionMember + " extension is not a JSON object")
	}
	if dup := firstDuplicate(names); dup != "" {
		return nil, invalid("the " + JWKExtensionMember + " extension repeats the member " + dup)
	}
	for _, name := range names {
		if !extensionMembers[name] {
			return nil, invalid("the " + JWKExtensionMember + " extension has an unknown member " + name)
		}
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var ext jwkExtension
	if err := decoder.Decode(&ext); err != nil {
		return nil, invalid("the " + JWKExtensionMember + " extension is malformed")
	}
	// Anything after the extension object is refused: a second value could carry
	// context the parser did not read.
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, invalid("the " + JWKExtensionMember + " extension has trailing content")
	}
	if ext.Version != JWKExtensionVersion {
		return nil, invalid("unsupported vault inbox config version")
	}
	if ext.Suite != SuiteLabel {
		return nil, invalid("the " + JWKExtensionMember + " suite is not the vault inbox suite")
	}
	return &ext, nil
}

// extensionMembers is the complete set of members the extension defines. It is
// kept beside the struct so adding a field to one without the other is visible
// rather than silent.
var extensionMembers = map[string]bool{
	"version":               true,
	"suite":                 true,
	"tenant_id":             true,
	"vault_boundary_id":     true,
	"key_generation":        true,
	"payload_scheme":        true,
	"submission_id":         true,
	"public_key_thumbprint": true,
	"content_type":          true,
}

// unmarshalStrictMember decodes one member, refusing a JSON null and any type
// mismatch instead of leaving the field at its zero value. A missing member is
// not an error here: the caller validates the value, and a zero value fails that
// validation for every field this document requires.
func unmarshalStrictMember(fields map[string]json.RawMessage, name string, target *string) error {
	raw, present := fields[name]
	if !present {
		return nil
	}
	if string(bytes.TrimSpace(raw)) == "null" {
		return invalid("public_jwk_json member " + name + " must not be null")
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return invalid("public_jwk_json member " + name + " is not a string")
	}
	return nil
}

// objectMemberNames returns the member names of exactly one JSON object, in
// order, and fails if the input is not a single object followed by nothing.
func objectMemberNames(raw []byte) ([]string, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if delim, ok := token.(json.Delim); !ok || delim != '{' {
		return nil, errors.New("not a JSON object")
	}
	var names []string
	for {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		if delim, ok := token.(json.Delim); ok {
			if delim == '}' {
				break
			}
			return nil, errors.New("unexpected nested object")
		}
		name, ok := token.(string)
		if !ok {
			return nil, errors.New("object member name is not a string")
		}
		names = append(names, name)
		if err := skipValue(decoder); err != nil {
			return nil, err
		}
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing content after the JSON object")
	}
	return names, nil
}

// skipValue consumes one complete JSON value, including nested containers.
func skipValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	if delim != '{' && delim != '[' {
		return errors.New("unexpected delimiter")
	}
	depth := 1
	for depth > 0 {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		if inner, ok := token.(json.Delim); ok {
			switch inner {
			case '{', '[':
				depth++
			case '}', ']':
				depth--
			}
		}
	}
	return nil
}

// firstDuplicate returns the first repeated name, or "" when all are unique.
func firstDuplicate(names []string) string {
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		if seen[name] {
			return name
		}
		seen[name] = true
	}
	return ""
}

// canonicalThumbprint is base64url(SHA-256(`{"alg":..,"kty":..,"pub":..}`)),
// matching Latchkey's encoding. Field order follows canonicalInboxJWK.
func canonicalThumbprint(alg, kty, pub string) (string, error) {
	canonical, err := json.Marshal(canonicalInboxJWK{Alg: alg, Kty: kty, Pub: pub})
	if err != nil {
		return "", invalid("public_jwk_json cannot be canonicalized")
	}
	digest := sha256.Sum256(canonical)
	return base64URL.EncodeToString(digest[:]), nil
}

// publicKeyFromRaw accepts exactly one public AKP JWK holding an X-Wing key.
func publicKeyFromRaw(pub string) (hpke.PublicKey, error) {
	raw, err := base64URL.DecodeString(pub)
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

// bindingBytes reproduces the Latchkey injective framing used as both the HPKE
// info and the AEAD AAD. Field order is fixed: domain label, envelope
// version (one byte), suite id, tenant, vault, inbox key id, key generation as
// ASCII decimal, payload scheme.
//
// The bytes depend on the binding values and not on where they were parsed from,
// which is what makes the JWK-configured provider byte-identical to the config
// message it replaces.
func bindingBytes(config *recipient) []byte {
	version := []byte{envelopeVersion}
	generation := []byte(fmt.Sprintf("%d", config.KeyGeneration))
	return framed(infoPrefix, [][]byte{
		version,
		[]byte(jwkAlg),
		[]byte(config.TenantID),
		[]byte(config.VaultBoundaryID),
		[]byte(config.InboxKeyID),
		generation,
		[]byte(config.PayloadScheme),
	})
}
