package jwe

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"encoding/base64"
	"encoding/json"
	"io"
	"strings"
	"unicode/utf8"

	"filippo.io/hpke"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	EncryptionProvider = "baton/jwe/v1"
	Algorithm          = "https://c1.ai/alg/hpke-xwing-hkdf-sha256-chacha20poly1305/v1"
	MaxPlaintextBytes  = 1024 * 1024
)

// MaxProtectedHeaderBytes bounds the decoded protected header JSON, matching
// the limit the consuming reader enforces. It is measured on the serialized
// header rather than on the key id, because JSON escaping can inflate a key id
// well past its own length.
const MaxProtectedHeaderBytes = 4096

type Provider struct{}

func (*Provider) ValidateConfig(_ context.Context, config *v2.EncryptionConfig) error {
	_, _, err := recipient(config)
	return err
}

func (*Provider) Encrypt(ctx context.Context, config *v2.EncryptionConfig, plaintext *v2.PlaintextData) (*v2.EncryptedData, error) {
	key, header, err := recipient(config)
	if err != nil {
		return nil, err
	}
	if plaintext == nil || len(plaintext.GetBytes()) > MaxPlaintextBytes {
		return nil, invalid("plaintext is missing or exceeds size limit")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	encodedHeader := base64.RawURLEncoding.EncodeToString(header)
	encodedAAD := base64.RawURLEncoding.EncodeToString(config.GetJwkPublicKeyConfig().GetAdditionalAuthenticatedData())
	// draft-ietf-jose-hpke-encrypt-22 section 5: empty info, one HPKE message.
	enc, sender, err := hpke.NewSender(key, hpke.HKDFSHA256(), hpke.ChaCha20Poly1305(), nil)
	if err != nil {
		return nil, status.Error(codes.Internal, "jwe: initialize encryption")
	}
	ciphertext, err := sender.Seal([]byte(encodedHeader+"."+encodedAAD), plaintext.GetBytes())
	if err != nil {
		return nil, status.Error(codes.Internal, "jwe: encrypt plaintext")
	}
	message, err := json.Marshal(FlattenedJWE{
		Protected:    encodedHeader,
		EncryptedKey: base64.RawURLEncoding.EncodeToString(enc),
		AAD:          encodedAAD,
		Ciphertext:   base64.RawURLEncoding.EncodeToString(ciphertext),
	})
	if err != nil {
		return nil, status.Error(codes.Internal, "jwe: encode ciphertext")
	}
	return v2.EncryptedData_builder{
		Provider:       EncryptionProvider,
		KeyIds:         []string{config.GetKeyId()},
		EncryptedBytes: message,
		Name:           plaintext.GetName(),
		Description:    plaintext.GetDescription(),
		Schema:         plaintext.GetSchema(),
	}.Build(), nil
}

// FlattenedJWE is the flattened JWE JSON serialization this profile emits:
// RFC 7516 section 7.2.2 with the members the profile fixes. IV and Tag are
// always present and empty because the HPKE authentication tag stays inside
// Ciphertext. It exists so a reader can decode the wire format without a second
// declaration of it; go-jose's equivalent is unexported and marks every member
// omitempty, so it cannot express the empty IV and Tag this profile requires.
type FlattenedJWE struct {
	Protected    string `json:"protected"`
	EncryptedKey string `json:"encrypted_key"`
	AAD          string `json:"aad"`
	IV           string `json:"iv"`
	Ciphertext   string `json:"ciphertext"`
	Tag          string `json:"tag"`
}

// protectedHeader is the only member set this profile permits on the wire.
// Member order carries no meaning: the HPKE additional data is built from the
// serialized bytes as transmitted, so a reader authenticates the exact string it
// received rather than a re-serialization of the same members.
type protectedHeader struct {
	Algorithm string `json:"alg"`
	KeyID     string `json:"kid"`
}

// recipient resolves and validates the configured recipient, returning the HPKE
// public key and the serialized protected header for the wire.
func recipient(config *v2.EncryptionConfig) (hpke.PublicKey, []byte, error) {
	if config == nil || strings.ToLower(strings.TrimSpace(config.GetProvider())) != EncryptionProvider || config.GetJwkPublicKeyConfig() == nil {
		return nil, nil, invalid("explicit JWE provider and JWK config are required")
	}
	kid := config.GetKeyId()
	if kid == "" || len(kid) > 1024 || !utf8.ValidString(kid) || strings.TrimSpace(kid) != kid {
		return nil, nil, invalid("invalid key id")
	}
	// The serialized header is bounded rather than the key id, because
	// json.Marshal escapes HTML characters and a key id can therefore serialize
	// to several times its own length. The same bytes go on the wire, so a reader
	// sees exactly what was measured here.
	header, err := json.Marshal(protectedHeader{Algorithm: Algorithm, KeyID: kid})
	if err != nil {
		return nil, nil, status.Error(codes.Internal, "jwe: encode protected header")
	}
	if len(header) > MaxProtectedHeaderBytes {
		return nil, nil, invalid("protected header exceeds size limit")
	}
	jwkConfig := config.GetJwkPublicKeyConfig()
	// The declared pub_key and authenticated-data bounds belong to the proto, so
	// the generated validator owns them. Reading them from one place keeps this
	// path and a direct caller from disagreeing about what is acceptable. The
	// failure is reported as one fixed message, so no part of the rejected
	// configuration reaches the caller.
	if err := jwkConfig.Validate(); err != nil {
		return nil, nil, invalid("JWK configuration is invalid")
	}
	fields, err := publicJWKFields(jwkConfig.GetPubKey())
	if err != nil {
		return nil, nil, err
	}
	var kty, alg, pub string
	if json.Unmarshal(fields["kty"], &kty) != nil || kty != "AKP" ||
		json.Unmarshal(fields["alg"], &alg) != nil || alg != Algorithm ||
		json.Unmarshal(fields["pub"], &pub) != nil || pub == "" {
		return nil, nil, invalid("unsupported or incomplete public JWK")
	}
	for _, name := range []string{"priv", "d", "k"} {
		if _, exists := fields[name]; exists {
			return nil, nil, invalid("private key material is not permitted")
		}
	}
	for name, expected := range map[string]string{"kid": kid, "use": "enc"} {
		if raw, exists := fields[name]; exists {
			var value string
			if json.Unmarshal(raw, &value) != nil || value != expected {
				return nil, nil, invalid("inconsistent JWK key id or use")
			}
		}
	}
	if raw, exists := fields["key_ops"]; exists {
		var operations []string
		if json.Unmarshal(raw, &operations) != nil || len(operations) != 1 || operations[0] != "encrypt" {
			return nil, nil, invalid("unsupported JWK key operations")
		}
	}
	keyBytes, err := base64.RawURLEncoding.Strict().DecodeString(pub)
	if err != nil || base64.RawURLEncoding.EncodeToString(keyBytes) != pub {
		return nil, nil, invalid("invalid public key encoding")
	}
	key, err := hpke.MLKEM768X25519().NewPublicKey(keyBytes)
	if err != nil {
		return nil, nil, invalid("invalid X-Wing public key")
	}
	// Parsing alone accepts low-order X25519 points; reject before issuance.
	scalar := [32]byte{1}
	probe, err := ecdh.X25519().NewPrivateKey(scalar[:])
	if err != nil {
		return nil, nil, status.Error(codes.Internal, "jwe: initialize key validation")
	}
	x25519Key, err := ecdh.X25519().NewPublicKey(keyBytes[len(keyBytes)-32:])
	if err != nil {
		return nil, nil, invalid("invalid X25519 public key")
	}
	if _, err := probe.ECDH(x25519Key); err != nil {
		return nil, nil, invalid("invalid X25519 public key")
	}
	return key, header, nil
}

// publicJWKFields decodes the top-level members of a public JWK. The pub_key
// size bound is enforced by the generated validator in recipient before this
// runs, so only emptiness and encoding are checked here.
func publicJWKFields(data []byte) (map[string]json.RawMessage, error) {
	if len(data) == 0 || !utf8.Valid(data) {
		return nil, invalid("empty or non-UTF-8 JWK")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, invalid("JWK must be an object")
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, invalid("malformed JWK")
		}
		name, ok := token.(string)
		if !ok {
			return nil, invalid("malformed JWK member")
		}
		if _, exists := fields[name]; exists {
			return nil, invalid("duplicate JWK member")
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, invalid("malformed JWK member")
		}
		fields[name] = value
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return nil, invalid("malformed JWK")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, invalid("trailing JWK data")
	}
	return fields, nil
}

func invalid(message string) error {
	return status.Error(codes.InvalidArgument, "jwe: "+message)
}
