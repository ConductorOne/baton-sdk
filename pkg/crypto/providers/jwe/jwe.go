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
	EncryptionProvider                  = "baton/jwe/v1"
	Algorithm                           = "https://conductorone.com/alg/hpke-xwing-hkdf-sha256-chacha20poly1305/v1"
	MaxJWKBytes                         = 16 * 1024
	MaxAdditionalAuthenticatedDataBytes = 16 * 1024
	MaxPlaintextBytes                   = 1024 * 1024
)

// MaxProtectedHeaderBytes bounds the decoded protected header JSON, matching
// the limit the consuming reader enforces. It is measured on the serialized
// header rather than on the key id, because JSON escaping can inflate a key id
// well past its own length.
const MaxProtectedHeaderBytes = 4096

type Provider struct{}

func (*Provider) ValidateConfig(_ context.Context, config *v2.EncryptionConfig) error {
	_, err := recipient(config)
	return err
}

func (*Provider) Encrypt(ctx context.Context, config *v2.EncryptionConfig, plaintext *v2.PlaintextData) (*v2.EncryptedData, error) {
	key, err := recipient(config)
	if err != nil {
		return nil, err
	}
	if plaintext == nil || len(plaintext.GetBytes()) > MaxPlaintextBytes {
		return nil, invalid("plaintext is missing or exceeds size limit")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	header, err := json.Marshal(struct {
		Algorithm string `json:"alg"`
		KeyID     string `json:"kid"`
	}{Algorithm, config.GetKeyId()})
	if err != nil {
		return nil, status.Error(codes.Internal, "jwe: encode protected header")
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
	message, err := json.Marshal(struct {
		Protected    string `json:"protected"`
		EncryptedKey string `json:"encrypted_key"`
		AAD          string `json:"aad"`
		IV           string `json:"iv"`
		Ciphertext   string `json:"ciphertext"`
		Tag          string `json:"tag"`
	}{
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

func recipient(config *v2.EncryptionConfig) (hpke.PublicKey, error) {
	if config == nil || strings.ToLower(strings.TrimSpace(config.GetProvider())) != EncryptionProvider || config.GetJwkPublicKeyConfig() == nil {
		return nil, invalid("explicit JWE provider and JWK config are required")
	}
	kid := config.GetKeyId()
	if kid == "" || len(kid) > 1024 || !utf8.ValidString(kid) || strings.TrimSpace(kid) != kid {
		return nil, invalid("invalid key id")
	}
	jwkConfig := config.GetJwkPublicKeyConfig()
	if len(jwkConfig.GetAdditionalAuthenticatedData()) > MaxAdditionalAuthenticatedDataBytes {
		return nil, invalid("authenticated data exceeds size limit")
	}
	fields, err := publicJWKFields(jwkConfig.GetPubKey())
	if err != nil {
		return nil, err
	}
	var kty, alg, pub string
	if json.Unmarshal(fields["kty"], &kty) != nil || kty != "AKP" ||
		json.Unmarshal(fields["alg"], &alg) != nil || alg != Algorithm ||
		json.Unmarshal(fields["pub"], &pub) != nil || pub == "" {
		return nil, invalid("unsupported or incomplete public JWK")
	}
	for _, name := range []string{"priv", "d", "k"} {
		if _, exists := fields[name]; exists {
			return nil, invalid("private key material is not permitted")
		}
	}
	for name, expected := range map[string]string{"kid": kid, "use": "enc"} {
		if raw, exists := fields[name]; exists {
			var value string
			if json.Unmarshal(raw, &value) != nil || value != expected {
				return nil, invalid("inconsistent JWK key id or use")
			}
		}
	}
	if raw, exists := fields["key_ops"]; exists {
		var operations []string
		if json.Unmarshal(raw, &operations) != nil || len(operations) != 1 || operations[0] != "encrypt" {
			return nil, invalid("unsupported JWK key operations")
		}
	}
	keyBytes, err := base64.RawURLEncoding.Strict().DecodeString(pub)
	if err != nil || base64.RawURLEncoding.EncodeToString(keyBytes) != pub {
		return nil, invalid("invalid public key encoding")
	}
	key, err := hpke.MLKEM768X25519().NewPublicKey(keyBytes)
	if err != nil {
		return nil, invalid("invalid X-Wing public key")
	}
	// Parsing alone accepts low-order X25519 points; reject before issuance.
	scalar := [32]byte{1}
	probe, err := ecdh.X25519().NewPrivateKey(scalar[:])
	if err != nil {
		return nil, status.Error(codes.Internal, "jwe: initialize key validation")
	}
	x25519Key, err := ecdh.X25519().NewPublicKey(keyBytes[len(keyBytes)-32:])
	if err != nil {
		return nil, invalid("invalid X25519 public key")
	}
	if _, err := probe.ECDH(x25519Key); err != nil {
		return nil, invalid("invalid X25519 public key")
	}
	return key, nil
}

func publicJWKFields(data []byte) (map[string]json.RawMessage, error) {
	if len(data) == 0 || len(data) > MaxJWKBytes || !utf8.Valid(data) {
		return nil, invalid("invalid JWK size or encoding")
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
