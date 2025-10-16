package jwk

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"filippo.io/age"
	"github.com/go-jose/go-jose/v4"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// TODO(morgabra): Fix the circular dependency/entire registry pattern here.
const EncryptionProviderJwk = "baton/jwk/v1"
const EncryptionProviderJwkPrivate = "baton/jwk/v1/private"

var ErrJWKInvalidKeyType = errors.New("jwk: invalid key type")
var ErrJWKUnsupportedKeyType = errors.New("jwk: unsupported key type")

func unmarshalJWK(jwkBytes []byte) (*jose.JSONWebKey, error) {
	jwk := &jose.JSONWebKey{}
	err := jwk.UnmarshalJSON(jwkBytes)
	if err != nil {
		return nil, fmt.Errorf("jwk: failed to unmarshal jwk: %w", err)
	}
	return jwk, nil
}

type JWKEncryptionProvider struct{}

func (j *JWKEncryptionProvider) GenerateKey(ctx context.Context) (*v2.EncryptionConfig, *jose.JSONWebKey, error) {
	_, privKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, nil, fmt.Errorf("jwk: failed to generate key: %w", err)
	}

	privKeyJWK := &jose.JSONWebKey{
		Key: privKey,
	}
	return j.marshalKey(ctx, privKeyJWK)
}

func (j *JWKEncryptionProvider) marshalKey(ctx context.Context, privKeyJWK *jose.JSONWebKey) (*v2.EncryptionConfig, *jose.JSONWebKey, error) {
	pubKeyJWK := privKeyJWK.Public()
	pubKeyJWKBytes, err := pubKeyJWK.MarshalJSON()
	if err != nil {
		return nil, nil, fmt.Errorf("jwk: failed to marshal public key: %w", err)
	}

	kid, err := Thumbprint(&pubKeyJWK)
	if err != nil {
		return nil, nil, err
	}

	return v2.EncryptionConfig_builder{
		Principal: nil,
		Provider:  EncryptionProviderJwk, // TODO(morgabra): Fix the circular dependency/entire registry pattern.
		KeyId:     kid,
		JwkPublicKeyConfig: v2.EncryptionConfig_JWKPublicKeyConfig_builder{
			PubKey: pubKeyJWKBytes,
		}.Build(),
	}.Build(), privKeyJWK, nil
}

func (j *JWKEncryptionProvider) Encrypt(ctx context.Context, conf *v2.EncryptionConfig, plainText *v2.PlaintextData) (*v2.EncryptedData, error) {
	jwk, err := unmarshalJWK(conf.GetJwkPublicKeyConfig().GetPubKey())
	if err != nil {
		return nil, err
	}

	var ciphertext []byte
	switch pubKey := jwk.Public().Key.(type) {
	case ed25519.PublicKey:
		ciphertext, err = EncryptED25519(pubKey, plainText.GetBytes())
		if err != nil {
			return nil, err
		}
	case *ecdsa.PublicKey:
		ciphertext, err = EncryptECDSA(pubKey, plainText.GetBytes())
		if err != nil {
			return nil, err
		}
	case *rsa.PublicKey:
		ciphertext, err = EncryptRSA(pubKey, plainText.GetBytes())
		if err != nil {
			return nil, err
		}
	default:
		return nil, ErrJWKUnsupportedKeyType
	}

	tp, err := Thumbprint(jwk)
	if err != nil {
		return nil, err
	}

	encCipherText := base64.StdEncoding.EncodeToString(ciphertext)

	return v2.EncryptedData_builder{
		Provider:       EncryptionProviderJwk,
		KeyId:          tp,
		Name:           plainText.GetName(),
		Description:    plainText.GetDescription(),
		Schema:         plainText.GetSchema(),
		EncryptedBytes: []byte(encCipherText),
		KeyIds:         []string{tp},
	}.Build(), nil
}

func (j *JWKEncryptionProvider) Decrypt(ctx context.Context, cipherText *v2.EncryptedData, jwk *jose.JSONWebKey) (*v2.PlaintextData, error) {
	decCipherText, err := base64.StdEncoding.DecodeString(string(cipherText.GetEncryptedBytes()))
	if err != nil {
		return nil, fmt.Errorf("jwk: failed to decode encrypted bytes: %w", err)
	}

	var plaintext []byte
	switch privKey := jwk.Key.(type) {
	case ed25519.PrivateKey:
		plaintext, err = DecryptED25519(privKey, decCipherText)
		if err != nil {
			return nil, err
		}
	case *ecdsa.PrivateKey:
		plaintext, err = DecryptECDSA(privKey, decCipherText)
		if err != nil {
			return nil, err
		}
	case *rsa.PrivateKey:
		plaintext, err = DecryptRSA(privKey, decCipherText)
		if err != nil {
			return nil, err
		}
	default:
		return nil, ErrJWKUnsupportedKeyType
	}

	return v2.PlaintextData_builder{
		Name:        cipherText.GetName(),
		Description: cipherText.GetDescription(),
		Schema:      cipherText.GetSchema(),
		Bytes:       plaintext,
	}.Build(), nil
}

func Thumbprint(jwk *jose.JSONWebKey) (string, error) {
	tp, err := jwk.Thumbprint(crypto.SHA256)
	if err != nil {
		return "", fmt.Errorf("jwk: failed to compute key id: %w", err)
	}
	return hex.EncodeToString(tp), nil
}

func ageEncrypt(r age.Recipient, plaintext []byte) ([]byte, error) {
	ciphertext := &bytes.Buffer{}
	w, err := age.Encrypt(ciphertext, r)
	if err != nil {
		return nil, fmt.Errorf("age: failed to encrypt: %w", err)
	}
	_, err = io.Copy(w, bytes.NewReader(plaintext))
	if err != nil {
		return nil, fmt.Errorf("age: failed to write encrypted data: %w", err)
	}
	err = w.Close()
	if err != nil {
		return nil, fmt.Errorf("age: failed to close: %w", err)
	}
	return ciphertext.Bytes(), nil
}

func ageDecrypt(identity age.Identity, ciphertext []byte) ([]byte, error) {
	out, err := age.Decrypt(bytes.NewReader(ciphertext), identity)
	if err != nil {
		return nil, fmt.Errorf("age: failed to decrypt: %w", err)
	}

	plaintext := &bytes.Buffer{}
	_, err = io.Copy(plaintext, out)
	if err != nil {
		return nil, fmt.Errorf("age: failed to read decrypted data: %w", err)
	}

	return plaintext.Bytes(), nil
}
