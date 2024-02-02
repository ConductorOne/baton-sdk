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
	"fmt"
	"io"

	"filippo.io/age"
	"github.com/go-jose/go-jose/v3"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// TODO(morgabra): Fix the circular dependency/entire registry pattern here.
const EncryptionProviderJwk = "baton/jwk/v1"

var JWKInvalidKeyTypeError = fmt.Errorf("jwk: invalid key type")
var JWKUnsupportedKeyTypeError = fmt.Errorf("jwk: unsupported key type")

func unmarshalJWK(jwkBytes []byte) (*jose.JSONWebKey, error) {
	jwk := &jose.JSONWebKey{}
	err := jwk.UnmarshalJSON(jwkBytes)
	if err != nil {
		return nil, fmt.Errorf("jwk: failed to unmarshal jwk: %w", err)
	}
	return jwk, nil
}

type JWKEncryptionProvider struct{}

func (j *JWKEncryptionProvider) GenerateKey(ctx context.Context) (*v2.EncryptionConfig, []byte, error) {
	_, privKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, nil, fmt.Errorf("jwk: failed to generate key: %w", err)
	}

	privKeyJWK := &jose.JSONWebKey{
		Key: privKey,
	}
	privKeyJWKBytes, err := privKeyJWK.MarshalJSON()
	if err != nil {
		return nil, nil, fmt.Errorf("jwk: failed to marshal private key: %w", err)
	}

	pubKeyJWK := privKeyJWK.Public()
	pubKeyJWKBytes, err := pubKeyJWK.MarshalJSON()
	if err != nil {
		return nil, nil, fmt.Errorf("jwk: failed to marshal public key: %w", err)
	}

	kid, err := thumbprint(&pubKeyJWK)
	if err != nil {
		return nil, nil, err
	}

	return &v2.EncryptionConfig{
		Principal: nil,
		Provider:  "baton/jwk/v1", // TODO(morgabra): Fix the circular dependency/entire registry pattern.
		KeyId:     kid,
		Config: &v2.EncryptionConfig_JwkPublicKeyConfig{
			JwkPublicKeyConfig: &v2.EncryptionConfig_JWKPublicKeyConfig{
				PubKey: pubKeyJWKBytes,
			},
		},
	}, privKeyJWKBytes, nil
}

func (j *JWKEncryptionProvider) Encrypt(ctx context.Context, conf *v2.EncryptionConfig, plainText *v2.PlaintextData) (*v2.EncryptedData, error) {
	jwk, err := unmarshalJWK(conf.GetJwkPublicKeyConfig().GetPubKey())
	if err != nil {
		return nil, err
	}

	var ciphertext []byte
	switch pubKey := jwk.Public().Key.(type) {
	case ed25519.PublicKey:
		ciphertext, err = EncryptED25519(pubKey, plainText.Bytes)
		if err != nil {
			return nil, err
		}
	case *ecdsa.PublicKey:
		ciphertext, err = EncryptECDSA(pubKey, plainText.Bytes)
		if err != nil {
			return nil, err
		}
	case *rsa.PublicKey:
		ciphertext, err = EncryptRSA(pubKey, plainText.Bytes)
		if err != nil {
			return nil, err
		}
	default:
		return nil, JWKUnsupportedKeyTypeError
	}

	tp, err := thumbprint(jwk)
	if err != nil {
		return nil, err
	}

	encCipherText := base64.StdEncoding.EncodeToString(ciphertext)

	return &v2.EncryptedData{
		Provider:       EncryptionProviderJwk,
		KeyId:          tp,
		Name:           plainText.Name,
		Description:    plainText.Description,
		Schema:         plainText.Schema,
		EncryptedBytes: []byte(encCipherText),
	}, nil
}

func (j *JWKEncryptionProvider) Decrypt(ctx context.Context, cipherText *v2.EncryptedData, privateKey []byte) (*v2.PlaintextData, error) {
	jwk, err := unmarshalJWK(privateKey)
	if err != nil {
		return nil, err
	}

	if jwk.IsPublic() {
		return nil, fmt.Errorf("%w: key is public", JWKInvalidKeyTypeError)
	}

	decCipherText, err := base64.StdEncoding.DecodeString(string(cipherText.EncryptedBytes))
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
		return nil, JWKUnsupportedKeyTypeError
	}

	return &v2.PlaintextData{
		Name:        cipherText.Name,
		Description: cipherText.Description,
		Schema:      cipherText.Schema,
		Bytes:       plaintext,
	}, nil
}

func thumbprint(jwk *jose.JSONWebKey) (string, error) {
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
