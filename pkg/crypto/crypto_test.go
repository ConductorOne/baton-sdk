package crypto

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"testing"

	"github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/jwk"
)

func marshalJWK(t *testing.T, privKey interface{}) (*v2.EncryptionConfig, *jose.JSONWebKey) {
	privJWK := &jose.JSONWebKey{Key: privKey}
	pubJWK := privJWK.Public()
	pubJWKBytes, err := pubJWK.MarshalJSON()
	require.NoError(t, err)

	config := &v2.EncryptionConfig{
		Provider: jwk.EncryptionProviderJwk,
		Config: &v2.EncryptionConfig_JwkPublicKeyConfig{
			JwkPublicKeyConfig: &v2.EncryptionConfig_JWKPublicKeyConfig{
				PubKey: pubJWKBytes,
			},
		},
	}

	return config, privJWK
}

func testEncryptionProvider(t *testing.T, ctx context.Context, config *v2.EncryptionConfig, privKey *jose.JSONWebKey) {
	provider, err := providers.GetEncryptionProvider(jwk.EncryptionProviderJwk)
	require.NoError(t, err)

	plainText := &v2.PlaintextData{
		Name:        "password",
		Description: "this is the password",
		Schema:      "",
		Bytes:       []byte("hunter2"),
	}
	cipherText, err := provider.Encrypt(ctx, config, plainText)
	require.NoError(t, err)

	require.Equal(t, plainText.Name, cipherText.Name)
	require.Equal(t, plainText.Description, cipherText.Description)
	require.Equal(t, plainText.Schema, cipherText.Schema)
	require.NotEqual(t, plainText.Bytes, cipherText.EncryptedBytes)
	require.Greater(t, len(cipherText.EncryptedBytes), len(plainText.Bytes))

	decryptedText, err := provider.Decrypt(ctx, cipherText, privKey)
	require.NoError(t, err)
	require.Equal(t, plainText.Name, decryptedText.Name)
	require.Equal(t, plainText.Description, decryptedText.Description)
	require.Equal(t, plainText.Schema, decryptedText.Schema)
	require.Equal(t, plainText.Bytes, decryptedText.Bytes)
}

// Test with the default GenerateKey().
func TestEncryptionProviderJWKDefault(t *testing.T) {
	ctx := context.Background()
	provider, err := providers.GetEncryptionProvider(jwk.EncryptionProviderJwk)
	require.NoError(t, err)
	config, privateKey, err := provider.GenerateKey(ctx)
	require.NoError(t, err)
	testEncryptionProvider(t, ctx, config, privateKey)
}

// Test with a byo ecdsa key.
func TestEncryptionProviderJWKECDSA(t *testing.T) {
	ctx := context.Background()

	privKey, err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	require.NoError(t, err)

	config, marshalledPrivKey := marshalJWK(t, privKey)
	testEncryptionProvider(t, ctx, config, marshalledPrivKey)
}

// Test with a byo rsa key.
func TestEncryptionProviderJWKRSA(t *testing.T) {
	ctx := context.Background()

	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	config, marshalledPrivKey := marshalJWK(t, privKey)

	testEncryptionProvider(t, ctx, config, marshalledPrivKey)
}

// Test with a byo ed25519 key.
func TestEncryptionProviderJWKED25519(t *testing.T) {
	ctx := context.Background()

	_, privKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	config, marshalledPrivKey := marshalJWK(t, privKey)
	testEncryptionProvider(t, ctx, config, marshalledPrivKey)
}

// Test with a byo symmetric key - this is disallowed.
func TestEncryptionProviderJWKSymmetric(t *testing.T) {
	ctx := context.Background()

	privKey := make([]byte, 32)
	_, err := rand.Read(privKey)
	require.NoError(t, err)

	privJWK := &jose.JSONWebKey{Key: privKey}
	privJWKBytes, err := privJWK.MarshalJSON()
	require.NoError(t, err)

	config := &v2.EncryptionConfig{
		Provider: jwk.EncryptionProviderJwk,
		Config: &v2.EncryptionConfig_JwkPublicKeyConfig{
			JwkPublicKeyConfig: &v2.EncryptionConfig_JWKPublicKeyConfig{
				PubKey: privJWKBytes,
			},
		},
	}

	provider, err := providers.GetEncryptionProvider(jwk.EncryptionProviderJwk)
	require.NoError(t, err)

	plainText := &v2.PlaintextData{
		Name:        "password",
		Description: "this is the password",
		Schema:      "",
		Bytes:       []byte("hunter2"),
	}
	cipherText, err := provider.Encrypt(ctx, config, plainText)
	require.ErrorIs(t, err, jwk.ErrJWKUnsupportedKeyType)
	require.Nil(t, cipherText)
}
