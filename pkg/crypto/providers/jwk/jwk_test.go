package jwk

import (
	"context"
	"testing"

	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/require"
)

func TestGenerateKey(t *testing.T) {
	provider := &JWKEncryptionProvider{}
	ctx := context.Background()

	config, privKey, err := provider.GenerateKey(ctx)
	require.NoError(t, err)
	require.NotNil(t, config)
	require.NotNil(t, privKey)
}

func TestEncryptDecrypt(t *testing.T) {
	provider := &JWKEncryptionProvider{}
	ctx := context.Background()

	config, privKey, err := provider.GenerateKey(ctx)
	require.NoError(t, err)
	require.NotNil(t, config)
	require.NotNil(t, privKey)

	plainText := &v2.PlaintextData{
		Name:        "test",
		Description: "test description",
		Schema:      "test schema",
		Bytes:       []byte("test data"),
	}

	encryptedData, err := provider.Encrypt(ctx, config, plainText)
	require.NoError(t, err)
	require.NotNil(t, encryptedData)

	decryptedData, err := provider.Decrypt(ctx, encryptedData, privKey)
	require.NoError(t, err)
	require.NotNil(t, decryptedData)
	require.Equal(t, plainText.Bytes, decryptedData.Bytes)
}

func TestInvalidKeyType(t *testing.T) {
	provider := &JWKEncryptionProvider{}
	ctx := context.Background()

	_, privKey, err := provider.GenerateKey(ctx)
	require.NoError(t, err)

	privKey.Key = []byte("invalid key type")

	privKeyBytes, err := privKey.MarshalJSON()
	require.NoError(t, err)

	plainText := &v2.PlaintextData{
		Name:        "test",
		Description: "test description",
		Schema:      "test schema",
		Bytes:       []byte("test data"),
	}

	_, err = provider.Encrypt(ctx, &v2.EncryptionConfig{
		Config: &v2.EncryptionConfig_JwkPublicKeyConfig{
			JwkPublicKeyConfig: &v2.EncryptionConfig_JWKPublicKeyConfig{
				PubKey: privKeyBytes,
			},
		},
	}, plainText)
	require.Error(t, err)
	require.Equal(t, ErrJWKUnsupportedKeyType, err)
}

func TestEncryptDecryptECDSAKey(t *testing.T) {
	provider := &JWKEncryptionProvider{}
	ctx := context.Background()

	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	require.NotNil(t, privKey)

	jwk := jose.JSONWebKey{Key: privKey, Algorithm: string(jose.ES256)}
	pubConfig, privKeyBytes, err := provider.marshalKey(context.TODO(), &jwk)
	require.NoError(t, err)
	require.NotNil(t, privKeyBytes)

	plainText := &v2.PlaintextData{
		Name:        "test",
		Description: "test description",
		Schema:      "test schema",
		Bytes:       []byte("test data"),
	}
	encryptedData, err := provider.Encrypt(ctx, pubConfig, plainText)
	require.NoError(t, err)
	require.NotNil(t, encryptedData)

	decryptedData, err := provider.Decrypt(ctx, encryptedData, privKeyBytes)
	require.NoError(t, err)
	require.NotNil(t, decryptedData)
	require.Equal(t, plainText.Bytes, decryptedData.Bytes)
}

func TestEncryptDecryptRSA1024Key(t *testing.T) {
	provider := &JWKEncryptionProvider{}
	ctx := context.Background()

	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	require.NotNil(t, privKey)

	jwk := jose.JSONWebKey{Key: privKey, Use: "sig", Algorithm: string(jose.RS256)}
	pubConfig, privKeyBytes, err := provider.marshalKey(context.TODO(), &jwk)
	require.NoError(t, err)
	require.NotNil(t, privKeyBytes)

	plainText := &v2.PlaintextData{
		Name:        "test",
		Description: "test description",
		Schema:      "test schema",
		Bytes:       []byte("test data"),
	}

	encryptedData, err := provider.Encrypt(ctx, pubConfig, plainText)
	require.NoError(t, err)
	require.NotNil(t, encryptedData)

	decryptedData, err := provider.Decrypt(ctx, encryptedData, privKeyBytes)
	require.NoError(t, err)
	require.NotNil(t, decryptedData)
	require.Equal(t, plainText.Bytes, decryptedData.Bytes)
}
