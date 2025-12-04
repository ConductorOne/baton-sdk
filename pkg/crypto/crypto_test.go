package crypto //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

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

	config := v2.EncryptionConfig_builder{
		Provider: jwk.EncryptionProviderJwk,
		JwkPublicKeyConfig: v2.EncryptionConfig_JWKPublicKeyConfig_builder{
			PubKey: pubJWKBytes,
		}.Build(),
	}.Build()

	return config, privJWK
}

func testEncryptionProvider(t *testing.T, ctx context.Context, config *v2.EncryptionConfig, privKey *jose.JSONWebKey) {
	provider, err := providers.GetEncryptionProvider(jwk.EncryptionProviderJwk)
	require.NoError(t, err)

	plainText := v2.PlaintextData_builder{
		Name:        "password",
		Description: "this is the password",
		Schema:      "",
		Bytes:       []byte("hunter2"),
	}.Build()
	cipherText, err := provider.Encrypt(ctx, config, plainText)
	require.NoError(t, err)

	require.Equal(t, plainText.GetName(), cipherText.GetName())
	require.Equal(t, plainText.GetDescription(), cipherText.GetDescription())
	require.Equal(t, plainText.GetSchema(), cipherText.GetSchema())
	require.NotEqual(t, plainText.GetBytes(), cipherText.GetEncryptedBytes())
	require.Greater(t, len(cipherText.GetEncryptedBytes()), len(plainText.GetBytes()))

	decryptedText, err := provider.Decrypt(ctx, cipherText, privKey)
	require.NoError(t, err)
	require.Equal(t, plainText.GetName(), decryptedText.GetName())
	require.Equal(t, plainText.GetDescription(), decryptedText.GetDescription())
	require.Equal(t, plainText.GetSchema(), decryptedText.GetSchema())
	require.Equal(t, plainText.GetBytes(), decryptedText.GetBytes())
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

	config := v2.EncryptionConfig_builder{
		Provider: jwk.EncryptionProviderJwk,
		JwkPublicKeyConfig: v2.EncryptionConfig_JWKPublicKeyConfig_builder{
			PubKey: privJWKBytes,
		}.Build(),
	}.Build()

	provider, err := providers.GetEncryptionProvider(jwk.EncryptionProviderJwk)
	require.NoError(t, err)

	plainText := v2.PlaintextData_builder{
		Name:        "password",
		Description: "this is the password",
		Schema:      "",
		Bytes:       []byte("hunter2"),
	}.Build()
	cipherText, err := provider.Encrypt(ctx, config, plainText)
	require.ErrorIs(t, err, jwk.ErrJWKUnsupportedKeyType)
	require.Nil(t, cipherText)
}

func TestConvertCredentialOptions(t *testing.T) {
	ctx := context.Background()
	provider, err := providers.GetEncryptionProvider(jwk.EncryptionProviderJwk)
	require.NoError(t, err)
	encryptionConfig, privateKey, err := provider.GenerateKey(ctx)
	require.NoError(t, err)
	encryptionConfigs := []*v2.EncryptionConfig{encryptionConfig}

	privateKey.KeyID = encryptionConfig.GetKeyId()
	t.Run("error when credential options is encrypted password but encryption configs are provided", func(t *testing.T) {
		opts := v2.CredentialOptions_builder{
			EncryptedPassword: v2.CredentialOptions_EncryptedPassword_builder{
				EncryptedPasswords: []*v2.EncryptedData{
					v2.EncryptedData_builder{
						Provider:       jwk.EncryptionProviderJwkPrivate,
						EncryptedBytes: []byte("invalid"),
					}.Build(),
				},
			}.Build(),
		}.Build()
		_, err := ConvertCredentialOptions(ctx, privateKey, opts, encryptionConfigs)
		require.Error(t, err)
	})
	t.Run("error when encrypted password is invalid", func(t *testing.T) {
		opts := v2.CredentialOptions_builder{
			EncryptedPassword: v2.CredentialOptions_EncryptedPassword_builder{
				EncryptedPasswords: []*v2.EncryptedData{
					v2.EncryptedData_builder{
						Provider:       jwk.EncryptionProviderJwkPrivate,
						EncryptedBytes: []byte("invalid"),
					}.Build(),
				},
			}.Build(),
		}.Build()
		_, err := ConvertCredentialOptions(ctx, privateKey, opts, nil)
		require.Error(t, err)
	})

	t.Run("decrypt password", func(t *testing.T) {
		password := "test_password"
		provider, err := providers.GetEncryptionProvider(jwk.EncryptionProviderJwkPrivate)
		require.NoError(t, err)
		require.NoError(t, err)

		encryptedPassword, err := provider.Encrypt(ctx, encryptionConfig, v2.PlaintextData_builder{
			Name:        "password",
			Description: "this is the password",
			Schema:      "",
			Bytes:       []byte(password),
		}.Build())
		require.NoError(t, err)

		encryptionConfig2, _, err := provider.GenerateKey(ctx)
		require.NoError(t, err)
		encryptedPassword2, err := provider.Encrypt(ctx, encryptionConfig2, v2.PlaintextData_builder{
			Name:        "password",
			Description: "this is the password",
			Schema:      "",
			Bytes:       []byte(password),
		}.Build())
		require.NoError(t, err)

		opts := v2.CredentialOptions_builder{
			EncryptedPassword: v2.CredentialOptions_EncryptedPassword_builder{
				EncryptedPasswords: []*v2.EncryptedData{encryptedPassword2, encryptedPassword},
			}.Build(),
		}.Build()
		localOpts, err := ConvertCredentialOptions(ctx, privateKey, opts, nil)
		require.NoError(t, err)
		require.Equal(t, password, localOpts.GetPlaintextPassword().GetPlaintextPassword())
	})
}
