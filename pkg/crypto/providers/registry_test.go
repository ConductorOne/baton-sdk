package providers

import (
	"context"
	"testing"

	filippoage "filippo.io/age"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	ageprovider "github.com/conductorone/baton-sdk/pkg/crypto/providers/age"
	"github.com/stretchr/testify/require"
)

func TestGetEncryptorForAgeRecipientConfig(t *testing.T) {
	identity, err := filippoage.GenerateHybridIdentity()
	require.NoError(t, err)
	config := v2.EncryptionConfig_builder{
		AgeRecipientConfig: v2.EncryptionConfig_AgeRecipientConfig_builder{
			Recipient: identity.Recipient().String(),
		}.Build(),
	}.Build()

	encryptor, err := GetEncryptorForConfig(context.Background(), config)
	require.NoError(t, err)
	require.IsType(t, &ageprovider.RecipientEncryptionProvider{}, encryptor)
}

func TestGetEncryptorForConfigRejectsUnknownExplicitProvider(t *testing.T) {
	config := v2.EncryptionConfig_builder{
		Provider: "unknown/provider",
		AgeRecipientConfig: v2.EncryptionConfig_AgeRecipientConfig_builder{
			Recipient: "age1ignored",
		}.Build(),
	}.Build()

	_, err := GetEncryptorForConfig(context.Background(), config)
	require.ErrorIs(t, err, ErrEncryptionProviderNotRegistered)
}

func TestConfigResolversRespectProviderCapabilities(t *testing.T) {
	identity, err := filippoage.GenerateX25519Identity()
	require.NoError(t, err)
	ageConfig := v2.EncryptionConfig_builder{
		AgeRecipientConfig: v2.EncryptionConfig_AgeRecipientConfig_builder{
			Recipient: identity.Recipient().String(),
		}.Build(),
	}.Build()

	_, err = GetEncryptorForConfig(context.Background(), ageConfig)
	require.NoError(t, err)
	_, err = GetEncryptionProviderForConfig(context.Background(), ageConfig)
	require.ErrorIs(t, err, ErrEncryptionProviderNotRegistered)

	jwkConfig := v2.EncryptionConfig_builder{
		JwkPublicKeyConfig: v2.EncryptionConfig_JWKPublicKeyConfig_builder{
			PubKey: []byte(`{"kty":"OKP","crv":"Ed25519","x":"11qYAYLefZ1kphVG3Gd5FDp1D-7BqQ3wQp4N_8BfW8o"}`),
		}.Build(),
	}.Build()

	_, err = GetEncryptorForConfig(context.Background(), jwkConfig)
	require.NoError(t, err)
	_, err = GetEncryptionProviderForConfig(context.Background(), jwkConfig)
	require.NoError(t, err)
}
