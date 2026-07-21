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
