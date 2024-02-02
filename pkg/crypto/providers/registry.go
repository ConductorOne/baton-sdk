package providers

import (
	"context"
	"fmt"
	"strings"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/jwk"
)

var EncryptionProviderNotRegisteredError = fmt.Errorf("crypto/providers: encryption provider not registered")

type EncryptionProvider interface {
	Encrypt(ctx context.Context, conf *v2.EncryptionConfig, plainText *v2.PlaintextData) (*v2.EncryptedData, error)
	Decrypt(ctx context.Context, cipherText *v2.EncryptedData, privateKey []byte) (*v2.PlaintextData, error)

	GenerateKey(ctx context.Context) (*v2.EncryptionConfig, []byte, error)
}

var providerRegistry = map[string]EncryptionProvider{
	normalizeProviderName(jwk.EncryptionProviderJwk): &jwk.JWKEncryptionProvider{},
}

func normalizeProviderName(name string) string {
	return strings.TrimSpace(strings.ToLower(name))
}

func GetEncryptionProvider(name string) (EncryptionProvider, error) {
	provider, ok := providerRegistry[normalizeProviderName(name)]
	if !ok {
		return nil, fmt.Errorf("%w (%s)", EncryptionProviderNotRegisteredError, name)
	}
	return provider, nil
}

// GetEncryptionProviderForConfig returns the encryption provider for the given config.
// If the config specifies a provider, we will fetch it directly by name and return an error if it's not found.
// If the config contains a non-nil well-known configuration (like JWKPublicKeyConfig), we will return the provider for that by name.
// If we can't find a provider, we return an EncryptionProviderNotRegisteredError.
func GetEncryptionProviderForConfig(ctx context.Context, conf *v2.EncryptionConfig) (EncryptionProvider, error) {
	providerName := normalizeProviderName(conf.GetProvider())

	// We weren't given an explicit provider, so we can try to infer one based on the config.
	// FIXME(morgabra): This seems bad, let's just require 'provider'?
	if providerName == "" {
		if conf.GetJwkPublicKeyConfig() != nil {
			providerName = jwk.EncryptionProviderJwk
		}
	}

	// If we don't have a provider by now, bail.
	if providerName == "" {
		return nil, EncryptionProviderNotRegisteredError
	}

	return GetEncryptionProvider(providerName)
}
