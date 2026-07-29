package providers

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-jose/go-jose/v4"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	ageprovider "github.com/conductorone/baton-sdk/pkg/crypto/providers/age"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/jwk"
)

var ErrEncryptionProviderNotRegistered = fmt.Errorf("crypto/providers: encryption provider not registered")
var ErrNoProviderSpecified = fmt.Errorf("crypto/providers: no provider specified")

// Encryptor encrypts connector plaintext for one configured recipient.
type Encryptor interface {
	Encrypt(ctx context.Context, conf *v2.EncryptionConfig, plainText *v2.PlaintextData) (*v2.EncryptedData, error)
}

type EncryptionProvider interface {
	Encryptor
	Decrypt(ctx context.Context, cipherText *v2.EncryptedData, privateKey *jose.JSONWebKey) (*v2.PlaintextData, error)

	GenerateKey(ctx context.Context) (*v2.EncryptionConfig, *jose.JSONWebKey, error)
}

// EncryptionConfigValidator is implemented by providers that can validate all
// provider-specific configuration without encrypting data. Builders use it
// before invoking a connector so a bad encryption key cannot strand a newly
// created credential in the provider.
type EncryptionConfigValidator interface {
	ValidateConfig(ctx context.Context, conf *v2.EncryptionConfig) error
}

type DecryptionConfig struct {
	Provider   string
	PrivateKey *jose.JSONWebKey
}

var providerRegistry = map[string]EncryptionProvider{
	normalizeProviderName(jwk.EncryptionProviderJwk):        &jwk.JWKEncryptionProvider{},
	normalizeProviderName(jwk.EncryptionProviderJwkPrivate): &jwk.JWKEncryptionProvider{},
}

var encryptorRegistry = map[string]Encryptor{
	normalizeProviderName(ageprovider.EncryptionProviderAge): &ageprovider.RecipientEncryptionProvider{},
	normalizeProviderName(jwk.EncryptionProviderJwk):         &jwk.JWKEncryptionProvider{},
	normalizeProviderName(jwk.EncryptionProviderJwkPrivate):  &jwk.JWKEncryptionProvider{},
}

func normalizeProviderName(name string) string {
	return strings.TrimSpace(strings.ToLower(name))
}

func GetEncryptionProvider(name string) (EncryptionProvider, error) {
	provider, ok := providerRegistry[normalizeProviderName(name)]
	if !ok {
		return nil, fmt.Errorf("%w (%s)", ErrEncryptionProviderNotRegistered, name)
	}
	return provider, nil
}

// GetEncryptor returns the encryption-capable provider registered under name.
func GetEncryptor(name string) (Encryptor, error) {
	provider, ok := encryptorRegistry[normalizeProviderName(name)]
	if !ok {
		return nil, fmt.Errorf("%w (%s)", ErrEncryptionProviderNotRegistered, name)
	}
	return provider, nil
}

// GetEncryptorForConfig resolves an encryption-capable provider from an EncryptionConfig.
func GetEncryptorForConfig(ctx context.Context, conf *v2.EncryptionConfig) (Encryptor, error) {
	providerName := normalizeProviderName(conf.GetProvider())
	if providerName == "" {
		switch {
		case conf.GetAgeRecipientConfig() != nil:
			providerName = ageprovider.EncryptionProviderAge
		case conf.GetJwkPublicKeyConfig() != nil:
			providerName = jwk.EncryptionProviderJwk
		}
	}
	if providerName == "" {
		return nil, ErrEncryptionProviderNotRegistered
	}
	return GetEncryptor(providerName)
}

// GetEncryptionProviderForConfig returns the full encryption provider for the given config.
// If the config specifies a provider, we will fetch it directly by name and return an error if it's not found.
// If the config contains a non-nil well-known configuration (like JWKPublicKeyConfig), we will return the provider for that by name.
// If we can't find a provider, we return an ErrEncryptionProviderNotRegistered.
//
// Deprecated: use GetEncryptorForConfig for encryption. This legacy resolver
// returns only providers that also implement decryption and key generation, so
// it cannot resolve encryption-only configurations such as age recipients.
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
		return nil, ErrEncryptionProviderNotRegistered
	}

	return GetEncryptionProvider(providerName)
}

func GetDecryptionProviderForConfig(ctx context.Context, conf *DecryptionConfig) (EncryptionProvider, error) {
	providerName := normalizeProviderName(conf.Provider)

	if providerName == "" {
		return nil, ErrNoProviderSpecified
	}

	return GetEncryptionProvider(providerName)
}
