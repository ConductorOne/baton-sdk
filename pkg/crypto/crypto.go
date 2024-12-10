package crypto

import (
	"context"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers"
)

type PlaintextCredential struct {
	Name        string
	Description string
	Schema      string
	Bytes       []byte
}

type EncryptionManager struct {
	opts               *v2.CredentialOptions
	providerConfigsMap map[string]([]*v2.EncryptionConfig)
}

// FIXME(morgabra) Be tolerant of failures here and return the encryptions that succeeded. We've likely already
// done things to generate the credentials we want to encrypt, so we should still return the created objects
// even if your encryption provider is misconfigured.
func (pkem *EncryptionManager) Encrypt(ctx context.Context, cred *v2.PlaintextData) ([]*v2.EncryptedData, error) {
	encryptedDatas := make([]*v2.EncryptedData, 0)

	for providerName, configs := range pkem.providerConfigsMap {
		provider, err := providers.GetEncryptionProvider(providerName)
		if err != nil {
			return nil, err
		}

		encryptedData, err := provider.Encrypt(ctx, configs, cred)
		if err != nil {
			return nil, err
		}

		encryptedDatas = append(encryptedDatas, encryptedData...)
	}
	return encryptedDatas, nil
}

// MJP creating the providerMap means parsing the configs and failing early instead of in Encrypt
func NewEncryptionManager(ctx context.Context, co *v2.CredentialOptions, ec []*v2.EncryptionConfig) (*EncryptionManager, error) {
	// Group the encryption configs by provider
	providerMap := make(map[string]([]*v2.EncryptionConfig))
	for _, config := range ec {
		providerName, err := providers.GetEncryptionProviderName(ctx, config)
		if err != nil {
			return nil, err
		}
		providerMap[providerName] = append(providerMap[providerName], config)
	}

	em := &EncryptionManager{
		opts:               co,
		providerConfigsMap: providerMap,
	}
	return em, nil
}
