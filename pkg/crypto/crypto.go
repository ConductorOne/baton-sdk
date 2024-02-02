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
	opts    *v2.CredentialOptions
	configs []*v2.EncryptionConfig
}

// FIXME(morgabra) Be tolerant of failures here and return the encryptions that succeeded. We've likely already
// done things to generate the credentials we want to encrypt, so we should still return the created objects
// even if your encryption provider is misconfigured.
func (pkem *EncryptionManager) Encrypt(ctx context.Context, cred *v2.PlaintextData) ([]*v2.EncryptedData, error) {
	encryptedDatas := make([]*v2.EncryptedData, 0, len(pkem.configs))

	for _, config := range pkem.configs {
		provider, err := providers.GetEncryptionProviderForConfig(ctx, config)
		if err != nil {
			return nil, err
		}

		encryptedData, err := provider.Encrypt(ctx, config, cred)
		if err != nil {
			return nil, err
		}

		encryptedDatas = append(encryptedDatas, encryptedData)
	}
	return encryptedDatas, nil
}

func NewEncryptionManager(co *v2.CredentialOptions, ec []*v2.EncryptionConfig) (*EncryptionManager, error) {
	em := &EncryptionManager{
		opts:    co,
		configs: ec,
	}
	return em, nil
}
