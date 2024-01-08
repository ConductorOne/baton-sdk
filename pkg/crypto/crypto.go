package crypto

import (
	"errors"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"

	"github.com/go-jose/go-jose/v3"
)

type PlaintextCredential struct {
	Name        string
	Description string
	Schema      string
	Bytes       []byte
}

type EncryptionManager interface {
	Encrypt(cred *PlaintextCredential) ([]*v2.EncryptedData, error)
}

type PubKeyEncryptionManager struct {
	opts    *v2.CredentialOptions
	configs []*v2.EncryptionConfig

	keys       map[string]*jose.JSONWebKey
	encrypters map[string]jose.Encrypter
}

func (pkem *PubKeyEncryptionManager) Encrypt(cred *PlaintextCredential) ([]*v2.EncryptedData, error) {
	encryptedDatas := make([]*v2.EncryptedData, 0, len(pkem.configs))

	for keyId, encrypter := range pkem.encrypters {
		jwe, err := encrypter.Encrypt(cred.Bytes)
		if err != nil {
			return nil, err
		}

		cypherText, err := jwe.CompactSerialize()
		if err != nil {
			return nil, err
		}

		encryptedData := &v2.EncryptedData{
			KeyId:          keyId,
			Name:           cred.Name,
			Description:    cred.Description,
			Schema:         cred.Schema,
			EncryptedBytes: []byte(cypherText),
		}
		encryptedDatas = append(encryptedDatas, encryptedData)
	}
	return encryptedDatas, nil
}

func NewPubKeyEncryptionManager(co *v2.CredentialOptions, ec []*v2.EncryptionConfig) (*PubKeyEncryptionManager, error) {
	if len(ec) == 0 {
		return nil, errors.New("public_key_encryption_manager: no encryption configs specified")
	}

	pkem := &PubKeyEncryptionManager{
		opts:       co,
		configs:    ec,
		keys:       make(map[string]*jose.JSONWebKey),
		encrypters: make(map[string]jose.Encrypter),
	}
	for _, conf := range ec {
		pkconf := conf.GetPublicKeyConfig()
		if pkconf == nil {
			return nil, errors.New("public_key_encryption_manager: public key config is required")
		}

		if pkconf.Provider != "" || pkconf.KeyId != "" {
			return nil, errors.New("public_key_encryption_manager: provider and key_id are not supported")
		}

		key, err := ParsePublicKey(pkconf.PubKey)
		if err != nil {
			return nil, fmt.Errorf("public_key_encryption_manager: failed parsing public key %w", err)
		}
		_, ok := pkem.keys[key.KeyID]
		if ok {
			return nil, fmt.Errorf("public_key_encryption_manager: duplicate key id %s", key.KeyID)
		}
		pkem.keys[key.KeyID] = key

		encryptor, err := jose.NewEncrypter(jose.A256GCM, jose.Recipient{Algorithm: jose.KeyAlgorithm(key.Algorithm), Key: key, KeyID: key.KeyID}, nil)
		if err != nil {
			return nil, err
		}
		_, ok = pkem.encrypters[key.KeyID]
		if ok {
			return nil, fmt.Errorf("public_key_encryption_manager: duplicate key id %s", key.KeyID)
		}
		pkem.encrypters[key.KeyID] = encryptor
	}

	return pkem, nil
}
