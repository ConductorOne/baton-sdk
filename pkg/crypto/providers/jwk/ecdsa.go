package jwk

import (
	"crypto/ecdsa"
	"fmt"

	"github.com/go-jose/go-jose/v3"
)

func EncryptECDSA(pubKey *ecdsa.PublicKey, plaintext []byte) ([]byte, error) {
	encryptor, err := jose.NewEncrypter(jose.A256GCM, jose.Recipient{Algorithm: jose.ECDH_ES, Key: pubKey}, nil)
	if err != nil {
		return nil, fmt.Errorf("jwk-ecdsa: failed to create encryptor: %w", err)
	}

	ciphertext, err := encryptor.Encrypt(plaintext)
	if err != nil {
		return nil, fmt.Errorf("jwk-ecdsa: failed to encrypt: %w", err)
	}

	jwe, err := ciphertext.CompactSerialize()
	if err != nil {
		return nil, fmt.Errorf("jwk-ecdsa: failed to marshal ciphertext: %w", err)
	}

	return []byte(jwe), nil
}

func DecryptECDSA(privKey *ecdsa.PrivateKey, ciphertext []byte) ([]byte, error) {
	jwe, err := jose.ParseEncrypted(string(ciphertext))
	if err != nil {
		return nil, fmt.Errorf("jwk-ecdsa: failed to parse ciphertext: %w", err)
	}

	plaintext, err := jwe.Decrypt(privKey)
	if err != nil {
		return nil, fmt.Errorf("jwk-ecdsa: failed to decrypt: %w", err)
	}

	return plaintext, nil
}
