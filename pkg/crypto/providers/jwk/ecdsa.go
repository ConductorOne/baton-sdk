package jwk

import (
	"crypto/ecdsa"
	"fmt"

	"github.com/go-jose/go-jose/v4"
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
	jwe, err := jose.ParseEncryptedCompact(string(ciphertext),
		[]jose.KeyAlgorithm{
			jose.ECDH_ES,
			jose.ECDH_ES_A128KW,
			jose.ECDH_ES_A192KW,
			jose.ECDH_ES_A256KW,
		},
		[]jose.ContentEncryption{
			jose.A128CBC_HS256,
			jose.A192CBC_HS384,
			jose.A256CBC_HS512,
			jose.A128GCM,
			jose.A192GCM,
			jose.A256GCM,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("jwk-ecdsa: failed to parse ciphertext: %w", err)
	}

	plaintext, err := jwe.Decrypt(privKey)
	if err != nil {
		return nil, fmt.Errorf("jwk-ecdsa: failed to decrypt: %w", err)
	}

	return plaintext, nil
}
