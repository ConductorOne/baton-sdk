package jwk

import (
	"crypto/rsa"
	"fmt"

	"filippo.io/age"
	"filippo.io/age/agessh"
	"golang.org/x/crypto/ssh"
)

func CreateRSARecipient(pubKey *rsa.PublicKey) (*agessh.RSARecipient, error) {
	sshPubKey, err := ssh.NewPublicKey(pubKey)
	if err != nil {
		return nil, fmt.Errorf("jwk-rsa: failed to convert public key to ssh format: %w", err)
	}

	recipient, err := agessh.NewRSARecipient(sshPubKey)
	if err != nil {
		return nil, fmt.Errorf("jwk-rsa: failed to create recipient: %w", err)
	}

	return recipient, nil
}

func EncryptRSA(pubKey *rsa.PublicKey, plaintext []byte) ([]byte, error) {
	recipient, err := CreateRSARecipient(pubKey)
	if err != nil {
		return nil, err
	}

	ciphertext, err := ageEncrypt([]age.Recipient{recipient}, plaintext)
	if err != nil {
		return nil, fmt.Errorf("jwk-rsa: %w", err)
	}
	return ciphertext, nil
}

func DecryptRSA(privKey *rsa.PrivateKey, ciphertext []byte) ([]byte, error) {
	identity, err := agessh.NewRSAIdentity(privKey)
	if err != nil {
		return nil, fmt.Errorf("jwk-rsa: failed to create identity: %w", err)
	}

	plaintext, err := ageDecrypt(identity, ciphertext)
	if err != nil {
		return nil, fmt.Errorf("jwk-rsa: %w", err)
	}
	return plaintext, nil
}
