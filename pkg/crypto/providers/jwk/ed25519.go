package jwk

import (
	"crypto/ed25519"
	"fmt"

	"filippo.io/age"
	"filippo.io/age/agessh"
	"golang.org/x/crypto/ssh"
)

func CreateED25519Recipient(pubKey ed25519.PublicKey) (*agessh.Ed25519Recipient, error) {
	sshPubKey, err := ssh.NewPublicKey(pubKey)
	if err != nil {
		return nil, fmt.Errorf("jwk-ed25519: failed to convert public key to ssh format: %w", err)
	}

	recipient, err := agessh.NewEd25519Recipient(sshPubKey)
	if err != nil {
		return nil, fmt.Errorf("jwk-ed25519: failed to create recipient: %w", err)
	}
	return recipient, nil
}

func EncryptED25519(pubKey ed25519.PublicKey, plaintext []byte) ([]byte, error) {
	recipient, err := CreateED25519Recipient(pubKey)
	if err != nil {
		return nil, err
	}

	ciphertext, err := ageEncrypt([]age.Recipient{recipient}, plaintext)
	if err != nil {
		return nil, fmt.Errorf("jwk-ed25519: %w", err)
	}
	return ciphertext, nil
}

func DecryptED25519(privKey ed25519.PrivateKey, ciphertext []byte) ([]byte, error) {
	identity, err := agessh.NewEd25519Identity(privKey)
	if err != nil {
		return nil, fmt.Errorf("jwk-ed25519: failed to create identity: %w", err)
	}

	plaintext, err := ageDecrypt(identity, ciphertext)
	if err != nil {
		return nil, fmt.Errorf("jwk-ed25519: %w", err)
	}
	return plaintext, nil
}
