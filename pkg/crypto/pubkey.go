package crypto

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/go-jose/go-jose/v3"
	"github.com/segmentio/ksuid"
)

var ErrInvalidPublicKey = errors.New("invalid public key")

func validatePublicKey(pubKey *jose.JSONWebKey) error {
	if pubKey.KeyID == "" {
		return fmt.Errorf("%w: kid is required", ErrInvalidPublicKey)
	}

	if !pubKey.Valid() {
		return ErrInvalidPublicKey
	}

	if !pubKey.IsPublic() {
		return fmt.Errorf("%w: key is not public", ErrInvalidPublicKey)
	}

	if pubKey.Use != "enc" {
		return fmt.Errorf("%w: invalid use (%s) - 'enc' is required", ErrInvalidPublicKey, pubKey.Use)
	}

	if pubKey.Algorithm != string(jose.ECDH_ES_A256KW) {
		return fmt.Errorf("%w: invalid algorithm (%s) - 'ECDH-ES+A256KW' is required", ErrInvalidPublicKey, pubKey.Algorithm)
	}

	_, ok := pubKey.Key.(*ecdsa.PublicKey)
	if !ok {
		return fmt.Errorf("%w: invalid key type - ecdsa is required", ErrInvalidPublicKey)
	}

	return nil
}

func NewEncryptor(pubKey *jose.JSONWebKey) (jose.Encrypter, error) {
	err := validatePublicKey(pubKey)
	if err != nil {
		return nil, err
	}
	return jose.NewEncrypter(jose.A256GCM, jose.Recipient{Algorithm: jose.KeyAlgorithm(pubKey.Algorithm), Key: pubKey, KeyID: pubKey.KeyID}, nil)
}

// GenKey generates a new ecdsa keypair and returns the private key and public key JWK
// It panics on any error.
func GenKey() (*ecdsa.PrivateKey, *jose.JSONWebKey) {
	key, err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	if err != nil {
		panic(err)
	}

	kid := ksuid.New().String()

	jsonPubKey := &jose.JSONWebKey{
		Key:       key.Public(),
		KeyID:     kid,
		Use:       "enc",
		Algorithm: string(jose.ECDH_ES_A256KW),
	}

	return key, jsonPubKey
}

// ParsePublicKey parses a public ecdsa JWK, all other key types return errors.
func ParsePublicKey(input []byte) (*jose.JSONWebKey, error) {
	pubKey := &jose.JSONWebKey{}
	err := pubKey.UnmarshalJSON(input)
	if err != nil {
		return nil, fmt.Errorf("%w: failed unmarshalling public key: %w", ErrInvalidPublicKey, err)
	}

	err = validatePublicKey(pubKey)
	if err != nil {
		return nil, err
	}

	return pubKey, nil
}
