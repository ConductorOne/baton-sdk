package xjwt

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/rsa"
	"encoding/hex"
	"fmt"

	jose "github.com/go-jose/go-jose/v3"
)

// ParsePrivateKey converts a private key from a PEM encoding to a *jose.JSONWebKey.
//
// Optionally, a passphrase for an encrypted X.509 document can be passed in.  If the
// contents of the PEM is not encrypted, passphrase is ignored.
func ParsePrivateKey(privateKey []byte, passphrase []byte) (*jose.JSONWebKey, jose.SignatureAlgorithm, error) {
	pk, err := unmarshalPrivateKey(privateKey, passphrase)
	if err != nil {
		return nil, "", err
	}

	key := &jose.JSONWebKey{
		Key: pk,
	}

	tp, err := key.Thumbprint(crypto.SHA512)
	if err != nil {
		return nil, "", err
	}

	key.KeyID = hex.EncodeToString(tp[0:8])

	var algorithm jose.SignatureAlgorithm
	switch pk := key.Key.(type) {
	case *rsa.PrivateKey:
		algorithm = jose.RS256
	case *ecdsa.PrivateKey:
		switch pk.Curve.Params().Name {
		case "P-256":
			algorithm = jose.ES256
		case "P-384":
			algorithm = jose.ES384
		case "P-512":
			algorithm = jose.ES512
		default:
			return nil, "", fmt.Errorf("unsupported key curve: %T (%s)", key.Key, pk.Curve.Params().Name)
		}
	default:
		return nil, "", fmt.Errorf("unsupported key type: %T", key.Key)
	}

	key.Algorithm = string(algorithm)
	return key, algorithm, nil
}
