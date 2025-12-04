package crypto //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"errors"

	"github.com/conductorone/baton-sdk/pkg/crypto/providers/jwk"
	"github.com/go-jose/go-jose/v4"
)

var ErrInvalidClientSecret = errors.New("invalid client secret")
var v1SecretTokenIdentifier = []byte("v1")

func ParseClientSecret(input []byte, setKeyID bool) (*jose.JSONWebKey, error) {
	items := bytes.SplitN(input, []byte(":"), 4)
	if len(items) != 4 {
		return nil, ErrInvalidClientSecret
	}

	if !bytes.Equal(items[2], v1SecretTokenIdentifier) {
		return nil, ErrInvalidClientSecret
	}

	jwkData, err := base64.RawURLEncoding.DecodeString(string(items[3]))
	if err != nil {
		return nil, ErrInvalidClientSecret
	}

	npk := &jose.JSONWebKey{}
	err = npk.UnmarshalJSON(jwkData)
	if err != nil {
		return nil, ErrInvalidClientSecret
	}

	if npk.IsPublic() || !npk.Valid() {
		return nil, ErrInvalidClientSecret
	}

	_, ok := npk.Key.(ed25519.PrivateKey)
	if !ok {
		return nil, ErrInvalidClientSecret
	}

	if setKeyID && npk.KeyID == "" {
		kid, err := jwk.Thumbprint(npk)
		if err != nil {
			return nil, err
		}

		npk.KeyID = kid
	}

	return npk, nil
}
