package udpop

import (
	"crypto"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"github.com/google/uuid"
)

const DPoPHeaderTyp = "dpop+jwt"

// DPoPClaims represents the claims in a DPoP proof token
type DPoPClaims struct {
	*jwt.Claims
	Htm   string `json:"htm"`
	Htu   string `json:"htu"`
	Ath   string `json:"ath,omitempty"` // Access token hash, when needed
	Nonce string `json:"nonce,omitempty"`
}

// DPoPSigner is responsible for creating DPoP proof tokens
type DPoPSigner struct {
	key   crypto.PrivateKey
	keyID string
}

func sha256AndBase64UrlEncode(input []byte) string {
	hash := sha256.Sum256(input)
	return base64.RawURLEncoding.EncodeToString(hash[:])
}

func keyThumbprint(key *jose.JSONWebKey) (string, error) {
	if key.IsPublic() {
		return "", errors.New("dpop_signer: invalid key: cannot generate thumbprint")
	}
	tb, err := key.Thumbprint(crypto.SHA256)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(tb), nil
}

// NewDPoPSigner creates a new DPoP signer with the given private key
func NewDPoPSigner(key crypto.PrivateKey) (*DPoPSigner, error) {
	// TODO(morgabra/kans): If we want to support other keys, they go here.
	_, ok := key.(ed25519.PrivateKey)
	if !ok {
		return nil, errors.New("dpop_signer: invalid key: must be an ed25519 private key")
	}

	keyID, err := keyThumbprint(&jose.JSONWebKey{Key: key})
	if err != nil {
		return nil, fmt.Errorf("dpop_signer: failed to generate key ID: %w", err)
	}
	return &DPoPSigner{
		key:   key,
		keyID: keyID,
	}, nil
}

func (s *DPoPSigner) Proof(method string, url string, accessToken string, nonce string) (string, error) {
	jti, err := uuid.NewRandom()
	if err != nil {
		return "", fmt.Errorf("dpop_signer: failed to generate JTI: %w", err)
	}

	claims := DPoPClaims{
		Claims: &jwt.Claims{
			IssuedAt: jwt.NewNumericDate(time.Now()),
			ID:       jti.String(),
		},
		Htm: method,
		Htu: url,
	}

	if nonce != "" {
		claims.Nonce = nonce
	}

	// ath:  Hash of the access token.  The value MUST be the result of a
	//    base64url encoding (as defined in Section 2 of [RFC7515]) the
	//    SHA-256 [SHS] hash of the ASCII encoding of the associated access
	//    token's value.
	if accessToken != "" {
		claims.Ath = sha256AndBase64UrlEncode([]byte(accessToken))
	}

	opts := &jose.SignerOptions{
		EmbedJWK: true,
		ExtraHeaders: map[jose.HeaderKey]interface{}{
			"typ": DPoPHeaderTyp,
		},
	}

	if s.keyID != "" {
		opts.ExtraHeaders["kid"] = s.keyID
	}

	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.EdDSA, Key: s.key}, opts)
	if err != nil {
		return "", fmt.Errorf("dpop_signer: failed to create signer: %w", err)
	}

	proof, err := jwt.Signed(signer).Claims(claims).Serialize()
	if err != nil {
		return "", fmt.Errorf("dpop_signer: failed to sign claims: %w", err)
	}

	return proof, nil
}
