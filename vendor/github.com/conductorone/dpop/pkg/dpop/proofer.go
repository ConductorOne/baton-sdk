// Package dpop provides the core DPoP proof generation and validation functionality.
package dpop

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
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

const (
	// DPoPHeaderTyp is the required type for DPoP JWTs
	DPoPHeaderTyp = "dpop+jwt"

	// DefaultProofValidityDuration is the default duration for which a proof is considered valid
	DefaultProofValidityDuration = 5 * time.Minute
)

var (
	// ErrInvalidKey is returned when an unsupported key type is provided
	ErrInvalidKey = errors.New("invalid key: must be an ed25519 private key, RSA private key, or ECDSA P-256 private key")

	// ErrInvalidRSAKeySize is returned when an RSA key is too small
	ErrInvalidRSAKeySize = errors.New("invalid RSA key size: must be at least 2048 bits")

	// ErrInvalidECDSACurve is returned when an ECDSA key uses an unsupported curve
	ErrInvalidECDSACurve = errors.New("invalid ECDSA curve: must be P-256")
)

// ProofOption configures the behavior of proof generation
type ProofOption func(*proofOptions)

type proofOptions struct {
	validityDuration time.Duration
	nonceFunc        func() (string, error)
	accessToken      string
	additionalClaims map[string]interface{}
	now              func() time.Time
	includeNotBefore bool
	includeExpiry    bool
}

// WithValidityDuration sets the duration for which the proof is valid
func WithValidityDuration(d time.Duration) ProofOption {
	return func(o *proofOptions) {
		o.validityDuration = d
	}
}

// WithNonceFunc sets a function that provides the most recent nonce for replay prevention
func WithNonceFunc(f func() (string, error)) ProofOption {
	return func(o *proofOptions) {
		o.nonceFunc = f
	}
}

// WithStaticNonce sets a static nonce value for replay prevention
// This is a helper function for backward compatibility
func WithStaticNonce(nonce string) ProofOption {
	return WithNonceFunc(func() (string, error) {
		return nonce, nil
	})
}

// WithAccessToken sets an access token to bind to this proof
func WithAccessToken(token string) ProofOption {
	return func(o *proofOptions) {
		o.accessToken = token
	}
}

// WithAdditionalClaims sets additional claims to include in the proof
func WithAdditionalClaims(claims map[string]interface{}) ProofOption {
	return func(o *proofOptions) {
		o.additionalClaims = claims
	}
}

// WithProofNowFunc sets a custom time function for the proof
func WithProofNowFunc(f func() time.Time) ProofOption {
	return func(o *proofOptions) {
		o.now = f
	}
}

// WithNotBefore sets whether to include the nbf claim in the proof
func WithNotBefore(include bool) ProofOption {
	return func(o *proofOptions) {
		o.includeNotBefore = include
	}
}

// WithExpiry sets whether to include the exp claim in the proof
func WithExpiry(include bool) ProofOption {
	return func(o *proofOptions) {
		o.includeExpiry = include
	}
}

// defaultOptions returns the default options for proof generation
func defaultOptions() *proofOptions {
	return &proofOptions{
		validityDuration: DefaultProofValidityDuration,
		now:              time.Now,
		includeNotBefore: true, // For backward compatibility
		includeExpiry:    true, // For backward compatibility
	}
}

// Proofer generates DPoP proofs
type Proofer struct {
	key   *jose.JSONWebKey
	keyID string
	alg   jose.SignatureAlgorithm
}

// NewProofer creates a new DPoP proofer with the given private key
func NewProofer(key *jose.JSONWebKey) (*Proofer, error) {
	if key == nil {
		return nil, errors.New("key cannot be nil")
	}

	if key.IsPublic() {
		return nil, errors.New("key must be a private key")
	}

	var alg jose.SignatureAlgorithm
	switch key.Key.(type) {
	case ed25519.PrivateKey:
		alg = jose.EdDSA
	case *rsa.PrivateKey:
		alg = jose.RS256
		// Check RSA key size
		rsaKey := key.Key.(*rsa.PrivateKey)
		if rsaKey.Size()*8 < 2048 {
			return nil, ErrInvalidRSAKeySize
		}
	case *ecdsa.PrivateKey:
		ecKey := key.Key.(*ecdsa.PrivateKey)
		// Only support P-256 curve for now
		if ecKey.Curve != elliptic.P256() {
			return nil, ErrInvalidECDSACurve
		}
		alg = jose.ES256
	default:
		return nil, ErrInvalidKey
	}

	thumbprint, err := key.Thumbprint(crypto.SHA256)
	if err != nil {
		return nil, fmt.Errorf("failed to generate key thumbprint: %w", err)
	}

	return &Proofer{
		key:   key,
		keyID: hex.EncodeToString(thumbprint),
		alg:   alg,
	}, nil
}

// CreateProof generates a new DPoP proof for the given HTTP method and URL
func (p *Proofer) CreateProof(ctx context.Context, method string, url string, opts ...ProofOption) (string, error) {
	options := defaultOptions()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(options)
	}

	jti, err := uuid.NewRandom()
	if err != nil {
		return "", fmt.Errorf("failed to generate JTI: %w", err)
	}

	var now time.Time
	if options.now != nil {
		now = options.now()
	} else {
		now = time.Now()
	}

	claims := Claims{
		Claims: &jwt.Claims{
			ID:       jti.String(),
			IssuedAt: jwt.NewNumericDate(now),
		},
		HTTPMethod: method,
		HTTPUri:    url,
	}

	if options.includeNotBefore {
		claims.Claims.NotBefore = jwt.NewNumericDate(now.Add(-30 * time.Second))
	}

	if options.includeExpiry {
		claims.Claims.Expiry = jwt.NewNumericDate(now.Add(options.validityDuration))
	}

	// Get nonce if function is provided
	if options.nonceFunc != nil {
		nonce, err := options.nonceFunc()
		if err != nil {
			return "", fmt.Errorf("failed to get nonce: %w", err)
		}
		claims.Nonce = nonce
	}

	if options.accessToken != "" {
		claims.TokenHash = hashAccessToken(options.accessToken)
	}

	// Create a copy of the public key
	publicKey := p.key.Public()

	signerOpts := &jose.SignerOptions{
		EmbedJWK: false,
		ExtraHeaders: map[jose.HeaderKey]interface{}{
			"typ": DPoPHeaderTyp,
			"jwk": publicKey,
		},
	}

	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: p.alg, Key: p.key.Key}, signerOpts)
	if err != nil {
		return "", fmt.Errorf("failed to create signer: %w", err)
	}

	// Create the final claims by merging the standard claims with any additional claims
	finalClaims := make(map[string]interface{})

	// Add the standard claims
	finalClaims["jti"] = claims.ID
	finalClaims["iat"] = claims.IssuedAt
	if claims.Claims.NotBefore != nil {
		finalClaims["nbf"] = claims.NotBefore
	}
	if claims.Claims.Expiry != nil {
		finalClaims["exp"] = claims.Expiry
	}
	finalClaims["htm"] = claims.HTTPMethod
	finalClaims["htu"] = claims.HTTPUri
	if claims.Nonce != "" {
		finalClaims["nonce"] = claims.Nonce
	}
	if claims.TokenHash != "" {
		finalClaims["ath"] = claims.TokenHash
	}

	// Merge any additional claims
	if options.additionalClaims != nil {
		for k, v := range options.additionalClaims {
			// Don't allow overriding of standard DPoP claims
			if _, exists := finalClaims[k]; !exists {
				finalClaims[k] = v
			}
		}
	}

	proof, err := jwt.Signed(signer).Claims(finalClaims).Serialize()
	if err != nil {
		return "", fmt.Errorf("failed to sign claims: %w", err)
	}

	return proof, nil
}

// hashAccessToken creates a base64url-encoded SHA-256 hash of an access token
func hashAccessToken(token string) string {
	hash := sha256.Sum256([]byte(token))
	return base64.RawURLEncoding.EncodeToString(hash[:])
}
