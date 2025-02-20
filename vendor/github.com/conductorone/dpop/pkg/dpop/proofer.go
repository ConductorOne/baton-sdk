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
	"encoding/json"
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

// Claims represents the claims in a DPoP proof token
type Claims struct {
	*jwt.Claims
	HTTPMethod string                 `json:"htm"`           // HTTP method for the request
	HTTPUri    string                 `json:"htu"`           // HTTP URI for the request
	TokenHash  string                 `json:"ath,omitempty"` // Access token hash, when needed
	Nonce      string                 `json:"nonce,omitempty"`
	Additional map[string]interface{} `json:"-"`
}

// knownJWTClaimsFields is used to detect if new fields are added to jwt.Claims
// that we need to handle in MarshalJSON/UnmarshalJSON
var knownJWTClaimsFields = []string{
	"iss", // Issuer
	"sub", // Subject
	"aud", // Audience
	"exp", // Expiry
	"nbf", // NotBefore
	"iat", // IssuedAt
	"jti", // ID
}

// MarshalJSON implements json.Marshaler for Claims
func (c *Claims) MarshalJSON() ([]byte, error) {
	// Create a map to hold all claims
	allClaims := make(map[string]interface{})

	// Add the jwt.Claims fields directly
	if c.Claims != nil {
		if c.Claims.Issuer != "" {
			allClaims["iss"] = c.Claims.Issuer
		}
		if c.Claims.Subject != "" {
			allClaims["sub"] = c.Claims.Subject
		}
		if len(c.Claims.Audience) > 0 {
			aud, err := c.Claims.Audience.MarshalJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal audience: %w", err)
			}
			allClaims["aud"] = json.RawMessage(aud)
		}
		if c.Claims.Expiry != nil {
			exp, err := c.Claims.Expiry.MarshalJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal expiry: %w", err)
			}
			allClaims["exp"] = json.RawMessage(exp)
		}
		if c.Claims.NotBefore != nil {
			nbf, err := c.Claims.NotBefore.MarshalJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal not before: %w", err)
			}
			allClaims["nbf"] = json.RawMessage(nbf)
		}
		if c.Claims.IssuedAt != nil {
			iat, err := c.Claims.IssuedAt.MarshalJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal issued at: %w", err)
			}
			allClaims["iat"] = json.RawMessage(iat)
		}
		if c.Claims.ID != "" {
			allClaims["jti"] = c.Claims.ID
		}
	}

	// Add the explicit fields
	allClaims["htm"] = c.HTTPMethod
	allClaims["htu"] = c.HTTPUri
	if c.TokenHash != "" {
		allClaims["ath"] = c.TokenHash
	}
	if c.Nonce != "" {
		allClaims["nonce"] = c.Nonce
	}

	// Add any additional claims
	for k, v := range c.Additional {
		// Don't override standard claims
		if _, exists := allClaims[k]; !exists {
			allClaims[k] = v
		}
	}

	return json.Marshal(allClaims)
}

// UnmarshalJSON implements json.Unmarshaler for Claims
func (c *Claims) UnmarshalJSON(data []byte) error {
	// Create a map to hold all claims
	var allClaims map[string]interface{}
	if err := json.Unmarshal(data, &allClaims); err != nil {
		return err
	}

	// Create a temporary struct to hold the standard fields
	type StandardClaims struct {
		HTTPMethod string `json:"htm"`
		HTTPUri    string `json:"htu"`
		TokenHash  string `json:"ath,omitempty"`
		Nonce      string `json:"nonce,omitempty"`
	}

	// Unmarshal the standard fields
	var std StandardClaims
	if err := json.Unmarshal(data, &std); err != nil {
		return err
	}

	// Set the standard fields
	c.HTTPMethod = std.HTTPMethod
	c.HTTPUri = std.HTTPUri
	c.TokenHash = std.TokenHash
	c.Nonce = std.Nonce

	// Create a new jwt.Claims and unmarshal into it
	c.Claims = new(jwt.Claims)
	if err := json.Unmarshal(data, c.Claims); err != nil {
		return err
	}

	// Initialize the Additional map
	c.Additional = make(map[string]interface{})

	// Add any non-standard claims to Additional
	standardFields := map[string]bool{
		"htm": true, "htu": true, "ath": true, "nonce": true,
		"iss": true, "sub": true, "aud": true, "exp": true,
		"nbf": true, "iat": true, "jti": true,
	}

	for k, v := range allClaims {
		if !standardFields[k] {
			c.Additional[k] = v
		}
	}

	return nil
}

// ProofOption configures the behavior of proof generation
type ProofOption func(*proofOptions)

type proofOptions struct {
	validityDuration time.Duration
	nonceFunc        func() (string, error)
	accessToken      string
	additionalClaims map[string]interface{}
	now              func() time.Time
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

// defaultOptions returns the default options for proof generation
func defaultOptions() *proofOptions {
	return &proofOptions{
		validityDuration: DefaultProofValidityDuration,
		now:              time.Now,
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
			ID:        jti.String(),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now.Add(-30 * time.Second)),
			Expiry:    jwt.NewNumericDate(now.Add(options.validityDuration)),
		},
		HTTPMethod: method,
		HTTPUri:    url,
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
	finalClaims["nbf"] = claims.NotBefore
	finalClaims["exp"] = claims.Expiry
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
