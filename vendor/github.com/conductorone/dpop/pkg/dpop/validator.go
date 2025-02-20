package dpop

import (
	"context"
	"crypto"
	"crypto/subtle"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

var (
	// ErrInvalidProof is returned when a proof is invalid
	ErrInvalidProof = errors.New("invalid DPoP proof")

	// ErrExpiredProof is returned when a proof has expired
	ErrExpiredProof = errors.New("expired DPoP proof")

	// ErrInvalidNonce is returned when a proof has an invalid nonce
	ErrInvalidNonce = errors.New("invalid nonce in DPoP proof")

	// ErrInvalidTokenBinding is returned when a proof has an invalid access token binding
	ErrInvalidTokenBinding = errors.New("invalid access token binding in DPoP proof")
)

const (
	// MaxJTISize is the maximum allowed size for the JTI claim in bytes
	MaxJTISize = 1024 // 1KB should be more than enough for a UUID
)

// NonceGenerator is a function that generates server-provided nonces for DPoP proofs
type NonceGenerator func(ctx context.Context) (string, error)

// NonceValidator is a function that validates server-provided nonces for DPoP proofs
type NonceValidator func(ctx context.Context, nonce string) error

// ValidationOptions configures the behavior of proof validation
type validationOptions struct {
	// MaxClockSkew is the maximum allowed clock skew for proof validation
	maxClockSkew time.Duration

	// NonceValidator is used to validate server-provided nonces
	nonceValidator NonceValidator

	// ExpectedAccessToken is the expected access token bound to the proof
	expectedAccessToken string

	// RequireAccessTokenBinding indicates whether access token binding is required
	requireAccessTokenBinding bool

	// AllowedSignatureAlgorithms specifies which signature algorithms are accepted
	allowedSignatureAlgorithms []jose.SignatureAlgorithm

	// JTIStore is used to track JTIs for replay prevention
	jtiStore CheckAndStoreJTI

	// Now returns the current time for proof validation
	now func() time.Time

	// ExpectedPublicKey is the expected public key that should be used to sign the proof
	expectedPublicKey *jose.JSONWebKey
}

// Option is a function that configures a validationOptions
type Option func(*validationOptions)

// WithMaxClockSkew sets the maximum allowed clock skew
func WithMaxClockSkew(d time.Duration) Option {
	return func(opts *validationOptions) {
		opts.maxClockSkew = d
	}
}

// WithNonceValidator sets the nonce validator function
func WithNonceValidator(v NonceValidator) Option {
	return func(opts *validationOptions) {
		opts.nonceValidator = v
	}
}

// WithExpectedAccessToken sets the expected access token
func WithExpectedAccessToken(token string) Option {
	return func(opts *validationOptions) {
		opts.expectedAccessToken = token
	}
}

// WithRequireAccessTokenBinding sets whether access token binding is required
func WithRequireAccessTokenBinding(required bool) Option {
	return func(opts *validationOptions) {
		opts.requireAccessTokenBinding = required
	}
}

// WithAllowedSignatureAlgorithms sets the allowed signature algorithms
func WithAllowedSignatureAlgorithms(algs []jose.SignatureAlgorithm) Option {
	return func(opts *validationOptions) {
		opts.allowedSignatureAlgorithms = algs
	}
}

// WithJTIStore sets the JTI store
func WithJTIStore(store CheckAndStoreJTI) Option {
	return func(opts *validationOptions) {
		opts.jtiStore = store
	}
}

// WithNowFunc sets the function used to get the current time
func WithNowFunc(f func() time.Time) Option {
	return func(opts *validationOptions) {
		opts.now = f
	}
}

// WithExpectedPublicKey sets the expected public key that should be used to sign the proof
func WithExpectedPublicKey(key *jose.JSONWebKey) Option {
	return func(opts *validationOptions) {
		opts.expectedPublicKey = key
	}
}

// Validator validates DPoP proofs
type Validator struct {
	opts *validationOptions
}

// NewValidator creates a new DPoP validator with the given options
func NewValidator(options ...Option) *Validator {
	opts := &validationOptions{}

	for _, option := range options {
		if option == nil {
			continue
		}
		option(opts)
	}

	if len(opts.allowedSignatureAlgorithms) == 0 {
		opts.allowedSignatureAlgorithms = []jose.SignatureAlgorithm{
			jose.EdDSA,
			jose.RS256,
			jose.ES256,
		}
	}

	if opts.maxClockSkew == 0 {
		opts.maxClockSkew = 30 * time.Second
	}

	if opts.jtiStore == nil {
		opts.jtiStore = NewMemoryJTIStore().CheckAndStoreJTI
	}

	if opts.now == nil {
		opts.now = time.Now
	}

	return &Validator{opts: opts}
}

// ValidateProof validates a DPoP proof for the given HTTP method and URL
func (v *Validator) ValidateProof(ctx context.Context, proof string, method string, url string) (*Claims, error) {
	token, err := jwt.ParseSigned(proof, v.opts.allowedSignatureAlgorithms)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to parse token: %v", ErrInvalidProof, err)
	}

	if len(token.Headers) != 1 {
		return nil, fmt.Errorf("%w: expected 1 header, got %d", ErrInvalidProof, len(token.Headers))
	}

	header := token.Headers[0]
	// Verify the token type
	if typ, ok := header.ExtraHeaders["typ"]; !ok || typ != DPoPHeaderTyp {
		return nil, fmt.Errorf("%w: invalid token type", ErrInvalidProof)
	}

	proofKey := header.JSONWebKey
	// Extract the embedded JWK
	if proofKey == nil {
		return nil, fmt.Errorf("%w: no embedded JWK", ErrInvalidProof)
	}

	if !proofKey.IsPublic() {
		return nil, fmt.Errorf("%w: expected public key", ErrInvalidProof)
	}

	if !proofKey.Valid() {
		return nil, fmt.Errorf("%w: invalid JWK", ErrInvalidProof)
	}

	if v.opts.expectedPublicKey != nil {
		expectedThumbprint, err := v.opts.expectedPublicKey.Thumbprint(crypto.SHA256)
		if err != nil {
			return nil, fmt.Errorf("%w: failed to generate expected thumbprint: %v", ErrInvalidProof, err)
		}
		proofThumbprint, err := proofKey.Thumbprint(crypto.SHA256)
		if err != nil {
			return nil, fmt.Errorf("%w: failed to generate proof thumbprint: %v", ErrInvalidProof, err)
		}
		if subtle.ConstantTimeCompare(expectedThumbprint, proofThumbprint) != 1 {
			return nil, fmt.Errorf("%w: invalid key binding", ErrInvalidProof)
		}
	}

	// Verify the claims
	var claims Claims
	if err := token.Claims(proofKey, &claims); err != nil {
		return nil, fmt.Errorf("%w: failed to verify claims: %v", ErrInvalidProof, err)
	}

	// Validate the proof
	if err := v.validateClaims(ctx, &claims, method, url); err != nil {
		return nil, err
	}

	return &claims, nil
}

// validateClaims validates the claims in a DPoP proof
func (v *Validator) validateClaims(ctx context.Context, claims *Claims, method, url string) error {
	now := v.opts.now()

	// Check required claims
	if claims.Claims.ID == "" {
		return fmt.Errorf("%w: missing required claim: jti", ErrInvalidProof)
	}

	if claims.Claims.IssuedAt == nil {
		return fmt.Errorf("%w: missing required claim: iat", ErrInvalidProof)
	}

	if claims.Claims.NotBefore == nil {
		return fmt.Errorf("%w: missing required claim: nbf", ErrInvalidProof)
	}

	if claims.Claims.Expiry == nil {
		return fmt.Errorf("%w: missing required claim: exp", ErrInvalidProof)
	}

	// Check that iat is not in the future (with leeway)
	if now.Add(v.opts.maxClockSkew).Before(claims.Claims.IssuedAt.Time()) {
		return fmt.Errorf("%w: invalid iat", ErrInvalidProof)
	}

	// Check time-based claims with leeway
	if now.Add(v.opts.maxClockSkew).Before(claims.Claims.NotBefore.Time()) {
		return fmt.Errorf("%w: not yet valid", ErrExpiredProof)
	}

	if now.Add(-v.opts.maxClockSkew).After(claims.Claims.Expiry.Time()) {
		return fmt.Errorf("%w: token has expired", ErrExpiredProof)
	}

	// Validate HTTP method
	if !strings.EqualFold(claims.HTTPMethod, method) {
		return fmt.Errorf("%w: method mismatch", ErrInvalidProof)
	}

	// Validate URL
	if !strings.EqualFold(claims.HTTPUri, url) {
		return fmt.Errorf("%w: URI mismatch", ErrInvalidProof)
	}

	// Validate nonce if validator is configured
	if v.opts.nonceValidator != nil {
		if claims.Nonce == "" {
			return fmt.Errorf("%w: nonce required but not provided", ErrInvalidNonce)
		}
		if err := v.opts.nonceValidator(ctx, claims.Nonce); err != nil {
			return err // Return the error directly from the nonce validator
		}
	}

	// Check JTI size to prevent memory exhaustion attacks
	if len(claims.Claims.ID) > MaxJTISize {
		return fmt.Errorf("%w: jti too large", ErrInvalidProof)
	}

	if claims.Claims.ID == "" {
		return fmt.Errorf("%w: jti required but not provided", ErrInvalidProof)
	}

	// Check for JTI replay if store is configured
	if v.opts.jtiStore != nil {
		if claims.Nonce == "" {
			return fmt.Errorf("%w: nonce required but not provided", ErrInvalidNonce)
		}
		if err := v.opts.jtiStore(ctx, claims.Claims.ID, claims.Nonce); err != nil {
			return fmt.Errorf("failed to check/store JTI: %w", err)
		}
	}

	// Validate access token binding if required
	if v.opts.requireAccessTokenBinding || v.opts.expectedAccessToken != "" {
		if claims.TokenHash == "" {
			return fmt.Errorf("%w: missing token hash", ErrInvalidTokenBinding)
		}

		if v.opts.expectedAccessToken != "" {
			expectedHash := hashAccessToken(v.opts.expectedAccessToken)
			if subtle.ConstantTimeCompare([]byte(claims.TokenHash), []byte(expectedHash)) != 1 {
				return ErrInvalidTokenBinding
			}
		}
	}

	return nil
}
