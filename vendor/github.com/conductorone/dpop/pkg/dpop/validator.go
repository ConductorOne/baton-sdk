package dpop

import (
	"context"
	"crypto"
	"crypto/subtle"
	"encoding/base64"
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

// AccessTokenBindingValidator validates that an access token is bound to a public key
type AccessTokenBindingValidator func(ctx context.Context, accessToken string, publicKey *jose.JSONWebKey) error

// validationOptions configures the behavior of a specific proof validation
type validationOptions struct {
	// ExpectedAccessToken is the expected access token bound to the proof
	expectedAccessToken string

	// ExpectedPublicKey is the expected public key that should be used to sign the proof
	expectedPublicKey *jose.JSONWebKey
}

// ValidationProofOption is a function that configures a validationOptions
type ValidationProofOption func(*validationOptions)

// WithProofExpectedAccessToken sets the expected access token for a specific proof validation
func WithProofExpectedAccessToken(token string) ValidationProofOption {
	return func(opts *validationOptions) {
		opts.expectedAccessToken = token
	}
}

// WithProofExpectedPublicKey sets the expected public key that should be used to sign the proof
func WithProofExpectedPublicKey(key *jose.JSONWebKey) ValidationProofOption {
	return func(opts *validationOptions) {
		opts.expectedPublicKey = key
	}
}

// options configures the behavior of proof validation
type options struct {
	// MaxClockSkew is the maximum allowed clock skew for proof validation
	maxClockSkew time.Duration

	// NonceValidator is used to validate server-provided nonces
	nonceValidator NonceValidator

	// AllowedSignatureAlgorithms specifies which signature algorithms are accepted
	allowedSignatureAlgorithms []jose.SignatureAlgorithm

	// JTIStore is used to track JTIs for replay prevention
	jtiStore CheckAndStoreJTI

	// Now returns the current time for proof validation
	now func() time.Time

	// AccessTokenBindingValidator validates that an access token is bound to a public key
	accessTokenBindingValidator AccessTokenBindingValidator
}

// Option is a function that configures a validationOptions
type Option func(*options)

// WithMaxClockSkew sets the maximum allowed clock skew
func WithMaxClockSkew(d time.Duration) Option {
	return func(opts *options) {
		opts.maxClockSkew = d
	}
}

// WithNonceValidator sets the nonce validator function
func WithNonceValidator(v NonceValidator) Option {
	return func(opts *options) {
		opts.nonceValidator = v
	}
}

// WithAllowedSignatureAlgorithms sets the allowed signature algorithms
func WithAllowedSignatureAlgorithms(algs []jose.SignatureAlgorithm) Option {
	return func(opts *options) {
		opts.allowedSignatureAlgorithms = algs
	}
}

// WithJTIStore sets the JTI store
func WithJTIStore(store CheckAndStoreJTI) Option {
	return func(opts *options) {
		opts.jtiStore = store
	}
}

// WithNowFunc sets the function used to get the current time
func WithNowFunc(f func() time.Time) Option {
	return func(opts *options) {
		opts.now = f
	}
}

// WithAccessTokenBindingValidator sets the access token binding validator
func WithAccessTokenBindingValidator(v AccessTokenBindingValidator) Option {
	return func(opts *options) {
		opts.accessTokenBindingValidator = v
	}
}

// Validator validates DPoP proofs
type Validator struct {
	opts *options
}

// NewValidator creates a new DPoP validator with the given options
func NewValidator(opts ...Option) *Validator {
	o := &options{}

	for _, option := range opts {
		if option == nil {
			continue
		}
		option(o)
	}

	if len(o.allowedSignatureAlgorithms) == 0 {
		o.allowedSignatureAlgorithms = []jose.SignatureAlgorithm{
			jose.EdDSA,
			jose.RS256,
			jose.ES256,
		}
	}

	if o.maxClockSkew == 0 {
		o.maxClockSkew = 30 * time.Second
	}

	if o.jtiStore == nil {
		o.jtiStore = NewMemoryJTIStore().CheckAndStoreJTI
	}

	if o.now == nil {
		o.now = time.Now
	}

	return &Validator{opts: o}
}

// ValidateProof validates a DPoP proof for the given HTTP method and URL
func (v *Validator) ValidateProof(ctx context.Context, proof string, method string, url string, options ...ValidationProofOption) (*Claims, error) {
	// Initialize validation proof options
	proofOpts := &validationOptions{}
	for _, option := range options {
		option(proofOpts)
	}

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

	if proofOpts.expectedPublicKey != nil {
		expectedThumbprint, err := proofOpts.expectedPublicKey.Thumbprint(crypto.SHA256)
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

	// Store the public key in the claims
	claims.publicKey = proofKey

	// Validate the proof
	if err := v.validateClaims(ctx, &claims, method, url, proofOpts); err != nil {
		return nil, err
	}

	return &claims, nil
}

// validateClaims validates the claims in a DPoP proof
func (v *Validator) validateClaims(ctx context.Context, claims *Claims, method, url string, proofOpts *validationOptions) error {
	now := v.opts.now()

	// Check required claims
	if claims.Claims.ID == "" {
		return fmt.Errorf("%w: missing required claim: jti", ErrInvalidProof)
	}
	if claims.Claims.IssuedAt == nil {
		return fmt.Errorf("%w: missing required claim: iat", ErrInvalidProof)
	}

	// Check that iat is ~now() (with skew leeway)
	iatTime := claims.Claims.IssuedAt.Time()
	if now.Add(-v.opts.maxClockSkew).After(iatTime) || now.Add(v.opts.maxClockSkew).Before(iatTime) {
		return fmt.Errorf("%w: issued at time is not within acceptable range of current time", ErrInvalidProof)
	}

	// nbf is optional, but if present, validate it
	if claims.Claims.NotBefore != nil {
		if now.Add(v.opts.maxClockSkew).Before(claims.Claims.NotBefore.Time()) {
			return fmt.Errorf("%w: not yet valid", ErrExpiredProof)
		}
	}

	// exp is optional, but if present, validate it
	if claims.Claims.Expiry != nil {
		if now.Add(-v.opts.maxClockSkew).After(claims.Claims.Expiry.Time()) {
			return fmt.Errorf("%w: token has expired", ErrExpiredProof)
		}
	}

	// Validate HTTP method
	if !strings.EqualFold(claims.HTTPMethod, method) {
		return fmt.Errorf("%w: method mismatch", ErrInvalidProof)
	}

	// Validate URL
	if !strings.EqualFold(claims.HTTPUri, url) {
		return fmt.Errorf("%w: URI mismatch", ErrInvalidProof)
	}

	// Validate access token binding
	if proofOpts.expectedAccessToken != "" {
		if claims.TokenHash == "" {
			return fmt.Errorf("%w: missing token hash", ErrInvalidTokenBinding)
		}

		// First validate the token hash
		expectedHash := hashAccessToken(proofOpts.expectedAccessToken)
		if subtle.ConstantTimeCompare([]byte(claims.TokenHash), []byte(expectedHash)) != 1 {
			return ErrInvalidTokenBinding
		}

		// If we have an access token, we must also validate that it is bound to public key in the proof.
		if v.opts.accessTokenBindingValidator == nil {
			return fmt.Errorf("%w: no access token binding validator provided", ErrInvalidTokenBinding)
		}
		if err := v.opts.accessTokenBindingValidator(ctx, proofOpts.expectedAccessToken, claims.publicKey); err != nil {
			return err
		}
	} else {
		// If we do not expect an access token, assert that we do not have a token hash in the proof
		if claims.TokenHash != "" {
			return fmt.Errorf("%w: unexpected token hash", ErrInvalidTokenBinding)
		}
	}

	// Validate nonce if validator is configured
	if v.opts.nonceValidator != nil {
		if claims.Nonce == "" {
			return fmt.Errorf("%w: nonce required but not provided", ErrInvalidNonce)
		}
		if err := v.opts.nonceValidator(ctx, claims.Nonce); err != nil {
			return err // Return the error directly from the nonce validator
		}
	} else {
		// If we do not expect a nonce, assert that we do not have one in the proof
		if claims.Nonce != "" {
			return fmt.Errorf("%w: unexpected nonce", ErrInvalidNonce)
		}
	}

	if claims.Claims.ID == "" {
		return fmt.Errorf("%w: jti required but not provided", ErrInvalidProof)
	}

	if len(claims.Claims.ID) > MaxJTISize {
		return fmt.Errorf("%w: jti too large", ErrInvalidProof)
	}

	// Check for JTI replay if store is configured
	if v.opts.jtiStore != nil {
		if err := v.opts.jtiStore(ctx, claims.Claims.ID); err != nil {
			return fmt.Errorf("failed to check/store JTI: %w", err)
		}
	}

	return nil
}

// NewJWTAccessTokenBindingValidator creates a validator that checks if a JWT access token
// is bound to a DPoP proof via the cnf/jkt claim
func NewJWTAccessTokenBindingValidator(allowedSignatureAlgorithms []jose.SignatureAlgorithm) AccessTokenBindingValidator {
	return func(ctx context.Context, accessToken string, publicKey *jose.JSONWebKey) error {
		// Parse the JWT
		token, err := jwt.ParseSigned(accessToken, allowedSignatureAlgorithms)
		if err != nil {
			return fmt.Errorf("%w: failed to parse access token: %v", ErrInvalidTokenBinding, err)
		}

		// Extract the claims without verification (we're only looking at the cnf claim)
		var claims map[string]interface{}
		if err := token.UnsafeClaimsWithoutVerification(&claims); err != nil {
			return fmt.Errorf("%w: failed to extract claims: %v", ErrInvalidTokenBinding, err)
		}

		// Check for cnf claim
		cnfRaw, ok := claims["cnf"]
		if !ok {
			return fmt.Errorf("%w: no cnf claim in access token", ErrInvalidTokenBinding)
		}

		// Convert to map
		cnf, ok := cnfRaw.(map[string]interface{})
		if !ok {
			return fmt.Errorf("%w: invalid cnf claim format", ErrInvalidTokenBinding)
		}

		// Check for jkt claim
		jktRaw, ok := cnf["jkt"]
		if !ok {
			return fmt.Errorf("%w: no jkt in cnf claim", ErrInvalidTokenBinding)
		}

		jkt, ok := jktRaw.(string)
		if !ok {
			return fmt.Errorf("%w: invalid jkt format", ErrInvalidTokenBinding)
		}

		// Calculate thumbprint of the proof key
		proofThumbprint, err := publicKey.Thumbprint(crypto.SHA256)
		if err != nil {
			return fmt.Errorf("%w: failed to generate proof thumbprint: %v", ErrInvalidTokenBinding, err)
		}

		// Decode the expected thumbprint
		expectedThumbprint, err := base64.RawURLEncoding.DecodeString(jkt)
		if err != nil {
			return fmt.Errorf("%w: invalid jkt format in token: %v", ErrInvalidTokenBinding, err)
		}

		// Compare thumbprints
		if subtle.ConstantTimeCompare(proofThumbprint, expectedThumbprint) != 1 {
			return fmt.Errorf("%w: jkt mismatch", ErrInvalidTokenBinding)
		}

		return nil
	}
}
