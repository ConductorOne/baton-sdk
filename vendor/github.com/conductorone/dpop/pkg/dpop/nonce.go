package dpop

import (
	"context"
	"fmt"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"github.com/google/uuid"
)

// JWTNonce implements both NonceGenerator and NonceValidator using signed JWTs
type JWTNonce struct {
	// Window is the time window for which nonces are valid
	Window time.Duration

	// Signer is used to sign nonce JWTs
	Signer jose.Signer

	// Key is used to verify nonce JWTs
	Key jose.JSONWebKey
}

// nonceClaims represents the claims in a nonce JWT
type nonceClaims struct {
	jwt.Claims
	Random string `json:"rnd"` // Additional random component
}

// NewJWTNonce creates a new JWTNonce with the given signing key and verification key
func NewJWTNonce(signingKey jose.JSONWebKey, verificationKey jose.JSONWebKey) (*JWTNonce, error) {
	// Create signer with appropriate options
	signer, err := jose.NewSigner(
		jose.SigningKey{Algorithm: jose.EdDSA, Key: signingKey.Key},
		&jose.SignerOptions{
			ExtraHeaders: map[jose.HeaderKey]interface{}{
				"typ": "dpop-nonce+jwt",
				"kid": signingKey.KeyID,
			},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create signer: %w", err)
	}

	return &JWTNonce{
		Window: 5 * time.Minute,
		Signer: signer,
		Key:    verificationKey,
	}, nil
}

// GenerateNonce generates a new signed nonce JWT
func (v *JWTNonce) GenerateNonce(ctx context.Context) (string, error) {
	if ctx.Err() != nil {
		return "", ctx.Err()
	}

	// Generate random UUID for the nonce
	random, err := uuid.NewRandom()
	if err != nil {
		return "", fmt.Errorf("failed to generate random nonce: %w", err)
	}

	now := time.Now()
	claims := &nonceClaims{
		Claims: jwt.Claims{
			ID:        random.String(),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now.Add(-30 * time.Second)), // Allow for clock skew
			Expiry:    jwt.NewNumericDate(now.Add(v.Window)),
		},
		Random: random.String(),
	}

	// Sign the claims
	token, err := jwt.Signed(v.Signer).Claims(claims).Serialize()
	if err != nil {
		return "", fmt.Errorf("failed to sign nonce: %w", err)
	}

	return token, nil
}

// ValidateNonce validates a signed nonce JWT
func (v *JWTNonce) ValidateNonce(ctx context.Context, nonce string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Parse and verify the JWT
	token, err := jwt.ParseSigned(nonce, []jose.SignatureAlgorithm{jose.EdDSA})
	if err != nil {
		return fmt.Errorf("invalid nonce format: %w", err)
	}

	// Verify header
	if len(token.Headers) != 1 {
		return fmt.Errorf("invalid nonce: expected exactly one header")
	}
	header := token.Headers[0]
	if header.KeyID != v.Key.KeyID {
		return fmt.Errorf("invalid nonce: key ID mismatch")
	}
	if header.Algorithm != string(jose.EdDSA) {
		return fmt.Errorf("invalid nonce: algorithm mismatch")
	}
	if header.ExtraHeaders["typ"] != "dpop-nonce+jwt" {
		return fmt.Errorf("invalid nonce: type mismatch")
	}

	// Verify claims
	var claims nonceClaims
	if err := token.Claims(v.Key, &claims); err != nil {
		return fmt.Errorf("invalid nonce signature: %w", err)
	}

	// Verify time-based claims
	now := time.Now()
	if claims.Claims.IssuedAt != nil && now.Before(claims.Claims.IssuedAt.Time()) {
		return fmt.Errorf("nonce timestamp is in the future")
	}

	if claims.Claims.NotBefore != nil && now.Before(claims.Claims.NotBefore.Time()) {
		return fmt.Errorf("nonce timestamp is in the future")
	}

	if claims.Claims.Expiry != nil && now.After(claims.Claims.Expiry.Time()) {
		return fmt.Errorf("nonce has expired")
	}

	return nil
}

// Ensure JWTNonce implements both interfaces
var (
	_ NonceGenerator = (*JWTNonce)(nil).GenerateNonce
	_ NonceValidator = (*JWTNonce)(nil).ValidateNonce
)
