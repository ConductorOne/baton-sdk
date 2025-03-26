package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/pquerna/xjwt"
	"github.com/pquerna/xjwt/xkeyset"
)

// Config holds the configuration for JWT validation.
type Config struct {
	PublicKeyJWK         string        // JWK format public key
	JWKSUrl              string        // URL to JWKS endpoint (mutually exclusive with PublicKeyJWK)
	Issuer               string        // Expected issuer
	Subject              string        // Expected subject (optional)
	Audience             string        // Expected audience (optional)
	Nonce                string        // Expected nonce (optional)
	MaxExpirationFromNow time.Duration // Maximum expiration time from now (optional, defaults to 24 hours)
}

type KeySet interface {
	Get() (*jose.JSONWebKeySet, error)
}

// Validator handles JWT validation with a specific configuration.
type Validator struct {
	config Config
	keySet KeySet

	now func() time.Time
}

type staticKeySet struct {
	keyset *jose.JSONWebKeySet
}

func (s *staticKeySet) Get() (*jose.JSONWebKeySet, error) {
	return s.keyset, nil
}

// NewValidator creates a new JWT validator with the given configuration.
func NewValidator(ctx context.Context, config Config) (*Validator, error) {
	// Ensure only one key source is provided.
	if config.PublicKeyJWK != "" && config.JWKSUrl != "" {
		return nil, fmt.Errorf("cannot specify both PublicKeyJWK and JWKSUrl")
	}
	if config.PublicKeyJWK == "" && config.JWKSUrl == "" {
		return nil, fmt.Errorf("must specify either PublicKeyJWK or JWKSUrl")
	}

	var keySet KeySet
	if config.PublicKeyJWK != "" {
		var jwk jose.JSONWebKey
		if err := json.Unmarshal([]byte(config.PublicKeyJWK), &jwk); err != nil {
			return nil, fmt.Errorf("failed to parse JWK: %w", err)
		}

		if !jwk.Valid() {
			return nil, fmt.Errorf("invalid JWK")
		}

		// Create a key set with the single key.
		keySet = &staticKeySet{
			keyset: &jose.JSONWebKeySet{
				Keys: []jose.JSONWebKey{jwk},
			},
		}
	} else {
		// Use JWKS URL.
		remoteKeySet, err := xkeyset.New(ctx, xkeyset.Options{
			URL:            config.JWKSUrl,
			RefreshContext: ctx,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create keyset from JWKS URL: %w", err)
		}

		// Load the cache.
		_, err = remoteKeySet.Get()
		if err != nil {
			return nil, fmt.Errorf("failed to get keyset from JWKS URL: %w", err)
		}
		keySet = remoteKeySet
	}

	// Set default MaxExpiry if not specified.
	if config.MaxExpirationFromNow == 0 {
		config.MaxExpirationFromNow = 24 * time.Hour // TODO(morgabra): ??
	}

	return &Validator{
		config: config,
		keySet: keySet,
		now:    time.Now,
	}, nil
}

// ValidateToken validates the JWT token string and returns the claims if valid.
func (v *Validator) ValidateToken(ctx context.Context, token string) (map[string]interface{}, error) {
	if token == "" {
		return nil, fmt.Errorf("token is empty")
	}

	keyset, err := v.keySet.Get()
	if err != nil {
		return nil, fmt.Errorf("failed to get keyset: %w", err)
	}

	// Now validate the token.
	vc := xjwt.VerifyConfig{
		ExpectedIssuer:             v.config.Issuer,
		ExpectedSubject:            v.config.Subject,
		ExpectedAudience:           v.config.Audience,
		ExpectedNonce:              v.config.Nonce,
		KeySet:                     keyset,
		Now:                        v.now,
		MaxExpirationFromNow:       v.config.MaxExpirationFromNow,
		ExpectSSignatureAlgorithms: []jose.SignatureAlgorithm{},
	}

	data, err := xjwt.Verify([]byte(token), vc)
	if err != nil {
		return nil, fmt.Errorf("token is invalid: %w", err)
	}

	return data, nil
}

// ExtractBearerToken extracts the bearer token from the Authorization header.
func ExtractBearerToken(authHeader string) (string, error) {
	const prefix = "Bearer "
	if authHeader == "" {
		return "", fmt.Errorf("missing Authorization header")
	}
	if len(authHeader) <= len(prefix) || authHeader[:len(prefix)] != prefix {
		return "", fmt.Errorf("invalid Authorization header format")
	}
	return authHeader[len(prefix):], nil
}
