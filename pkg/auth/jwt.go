package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
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
	JWKSClient           *http.Client
	// RefreshOnUnknownKey retries a missing token key against a fresh JWKS,
	// subject to a short minimum interval between forced refresh attempts.
	RefreshOnUnknownKey bool
}

type KeySet interface {
	Get() (*jose.JSONWebKeySet, error)
}

var (
	ErrKeySetUnavailable = errors.New("verification key set unavailable")
	ErrTokenKeyNotFound  = errors.New("token verification key not found")
)

// Keep issuer rotation delays short without fetching on every unknown token key.
const forcedKeyRefreshMinInterval = 30 * time.Second

type keySetLoadError struct{ err error }

func (e *keySetLoadError) Error() string      { return e.err.Error() }
func (e *keySetLoadError) Unwrap() error      { return e.err }
func (*keySetLoadError) Is(target error) bool { return target == ErrKeySetUnavailable }

// Validator handles JWT validation with a specific configuration.
type Validator struct {
	mu                sync.RWMutex
	config            Config
	keySet            KeySet
	refreshContext    context.Context
	cancelKeys        context.CancelFunc
	lastForcedRefresh time.Time

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
		remoteKeySet, cancel, err := loadRemoteKeySet(ctx, ctx, config)
		if err != nil {
			return nil, err
		}
		return newValidator(config, remoteKeySet, ctx, cancel), nil
	}

	return newValidator(config, keySet, ctx, nil), nil
}

func newValidator(config Config, keySet KeySet, refreshContext context.Context, cancel context.CancelFunc) *Validator {
	if config.MaxExpirationFromNow == 0 {
		config.MaxExpirationFromNow = 24 * time.Hour
	}
	return &Validator{config: config, keySet: keySet, refreshContext: refreshContext, cancelKeys: cancel, now: time.Now}
}

func loadRemoteKeySet(ctx, refreshContext context.Context, config Config) (KeySet, context.CancelFunc, error) {
	refreshContext, cancel := context.WithCancel(refreshContext)
	keySet, err := xkeyset.New(ctx, xkeyset.Options{URL: config.JWKSUrl, RefreshContext: refreshContext, Client: config.JWKSClient})
	if err != nil {
		cancel()
		return nil, nil, &keySetLoadError{err: fmt.Errorf("failed to create keyset from JWKS URL: %w", err)}
	}
	return keySet, cancel, nil
}

func (v *Validator) Close() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.closeKeys()
}

func (v *Validator) closeKeys() {
	if v.cancelKeys != nil {
		v.cancelKeys()
	}
	if closer, ok := v.keySet.(interface{ Close() }); ok {
		closer.Close()
	}
}

// ValidateToken validates the JWT token string and returns the claims if valid.
func (v *Validator) ValidateToken(ctx context.Context, token string) (map[string]interface{}, error) {
	if v.config.RefreshOnUnknownKey {
		v.mu.Lock()
		defer v.mu.Unlock()
	} else {
		v.mu.RLock()
		defer v.mu.RUnlock()
	}
	if token == "" {
		return nil, fmt.Errorf("token is empty")
	}

	keyset, err := v.keySet.Get()
	if err != nil {
		return nil, &keySetLoadError{err: fmt.Errorf("failed to get keyset: %w", err)}
	}
	if v.config.RefreshOnUnknownKey && tokenKeyMissing(token, keyset) {
		if v.config.JWKSUrl != "" {
			now := v.now()
			if !v.lastForcedRefresh.IsZero() && now.Sub(v.lastForcedRefresh) < forcedKeyRefreshMinInterval {
				return nil, ErrTokenKeyNotFound
			}
			v.lastForcedRefresh = now
			replacement, cancel, err := loadRemoteKeySet(ctx, v.refreshContext, v.config)
			if err != nil {
				return nil, err
			}
			v.closeKeys()
			v.keySet, v.cancelKeys = replacement, cancel
			keyset, err = v.keySet.Get()
			if err != nil {
				return nil, &keySetLoadError{err: fmt.Errorf("failed to get keyset: %w", err)}
			}
		}
		if tokenKeyMissing(token, keyset) {
			return nil, ErrTokenKeyNotFound
		}
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

func tokenKeyMissing(token string, keyset *jose.JSONWebKeySet) bool {
	object, err := jose.ParseSignedCompact(token, xjwt.DefaultSignatureAlgorithms)
	if err != nil || len(object.Signatures) != 1 {
		return false
	}
	kid := object.Signatures[0].Header.KeyID
	if kid == "" {
		return len(keyset.Keys) == 0
	}
	return len(keyset.Key(kid)) == 0
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
