package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/require"
)

func TestRemoteValidatorLoadsAuthenticatesAndReusesKeys(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"keys":[%s]}`, testPublicKeyJWK)
	}))
	defer server.Close()
	validator, err := NewValidator(context.Background(), Config{
		JWKSUrl: server.URL, Issuer: "test-issuer", Subject: "test-subject", Audience: "test-audience",
		JWKSClient: server.Client(),
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, requests.Load())
	defer validator.Close()
	token := createTestToken(t, map[string]interface{}{
		"iss": "test-issuer", "sub": "test-subject", "aud": "test-audience",
		"exp": time.Now().Add(time.Minute).Unix(), "nbf": time.Now().Add(-time.Minute).Unix(), "iat": time.Now().Unix(),
	})
	_, err = validator.ValidateToken(context.Background(), token)
	require.NoError(t, err)
	require.EqualValues(t, 1, requests.Load(), "validation reuses the fetched keyset")
}

type countedRemoteKeySet struct {
	KeySet
	closes *atomic.Int32
}

func (s *countedRemoteKeySet) Close() {
	s.closes.Add(1)
	if closer, ok := s.KeySet.(interface{ Close() }); ok {
		closer.Close()
	}
}

func TestRemoteValidatorRefreshIsOptInAndClosesReplacedKeys(t *testing.T) {
	for _, refresh := range []bool{false, true} {
		var requests, closes atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if requests.Add(1) == 1 {
				_, _ = w.Write([]byte(`{"keys":[]}`))
			} else {
				_, _ = fmt.Fprintf(w, `{"keys":[%s]}`, testPublicKeyJWK)
			}
		}))
		processCtx := &countedRefreshContext{Context: context.Background(), done: make(chan struct{})}
		validator, err := NewValidator(processCtx, Config{JWKSUrl: server.URL, JWKSClient: server.Client(), RefreshOnUnknownKey: refresh})
		require.NoError(t, err)
		require.EqualValues(t, 1, processCtx.active.Load(), "initial keys own one process-derived refresh context")
		validator.keySet = &countedRemoteKeySet{KeySet: validator.keySet, closes: &closes}
		token := createTestToken(t, map[string]interface{}{
			"exp": time.Now().Add(time.Minute).Unix(), "iat": time.Now().Unix(),
		})
		invoke, endInvoke := context.WithCancel(context.Background())
		_, err = validator.ValidateToken(invoke, token)
		endInvoke()
		require.EqualValues(t, 1, processCtx.active.Load(), "refresh ownership stays on the process after the invoke ends")
		if refresh {
			require.NoError(t, err)
			require.EqualValues(t, 2, requests.Load())
			require.EqualValues(t, 1, closes.Load())
			validator.keySet = &countedRemoteKeySet{KeySet: validator.keySet, closes: &closes}
			_, err = validator.ValidateToken(context.Background(), token)
			require.NoError(t, err, "refreshed keys outlive their fetching invoke")
			require.EqualValues(t, 2, requests.Load())
		} else {
			require.Error(t, err)
			require.EqualValues(t, 1, requests.Load())
			require.Zero(t, closes.Load())
		}
		validator.Close()
		require.Zero(t, processCtx.active.Load(), "Close releases the active refresh context")
		if refresh {
			require.EqualValues(t, 2, closes.Load())
		} else {
			require.EqualValues(t, 1, closes.Load())
		}
		server.Close()
	}
}

func TestKeySetLoadErrorsPreserveCauseAndClassification(t *testing.T) {
	cause := fmt.Errorf("fixture load error")
	validator := newValidator(Config{}, failingKeySet{err: cause}, context.Background(), nil)
	_, err := validator.ValidateToken(context.Background(), "signed-token")
	require.ErrorIs(t, err, ErrKeySetUnavailable)
	require.ErrorIs(t, err, cause)
	require.EqualError(t, err, "failed to get keyset: fixture load error")
}

type failingKeySet struct{ err error }

func (s failingKeySet) Get() (*jose.JSONWebKeySet, error) { return nil, s.err }

type countedRefreshContext struct {
	context.Context
	done   chan struct{}
	active atomic.Int32
}

func (c *countedRefreshContext) Done() <-chan struct{} { return c.done }
func (c *countedRefreshContext) AfterFunc(func()) func() bool {
	c.active.Add(1)
	return func() bool { c.active.Add(-1); return true }
}

func TestFailedRemoteKeyLoadReleasesRefreshContext(t *testing.T) {
	ctx := &countedRefreshContext{Context: context.Background(), done: make(chan struct{})}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	validator, err := NewValidator(ctx, Config{JWKSUrl: server.URL, JWKSClient: server.Client()})
	require.ErrorIs(t, err, ErrKeySetUnavailable)
	require.Nil(t, validator)
	require.Zero(t, ctx.active.Load(), "failed initial loads release their refresh-context registration")
}

func TestRemoteValidatorThrottlesForcedRefresh(t *testing.T) {
	require.GreaterOrEqual(t, forcedKeyRefreshMinInterval, 10*time.Second)
	require.LessOrEqual(t, forcedKeyRefreshMinInterval, time.Minute)
	for _, failFirstRefresh := range []bool{false, true} {
		t.Run(fmt.Sprintf("failed-load=%t", failFirstRefresh), func(t *testing.T) {
			var privateKey jose.JSONWebKey
			require.NoError(t, json.Unmarshal([]byte(testPrivateKeyJWK), &privateKey))
			publicKey := privateKey.Public()
			publicKey.KeyID = "known"
			knownJSON, err := json.Marshal(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{publicKey}})
			require.NoError(t, err)
			publicKey.KeyID = "unknown-2"
			rotatedJSON, err := json.Marshal(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{publicKey}})
			require.NoError(t, err)
			var requests atomic.Int32
			var failRefresh, rotated atomic.Bool
			failRefresh.Store(failFirstRefresh)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				n := requests.Add(1)
				if n > 1 && failRefresh.Load() {
					w.WriteHeader(http.StatusServiceUnavailable)
				} else if rotated.Load() {
					_, _ = w.Write(rotatedJSON)
				} else {
					_, _ = w.Write(knownJSON)
				}
			}))
			defer server.Close()
			validator, err := NewValidator(context.Background(), Config{
				JWKSUrl: server.URL, JWKSClient: server.Client(), RefreshOnUnknownKey: true, Issuer: "test-issuer",
			})
			require.NoError(t, err)
			defer validator.Close()
			require.EqualValues(t, 1, requests.Load(), "initial load is not the forced refresh")
			now := time.Now()
			validator.now = func() time.Time { return now }
			sign := func(kid string) string {
				signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: privateKey.Key},
					(&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", kid))
				require.NoError(t, err)
				payload, err := json.Marshal(map[string]any{"iss": "test-issuer", "iat": now.Unix(), "exp": now.Add(5 * time.Minute).Unix()})
				require.NoError(t, err)
				signed, err := signer.Sign(payload)
				require.NoError(t, err)
				token, err := signed.CompactSerialize()
				require.NoError(t, err)
				return token
			}
			first, second, known := sign("unknown-1"), sign("unknown-2"), sign("known")
			_, err = validator.ValidateToken(context.Background(), first)
			if failFirstRefresh {
				require.ErrorIs(t, err, ErrKeySetUnavailable)
			} else {
				require.ErrorIs(t, err, ErrTokenKeyNotFound)
			}
			require.EqualValues(t, 2, requests.Load())
			_, err = validator.ValidateToken(context.Background(), second)
			require.ErrorIs(t, err, ErrTokenKeyNotFound)
			require.EqualValues(t, 2, requests.Load(), "two consecutive unknown kids cause only one forced fetch")
			_, err = validator.ValidateToken(context.Background(), known)
			require.NoError(t, err, "cached matching keys still validate inside the interval")
			now = now.Add(forcedKeyRefreshMinInterval - time.Nanosecond)
			_, err = validator.ValidateToken(context.Background(), second)
			require.ErrorIs(t, err, ErrTokenKeyNotFound)
			require.EqualValues(t, 2, requests.Load())
			now = now.Add(time.Nanosecond)
			failRefresh.Store(false)
			rotated.Store(true)
			_, err = validator.ValidateToken(context.Background(), second)
			require.NoError(t, err, "issuer rotation is discovered at the short interval boundary")
			require.EqualValues(t, 3, requests.Load())
		})
	}
}
