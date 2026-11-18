package auth

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRemoteValidatorLoadsAuthenticatesAndCloses(t *testing.T) {
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
