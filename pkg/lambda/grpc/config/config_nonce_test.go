package config

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/require"
)

// issuedNonce is the value the fake authorization server hands back on a
// use_dpop_nonce challenge. The retry only succeeds if the client echoes it
// inside the DPoP proof, which only happens when a NonceStore is wired.
const issuedNonce = "test-dpop-nonce-7f3a"

// TestNewDPoPClientSatisfiesNonceChallenge proves the lambda config-fetch token
// source now satisfies a server use_dpop_nonce challenge: the server rejects the
// first request with 400 use_dpop_nonce + a DPoP-Nonce header and only returns a
// token once the retry carries that nonce in its DPoP proof. Without
// WithNonceStore wired the retry omits the nonce and this round-trip fails.
func TestNewDPoPClientSatisfiesNonceChallenge(t *testing.T) {
	var requests int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		w.Header().Set("Content-Type", "application/json")

		if !proofCarriesNonce(t, r.Header.Get("DPoP"), issuedNonce) {
			w.Header().Set("DPoP-Nonce", issuedNonce)
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"use_dpop_nonce","error_description":"nonce required"}`))
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"access_token":"test-access-token","token_type":"DPoP","expires_in":3600}`))
	}))
	defer server.Close()

	// Trust the test server's CA so the lambda HTTP client can reach it.
	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: server.Certificate().Raw,
	})
	certPath := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(certPath, certPEM, 0o600), "write CA cert")
	t.Setenv(lambdaCACertPathEnv, certPath)

	serverURL, err := url.Parse(server.URL)
	require.NoError(t, err, "parse server URL")
	t.Setenv(lambdaTokenHostEnv, serverURL.Host)
	t.Setenv(lambdaConfigurationHostEnv, serverURL.Host)

	// Static credentials let the SigV4 identity-attestation marshaler sign
	// locally without reaching AWS.
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIDTEST")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secrettest")
	t.Setenv("AWS_REGION", "us-east-1")

	clientID := "test@" + serverURL.Host + "/lambda"
	clientSecret := newTestClientSecret(t)

	_, _, tokenSource, err := NewDPoPClient(context.Background(), clientID, clientSecret)
	require.NoError(t, err, "NewDPoPClient")
	require.NotNil(t, tokenSource, "token source")

	token, err := tokenSource.Token()
	require.NoError(t, err, "token fetch should succeed after satisfying the nonce challenge")
	require.Equal(t, "test-access-token", token.AccessToken, "access token")
	require.Equal(t, int32(2), atomic.LoadInt32(&requests), "expected an initial challenge plus one nonce-bearing retry")
}

// proofCarriesNonce reports whether the DPoP proof JWT carries the given nonce
// claim. The proof is a signed JWS; we only need to read the (unverified)
// payload to assert the client embedded the nonce on retry.
func proofCarriesNonce(t *testing.T, proof, nonce string) bool {
	t.Helper()
	if proof == "" {
		return false
	}
	parts := strings.Split(proof, ".")
	if len(parts) < 2 {
		return false
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return false
	}
	var claims struct {
		Nonce string `json:"nonce"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return false
	}
	return claims.Nonce == nonce
}

// newTestClientSecret builds a v1 baton client secret carrying an ed25519
// private key, matching the format crypto.ParseClientSecret expects.
func newTestClientSecret(t *testing.T) string {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err, "generate ed25519 key")

	key := jose.JSONWebKey{
		Key:       priv,
		KeyID:     "test",
		Algorithm: string(jose.EdDSA),
		Use:       "sig",
	}
	raw, err := key.MarshalJSON()
	require.NoError(t, err, "marshal JWK")

	return "clientname:clientsecret:v1:" + base64.RawURLEncoding.EncodeToString(raw)
}
