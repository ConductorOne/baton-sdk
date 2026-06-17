package config

import (
	"context"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLambdaTLSConfigTrustsConfiguredCA(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: server.Certificate().Raw,
	})
	certPath := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(certPath, certPEM, 0o600), "write CA cert")
	t.Setenv(lambdaCACertPathEnv, certPath)

	tlsConfig, err := lambdaTLSConfig()
	require.NoError(t, err, "lambdaTLSConfig")
	client := lambdaHTTPClient(tlsConfig)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
	require.NoError(t, err, "NewRequestWithContext")
	resp, err := client.Do(req)
	require.NoError(t, err, "GET with configured CA")
	_ = resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode, "status")
}

func TestLambdaTLSConfigRejectsInvalidCAPath(t *testing.T) {
	t.Setenv(lambdaCACertPathEnv, filepath.Join(t.TempDir(), "missing.pem"))

	_, err := lambdaTLSConfig()
	require.Error(t, err, "expected error for missing CA path")
}
