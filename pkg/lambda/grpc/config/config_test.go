package config

import (
	"context"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
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
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatalf("write CA cert: %v", err)
	}
	t.Setenv(lambdaCACertPathEnv, certPath)

	tlsConfig, err := lambdaTLSConfig()
	if err != nil {
		t.Fatalf("lambdaTLSConfig: %v", err)
	}
	client := lambdaHTTPClient(tlsConfig)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET with configured CA: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
}

func TestLambdaTLSConfigRejectsInvalidCAPath(t *testing.T) {
	t.Setenv(lambdaCACertPathEnv, filepath.Join(t.TempDir(), "missing.pem"))

	_, err := lambdaTLSConfig()
	if err == nil {
		t.Fatal("expected error for missing CA path")
	}
}
