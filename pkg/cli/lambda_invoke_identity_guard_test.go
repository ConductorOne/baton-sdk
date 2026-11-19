//go:build baton_lambda_support

package cli

import (
	"context"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	pbtransport "github.com/conductorone/baton-sdk/pb/c1/transport/v1"
	"github.com/conductorone/baton-sdk/pkg/crypto"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
)

func TestInvokeIdentityJWKSClientTrustsConfiguredCA(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"keys":[]}`)) }))
	defer server.Close()
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o600))

	t.Setenv("BATON_LAMBDA_CA_CERT_PATH", "")
	client, err := lambdaIdentityJWKSClient()
	require.NoError(t, err)
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
	require.NoError(t, err)
	untrustedResponse, err := client.Do(request)
	if err == nil {
		require.NoError(t, untrustedResponse.Body.Close())
	}
	require.Error(t, err, "the fixture's certificate is not in the system roots")

	t.Setenv("BATON_LAMBDA_CA_CERT_PATH", caPath)
	client, err = lambdaIdentityJWKSClient()
	require.NoError(t, err)
	response, err := client.Do(request)
	require.NoError(t, err, "the configured CA bundle must be trusted for the key fetch")
	require.NoError(t, response.Body.Close())
	require.Equal(t, http.StatusOK, response.StatusCode)

	t.Setenv("BATON_LAMBDA_CA_CERT_PATH", filepath.Join(dir, "missing.pem"))
	_, err = newLambdaInvokeIdentityHandler(context.Background(), ".example.com", nil)
	require.ErrorContains(t, err, "identity key client", "an unreadable CA bundle fails at startup, not on every invoke")
}

func TestInvokeIdentityFailedFirstBuildRestoresLogLevel(t *testing.T) {
	for _, failure := range []string{"error", "panic"} {
		t.Run(failure, func(t *testing.T) {
			processCtx, err := logging.Init(context.Background(), logging.WithLogLevel("info"), logging.WithOutputPaths([]string{os.DevNull}))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, logging.SetLogLevel("info")) })
			debugEnabled := func() bool { return ctxzap.Extract(processCtx).Core().Enabled(zap.DebugLevel) }
			key := identityTestKey(t)
			validations, builds := 0, 0
			h, err := newLambdaInvokeIdentityHandler(processCtx, ".example.com", func(ctx context.Context, _ *pbtransport.InvokeIdentity, _ string) (*lambdaIdentityRuntime, error) {
				ctx = context.WithValue(ctx, crypto.ContextClientSecretKey, key)
				return &lambdaIdentityRuntime{ctx: ctx, close: func() {}, build: func(context.Context, string) (*lambdaConnectorGeneration, error) {
					builds++
					// The real builder applies the fetched level before it constructs the connector.
					logging := lambdaLogLevelConfig{level: "debug"}
					require.NoError(t, applyLambdaLogLevel(logging, time.Now()))
					if builds > 1 {
						return &lambdaConnectorGeneration{version: "v1", connector: &identityTestConnector{}, logging: logging}, nil
					}
					if failure == "panic" {
						panic("constructor panic")
					}
					return nil, errors.New("constructor failure")
				}}, nil
			})
			require.NoError(t, err)
			defer h.Close()
			h.setStartupLogLevel(lambdaLogLevelConfig{level: "info"})
			identityTestValidator(t, h, key, &validations)
			req := identityTestRequest(t, identityTestClient, "marker-secret", identityTestToken(t, key, "tenant.example.com", nil), "v1")

			require.False(t, debugEnabled())
			require.Equal(t, codes.Unavailable, identityTestCode(t, h, identityTestContext(context.Background()), req))
			require.False(t, debugEnabled(), "a failed first build must not leave the failed configuration's level active")

			require.Equal(t, codes.OK, identityTestCode(t, h, identityTestContext(context.Background()), req))
			require.True(t, debugEnabled(), "a registered generation keeps its own level")
		})
	}
}

func TestInvokeIdentityMissingTokenBuildsNoValidator(t *testing.T) {
	key := identityTestKey(t)
	validations, factories := 0, 0
	h, err := newLambdaInvokeIdentityHandler(context.Background(), ".example.com", func(context.Context, *pbtransport.InvokeIdentity, string) (*lambdaIdentityRuntime, error) {
		factories++
		return nil, errors.New("unexpected construction")
	})
	require.NoError(t, err)
	identityTestValidator(t, h, key, &validations)

	missing := identityTestRequest(t, identityTestClient, "marker-secret", "", "v1")
	require.Equal(t, codes.Unauthenticated, identityTestCode(t, h, identityTestContext(context.Background()), missing))
	require.Zero(t, validations, "a request without a bearer token must not cause a key fetch")

	malformed := identityTestRequest(t, identityTestClient, "marker-secret", "not-a-jwt", "v1")
	require.Equal(t, codes.Unauthenticated, identityTestCode(t, h, identityTestContext(context.Background()), malformed))
	require.Equal(t, 1, validations, "a present token is judged by the validator")
	require.Zero(t, factories)
}
