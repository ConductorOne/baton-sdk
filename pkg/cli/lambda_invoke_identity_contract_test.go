//go:build baton_lambda_support

package cli

import (
	"context"
	"crypto/ed25519"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/conductorone/baton-sdk/internal/connector"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	pbtransport "github.com/conductorone/baton-sdk/pb/c1/transport/v1"
	"github.com/conductorone/baton-sdk/pkg/auth"
	"github.com/conductorone/baton-sdk/pkg/crypto"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/jwk"
	"github.com/conductorone/baton-sdk/pkg/field"
	lambdaGrpc "github.com/conductorone/baton-sdk/pkg/lambda/grpc"
	"github.com/conductorone/baton-sdk/pkg/lambda/grpc/middleware"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/bridges/otelzap"
	"go.opentelemetry.io/otel/attribute"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
)

type identityLogExporter struct {
	mu      sync.Mutex
	records []sdklog.Record
}

func (e *identityLogExporter) Export(_ context.Context, records []sdklog.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, record := range records {
		e.records = append(e.records, record.Clone())
	}
	return nil
}
func (*identityLogExporter) Shutdown(context.Context) error   { return nil }
func (*identityLogExporter) ForceFlush(context.Context) error { return nil }

func TestInvokeIdentityExportedLogFields(t *testing.T) {
	exporter := &identityLogExporter{}
	provider := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(exporter)))
	defer func() { require.NoError(t, provider.Shutdown(context.Background())) }()
	logger := zap.New(otelzap.NewCore("identity-test", otelzap.WithLoggerProvider(provider)))
	previous := zap.L()
	defer func() { zap.ReplaceGlobals(previous) }()
	processCtx := ctxzap.ToContext(context.Background(), logger)
	key := identityTestKey(t)
	validations := 0
	h, err := newLambdaInvokeIdentityHandler(processCtx, ".example.com", func(ctx context.Context, _ *pbtransport.InvokeIdentity, _ string) (*lambdaIdentityRuntime, error) {
		ctx = context.WithValue(ctx, crypto.ContextClientSecretKey, key)
		return &lambdaIdentityRuntime{ctx: ctx, close: func() {}, build: func(context.Context, string) (*lambdaConnectorGeneration, error) {
			return &lambdaConnectorGeneration{version: "v1", connector: &identityTestConnector{call: func(ctx context.Context) error {
				ctxzap.Extract(ctx).Info("dispatched")
				return nil
			}}}, nil
		}}, nil
	})
	require.NoError(t, err)
	defer h.Close()
	identityTestValidator(t, h, key, &validations)
	token := identityTestToken(t, key, "tenant.example.com", nil)
	require.Equal(t, codes.OK, identityTestCode(t, h, identityTestContext(context.Background()), identityTestRequest(t, identityTestClient, "marker-secret", token, "v1")))
	require.NoError(t, provider.ForceFlush(context.Background()))
	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	require.Len(t, exporter.records, 1)
	fields := map[string]string{}
	exporter.records[0].WalkAttributes(func(kv attribute.KeyValue) bool { fields[string(kv.Key)] = kv.Value.AsString(); return true })
	require.Equal(t, identityTestScope, fields["execution_scope"])
	require.Equal(t, identityTestClient, fields["client_id"])
}

func TestLegacyReloaderIgnoresInvokeIdentity(t *testing.T) {
	key := identityTestKey(t)
	public := key.Public()
	publicJSON, err := public.MarshalJSON()
	require.NoError(t, err)
	interceptor, err := middleware.WithAuth(context.Background(), auth.Config{
		PublicKeyJWK: string(publicJSON), Issuer: "https://tenant.example.com", Subject: identityTestScope, Audience: identityTestScope,
	})
	require.NoError(t, err)
	server := lambdaGrpc.NewServer(lambdaUnaryInterceptorChain(interceptor))
	called := false
	active := &identityTestConnector{call: func(context.Context) error { called = true; return nil }}
	connector.Register(context.Background(), server, active, &connector.RegisterOps{})
	r := &lambdaConnectorReloader{server: server, current: &lambdaConnectorGeneration{connector: active, version: "v1"}}
	token := identityTestToken(t, key, "tenant.example.com", nil)
	response, err := r.Handler(context.Background(), identityTestRequest(t, "not-an-allowed-host", "marker-secret", token, "v1"))
	require.NoError(t, err)
	st, err := response.Status()
	require.NoError(t, err)
	require.Equal(t, codes.OK, st.Code())
	require.True(t, called)
}

func TestInvokeIdentityModeEnvironmentAndFlag(t *testing.T) {
	for _, fromEnv := range []bool{false, true} {
		t.Run(map[bool]string{false: "flag", true: "env"}[fromEnv], func(t *testing.T) {
			v := viper.New()
			v.SetEnvPrefix("BATON")
			v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
			v.AutomaticEnv()
			args := []string{"lambda", "--lambda-per-invoke-identity"}
			if fromEnv {
				t.Setenv("BATON_LAMBDA_PER_INVOKE_IDENTITY", "true")
				args = []string{"lambda"}
			}
			cmd := &cobra.Command{Use: "test"}
			err := OptionallyAddLambdaCommand(context.Background(), "test", v, func(context.Context, *viper.Viper, RunTimeOpts) (types.ConnectorServer, error) {
				t.Fatal("unexpected construction")
				return nil, nil
			}, field.Configuration{}, cmd, false)
			require.NoError(t, err)
			cmd.SetArgs(args)
			require.ErrorContains(t, cmd.Execute(), "lambda-allowed-host-suffix")
		})
	}
}

type identityPartialConnector struct {
	identityTestConnector
	closes atomic.Int32
}

func (c *identityPartialConnector) Close(context.Context) error { c.closes.Add(1); return nil }

func TestInvokeIdentityPartialConstructionClosesConnector(t *testing.T) {
	key := identityTestKey(t)
	encrypted, err := jwk.EncryptED25519(key.Key.(ed25519.PrivateKey).Public().(ed25519.PublicKey), []byte(`{}`))
	require.NoError(t, err)
	response := v1.GetConnectorConfigResponse_builder{Config: encrypted}.Build()
	response.SetExpectedExecutionScope(identityTestScope)
	conn := &identityConfigConn{response: response}
	partial := &identityPartialConnector{}
	build := newLambdaGenerationBuilder(context.Background(), viper.New(), field.Configuration{}, func(context.Context, *viper.Viper, RunTimeOpts) (types.ConnectorServer, error) {
		return partial, errors.New("marker-secret")
	}, false, conn, key, true, identityTestScope)
	_, err = buildIdentityGeneration(context.Background(), context.Background(), time.Second, build, "v1")
	require.Error(t, err)
	require.EqualValues(t, 1, partial.closes.Load())
}

func TestInvokeIdentityNilConstructionFailsBeforeRegistration(t *testing.T) {
	key := identityTestKey(t)
	encrypted, err := jwk.EncryptED25519(key.Key.(ed25519.PrivateKey).Public().(ed25519.PublicKey), []byte(`{}`))
	require.NoError(t, err)
	response := v1.GetConnectorConfigResponse_builder{Config: encrypted}.Build()
	response.SetExpectedExecutionScope(identityTestScope)
	build := newLambdaGenerationBuilder(context.Background(), viper.New(), field.Configuration{}, func(context.Context, *viper.Viper, RunTimeOpts) (types.ConnectorServer, error) {
		return nil, nil
	}, false, &identityConfigConn{response: response}, key, true, identityTestScope)
	generation, err := buildIdentityGeneration(context.Background(), context.Background(), time.Second, build, "v1")
	require.Error(t, err)
	require.Nil(t, generation)
}
