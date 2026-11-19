//go:build baton_lambda_support

package cli

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"testing"
	"time"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	pbtransport "github.com/conductorone/baton-sdk/pb/c1/transport/v1"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/jwk"
	"github.com/conductorone/baton-sdk/pkg/field"
	lambdaGrpc "github.com/conductorone/baton-sdk/pkg/lambda/grpc"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestInvokeIdentityBearerRejectedBeforeValidator(t *testing.T) {
	for _, authorization := range [][]string{nil, {"Basic marker-secret"}, {"Bearer"}, {"Bearer a", "Bearer b"}} {
		key := identityTestKey(t)
		validations := 0
		h, err := newLambdaInvokeIdentityHandler(context.Background(), ".example.com", nil)
		require.NoError(t, err)
		identityTestValidator(t, h, key, &validations)
		headers, err := lambdaGrpc.MarshalMetadata(metadata.MD{"authorization": authorization})
		require.NoError(t, err)
		payload, err := protojson.Marshal(pbtransport.Request_builder{
			Headers: headers, InvokeIdentity: pbtransport.InvokeIdentity_builder{ClientId: identityTestClient, ClientSecret: "marker-secret"}.Build(),
		}.Build())
		require.NoError(t, err)
		var req lambdaGrpc.Request
		require.NoError(t, json.Unmarshal(payload, &req))
		require.Equal(t, codes.Unauthenticated, identityTestCode(t, h, identityTestContext(context.Background()), &req))
		require.Zero(t, validations)
		require.Nil(t, h.runtime)
	}
}

type feedbackExtraService interface{ Extra() }
type feedbackExtraConnector struct{ identityTestConnector }

func (*feedbackExtraConnector) Extra() {}

func TestLambdaReloadClosesUnswappedGenerationInBothModes(t *testing.T) {
	for _, identityMode := range []bool{false, true} {
		for _, replacementError := range []bool{false, true} {
			server := lambdaGrpc.NewServer(nil)
			active := &feedbackExtraConnector{}
			if replacementError {
				server.RegisterService(&grpc.ServiceDesc{ServiceName: "extra", HandlerType: (*feedbackExtraService)(nil)}, active)
			}
			next := &identityPartialConnector{}
			generationCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			current := &lambdaConnectorGeneration{version: "v1", connector: active, logging: lambdaLogLevelConfig{level: "info"}, identityMode: identityMode}
			r := &lambdaConnectorReloader{server: server, current: current, identityMode: identityMode,
				build: func(context.Context, string) (*lambdaConnectorGeneration, error) {
					return &lambdaConnectorGeneration{version: "v2", connector: next, cancel: cancel, identityMode: identityMode}, nil
				},
			}
			err := r.reloadIfNeeded(context.Background(), "v2")
			require.Error(t, err)
			if replacementError {
				require.ErrorContains(t, err, "does not satisfy")
			} else {
				require.ErrorContains(t, err, "no registered services matched")
			}
			require.Same(t, current, r.current)
			require.EqualValues(t, 1, next.closes.Load())
			require.ErrorIs(t, generationCtx.Err(), context.Canceled)
		}
	}
}

type feedbackConfigConn struct {
	identityConfigConn
	err error
}

func (c *feedbackConfigConn) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	if c.err != nil {
		return c.err
	}
	return c.identityConfigConn.Invoke(ctx, method, args, reply, opts...)
}

func TestLambdaGenerationBuilderFailureStages(t *testing.T) {
	key := identityTestKey(t)
	for _, stage := range []string{"fetch", "scope", "decrypt", "unmarshal", "validate", "construct"} {
		t.Run(stage, func(t *testing.T) {
			plaintext := []byte(`{}`)
			if stage == "unmarshal" {
				plaintext = []byte(`{`)
			}
			encrypted, err := jwk.EncryptED25519(key.Key.(ed25519.PrivateKey).Public().(ed25519.PublicKey), plaintext)
			require.NoError(t, err)
			response := v1.GetConnectorConfigResponse_builder{Config: encrypted}.Build()
			response.SetExpectedExecutionScope(identityTestScope)
			conn := &feedbackConfigConn{identityConfigConn: identityConfigConn{response: response}}
			schema := field.Configuration{}
			cause := status.Error(codes.PermissionDenied, "marker-secret")
			switch stage {
			case "fetch":
				conn.err = cause
			case "scope":
				response.SetExpectedExecutionScope("wrong")
			case "decrypt":
				response.SetConfig([]byte("marker-secret"))
			case "validate":
				schema = field.NewConfiguration([]field.SchemaField{field.StringField("required", field.WithRequired(true))})
			}
			constructed := 0
			build := newLambdaGenerationBuilder(context.Background(), viper.New(), schema,
				func(context.Context, *viper.Viper, RunTimeOpts) (types.ConnectorServer, error) {
					constructed++
					return nil, cause
				}, false, conn, key, true, identityTestScope)
			generation, err := build(context.Background(), "v1")
			require.Error(t, err)
			require.Nil(t, generation)
			require.Equal(t, stage, lambdaBuildErrorStage(err))
			if stage == "construct" || stage == "fetch" {
				require.ErrorIs(t, err, cause)
				require.Equal(t, codes.PermissionDenied, status.Code(err))
			}
			if stage == "scope" {
				require.ErrorIs(t, err, errLambdaExecutionScope)
			}
			if stage != "construct" {
				require.Zero(t, constructed)
			} else {
				require.Equal(t, 1, constructed)
			}
		})
	}
}

func TestLambdaGenerationIdentityModeIsExplicit(t *testing.T) {
	key := identityTestKey(t)
	encrypted, err := jwk.EncryptED25519(key.Key.(ed25519.PrivateKey).Public().(ed25519.PublicKey), []byte(`{}`))
	require.NoError(t, err)
	for _, identityMode := range []bool{false, true} {
		runCtx, endRun := context.WithCancel(context.Background())
		defer endRun()
		buildCtx, endBuild := context.WithCancel(runCtx)
		defer endBuild()
		response := v1.GetConnectorConfigResponse_builder{Config: encrypted}.Build()
		response.SetExpectedExecutionScope(identityTestScope)
		var constructionCtx context.Context
		build := newLambdaGenerationBuilder(runCtx, viper.New(), field.Configuration{},
			func(ctx context.Context, _ *viper.Viper, _ RunTimeOpts) (types.ConnectorServer, error) {
				constructionCtx = ctx
				return &identityTestConnector{}, nil
			}, false, &identityConfigConn{response: response}, key, identityMode, identityTestScope)
		generation, err := build(buildCtx, "v1")
		require.NoError(t, err)
		require.Equal(t, identityMode, generation.identityMode)
		if identityMode {
			require.Same(t, buildCtx, constructionCtx)
		} else {
			require.Same(t, runCtx, constructionCtx)
		}
	}
}

func TestIdentityBuildHonorsInvokeCancellationOnlyDuringBuild(t *testing.T) {
	for _, duringBuild := range []bool{false, true} {
		invokeCtx, endInvoke := context.WithCancel(context.Background())
		defer endInvoke()
		var constructionCtx context.Context
		generation, err := buildIdentityGeneration(context.Background(), invokeCtx, time.Minute,
			func(ctx context.Context, _ string) (*lambdaConnectorGeneration, error) {
				constructionCtx = ctx
				if duringBuild {
					endInvoke()
					select {
					case <-ctx.Done():
					case <-time.After(time.Second):
						require.FailNow(t, "invoke cancellation did not cancel construction")
					}
					return nil, ctx.Err()
				}
				return &lambdaConnectorGeneration{connector: &identityTestConnector{}}, nil
			}, "v1")
		if duringBuild {
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, generation)
		} else {
			require.NoError(t, err)
			endInvoke()
			require.NoError(t, constructionCtx.Err())
			closeIdentityGeneration(context.Background(), generation)
			require.ErrorIs(t, constructionCtx.Err(), context.Canceled)
		}
	}
}

type feedbackCloseErrorConnector struct{ identityTestConnector }

func (*feedbackCloseErrorConnector) Close(context.Context) error { return errors.New("marker-secret") }

func TestGenerationCloseRedactionUsesIdentityModeNotCancel(t *testing.T) {
	core, records := observer.New(zap.DebugLevel)
	previous := zap.L()
	defer zap.ReplaceGlobals(previous)
	zap.ReplaceGlobals(zap.New(core))
	ctx := ctxzap.ToContext(context.Background(), zap.L())
	drained := make(chan struct{})
	close(drained)
	closeConnectorGenerationAfterDrain(ctx, &lambdaConnectorGeneration{connector: &feedbackCloseErrorConnector{}, identityMode: true}, drained, time.Second, time.Second)
	require.Len(t, records.All(), 1)
	require.Empty(t, records.All()[0].ContextMap())
	closeConnectorGenerationAfterDrain(ctx, &lambdaConnectorGeneration{connector: &feedbackCloseErrorConnector{}, cancel: func() {}, version: "v1"}, drained, time.Second, time.Second)
	require.Len(t, records.All(), 2)
	require.Equal(t, "v1", records.All()[1].ContextMap()["config_version"])
	require.Equal(t, "marker-secret", records.All()[1].ContextMap()["error"])
}

func TestIdentityRecoveriesTreatNilPanicAsFailure(t *testing.T) {
	core, records := observer.New(zap.DebugLevel)
	ctx := ctxzap.ToContext(context.Background(), zap.New(core))
	key := identityTestKey(t)
	validations := 0
	h, err := newLambdaInvokeIdentityHandler(ctx, ".example.com", func(context.Context, *pbtransport.InvokeIdentity, string) (*lambdaIdentityRuntime, error) {
		panic(nil)
	})
	require.NoError(t, err)
	identityTestValidator(t, h, key, &validations)
	req := identityTestRequest(t, identityTestClient, "marker-secret", identityTestToken(t, key, "tenant.example.com", nil), "v2")
	require.Equal(t, codes.Unavailable, identityTestCode(t, h, identityTestContext(context.Background()), req))
	r := &lambdaConnectorReloader{
		identityMode: true,
		current:      &lambdaConnectorGeneration{version: "v1", logging: lambdaLogLevelConfig{level: "info"}},
		build:        func(context.Context, string) (*lambdaConnectorGeneration, error) { panic(nil) },
	}
	response, err := r.Handler(ctx, req)
	require.NoError(t, err)
	st, err := response.Status()
	require.NoError(t, err)
	require.Equal(t, codes.Unavailable, st.Code())
	responseAny, err := lambdaIdentityRecoveryInterceptor()(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/test.Service/Call"},
		func(context.Context, any) (any, error) { panic(nil) })
	require.Nil(t, responseAny)
	require.Equal(t, codes.Internal, status.Code(err))
	require.Len(t, records.All(), 3)
	for _, record := range records.All() {
		require.NotEmpty(t, record.ContextMap()["stack"])
		require.NotContains(t, record.ContextMap(), "panic")
	}
}
