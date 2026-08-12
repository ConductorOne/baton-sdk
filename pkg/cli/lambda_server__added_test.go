//go:build baton_lambda_support

package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	c1_lambda_grpc "github.com/conductorone/baton-sdk/pkg/lambda/grpc"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/conductorone/baton-sdk/pkg/ugrpc"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestEgressPolicyFromResponse(t *testing.T) {
	t.Parallel()

	newResp := func(cv string, env *v1.ServedPolicyEnvelope) *v1.GetConnectorConfigResponse {
		r := &v1.GetConnectorConfigResponse{}
		if cv != "" {
			r.SetConfigVersion(cv)
		}
		if env != nil {
			r.SetServedPolicyEnvelope(env)
		}
		return r
	}
	goodEnvelope := func(cv string) *v1.ServedPolicyEnvelope {
		return v1.ServedPolicyEnvelope_builder{
			EnvelopeVersion: servedPolicyEnvelopeVersion,
			ConfigVersion:   cv,
			Egress: v1.EgressSection_builder{
				SchemaVersion: egressSectionSchemaVersion,
				HttpsOnly:     true,
				AllowedHosts:  []string{"api.example.com"},
			}.Build(),
		}.Build()
	}

	t.Run("no envelope is ungoverned", func(t *testing.T) {
		t.Parallel()
		require.Nil(t, egressPolicyFromResponse(newResp("cv-1", nil)))
		require.Nil(t, egressPolicyFromResponse(nil))
	})

	t.Run("valid envelope surfaces hosts and https_only", func(t *testing.T) {
		t.Parallel()
		p := egressPolicyFromResponse(newResp("cv-1", goodEnvelope("cv-1")))
		require.NotNil(t, p)
		require.True(t, p.HTTPSOnly)
		require.Equal(t, []string{"api.example.com"}, p.AllowedHosts)
	})

	t.Run("binding mismatch is governed deny-all", func(t *testing.T) {
		t.Parallel()
		// Envelope config_version differs from the response's.
		p := egressPolicyFromResponse(newResp("cv-1", goodEnvelope("cv-2")))
		require.NotNil(t, p)
		require.Empty(t, p.AllowedHosts)
	})

	t.Run("empty response config_version is governed deny-all", func(t *testing.T) {
		t.Parallel()
		p := egressPolicyFromResponse(newResp("", goodEnvelope("")))
		require.NotNil(t, p)
		require.Empty(t, p.AllowedHosts)
	})

	t.Run("unsupported envelope version is governed deny-all", func(t *testing.T) {
		t.Parallel()
		env := goodEnvelope("cv-1")
		env.SetEnvelopeVersion(999)
		p := egressPolicyFromResponse(newResp("cv-1", env))
		require.NotNil(t, p)
		require.Empty(t, p.AllowedHosts)
	})

	t.Run("unsupported egress schema version is governed deny-all", func(t *testing.T) {
		t.Parallel()
		env := goodEnvelope("cv-1")
		env.GetEgress().SetSchemaVersion(999)
		p := egressPolicyFromResponse(newResp("cv-1", env))
		require.NotNil(t, p)
		require.Empty(t, p.AllowedHosts)
	})
}

func TestLambdaConnectorConfigVersion(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 7, 27, 10, 30, 0, 0, time.UTC)

	// config_version stamped: preferred even when last_updated is also present.
	preferConfigVersion := &v1.GetConnectorConfigResponse{}
	preferConfigVersion.SetConfigVersion("cv-1")
	preferConfigVersion.SetLastUpdated(timestamppb.New(ts))

	// Older server: no config_version, falls back to last_updated.
	lastUpdatedOnly := &v1.GetConnectorConfigResponse{}
	lastUpdatedOnly.SetLastUpdated(timestamppb.New(ts))

	for _, tc := range []struct {
		name   string
		config *v1.GetConnectorConfigResponse
		want   string
	}{
		{"nil config", nil, ""},
		{"config_version preferred over last_updated", preferConfigVersion, "cv-1"},
		{"falls back to last_updated when config_version absent", lastUpdatedOnly, ts.Format(time.RFC3339Nano)},
		{"empty when neither is set", &v1.GetConnectorConfigResponse{}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, lambdaConnectorConfigVersion(tc.config))
		})
	}
}

func TestLambdaLogLevelConfigExpiresDebug(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 29, 12, 0, 0, 0, time.UTC)
	v := viper.New()
	v.Set("log-level", "debug")
	v.Set("log-level-debug-expires-at", now.Add(-time.Minute).Format(time.RFC3339))

	config, err := lambdaLogLevelConfigFromViper(v)
	require.NoError(t, err, "lambdaLogLevelConfigFromViper")
	require.Equal(t, "info", config.effective(now), "effective log level")
}

func TestLambdaLogLevelConfigKeepsUnexpiredDebug(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 29, 12, 0, 0, 0, time.UTC)
	v := viper.New()
	v.Set("log-level", "debug")
	v.Set("log-level-debug-expires-at", now.Add(time.Minute).Format(time.RFC3339))

	config, err := lambdaLogLevelConfigFromViper(v)
	require.NoError(t, err, "lambdaLogLevelConfigFromViper")
	require.Equal(t, "debug", config.effective(now), "effective log level")
}

func TestLambdaLogLevelConfigRejectsInvalidLevel(t *testing.T) {
	t.Parallel()

	v := viper.New()
	v.Set("log-level", "verbose")

	_, err := lambdaLogLevelConfigFromViper(v)
	require.Error(t, err, "expected invalid log level to fail")
}

// Connector construction on a config-version reload runs before the interceptor
// chain, so the reloader is the one place a panic can still reach the lambda
// runtime and be reported as a statusless crashed invocation.
func TestLambdaConnectorReloaderRecoversBuildPanic(t *testing.T) {
	// reloadIfNeeded's deferred guard restores the active generation's log level
	// through the process-wide handle, so this test cannot run in parallel.
	r := &lambdaConnectorReloader{
		current: &lambdaConnectorGeneration{version: "cv-1"},
		build: func(ctx context.Context, version string) (*lambdaConnectorGeneration, error) {
			panic("connector construction exploded")
		},
	}

	req, err := c1_lambda_grpc.NewRequest(
		"/test.Service/Method",
		&v1.GetConnectorConfigRequest{},
		metadata.Pairs(lambdaConnectorConfigVersionHeader, "cv-2"),
	)
	require.NoError(t, err, "NewRequest")

	logged := &bytes.Buffer{}
	ctx := ctxzap.ToContext(context.Background(), newCaptureLogger(logged))

	resp, err := r.Handler(ctx, req)
	require.NoError(t, err, "a recovered panic must not propagate as an invocation error")
	require.NotNil(t, resp)

	st, err := resp.Status()
	require.NoError(t, err, "Status")
	require.Equal(t, codes.Internal, st.Code())

	// This panic happened before the interceptor chain could run, and the log
	// message is shared with in-chain panics, so the site field is what tells an
	// operator which one they are looking at.
	require.Contains(t, logged.String(), "connector construction exploded")
	require.Contains(t, logged.String(), `"recovery_site":"`+ugrpc.RecoverySiteLambdaHandler+`"`)
}

// A build that returns an error rather than panicking must keep its own
// classification: the recovery wrapper is not allowed to turn ordinary failures
// into Internal.
func TestLambdaConnectorReloaderPassesThroughBuildError(t *testing.T) {
	// reloadIfNeeded's deferred guard restores the active generation's log level
	// through the process-wide handle, so this test cannot run in parallel.
	r := &lambdaConnectorReloader{
		current: &lambdaConnectorGeneration{version: "cv-1"},
		build: func(ctx context.Context, version string) (*lambdaConnectorGeneration, error) {
			return nil, errors.New("config fetch failed")
		},
	}

	req, err := c1_lambda_grpc.NewRequest(
		"/test.Service/Method",
		&v1.GetConnectorConfigRequest{},
		metadata.Pairs(lambdaConnectorConfigVersionHeader, "cv-2"),
	)
	require.NoError(t, err, "NewRequest")

	resp, err := r.Handler(context.Background(), req)
	require.NoError(t, err)

	st, err := resp.Status()
	require.NoError(t, err, "Status")
	require.Equal(t, codes.Unavailable, st.Code(), "a build error must not be reclassified as Internal")
}

// reloadIfNeeded's log level obligation at every exit. The refactor collapsed
// three explicit reverts into one r.current != active condition, so each exit is
// a cell here rather than a sampled case: build applies the incoming generation's
// level before it can fail, and only an exit that actually activated that
// generation may keep it.
func TestLambdaConnectorReloaderLogLevelAtEveryExit(t *testing.T) {
	// Every case sets the process-wide log level, so none of this runs in parallel.
	activeConnector := &stubConnectorServer{}
	nextGeneration := func(version string) (*lambdaConnectorGeneration, error) {
		return &lambdaConnectorGeneration{
			version:   version,
			connector: &stubConnectorServer{},
			logging:   lambdaLogLevelConfig{level: "error"},
		}, nil
	}

	for _, tc := range []struct {
		name              string
		registerActive    bool
		build             func(version string) (*lambdaConnectorGeneration, error)
		wantActiveVersion string
		wantLevelRestored bool
	}{
		{
			name:              "build returns an error",
			registerActive:    true,
			build:             func(string) (*lambdaConnectorGeneration, error) { return nil, errors.New("config fetch failed") },
			wantActiveVersion: "cv-1",
			wantLevelRestored: true,
		},
		{
			name:              "build panics",
			registerActive:    true,
			build:             func(string) (*lambdaConnectorGeneration, error) { panic("connector construction exploded") },
			wantActiveVersion: "cv-1",
			wantLevelRestored: true,
		},
		{
			// ReplaceServiceImplementation rejects a nil implementation.
			name:           "built generation carries no connector",
			registerActive: true,
			build: func(version string) (*lambdaConnectorGeneration, error) {
				return &lambdaConnectorGeneration{version: version, logging: lambdaLogLevelConfig{level: "error"}}, nil
			},
			wantActiveVersion: "cv-1",
			wantLevelRestored: true,
		},
		{
			// Nothing registered, so no service matches the active generation and
			// ReplaceServiceImplementation reports zero replacements.
			name:              "no registered service matches",
			registerActive:    false,
			build:             nextGeneration,
			wantActiveVersion: "cv-1",
			wantLevelRestored: true,
		},
		{
			name:              "successful swap keeps the new level",
			registerActive:    true,
			build:             nextGeneration,
			wantActiveVersion: "cv-2",
			wantLevelRestored: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Discard the output: these cases assert on the logger's level, never
			// on what it wrote, and zap keeps a sink open for the life of the
			// process, so a file under t.TempDir() would block the directory's
			// cleanup on Windows, where an open handle cannot be deleted.
			ctx, err := logging.Init(context.Background(),
				logging.WithLogLevel("debug"),
				logging.WithOutputPaths([]string{os.DevNull}),
			)
			require.NoError(t, err, "logging.Init")
			logger := ctxzap.Extract(ctx)
			require.True(t, logger.Core().Enabled(zapcore.DebugLevel), "precondition: debug is enabled")

			server := c1_lambda_grpc.NewServer(lambdaUnaryInterceptorChain())
			if tc.registerActive {
				server.RegisterService(&grpc.ServiceDesc{
					ServiceName: "test.ConnectorService",
					HandlerType: (*types.ConnectorServer)(nil),
				}, activeConnector)
			}

			r := &lambdaConnectorReloader{
				server:  server,
				current: &lambdaConnectorGeneration{version: "cv-1", connector: activeConnector, logging: lambdaLogLevelConfig{level: "debug"}},
				build: func(ctx context.Context, version string) (*lambdaConnectorGeneration, error) {
					// Mirror a real build, which applies the incoming level first.
					require.NoError(t, applyLambdaLogLevel(lambdaLogLevelConfig{level: "error"}, time.Now()))
					return tc.build(version)
				},
			}

			// A panic is one exit among these; turning it into a status is the
			// handler's job and is asserted separately.
			func() {
				defer func() { _ = recover() }()
				_ = r.reloadIfNeeded(ctx, "cv-2")
			}()

			require.Equal(t, tc.wantActiveVersion, r.current.version, "active generation")
			require.Equal(t, tc.wantLevelRestored, logger.Core().Enabled(zapcore.DebugLevel), "log level restored")
		})
	}
}

// The restore lives in reloadIfNeeded's defer and the recovery in Handler; the
// every-exit table reaches the panic exit with a local recover, so this is the
// one test that pins the seam between them: a panic recovered by Handler must
// return Internal and leave the log level restored, in the same invocation.
func TestLambdaConnectorReloaderRecoveredPanicRestoresLogLevel(t *testing.T) {
	// Sets the process-wide log level, so this test cannot run in parallel.
	ctx, err := logging.Init(context.Background(),
		logging.WithLogLevel("debug"),
		logging.WithOutputPaths([]string{os.DevNull}),
	)
	require.NoError(t, err, "logging.Init")
	logger := ctxzap.Extract(ctx)
	require.True(t, logger.Core().Enabled(zapcore.DebugLevel), "precondition: debug is enabled")

	r := &lambdaConnectorReloader{
		current: &lambdaConnectorGeneration{version: "cv-1", logging: lambdaLogLevelConfig{level: "debug"}},
		build: func(ctx context.Context, version string) (*lambdaConnectorGeneration, error) {
			// Mirror a real build, which applies the incoming level first.
			require.NoError(t, applyLambdaLogLevel(lambdaLogLevelConfig{level: "error"}, time.Now()))
			panic("connector construction exploded")
		},
	}

	req, err := c1_lambda_grpc.NewRequest(
		"/test.Service/Method",
		&v1.GetConnectorConfigRequest{},
		metadata.Pairs(lambdaConnectorConfigVersionHeader, "cv-2"),
	)
	require.NoError(t, err, "NewRequest")

	resp, err := r.Handler(ctx, req)
	require.NoError(t, err, "a recovered panic must not propagate as an invocation error")
	require.NotNil(t, resp)

	st, err := resp.Status()
	require.NoError(t, err, "Status")
	require.Equal(t, codes.Internal, st.Code())
	require.True(t, logger.Core().Enabled(zapcore.DebugLevel), "log level restored after a recovered reload panic")
}

// stubConnectorServer embeds the interface so it satisfies types.ConnectorServer
// without implementing it; these tests only ever use it as an identity to swap.
type stubConnectorServer struct {
	types.ConnectorServer
}

func newCaptureLogger(buf *bytes.Buffer) *zap.Logger {
	encCfg := zap.NewProductionEncoderConfig()
	encCfg.TimeKey = "" // keep test output deterministic
	return zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(encCfg),
		zapcore.AddSync(buf),
		zap.DebugLevel,
	))
}

// The lambda transport reports an escaping panic as a crashed invocation with no
// gRPC status, so recovery has to be in this chain and has to sit outside the
// other interceptors. Both properties are asserted here because the defect being
// guarded against is recovery silently missing from the lambda path while the
// normal gRPC path has it.
func TestLambdaUnaryInterceptorChainRecoversPanics(t *testing.T) {
	t.Parallel()

	okHandler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "response", nil
	}
	info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}

	t.Run("panic in the handler", func(t *testing.T) {
		t.Parallel()

		chain := lambdaUnaryInterceptorChain()
		_, err := chain(context.Background(), nil, info, func(ctx context.Context, req interface{}) (interface{}, error) {
			panic("connector exploded")
		})

		require.Equal(t, codes.Internal, status.Code(err))
	})

	t.Run("panic in a wrapped interceptor", func(t *testing.T) {
		t.Parallel()

		panicking := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
			panic("auth exploded")
		}

		chain := lambdaUnaryInterceptorChain(panicking)
		_, err := chain(context.Background(), nil, info, okHandler)

		require.Equal(t, codes.Internal, status.Code(err))
	})

	t.Run("wrapped interceptors still run", func(t *testing.T) {
		t.Parallel()

		called := false
		marker := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
			called = true
			return handler(ctx, req)
		}

		chain := lambdaUnaryInterceptorChain(marker)
		resp, err := chain(context.Background(), nil, info, okHandler)

		require.NoError(t, err)
		require.Equal(t, "response", resp)
		require.True(t, called, "wrapped interceptor did not run")
	})
}
