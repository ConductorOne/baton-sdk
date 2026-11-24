//go:build baton_lambda_support

package cli

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	c1_lambda_grpc "github.com/conductorone/baton-sdk/pkg/lambda/grpc"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

type strictVersionConnector struct {
	types.ConnectorServer
	version    string
	dispatched *[]string
}

func (c *strictVersionConnector) Grant(context.Context, *v2.GrantManagerServiceGrantRequest) (*v2.GrantManagerServiceGrantResponse, error) {
	*c.dispatched = append(*c.dispatched, c.version)
	return &v2.GrantManagerServiceGrantResponse{}, nil
}

type strictVersionFixture struct {
	reloader      *lambdaConnectorReloader
	servedVersion string
	buildErr      error
	builds        []string
	dispatched    []string
}

func newStrictVersionFixture(t *testing.T, version string) *strictVersionFixture {
	t.Helper()
	f := &strictVersionFixture{servedVersion: version}
	active := &strictVersionConnector{version: version, dispatched: &f.dispatched}
	server := c1_lambda_grpc.NewServer(lambdaUnaryInterceptorChain())
	v2.RegisterGrantManagerServiceServer(server, active)
	f.reloader = &lambdaConnectorReloader{
		server: server,
		current: &lambdaConnectorGeneration{
			version: version, connector: active, logging: lambdaLogLevelConfig{level: "info"},
		},
		build: func(_ context.Context, requested string) (*lambdaConnectorGeneration, error) {
			f.builds = append(f.builds, requested)
			if f.buildErr != nil {
				return nil, f.buildErr
			}
			return &lambdaConnectorGeneration{
				version:   f.servedVersion,
				connector: &strictVersionConnector{version: f.servedVersion, dispatched: &f.dispatched},
				logging:   lambdaLogLevelConfig{level: "info"},
			}, nil
		},
	}
	_, err := logging.Init(context.Background(), logging.WithLogLevel("info"), logging.WithOutputPaths([]string{os.DevNull}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, logging.SetLogLevel("info")) })
	return f
}

func (f *strictVersionFixture) invoke(t *testing.T, headers metadata.MD) codes.Code {
	t.Helper()
	req, err := c1_lambda_grpc.NewRequest(v2.GrantManagerService_Grant_FullMethodName, &v2.GrantManagerServiceGrantRequest{}, headers)
	require.NoError(t, err)
	payload, err := json.Marshal(req)
	require.NoError(t, err)
	var decoded c1_lambda_grpc.Request
	require.NoError(t, json.Unmarshal(payload, &decoded))
	resp, err := f.reloader.Handler(context.Background(), &decoded)
	require.NoError(t, err)
	require.NotNil(t, resp)
	payload, err = json.Marshal(resp)
	require.NoError(t, err)
	var received c1_lambda_grpc.Response
	require.NoError(t, json.Unmarshal(payload, &received))
	st, err := received.Status()
	require.NoError(t, err)
	if st.Code() == codes.OK {
		require.NoError(t, received.UnmarshalResponse(&v2.GrantManagerServiceGrantResponse{}))
	}
	return st.Code()
}

func TestLambdaStrictConfigVersionCapsAndConvergence(t *testing.T) {
	for _, capName := range []string{"global floor", "repeated version"} {
		t.Run(capName, func(t *testing.T) {
			f := newStrictVersionFixture(t, "initial")
			f.servedVersion = "A"
			require.Equal(t, codes.OK, f.invoke(t, metadata.Pairs("x-baton-connector-config-version", "A")))
			require.Equal(t, []string{"A"}, f.dispatched)
			require.Equal(t, []string{"A"}, f.builds)

			if capName == "repeated version" {
				f.reloader.lastRebuildAt = time.Now().Add(-2 * time.Second)
				require.Equal(t, codes.OK, f.invoke(t, metadata.Pairs("x-baton-connector-config-version", "B")))
				require.Equal(t, []string{"A", "B"}, f.builds)
				require.Equal(t, []string{"A", "A"}, f.dispatched)
				f.reloader.lastRebuildAt = time.Now().Add(-2 * time.Second)
				require.Greater(t, time.Since(f.reloader.lastRebuildAt), lambdaConnectorReloadGlobalFloor)
				require.Less(t, time.Since(f.reloader.lastRebuildAt), lambdaConnectorReloadMinInterval)
				require.Equal(t, "B", f.reloader.lastRebuildRequestedVersion)
			} else {
				require.Equal(t, "A", f.reloader.lastRebuildRequestedVersion)
				require.Less(t, time.Since(f.reloader.lastRebuildAt), lambdaConnectorReloadGlobalFloor)
			}
			require.Equal(t, "A", f.reloader.currentVersion())
			buildCount, dispatchCount := len(f.builds), len(f.dispatched)
			strictB := metadata.Pairs("x-baton-connector-config-version", "B", "x-baton-connector-config-version-strict", "true")
			require.Equal(t, codes.FailedPrecondition, f.invoke(t, strictB))
			require.Len(t, f.dispatched, dispatchCount, "a strict refusal must not dispatch Grant")
			require.Len(t, f.builds, buildCount, "the capped request must not rebuild")
			require.Equal(t, "A", f.reloader.currentVersion())

			require.Equal(t, codes.OK, f.invoke(t, metadata.Pairs("x-baton-connector-config-version", "B")))
			require.Len(t, f.builds, buildCount)
			require.Len(t, f.dispatched, dispatchCount+1)
			require.Equal(t, "A", f.dispatched[dispatchCount], "non-strict capped requests still dispatch A")

			f.reloader.lastRebuildAt = time.Now().Add(-6 * time.Second)
			f.servedVersion = "B"
			require.Equal(t, codes.OK, f.invoke(t, strictB))
			require.Len(t, f.builds, buildCount+1)
			require.Equal(t, "B", f.builds[buildCount])
			require.Equal(t, "B", f.reloader.currentVersion())
			require.Len(t, f.dispatched, dispatchCount+2)
			require.Equal(t, "B", f.dispatched[dispatchCount+1], "the converged call must dispatch B")
		})
	}
}

func TestLambdaStrictConfigVersionRejectsServedSkew(t *testing.T) {
	f := newStrictVersionFixture(t, "A")
	headers := metadata.Pairs("x-baton-connector-config-version", "B", "x-baton-connector-config-version-strict", "true")
	require.Equal(t, codes.FailedPrecondition, f.invoke(t, headers))
	require.Equal(t, []string{"B"}, f.builds, "reload must run before the strict check")
	require.Equal(t, "A", f.reloader.currentVersion())
	require.Empty(t, f.dispatched)
}

func TestLambdaStrictConfigVersionHeaderPresence(t *testing.T) {
	for _, tc := range []struct {
		name    string
		current string
		headers metadata.MD
		want    codes.Code
	}{
		{"matching", "A", metadata.Pairs("x-baton-connector-config-version", "A", "x-baton-connector-config-version-strict", "true"), codes.OK},
		{"empty strict value is absent", "A", metadata.Pairs("x-baton-connector-config-version", "B", "x-baton-connector-config-version-strict", ""), codes.OK},
		{"false strict value", "A", metadata.Pairs("x-baton-connector-config-version", "B", "x-baton-connector-config-version-strict", "false"), codes.FailedPrecondition},
		{"missing stamp", "A", metadata.Pairs("x-baton-connector-config-version-strict", "true"), codes.FailedPrecondition},
		{"empty stamp", "A", metadata.Pairs("x-baton-connector-config-version", "", "x-baton-connector-config-version-strict", "true"), codes.FailedPrecondition},
		{"matching empty version", "", metadata.Pairs("x-baton-connector-config-version-strict", "true"), codes.OK},
		{"no headers", "A", nil, codes.OK},
		{"non-strict empty stamp", "A", metadata.Pairs("x-baton-connector-config-version", ""), codes.OK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newStrictVersionFixture(t, tc.current)
			f.reloader.lastRebuildAt = time.Now()
			require.Equal(t, tc.want, f.invoke(t, tc.headers))
			require.Empty(t, f.builds)
			if tc.want == codes.OK {
				require.Equal(t, []string{tc.current}, f.dispatched)
			} else {
				require.Empty(t, f.dispatched)
			}
		})
	}
}

func TestLambdaStrictConfigVersionReloadError(t *testing.T) {
	for _, identityMode := range []bool{false, true} {
		for _, strict := range []bool{false, true} {
			name := "legacy"
			if identityMode {
				name = "identity"
			}
			if strict {
				name += "/strict"
			} else {
				name += "/non-strict"
			}
			t.Run(name, func(t *testing.T) {
				f := newStrictVersionFixture(t, "A")
				f.reloader.identityMode = identityMode
				f.buildErr = errors.New("config fetch failed")
				headers := metadata.Pairs("x-baton-connector-config-version", "B")
				if strict {
					headers.Set("x-baton-connector-config-version-strict", "true")
				}
				require.Equal(t, codes.Unavailable, f.invoke(t, headers))
				require.Equal(t, []string{"B"}, f.builds)
				require.Equal(t, "A", f.reloader.currentVersion())
				require.Empty(t, f.dispatched)
			})
		}
	}
}
