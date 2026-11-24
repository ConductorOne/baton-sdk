//go:build baton_lambda_support

package cli

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	c1_lambda_grpc "github.com/conductorone/baton-sdk/pkg/lambda/grpc"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

type strictVersionConnector struct {
	types.ConnectorServer
	version    string
	dispatched *[]string
	closed     chan struct{}
}

func (c *strictVersionConnector) Grant(context.Context, *v2.GrantManagerServiceGrantRequest) (*v2.GrantManagerServiceGrantResponse, error) {
	*c.dispatched = append(*c.dispatched, c.version)
	return &v2.GrantManagerServiceGrantResponse{}, nil
}

func (c *strictVersionConnector) Close() error {
	if c.closed != nil {
		close(c.closed)
	}
	return nil
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

type strictVersionInvokeResult struct {
	response *c1_lambda_grpc.Response
	err      error
}

func startStrictVersionInvoke(t *testing.T, r *lambdaConnectorReloader, headers metadata.MD) <-chan strictVersionInvokeResult {
	t.Helper()
	req, err := c1_lambda_grpc.NewRequest(v2.GrantManagerService_Grant_FullMethodName, &v2.GrantManagerServiceGrantRequest{}, headers)
	require.NoError(t, err)
	results := make(chan strictVersionInvokeResult, 1)
	go func() {
		resp, err := r.Handler(context.Background(), req)
		results <- strictVersionInvokeResult{response: resp, err: err}
	}()
	return results
}

func waitStrictVersionInvoke(t *testing.T, results <-chan strictVersionInvokeResult) codes.Code {
	t.Helper()
	select {
	case result := <-results:
		require.NoError(t, result.err)
		require.NotNil(t, result.response)
		st, err := result.response.Status()
		require.NoError(t, err)
		return st.Code()
	case <-time.After(5 * time.Second):
		t.Fatal("invocation did not finish")
		return codes.Unknown
	}
}

func waitStrictVersionBarrier(t *testing.T, barrier <-chan struct{}) {
	t.Helper()
	select {
	case <-barrier:
	case <-time.After(5 * time.Second):
		t.Fatal("invocation did not reach the barrier")
	}
}

func TestLambdaStrictConfigVersionConcurrentReplacementBeforeAcquire(t *testing.T) {
	for _, reuse := range []bool{false, true} {
		name := "new implementation"
		if reuse {
			name = "reused implementation"
		}
		t.Run(name, func(t *testing.T) {
			f := newStrictVersionFixture(t, "A")
			f.servedVersion = "B"
			if reuse {
				active := f.reloader.current.connector.(*strictVersionConnector)
				f.reloader.build = func(_ context.Context, requested string) (*lambdaConnectorGeneration, error) {
					f.builds = append(f.builds, requested)
					active.version = "B"
					return &lambdaConnectorGeneration{version: "B", connector: active, logging: lambdaLogLevelConfig{level: "info"}}, nil
				}
			}
			checked, resume := make(chan struct{}), make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(resume) }) }
			defer release()
			f.reloader.testHooks = &lambdaConnectorReloaderTestHooks{
				beforeDispatch: func(req *c1_lambda_grpc.Request) {
					if len(req.Headers().Get("x-baton-connector-config-version-strict")) > 0 {
						close(checked)
						<-resume
					}
				},
			}
			strictA := startStrictVersionInvoke(t, f.reloader, metadata.Pairs("x-baton-connector-config-version", "A", "x-baton-connector-config-version-strict", "true"))
			waitStrictVersionBarrier(t, checked)
			require.Empty(t, f.builds)
			require.Empty(t, f.dispatched)
			require.Equal(t, codes.OK, f.invoke(t, metadata.Pairs("x-baton-connector-config-version", "B")))
			require.Equal(t, []string{"B"}, f.builds)
			require.Equal(t, "B", f.reloader.currentVersion())
			require.Equal(t, []string{"B"}, f.dispatched)
			release()
			strictCode := waitStrictVersionInvoke(t, strictA)
			require.Equal(t, []string{"B"}, f.dispatched, "strict A must never dispatch on replacement B")
			require.Equal(t, codes.FailedPrecondition, strictCode)
		})
	}
}

func TestLambdaStrictConfigVersionConcurrentReplacementAfterAcquire(t *testing.T) {
	f := newStrictVersionFixture(t, "A")
	f.servedVersion = "B"
	active := f.reloader.current.connector.(*strictVersionConnector)
	active.closed = make(chan struct{})
	acquired, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(resume) }) }
	defer release()
	server := c1_lambda_grpc.NewServer(lambdaUnaryInterceptorChain(
		func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
			if info.Server == active {
				close(acquired)
				<-resume
			}
			return handler(ctx, req)
		},
	))
	v2.RegisterGrantManagerServiceServer(server, active)
	f.reloader.server = server
	strictA := startStrictVersionInvoke(t, f.reloader, metadata.Pairs("x-baton-connector-config-version", "A", "x-baton-connector-config-version-strict", "true"))
	waitStrictVersionBarrier(t, acquired)
	require.Equal(t, codes.OK, f.invoke(t, metadata.Pairs("x-baton-connector-config-version", "B")))
	require.Equal(t, []string{"B"}, f.builds)
	require.Equal(t, []string{"B"}, f.dispatched)
	replaced, drained, err := server.ReplaceServiceImplementation(active, f.reloader.current.connector)
	require.NoError(t, err)
	require.Zero(t, replaced, "B has already replaced A")
	select {
	case <-drained:
		t.Fatal("acquired generation A must retain an in-flight reference")
	default:
	}
	select {
	case <-active.closed:
		t.Fatal("acquired generation A closed before its invocation finished")
	default:
	}
	release()
	require.Equal(t, codes.OK, waitStrictVersionInvoke(t, strictA))
	require.Equal(t, []string{"B", "A"}, f.dispatched)
	waitStrictVersionBarrier(t, drained)
	waitStrictVersionBarrier(t, active.closed)
}

func TestLambdaStrictConfigVersionWithoutGeneration(t *testing.T) {
	f := newStrictVersionFixture(t, "A")
	f.reloader.current = nil
	require.Equal(t, codes.FailedPrecondition, f.invoke(t, metadata.Pairs("x-baton-connector-config-version-strict", "true")))
	require.Empty(t, f.builds)
	require.Empty(t, f.dispatched)
}
