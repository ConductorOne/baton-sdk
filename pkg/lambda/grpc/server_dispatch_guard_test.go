package grpc

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type guardedGrantServer struct {
	v2.UnimplementedGrantManagerServiceServer
	calls       int
	err         error
	beforeGrant func()
}

func (s *guardedGrantServer) Grant(context.Context, *v2.GrantManagerServiceGrantRequest) (*v2.GrantManagerServiceGrantResponse, error) {
	s.beforeGrant()
	s.calls++
	return &v2.GrantManagerServiceGrantResponse{}, s.err
}

func TestHandlerWithDispatchGuard(t *testing.T) {
	t.Parallel()
	for _, wireV2 := range []bool{false, true} {
		for _, tc := range []struct {
			name       string
			method     string
			guardError error
			appError   error
			headers    metadata.MD
			want       codes.Code
			calls      int
		}{
			{"matching", v2.GrantManagerService_Grant_FullMethodName, nil, nil, nil, codes.OK, 1},
			{"guard refusal", v2.GrantManagerService_Grant_FullMethodName, status.Error(codes.FailedPrecondition, "generation changed"), nil, nil, codes.FailedPrecondition, 0},
			{"malformed method", "malformed", nil, nil, nil, codes.Unimplemented, 0},
			{"unknown service", "/unknown/Grant", nil, nil, nil, codes.Unimplemented, 0},
			{"unknown method", "/c1.connector.v2.GrantManagerService/Unknown", nil, nil, nil, codes.Unimplemented, 0},
			{"application error", v2.GrantManagerService_Grant_FullMethodName, nil, status.Error(codes.Unavailable, "vendor unavailable"), nil, codes.Unavailable, 1},
			{"timeout decode error", v2.GrantManagerService_Grant_FullMethodName, nil, nil, metadata.Pairs("grpc-timeout", "bogus"), codes.Internal, 0},
		} {
			name := "legacy wire/" + tc.name
			if wireV2 {
				name = "v2 wire/" + tc.name
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				guardCalls, releases := 0, 0
				active := &guardedGrantServer{
					err:         tc.appError,
					beforeGrant: func() { require.Equal(t, 1, releases, "release the external lock before calling the connector") },
				}
				server := NewServer(nil)
				v2.RegisterGrantManagerServiceServer(server, active)
				req, err := NewRequest(tc.method, &v2.GrantManagerServiceGrantRequest{}, tc.headers)
				require.NoError(t, err)
				req.wireV2 = wireV2
				resp, err := server.HandlerWithDispatchGuard(context.Background(), req, func() (func(), error) {
					guardCalls++
					return func() { releases++ }, tc.guardError
				})
				require.NoError(t, err)
				require.NotNil(t, resp)
				require.Equal(t, wireV2, resp.wireV2)
				st, err := resp.Status()
				require.NoError(t, err)
				require.Equal(t, tc.want, st.Code())
				require.Equal(t, tc.calls, active.calls)
				require.Equal(t, 1, guardCalls)
				require.Equal(t, 1, releases, "release exactly once, including on errors")
				require.Empty(t, server.activeServiceInstances, "every in-flight reference must be released")
			})
		}
	}
}

func TestDispatchGuardHeldUntilImplementationAcquired(t *testing.T) {
	t.Parallel()
	active := &guardedGrantServer{beforeGrant: func() {}}
	server := NewServer(nil)
	v2.RegisterGrantManagerServiceServer(server, active)
	req, err := NewRequest(v2.GrantManagerService_Grant_FullMethodName, &v2.GrantManagerServiceGrantRequest{}, nil)
	require.NoError(t, err)
	server.mu.Lock()
	var unlockOnce sync.Once
	unlockServer := func() { unlockOnce.Do(server.mu.Unlock) }
	defer unlockServer()
	var replacementMu sync.Mutex
	checked := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		resp, err := server.HandlerWithDispatchGuard(context.Background(), req, func() (func(), error) {
			replacementMu.Lock()
			close(checked)
			return replacementMu.Unlock, nil
		})
		if err == nil {
			var st *status.Status
			st, err = resp.Status()
			if err == nil {
				err = st.Err()
			}
		}
		done <- err
	}()
	select {
	case <-checked:
	case <-time.After(5 * time.Second):
		t.Fatal("dispatch guard did not run")
	}
	blocked := false
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var stack [1 << 20]byte
		n := runtime.Stack(stack[:], true)
		for _, goroutine := range strings.Split(string(stack[:n]), "\n\n") {
			if strings.Contains(goroutine, "(*Server).acquireServiceForRequest") && strings.Contains(goroutine, "[sync.Mutex.Lock") {
				blocked = true
				break
			}
		}
		if blocked {
			break
		}
		runtime.Gosched()
	}
	releasedEarly := replacementMu.TryLock()
	if releasedEarly {
		replacementMu.Unlock()
	}
	unlockServer()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("guarded invocation did not finish")
	}
	require.True(t, blocked, "observe dispatch waiting to acquire the registry mutex")
	require.False(t, releasedEarly, "external guard must remain held until implementation acquisition")
	require.True(t, replacementMu.TryLock(), "external guard must be released after acquisition")
	replacementMu.Unlock()
	require.Equal(t, 1, active.calls)
	require.Empty(t, server.activeServiceInstances)
}
