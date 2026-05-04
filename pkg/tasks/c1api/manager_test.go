package c1api

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/types"
)

// fakeBatonServiceClient is a BatonServiceClient stub that lets tests drive
// Hello responses deterministically.
type fakeBatonServiceClient struct {
	mu sync.Mutex

	helloResponses []error
	helloCalls     int
}

func newFakeBatonServiceClient(helloResponses []error) *fakeBatonServiceClient {
	return &fakeBatonServiceClient{helloResponses: helloResponses}
}

func (f *fakeBatonServiceClient) Hello(ctx context.Context, req *v1.BatonServiceHelloRequest) (*v1.BatonServiceHelloResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.helloCalls++
	if len(f.helloResponses) == 0 {
		return &v1.BatonServiceHelloResponse{}, nil
	}
	err := f.helloResponses[0]
	f.helloResponses = f.helloResponses[1:]
	if err != nil {
		return nil, err
	}
	return &v1.BatonServiceHelloResponse{}, nil
}

func (f *fakeBatonServiceClient) GetTask(context.Context, *v1.BatonServiceGetTaskRequest) (*v1.BatonServiceGetTaskResponse, error) {
	return v1.BatonServiceGetTaskResponse_builder{}.Build(), nil
}

func (f *fakeBatonServiceClient) Heartbeat(context.Context, *v1.BatonServiceHeartbeatRequest) (*v1.BatonServiceHeartbeatResponse, error) {
	return &v1.BatonServiceHeartbeatResponse{}, nil
}

func (f *fakeBatonServiceClient) FinishTask(context.Context, *v1.BatonServiceFinishTaskRequest) (*v1.BatonServiceFinishTaskResponse, error) {
	return &v1.BatonServiceFinishTaskResponse{}, nil
}

func (f *fakeBatonServiceClient) Upload(context.Context, *v1.Task, io.ReadSeeker) error {
	return nil
}

// fakeConnectorClient provides the minimal surface the Hello handshake uses
// (GetMetadata). Other methods are satisfied by embedding the interface.
type fakeConnectorClient struct {
	types.ConnectorClient
	metadataErr atomic.Value // error, lazily set
}

func (f *fakeConnectorClient) GetMetadata(ctx context.Context, req *v2.ConnectorServiceGetMetadataRequest, opts ...grpc.CallOption) (*v2.ConnectorServiceGetMetadataResponse, error) {
	if v := f.metadataErr.Load(); v != nil {
		if err, ok := v.(error); ok && err != nil {
			return nil, err
		}
	}
	return v2.ConnectorServiceGetMetadataResponse_builder{
		Metadata: v2.ConnectorMetadata_builder{
			DisplayName: "test",
			Description: "test connector",
		}.Build(),
	}.Build(), nil
}

// withFastBackoff replaces the Hello backoff bounds with tiny durations so
// tests don't sleep real seconds. It restores the originals on cleanup.
func withFastBackoff(t *testing.T) {
	t.Helper()

	origInitial := initialHelloBackoff
	origMax := maxHelloBackoff
	initialHelloBackoff = time.Millisecond
	maxHelloBackoff = 10 * time.Millisecond
	t.Cleanup(func() {
		initialHelloBackoff = origInitial
		maxHelloBackoff = origMax
	})
}

func newTestManager(sc BatonServiceClient) *c1ApiTaskManager {
	return &c1ApiTaskManager{serviceClient: sc}
}

func TestBootstrapSucceedsOnFirstAttempt(t *testing.T) {
	sc := newFakeBatonServiceClient([]error{nil})
	cc := &fakeConnectorClient{}
	mgr := newTestManager(sc)

	if err := mgr.Bootstrap(context.Background(), cc); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if sc.helloCalls != 1 {
		t.Fatalf("expected 1 Hello call, got %d", sc.helloCalls)
	}
}

func TestBootstrapRetriesOnTransientFailure(t *testing.T) {
	withFastBackoff(t)

	transient := status.Error(codes.Unavailable, "server busy")
	sc := newFakeBatonServiceClient([]error{transient, transient, nil})
	cc := &fakeConnectorClient{}
	mgr := newTestManager(sc)

	if err := mgr.Bootstrap(context.Background(), cc); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if sc.helloCalls != 3 {
		t.Fatalf("expected 3 Hello calls (2 transient failures + success), got %d", sc.helloCalls)
	}
}

func TestBootstrapStopsOnNonRetryableError(t *testing.T) {
	withFastBackoff(t)

	badCreds := status.Error(codes.Unauthenticated, "bad token")
	sc := newFakeBatonServiceClient([]error{badCreds})
	cc := &fakeConnectorClient{}
	mgr := newTestManager(sc)

	err := mgr.Bootstrap(context.Background(), cc)
	if err == nil {
		t.Fatal("expected Bootstrap to return an error for non-retryable Hello failure")
	}
	if got := status.Code(err); got != codes.Unauthenticated {
		t.Fatalf("error code = %s, want Unauthenticated", got)
	}
	if sc.helloCalls != 1 {
		t.Fatalf("expected exactly 1 Hello call (no retries on non-retryable), got %d", sc.helloCalls)
	}
}

func TestBootstrapHonorsContextCancellationDuringBackoff(t *testing.T) {
	origInitial := initialHelloBackoff
	origMax := maxHelloBackoff
	initialHelloBackoff = 200 * time.Millisecond
	maxHelloBackoff = 200 * time.Millisecond
	t.Cleanup(func() {
		initialHelloBackoff = origInitial
		maxHelloBackoff = origMax
	})

	transient := status.Error(codes.Unavailable, "server busy")
	responses := make([]error, 100)
	for i := range responses {
		responses[i] = transient
	}
	sc := newFakeBatonServiceClient(responses)
	cc := &fakeConnectorClient{}
	mgr := newTestManager(sc)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := mgr.Bootstrap(ctx, cc)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestBootstrapPropagatesGetMetadataError(t *testing.T) {
	withFastBackoff(t)

	sc := newFakeBatonServiceClient(nil)
	cc := &fakeConnectorClient{}
	cc.metadataErr.Store(status.Error(codes.PermissionDenied, "no metadata"))
	mgr := newTestManager(sc)

	err := mgr.Bootstrap(context.Background(), cc)
	if err == nil {
		t.Fatal("expected Bootstrap to return an error when GetMetadata fails")
	}
	if got := status.Code(err); got != codes.PermissionDenied {
		t.Fatalf("error code = %s, want PermissionDenied", got)
	}
	if sc.helloCalls != 0 {
		t.Fatalf("Hello should not be invoked when GetMetadata fails, got %d calls", sc.helloCalls)
	}
}

func TestNextDoesNotSelfQueueHello(t *testing.T) {
	// With Bootstrap owning the startup handshake, Next() must no longer
	// self-enqueue a Hello task. First Next() call should just go to GetTask.
	sc := newFakeBatonServiceClient(nil)
	mgr := newTestManager(sc)

	task, _, err := mgr.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if task != nil && task.GetHello() != nil {
		t.Fatalf("Next must not self-queue a Hello task, got %+v", task)
	}
}

func TestIsRetryableHelloErrorClassification(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"ctx-canceled", context.Canceled, false},
		{"ctx-deadline", context.DeadlineExceeded, false},
		{"unauthenticated", status.Error(codes.Unauthenticated, "bad token"), false},
		{"permission-denied", status.Error(codes.PermissionDenied, "forbidden"), false},
		{"invalid-argument", status.Error(codes.InvalidArgument, "malformed"), false},
		{"unimplemented", status.Error(codes.Unimplemented, "no"), false},
		{"failed-precondition", status.Error(codes.FailedPrecondition, "state"), false},
		{"not-found", status.Error(codes.NotFound, "nope"), false},
		{"unavailable", status.Error(codes.Unavailable, "busy"), true},
		{"deadline-exceeded-status", status.Error(codes.DeadlineExceeded, "slow"), true},
		{"resource-exhausted", status.Error(codes.ResourceExhausted, "rl"), true},
		{"internal", status.Error(codes.Internal, "hiccup"), true},
		{"aborted", status.Error(codes.Aborted, "race"), true},
		{"plain-error", errors.New("boom"), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isRetryableHelloError(tc.err); got != tc.want {
				t.Fatalf("isRetryableHelloError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
