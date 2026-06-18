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
	"google.golang.org/protobuf/types/known/durationpb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/stretchr/testify/require"
)

// fakeBatonServiceClient is a BatonServiceClient stub that lets tests drive
// Hello responses deterministically.
type fakeBatonServiceClient struct {
	mu sync.Mutex

	helloResponses []error
	helloCalls     int
	getTasksReqs   []*v1.BatonServiceGetTasksRequest
	getTasksResp   *v1.BatonServiceGetTasksResponse
	getTasksErr    error
	getTaskCalls   int
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
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getTaskCalls++
	return v1.BatonServiceGetTaskResponse_builder{}.Build(), nil
}

func (f *fakeBatonServiceClient) GetTasks(_ context.Context, req *v1.BatonServiceGetTasksRequest) (*v1.BatonServiceGetTasksResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getTasksReqs = append(f.getTasksReqs, req)
	if f.getTasksErr != nil {
		return nil, f.getTasksErr
	}
	if f.getTasksResp != nil {
		return f.getTasksResp, nil
	}
	return v1.BatonServiceGetTasksResponse_builder{}.Build(), nil
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
	return &c1ApiTaskManager{
		serviceClient:   sc,
		taskQueue:       newTaskQueue(3),
		getTasksEnabled: getTasksEnabledFromEnv(),
	}
}

func enableGetTasks(t *testing.T) {
	t.Helper()
	t.Setenv(getTasksEnv, "true")
}

func TestBootstrapSucceedsOnFirstAttempt(t *testing.T) {
	sc := newFakeBatonServiceClient([]error{nil})
	cc := &fakeConnectorClient{}
	mgr := newTestManager(sc)

	require.NoError(t, mgr.Bootstrap(context.Background(), cc))
	require.Equal(t, 1, sc.helloCalls)
}

func TestBootstrapRetriesOnTransientFailure(t *testing.T) {
	withFastBackoff(t)

	transient := status.Error(codes.Unavailable, "server busy")
	sc := newFakeBatonServiceClient([]error{transient, transient, nil})
	cc := &fakeConnectorClient{}
	mgr := newTestManager(sc)

	require.NoError(t, mgr.Bootstrap(context.Background(), cc))
	require.Equal(t, 3, sc.helloCalls, "expected 3 Hello calls (2 transient failures + success)")
}

func TestBootstrapStopsOnNonRetryableError(t *testing.T) {
	withFastBackoff(t)

	badCreds := status.Error(codes.Unauthenticated, "bad token")
	sc := newFakeBatonServiceClient([]error{badCreds})
	cc := &fakeConnectorClient{}
	mgr := newTestManager(sc)

	err := mgr.Bootstrap(context.Background(), cc)
	require.Error(t, err, "expected Bootstrap to return an error for non-retryable Hello failure")
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.Equal(t, 1, sc.helloCalls, "expected exactly 1 Hello call (no retries on non-retryable)")
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
	require.ErrorIs(t, err, context.Canceled)
}

func TestBootstrapPropagatesGetMetadataError(t *testing.T) {
	withFastBackoff(t)

	sc := newFakeBatonServiceClient(nil)
	cc := &fakeConnectorClient{}
	cc.metadataErr.Store(status.Error(codes.PermissionDenied, "no metadata"))
	mgr := newTestManager(sc)

	err := mgr.Bootstrap(context.Background(), cc)
	require.Error(t, err, "expected Bootstrap to return an error when GetMetadata fails")
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.Equal(t, 0, sc.helloCalls, "Hello should not be invoked when GetMetadata fails")
}

func TestNextDoesNotSelfQueueHello(t *testing.T) {
	// With Bootstrap owning the startup handshake, Next() must no longer
	// self-enqueue a Hello task. First Next() call should just poll C1.
	sc := newFakeBatonServiceClient(nil)
	mgr := newTestManager(sc)

	task, _, err := mgr.Next(context.Background())
	require.NoError(t, err)
	require.True(t, task == nil || task.GetHello() == nil, "Next must not self-queue a Hello task, got %+v", task)
	require.Equal(t, 1, sc.getTaskCalls, "expected default path to use GetTask once")
	require.Empty(t, sc.getTasksReqs, "expected default path not to use GetTasks")
}

func TestNextUsesGetTasksAndQueuesBatch(t *testing.T) {
	enableGetTasks(t)
	task1 := v1.Task_builder{Id: "aaaaaaaaaaaaaaaaaaaaaaaaaaa", Hello: &v1.Task_HelloTask{}}.Build()
	task2 := v1.Task_builder{Id: "bbbbbbbbbbbbbbbbbbbbbbbbbbb", Hello: &v1.Task_HelloTask{}}.Build()
	sc := newFakeBatonServiceClient(nil)
	sc.getTasksResp = v1.BatonServiceGetTasksResponse_builder{
		Tasks:    []*v1.Task{task1, task2},
		NextPoll: durationpb.New(time.Hour),
	}.Build()
	mgr := newTestManager(sc)

	got1, _, err := mgr.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, task1.GetId(), got1.GetId())

	got2, _, err := mgr.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, task2.GetId(), got2.GetId())
	require.Len(t, sc.getTasksReqs, 1, "expected one GetTasks call")
}

func TestNextSendsKnownTaskIDs(t *testing.T) {
	enableGetTasks(t)
	task1 := v1.Task_builder{Id: "aaaaaaaaaaaaaaaaaaaaaaaaaaa", Hello: &v1.Task_HelloTask{}}.Build()
	task2 := v1.Task_builder{Id: "bbbbbbbbbbbbbbbbbbbbbbbbbbb", Hello: &v1.Task_HelloTask{}}.Build()
	sc := newFakeBatonServiceClient(nil)
	sc.getTasksResp = v1.BatonServiceGetTasksResponse_builder{
		Tasks: []*v1.Task{task1, task2},
	}.Build()
	mgr := newTestManager(sc)

	got1, _, err := mgr.Next(context.Background())
	require.NoError(t, err)
	sc.getTasksResp = v1.BatonServiceGetTasksResponse_builder{}.Build()

	_, _, err = mgr.Next(context.Background())
	require.NoError(t, err)

	require.Len(t, sc.getTasksReqs, 2, "expected two GetTasks calls")
	known := sc.getTasksReqs[1].GetKnownTaskIds()
	require.True(t, containsString(known, got1.GetId()), "known task IDs %v did not include in-flight task %q", known, got1.GetId())
}

func TestNextRequestsOnlyEnoughTasksToReachKnownTarget(t *testing.T) {
	enableGetTasks(t)
	task1 := v1.Task_builder{Id: "aaaaaaaaaaaaaaaaaaaaaaaaaaa", Hello: &v1.Task_HelloTask{}}.Build()
	task2 := v1.Task_builder{Id: "bbbbbbbbbbbbbbbbbbbbbbbbbbb", Hello: &v1.Task_HelloTask{}}.Build()
	sc := newFakeBatonServiceClient(nil)
	sc.getTasksResp = v1.BatonServiceGetTasksResponse_builder{
		Tasks: []*v1.Task{task1, task2},
	}.Build()
	mgr := newTestManager(sc)

	_, _, err := mgr.Next(context.Background())
	require.NoError(t, err, "Next first fetch")
	sc.getTasksResp = v1.BatonServiceGetTasksResponse_builder{}.Build()
	_, _, err = mgr.Next(context.Background())
	require.NoError(t, err, "Next queued")

	require.Len(t, sc.getTasksReqs, 2, "expected two GetTasks calls")
	require.Equal(t, uint32(6), sc.getTasksReqs[0].GetPageSize(), "first page size")
	require.Equal(t, uint32(4), sc.getTasksReqs[1].GetPageSize(), "second page size")
}

func TestNextDoesNotTopUpQueuedTasksBeforeNextPoll(t *testing.T) {
	enableGetTasks(t)
	task1 := v1.Task_builder{Id: "aaaaaaaaaaaaaaaaaaaaaaaaaaa", Hello: &v1.Task_HelloTask{}}.Build()
	task2 := v1.Task_builder{Id: "bbbbbbbbbbbbbbbbbbbbbbbbbbb", Hello: &v1.Task_HelloTask{}}.Build()
	sc := newFakeBatonServiceClient(nil)
	sc.getTasksResp = v1.BatonServiceGetTasksResponse_builder{
		Tasks:    []*v1.Task{task1, task2},
		NextPoll: durationpb.New(time.Hour),
	}.Build()
	mgr := newTestManager(sc)

	_, _, err := mgr.Next(context.Background())
	require.NoError(t, err, "Next first fetch")
	_, _, err = mgr.Next(context.Background())
	require.NoError(t, err, "Next queued")
	require.Len(t, sc.getTasksReqs, 1, "expected no top-up before next_poll")
}

func TestNextReturnsWaitWhenBatchIsEmpty(t *testing.T) {
	enableGetTasks(t)
	sc := newFakeBatonServiceClient(nil)
	sc.getTasksResp = v1.BatonServiceGetTasksResponse_builder{
		NextPoll: durationpb.New(time.Hour),
	}.Build()
	mgr := newTestManager(sc)

	task, wait, err := mgr.Next(context.Background())
	require.NoError(t, err)
	require.Nil(t, task, "expected no task, got %+v", task)
	require.Positive(t, wait, "expected positive wait for empty batch, got %s", wait)
}

func TestNextWaitsWhenEnoughKnownTasksAreInFlight(t *testing.T) {
	enableGetTasks(t)
	sc := newFakeBatonServiceClient(nil)
	mgr := newTestManager(sc)
	for _, id := range []string{
		"aaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"bbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"ccccccccccccccccccccccccccc",
	} {
		mgr.taskQueue.inFlight[id] = struct{}{}
	}

	task, wait, err := mgr.Next(context.Background())
	require.NoError(t, err)
	require.Nil(t, task, "expected no queued task, got %+v", task)
	require.Positive(t, wait, "expected positive wait when enough tasks are already in flight, got %s", wait)
	require.Empty(t, sc.getTasksReqs, "expected no GetTasks call while enough tasks are in flight")
}

func TestNextDoesNotTopUpWhenKnownTasksAtLowWater(t *testing.T) {
	enableGetTasks(t)
	task1 := v1.Task_builder{Id: "aaaaaaaaaaaaaaaaaaaaaaaaaaa", Hello: &v1.Task_HelloTask{}}.Build()
	task2 := v1.Task_builder{Id: "bbbbbbbbbbbbbbbbbbbbbbbbbbb", Hello: &v1.Task_HelloTask{}}.Build()
	task3 := v1.Task_builder{Id: "ccccccccccccccccccccccccccc", Hello: &v1.Task_HelloTask{}}.Build()
	sc := newFakeBatonServiceClient(nil)
	sc.getTasksResp = v1.BatonServiceGetTasksResponse_builder{
		Tasks: []*v1.Task{task1, task2, task3},
	}.Build()
	mgr := newTestManager(sc)

	_, _, err := mgr.Next(context.Background())
	require.NoError(t, err, "Next first fetch")
	_, _, err = mgr.Next(context.Background())
	require.NoError(t, err, "Next queued")
	require.Len(t, sc.getTasksReqs, 1, "expected no top-up at low-water")
}

func TestNextTopsUpQueuedTasksAfterNextPoll(t *testing.T) {
	enableGetTasks(t)
	task1 := v1.Task_builder{Id: "aaaaaaaaaaaaaaaaaaaaaaaaaaa", Hello: &v1.Task_HelloTask{}}.Build()
	task2 := v1.Task_builder{Id: "bbbbbbbbbbbbbbbbbbbbbbbbbbb", Hello: &v1.Task_HelloTask{}}.Build()
	task3 := v1.Task_builder{Id: "ccccccccccccccccccccccccccc", Hello: &v1.Task_HelloTask{}}.Build()
	sc := newFakeBatonServiceClient(nil)
	sc.getTasksResp = v1.BatonServiceGetTasksResponse_builder{
		Tasks: []*v1.Task{task1, task2},
	}.Build()
	mgr := newTestManager(sc)

	_, _, err := mgr.Next(context.Background())
	require.NoError(t, err, "Next first fetch")
	sc.getTasksResp = v1.BatonServiceGetTasksResponse_builder{
		Tasks: []*v1.Task{task3},
	}.Build()
	_, _, err = mgr.Next(context.Background())
	require.NoError(t, err, "Next queued/top-up")
	require.Len(t, sc.getTasksReqs, 2, "expected top-up after elapsed next_poll")
}

func TestProcessRemovesKnownTaskID(t *testing.T) {
	task := v1.Task_builder{Id: "aaaaaaaaaaaaaaaaaaaaaaaaaaa", Hello: &v1.Task_HelloTask{}}.Build()
	sc := newFakeBatonServiceClient(nil)
	mgr := newTestManager(sc)
	mgr.taskQueue.inFlight[task.GetId()] = struct{}{}

	require.NoError(t, mgr.Process(context.Background(), task, &fakeConnectorClient{}))
	got, _ := mgr.taskQueue.fetchParams()
	require.False(t, containsString(got, task.GetId()), "known task IDs %v still include completed task %q", got, task.GetId())
}

func TestNextReturnsGetTasksErrorWhenEnabled(t *testing.T) {
	enableGetTasks(t)
	sc := newFakeBatonServiceClient(nil)
	sc.getTasksErr = status.Error(codes.Unimplemented, "not implemented")
	mgr := newTestManager(sc)

	_, _, err := mgr.Next(context.Background())
	require.Equal(t, codes.Unimplemented, status.Code(err), "expected Unimplemented error, got %v", err)
	require.Equal(t, 0, sc.getTaskCalls, "expected no GetTask fallback calls")
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
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
			require.Equal(t, tc.want, isRetryableHelloError(tc.err))
		})
	}
}
