package c1api

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestC1ServiceClientReusesConnection(t *testing.T) {
	bc := newBenchmarkServiceClient(t, 0, &benchmarkBatonService{})
	bc.client.idleCloseThreshold = time.Hour

	ctx := context.Background()
	for range 3 {
		_, err := bc.client.GetTask(ctx, &v1.BatonServiceGetTaskRequest{})
		if err != nil {
			t.Fatal(err)
		}
	}

	if got := bc.dialCount.Load(); got != 1 {
		t.Fatalf("expected one dial for reused connection, got %d", got)
	}
}

func TestC1ServiceClientKnownIdleClose(t *testing.T) {
	bc := newBenchmarkServiceClient(t, 0, &benchmarkBatonService{})
	bc.client.idleCloseThreshold = time.Minute

	ctx := context.Background()
	_, err := bc.client.GetTask(ctx, &v1.BatonServiceGetTaskRequest{})
	if err != nil {
		t.Fatal(err)
	}

	if err := bc.client.CloseIfIdleFor(2 * time.Minute); err != nil {
		t.Fatal(err)
	}

	_, err = bc.client.GetTask(ctx, &v1.BatonServiceGetTaskRequest{})
	if err != nil {
		t.Fatal(err)
	}

	if got := bc.dialCount.Load(); got != 2 {
		t.Fatalf("expected redial after known-idle close, got %d dials", got)
	}
}

func TestC1ServiceClientKnownIdleDoesNotCloseWhenNextUseIsSoon(t *testing.T) {
	bc := newBenchmarkServiceClient(t, 0, &benchmarkBatonService{})
	bc.client.idleCloseThreshold = time.Minute

	ctx := context.Background()
	_, err := bc.client.GetTask(ctx, &v1.BatonServiceGetTaskRequest{})
	if err != nil {
		t.Fatal(err)
	}

	if err := bc.client.CloseIfIdleFor(30 * time.Second); err != nil {
		t.Fatal(err)
	}

	_, err = bc.client.GetTask(ctx, &v1.BatonServiceGetTaskRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if got := bc.dialCount.Load(); got != 1 {
		t.Fatalf("expected soon next use to keep cached connection, got %d dials", got)
	}
}

func TestC1ServiceClientKnownIdleWaitsForInFlightRPC(t *testing.T) {
	bc := newBenchmarkServiceClient(t, 0, &benchmarkBatonService{getTaskLatency: 75 * time.Millisecond})
	bc.client.idleCloseThreshold = time.Minute

	ctx := context.Background()
	errCh := make(chan error, 1)
	go func() {
		_, err := bc.client.GetTask(ctx, &v1.BatonServiceGetTaskRequest{})
		errCh <- err
	}()

	waitForCondition(t, time.Second, func() bool {
		bc.client.mtx.Lock()
		defer bc.client.mtx.Unlock()
		return bc.client.active != nil && bc.client.active.inFlight == 1
	})

	if err := bc.client.CloseIfIdleFor(2 * time.Minute); err != nil {
		t.Fatal(err)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("in-flight RPC failed after known-idle close request: %v", err)
	}

	_, err := bc.client.GetTask(ctx, &v1.BatonServiceGetTaskRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if got := bc.dialCount.Load(); got != 2 {
		t.Fatalf("expected known-idle close after in-flight RPC completed, got %d dials", got)
	}
}

func TestC1ServiceClientFailedDialDoesNotPoisonClient(t *testing.T) {
	lis, err := new(net.ListenConfig).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	v1.RegisterBatonServiceServer(server, &benchmarkBatonService{})
	go func() {
		if serveErr := server.Serve(lis); serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			t.Errorf("benchmark gRPC server failed: %v", serveErr)
		}
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = lis.Close()
	})

	var attempts atomic.Int64
	netDialer := &net.Dialer{}
	client := &c1ServiceClient{
		addr: lis.Addr().String(),
		dialOpts: []grpc.DialOption{
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				if attempts.Add(1) == 1 {
					return nil, sleepContext(ctx, 50*time.Millisecond)
				}
				return netDialer.DialContext(ctx, "tcp", lis.Addr().String())
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(), //nolint:staticcheck // Mirrors production dial behavior.
		},
		idleCloseThreshold: time.Hour,
	}
	t.Cleanup(func() { _ = client.TeardownServiceClient() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err = client.GetTask(ctx, &v1.BatonServiceGetTaskRequest{})
	if err == nil {
		t.Fatal("expected first dial to fail")
	}

	_, err = client.GetTask(context.Background(), &v1.BatonServiceGetTaskRequest{})
	if err != nil {
		t.Fatalf("expected client to recover after failed dial: %v", err)
	}
	if got := attempts.Load(); got < 2 {
		t.Fatalf("expected at least two dial attempts, got %d", got)
	}
}

func TestC1ServiceClientDoesNotResetAfterServerUnavailable(t *testing.T) {
	bc := newBenchmarkServiceClient(t, 0, &unavailableOnceService{})
	bc.client.idleCloseThreshold = time.Hour

	ctx := context.Background()
	_, err := bc.client.GetTask(ctx, &v1.BatonServiceGetTaskRequest{})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected unavailable error, got %v", err)
	}

	_, err = bc.client.GetTask(ctx, &v1.BatonServiceGetTaskRequest{})
	if err != nil {
		t.Fatalf("expected client to redial and recover after unavailable: %v", err)
	}
	if got := bc.dialCount.Load(); got != 1 {
		t.Fatalf("expected server unavailable to keep healthy connection, got %d dials", got)
	}
}

func TestC1ServiceClientRetriesAfterTransportClose(t *testing.T) {
	bc := newBenchmarkServiceClient(t, 0, &benchmarkBatonService{})
	bc.client.idleCloseThreshold = time.Hour

	ctx := context.Background()
	_, err := bc.client.GetTask(ctx, &v1.BatonServiceGetTaskRequest{})
	if err != nil {
		t.Fatal(err)
	}

	conn := requireActiveConn(t, bc.client)
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = bc.client.GetTask(ctx, &v1.BatonServiceGetTaskRequest{})
	if err == nil {
		t.Fatal("expected GetTask on stale transport to fail without retrying")
	}
	if got := bc.dialCount.Load(); got != 1 {
		t.Fatalf("expected GetTask not to redial until next call, got %d dials", got)
	}

	_, err = bc.client.GetTask(ctx, &v1.BatonServiceGetTaskRequest{})
	if err != nil {
		t.Fatalf("expected next GetTask to redial and recover after transport close: %v", err)
	}
	if got := bc.dialCount.Load(); got != 2 {
		t.Fatalf("expected redial after transport close reset, got %d dials", got)
	}
}

func TestC1ServiceClientDoesNotRetryFinishTaskAfterTransportClose(t *testing.T) {
	bc := newBenchmarkServiceClient(t, 0, &benchmarkBatonService{})
	bc.client.idleCloseThreshold = time.Hour

	ctx := context.Background()
	_, err := bc.client.GetTask(ctx, &v1.BatonServiceGetTaskRequest{})
	if err != nil {
		t.Fatal(err)
	}

	conn := requireActiveConn(t, bc.client)
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = bc.client.FinishTask(ctx, v1.BatonServiceFinishTaskRequest_builder{
		TaskId:  "benchmark-task",
		Success: v1.BatonServiceFinishTaskRequest_Success_builder{}.Build(),
	}.Build())
	if err == nil {
		t.Fatal("expected FinishTask on stale transport to fail without retrying")
	}
	if got := bc.dialCount.Load(); got != 1 {
		t.Fatalf("expected FinishTask not to redial until next call, got %d dials", got)
	}

	_, err = bc.client.FinishTask(ctx, v1.BatonServiceFinishTaskRequest_builder{
		TaskId:  "benchmark-task",
		Success: v1.BatonServiceFinishTaskRequest_Success_builder{}.Build(),
	}.Build())
	if err != nil {
		t.Fatalf("expected next FinishTask to redial and recover after transport close: %v", err)
	}
	if got := bc.dialCount.Load(); got != 2 {
		t.Fatalf("expected redial after transport close reset, got %d dials", got)
	}
}

func TestC1ServiceClientResetDoesNotClearNewerConnection(t *testing.T) {
	bc := newBenchmarkServiceClient(t, 0, &benchmarkBatonService{})
	bc.client.idleCloseThreshold = time.Hour

	ctx := context.Background()
	_, err := bc.client.GetTask(ctx, &v1.BatonServiceGetTaskRequest{})
	if err != nil {
		t.Fatal(err)
	}

	oldHandle := requireActiveHandle(t, bc.client)
	oldConn := oldHandle.cc
	t.Cleanup(func() { _ = oldConn.Close() })

	newConn, err := bc.client.dial(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = newConn.Close() })

	bc.client.mtx.Lock()
	oldHandle.retired = true
	oldHandle.inFlight = 1
	bc.client.active = &c1ServiceConn{
		cc:       newConn,
		client:   v1.NewBatonServiceClient(newConn),
		inFlight: 1,
	}
	bc.client.draining = append(bc.client.draining, oldHandle)
	bc.client.mtx.Unlock()

	bc.client.endRPC(oldConn, io.EOF)

	gotConn := requireActiveConn(t, bc.client)
	if gotConn != newConn {
		t.Fatal("reset from old connection cleared newer cached connection")
	}
}

func TestC1ServiceClientDoesNotCloseRetiredConnectionUntilInFlightDrains(t *testing.T) {
	bc := newBenchmarkServiceClient(t, 0, &benchmarkBatonService{})
	bc.client.idleCloseThreshold = time.Hour

	ctx := context.Background()
	_, err := bc.client.GetTask(ctx, &v1.BatonServiceGetTaskRequest{})
	if err != nil {
		t.Fatal(err)
	}

	oldHandle := requireActiveHandle(t, bc.client)
	bc.client.mtx.Lock()
	oldHandle.inFlight = 2
	bc.client.mtx.Unlock()
	oldConn := oldHandle.cc

	bc.client.endRPC(oldConn, io.EOF)

	oldClient := v1.NewBatonServiceClient(oldConn)
	_, err = oldClient.GetTask(ctx, &v1.BatonServiceGetTaskRequest{})
	if err != nil {
		t.Fatalf("retired connection was closed before in-flight RPCs drained: %v", err)
	}

	bc.client.endRPC(oldConn, nil)
	waitForCondition(t, time.Second, func() bool {
		_, err = oldClient.GetTask(ctx, &v1.BatonServiceGetTaskRequest{})
		return err != nil
	})
}

func TestC1ServiceClientCloseIsIdempotentAndPreventsReuse(t *testing.T) {
	bc := newBenchmarkServiceClient(t, 0, &benchmarkBatonService{})
	bc.client.idleCloseThreshold = time.Hour

	ctx := context.Background()
	_, err := bc.client.GetTask(ctx, &v1.BatonServiceGetTaskRequest{})
	if err != nil {
		t.Fatal(err)
	}

	if err := bc.client.TeardownServiceClient(); err != nil {
		t.Fatal(err)
	}
	if err := bc.client.TeardownServiceClient(); err != nil {
		t.Fatal(err)
	}

	_, err = bc.client.GetTask(ctx, &v1.BatonServiceGetTaskRequest{})
	if err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("expected clear closed-client error, got %v", err)
	}
}

func TestC1ServiceClientCloseCancelsDialAndUnblocksWaiters(t *testing.T) {
	client := &c1ServiceClient{
		addr: "blocked-dial",
		dialOpts: []grpc.DialOption{
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return nil, sleepContext(ctx, 5*time.Second)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(), //nolint:staticcheck // Mirrors production dial behavior.
		},
		idleCloseThreshold: time.Hour,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		_, err := client.GetTask(ctx, &v1.BatonServiceGetTaskRequest{})
		errCh <- err
	}()

	waitForCondition(t, time.Second, func() bool {
		client.mtx.Lock()
		defer client.mtx.Unlock()
		return client.dialDone != nil
	})

	waiterErrCh := make(chan error, 1)
	go func() {
		_, err := client.GetTask(context.Background(), &v1.BatonServiceGetTaskRequest{})
		waiterErrCh <- err
	}()

	start := time.Now()
	if err := client.TeardownServiceClient(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("Close waited for in-progress dial: %s", elapsed)
	}

	select {
	case err := <-waiterErrCh:
		if err == nil || !strings.Contains(err.Error(), "closed") {
			t.Fatalf("expected waiting RPC to see closed client, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiting RPC did not exit after Close")
	}

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected in-progress dial to return an error after Close")
		}
	case <-time.After(time.Second):
		t.Fatal("in-progress dial did not exit after Close")
	}
	cancel()
}

func TestC1ServiceClientConcurrentRPCsShareConnection(t *testing.T) {
	bc := newBenchmarkServiceClient(t, 0, &benchmarkBatonService{getTaskLatency: 10 * time.Millisecond})
	bc.client.idleCloseThreshold = time.Hour

	ctx := context.Background()
	const concurrency = 12
	var wg sync.WaitGroup
	errCh := make(chan error, concurrency)
	wg.Add(concurrency)
	for range concurrency {
		go func() {
			defer wg.Done()
			_, err := bc.client.GetTask(ctx, &v1.BatonServiceGetTaskRequest{})
			errCh <- err
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	if got := bc.dialCount.Load(); got != 1 {
		t.Fatalf("expected concurrent RPCs to share one connection, got %d dials", got)
	}
	bc.client.mtx.Lock()
	inFlight := 0
	if bc.client.active != nil {
		inFlight = bc.client.active.inFlight
	}
	bc.client.mtx.Unlock()
	if inFlight != 0 {
		t.Fatalf("expected no in-flight RPCs after concurrency test, got %d", inFlight)
	}
}

type unavailableOnceService struct {
	v1.UnimplementedBatonServiceServer
	calls atomic.Int64
}

func (s *unavailableOnceService) GetTask(context.Context, *v1.BatonServiceGetTaskRequest) (*v1.BatonServiceGetTaskResponse, error) {
	if s.calls.Add(1) == 1 {
		return nil, status.Error(codes.Unavailable, "synthetic unavailable")
	}
	return v1.BatonServiceGetTaskResponse_builder{}.Build(), nil
}

func waitForCondition(t *testing.T, timeout time.Duration, f func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}

func requireActiveHandle(t *testing.T, client *c1ServiceClient) *c1ServiceConn {
	t.Helper()

	client.mtx.Lock()
	defer client.mtx.Unlock()
	if client.active == nil {
		t.Fatal("expected cached connection")
	}
	return client.active
}

func requireActiveConn(t *testing.T, client *c1ServiceClient) *grpc.ClientConn {
	t.Helper()

	handle := requireActiveHandle(t, client)
	if handle.cc == nil {
		t.Fatal("expected cached connection")
	}
	return handle.cc
}
