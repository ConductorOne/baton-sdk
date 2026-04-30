package c1api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"
)

type benchmarkBatonService struct {
	v1.UnimplementedBatonServiceServer

	getTaskLatency    time.Duration
	finishTaskLatency time.Duration
}

func (s *benchmarkBatonService) GetTask(ctx context.Context, _ *v1.BatonServiceGetTaskRequest) (*v1.BatonServiceGetTaskResponse, error) {
	if err := sleepContext(ctx, s.getTaskLatency); err != nil {
		return nil, err
	}
	return v1.BatonServiceGetTaskResponse_builder{
		NextPoll: durationpb.New(time.Second),
	}.Build(), nil
}

func (s *benchmarkBatonService) Heartbeat(ctx context.Context, _ *v1.BatonServiceHeartbeatRequest) (*v1.BatonServiceHeartbeatResponse, error) {
	if err := sleepContext(ctx, s.getTaskLatency); err != nil {
		return nil, err
	}
	return v1.BatonServiceHeartbeatResponse_builder{
		NextHeartbeat: durationpb.New(time.Second),
	}.Build(), nil
}

func (s *benchmarkBatonService) FinishTask(ctx context.Context, _ *v1.BatonServiceFinishTaskRequest) (*v1.BatonServiceFinishTaskResponse, error) {
	if err := sleepContext(ctx, s.finishTaskLatency); err != nil {
		return nil, err
	}
	return v1.BatonServiceFinishTaskResponse_builder{}.Build(), nil
}

func (s *benchmarkBatonService) UploadAsset(stream grpc.ClientStreamingServer[v1.BatonServiceUploadAssetRequest, v1.BatonServiceUploadAssetResponse]) error {
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(v1.BatonServiceUploadAssetResponse_builder{}.Build())
		}
		if err != nil {
			return err
		}
	}
}

type benchmarkServiceClient struct {
	client    *c1ServiceClient
	dialCount atomic.Int64
	close     func()
}

func newBenchmarkServiceClient(tb testing.TB, dialLatency time.Duration, service v1.BatonServiceServer) *benchmarkServiceClient {
	tb.Helper()

	lis, err := new(net.ListenConfig).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	server := grpc.NewServer()
	v1.RegisterBatonServiceServer(server, service)

	var serveWG sync.WaitGroup
	serveWG.Add(1)
	go func() {
		defer serveWG.Done()
		if err := server.Serve(lis); err != nil {
			if !errors.Is(err, grpc.ErrServerStopped) {
				tb.Errorf("benchmark gRPC server failed: %v", err)
			}
		}
	}()

	bc := &benchmarkServiceClient{}
	netDialer := &net.Dialer{}
	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		if err := sleepContext(ctx, dialLatency); err != nil {
			return nil, err
		}
		bc.dialCount.Add(1)
		return netDialer.DialContext(ctx, "tcp", lis.Addr().String())
	}

	bc.client = &c1ServiceClient{
		addr: lis.Addr().String(),
		dialOpts: []grpc.DialOption{
			grpc.WithContextDialer(dialer),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(), //nolint:staticcheck // Mirrors production dial behavior for the baseline benchmark.
		},
	}
	bc.close = func() {
		_ = bc.client.TeardownServiceClient()
		server.Stop()
		_ = lis.Close()
		serveWG.Wait()
	}
	tb.Cleanup(bc.close)

	return bc
}

func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func BenchmarkServiceClientGetTask(b *testing.B) {
	for _, dialLatency := range []time.Duration{50 * time.Millisecond, 100 * time.Millisecond, 300 * time.Millisecond} {
		b.Run(fmt.Sprintf("dial_latency_%s", dialLatency), func(b *testing.B) {
			bc := newBenchmarkServiceClient(b, dialLatency, &benchmarkBatonService{})
			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := bc.client.GetTask(ctx, &v1.BatonServiceGetTaskRequest{})
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()

			if elapsed := b.Elapsed(); elapsed > 0 {
				b.ReportMetric(float64(b.N)/elapsed.Seconds(), "rpc/s")
			}
			b.ReportMetric(float64(bc.dialCount.Load())/float64(b.N), "dials/op")
		})
	}
}

func BenchmarkServiceClientActionSequence(b *testing.B) {
	dialLatencies := []time.Duration{50 * time.Millisecond, 100 * time.Millisecond, 300 * time.Millisecond}
	taskRuntimes := []time.Duration{50 * time.Millisecond, 250 * time.Millisecond, 1500 * time.Millisecond}

	for _, dialLatency := range dialLatencies {
		for _, taskRuntime := range taskRuntimes {
			b.Run(fmt.Sprintf("dial_latency_%s/task_runtime_%s", dialLatency, taskRuntime), func(b *testing.B) {
				bc := newBenchmarkServiceClient(b, dialLatency, &benchmarkBatonService{})
				ctx := context.Background()

				b.ResetTimer()
				runActionSequenceBenchmark(b, ctx, bc.client, taskRuntime)
				b.StopTimer()

				if elapsed := b.Elapsed(); elapsed > 0 {
					b.ReportMetric(float64(b.N)/elapsed.Seconds(), "actions/s")
				}
				b.ReportMetric(float64(bc.dialCount.Load())/float64(b.N), "dials/action")
			})
		}
	}
}

func BenchmarkServiceClientActionSequenceCalibratedWorkload(b *testing.B) {
	// This workload was calibrated from the observed pre-change throughput. It
	// measures the current client implementation under that same synthetic shape.
	const (
		dialLatency = 50 * time.Millisecond
		taskRuntime = 200 * time.Millisecond
	)

	bc := newBenchmarkServiceClient(b, dialLatency, &benchmarkBatonService{})
	ctx := context.Background()

	b.ResetTimer()
	runActionSequenceBenchmark(b, ctx, bc.client, taskRuntime)
	b.StopTimer()

	if elapsed := b.Elapsed(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "actions/s")
	}
	b.ReportMetric(float64(bc.dialCount.Load())/float64(b.N), "dials/action")
}

func runActionSequenceBenchmark(b *testing.B, ctx context.Context, client *c1ServiceClient, taskRuntime time.Duration) {
	b.Helper()

	const concurrency = 3
	workCh := make(chan struct{}, concurrency)

	var wg sync.WaitGroup
	wg.Add(concurrency)
	for range concurrency {
		go func() {
			defer wg.Done()
			for range workCh {
				_, err := client.GetTask(ctx, &v1.BatonServiceGetTaskRequest{})
				if err != nil {
					b.Error(err)
					continue
				}
				if err := sleepContext(ctx, taskRuntime); err != nil {
					b.Error(err)
					continue
				}
				_, err = client.FinishTask(ctx, v1.BatonServiceFinishTaskRequest_builder{
					TaskId:  "benchmark-task",
					Success: v1.BatonServiceFinishTaskRequest_Success_builder{}.Build(),
				}.Build())
				if err != nil {
					b.Error(err)
				}
			}
		}()
	}

	for i := 0; i < b.N; i++ {
		workCh <- struct{}{}
	}
	close(workCh)
	wg.Wait()
}
