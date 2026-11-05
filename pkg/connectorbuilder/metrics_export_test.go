package connectorbuilder

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/types/tasks"
	"github.com/conductorone/baton-sdk/pkg/uotel"
)

// metricsReceiver is an in-process OTLP/gRPC metrics collector that records
// the task_type attribute of every baton_sdk.task_success data point.
type metricsReceiver struct {
	colmetricspb.UnimplementedMetricsServiceServer

	mtx          sync.Mutex
	successTypes []string
}

func (r *metricsReceiver) Export(_ context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	for _, rm := range req.GetResourceMetrics() {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				if m.GetName() != "baton_sdk.task_success" {
					continue
				}
				for _, dp := range m.GetSum().GetDataPoints() {
					for _, kv := range dp.GetAttributes() {
						if kv.GetKey() == "task_type" {
							r.successTypes = append(r.successTypes, kv.GetValue().GetStringValue())
						}
					}
				}
			}
		}
	}
	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}

func (r *metricsReceiver) taskSuccessTypes() []string {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return append([]string(nil), r.successTypes...)
}

// TestConnectorMetricsReachCollector checks that a connector built by
// NewConnector without WithMetricsHandler exports its task metrics once
// uotel.InitOtel has installed a MeterProvider, and that they are flushed by
// the shutdown function.
func TestConnectorMetricsReachCollector(t *testing.T) {
	prevMP := otel.GetMeterProvider()
	t.Cleanup(func() {
		if otel.GetMeterProvider() != prevMP {
			otel.SetMeterProvider(prevMP)
		}
	})

	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	r := &metricsReceiver{}
	srv := grpc.NewServer()
	colmetricspb.RegisterMetricsServiceServer(srv, r)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	ctx, shutdown, err := uotel.InitOtel(t.Context(),
		uotel.WithServiceName("connectorbuilder-test"),
		uotel.WithInsecureOtelEndpoint(lis.Addr().String()),
		uotel.WithTracingDisabled(),
		uotel.WithLoggingDisabled(),
		uotel.WithMetricsExportInterval(time.Hour),
	)
	require.NoError(t, err)

	c, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{newTestResourceSyncer("user")}))
	require.NoError(t, err)
	_, err = c.ListResourceTypes(ctx, &v2.ResourceTypesServiceListResourceTypesRequest{})
	require.NoError(t, err)

	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, shutdown(closeCtx))

	require.Contains(t, r.taskSuccessTypes(), tasks.ListResourceTypesType.String())
}
