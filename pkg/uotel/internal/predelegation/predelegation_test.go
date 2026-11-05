package predelegation

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
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/metrics"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-sdk/pkg/types/tasks"
	"github.com/conductorone/baton-sdk/pkg/uotel"
)

// receiver is an in-process OTLP/gRPC metrics collector that records the
// task_type of every baton_sdk.task_success data point.
type receiver struct {
	colmetricspb.UnimplementedMetricsServiceServer

	mtx          sync.Mutex
	successTypes []string
}

func (r *receiver) Export(_ context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
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

func (r *receiver) taskSuccessTypes() []string {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return append([]string(nil), r.successTypes...)
}

// connector is a minimal ConnectorBuilderV2 with one resource type.
type connector struct{}

func (connector) Metadata(context.Context) (*v2.ConnectorMetadata, error) {
	return v2.ConnectorMetadata_builder{DisplayName: "predelegation"}.Build(), nil
}

func (connector) Validate(context.Context) (annotations.Annotations, error) { return nil, nil }

func (connector) ResourceSyncers(context.Context) []connectorbuilder.ResourceSyncerV2 {
	return []connectorbuilder.ResourceSyncerV2{syncer{}}
}

type syncer struct{}

func (syncer) ResourceType(context.Context) *v2.ResourceType {
	return v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
}

func (syncer) List(context.Context, *v2.ResourceId, resource.SyncOpAttrs) ([]*v2.Resource, *resource.SyncOpResults, error) {
	return nil, nil, nil
}

func (syncer) Entitlements(context.Context, *v2.Resource, resource.SyncOpAttrs) ([]*v2.Entitlement, *resource.SyncOpResults, error) {
	return nil, nil, nil
}

func (syncer) Grants(context.Context, *v2.Resource, resource.SyncOpAttrs) ([]*v2.Grant, *resource.SyncOpResults, error) {
	return nil, nil, nil
}

var ranOnce bool

// TestHandlersCreatedBeforeInitOtelExport builds a connector and a default
// metrics handler while the global MeterProvider is still the otel no-op
// delegate, then calls uotel.InitOtel and checks that metrics recorded
// through both reach the collector, just as spans from package-level
// otel.Tracer values do.
func TestHandlersCreatedBeforeInitOtelExport(t *testing.T) {
	// The first run installs the global MeterProvider for the process, so a
	// repeat under -count has nothing left to test.
	if ranOnce {
		t.Skip("global MeterProvider already installed by an earlier run in this process")
	}
	ranOnce = true

	// Guard: this test relies on being the first to set the global provider.
	_, isSDK := otel.GetMeterProvider().(interface{ ForceFlush(context.Context) error })
	require.False(t, isSDK, "a MeterProvider was installed before this test; it must run in its own package")

	ctx := t.Context()
	handler := metrics.NewDefaultHandler(ctx)
	c, err := connectorbuilder.NewConnector(ctx, connector{})
	require.NoError(t, err)

	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	r := &receiver{}
	srv := grpc.NewServer()
	colmetricspb.RegisterMetricsServiceServer(srv, r)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	ctx, shutdown, err := uotel.InitOtel(ctx,
		uotel.WithServiceName("predelegation-test"),
		uotel.WithInsecureOtelEndpoint(lis.Addr().String()),
		uotel.WithTracingDisabled(),
		uotel.WithLoggingDisabled(),
		uotel.WithMetricsExportInterval(time.Hour),
	)
	require.NoError(t, err)

	metrics.New(handler).RecordTaskSuccess(ctx, tasks.FullSyncType, time.Millisecond)
	_, err = c.ListResourceTypes(ctx, &v2.ResourceTypesServiceListResourceTypesRequest{})
	require.NoError(t, err)

	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, shutdown(closeCtx))

	got := r.taskSuccessTypes()
	require.Contains(t, got, tasks.FullSyncType.String(), "handler created before InitOtel did not export")
	require.Contains(t, got, tasks.ListResourceTypesType.String(), "connector built before InitOtel did not export")
}
