//go:build baton_lambda_support

package cli

import (
	"context"
	"reflect"
	"testing"
	"time"

	connectorpkg "github.com/conductorone/baton-sdk/internal/connector"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	c1_lambda_grpc "github.com/conductorone/baton-sdk/pkg/lambda/grpc"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

type testLambdaConnectorServer struct {
	v2.UnimplementedConnectorServiceServer
	v2.UnimplementedResourceTypesServiceServer
	v2.UnimplementedResourcesServiceServer
	v2.UnimplementedEntitlementsServiceServer
	v2.UnimplementedGrantsServiceServer
	v2.UnimplementedAssetServiceServer
	v2.UnimplementedGrantManagerServiceServer
	v2.UnimplementedResourceManagerServiceServer
	v2.UnimplementedResourceDeleterServiceServer
	v2.UnimplementedAccountManagerServiceServer
	v2.UnimplementedCredentialManagerServiceServer
	v2.UnimplementedEventServiceServer
	v2.UnimplementedTicketsServiceServer
	v2.UnimplementedActionServiceServer
	v2.UnimplementedResourceGetterServiceServer
}

type testLambdaConnectorCloseWithoutContext struct {
	testLambdaConnectorServer
	closed bool
}

func (c *testLambdaConnectorCloseWithoutContext) Close() error {
	c.closed = true
	return nil
}

type testLambdaConnectorCloseWithContext struct {
	testLambdaConnectorServer
	closed bool
}

func (c *testLambdaConnectorCloseWithContext) Close(context.Context) error {
	c.closed = true
	return nil
}

type testLambdaConnectorCloseSignal struct {
	testLambdaConnectorServer
	closed chan struct{}
}

func (c *testLambdaConnectorCloseSignal) Close() error {
	close(c.closed)
	return nil
}

type testLambdaConnectorWithExtraService struct {
	testLambdaConnectorCloseSignal
}

func (*testLambdaConnectorWithExtraService) testLambdaExtraService() {}

type testLambdaExtraServiceServer interface {
	testLambdaExtraService()
}

var testLambdaExtraServiceDesc = grpc.ServiceDesc{
	ServiceName: "test.lambda.ExtraService",
	HandlerType: (*testLambdaExtraServiceServer)(nil),
}

func TestCloseLambdaConnectorGenerationSupportsCloseWithoutContext(t *testing.T) {
	connector := &testLambdaConnectorCloseWithoutContext{}

	if err := closeLambdaConnectorGeneration(context.Background(), connector); err != nil {
		t.Fatalf("closeLambdaConnectorGeneration: %v", err)
	}
	if !connector.closed {
		t.Fatal("expected Close() to be called")
	}
}

func TestCloseLambdaConnectorGenerationSupportsCloseWithContext(t *testing.T) {
	connector := &testLambdaConnectorCloseWithContext{}

	if err := closeLambdaConnectorGeneration(context.Background(), connector); err != nil {
		t.Fatalf("closeLambdaConnectorGeneration: %v", err)
	}
	if !connector.closed {
		t.Fatal("expected Close(context.Context) to be called")
	}
}

func TestLambdaConnectorReloaderClosesNextGenerationWhenReplaceFails(t *testing.T) {
	server := c1_lambda_grpc.NewServer(nil)
	previous := &testLambdaConnectorWithExtraService{
		testLambdaConnectorCloseSignal: testLambdaConnectorCloseSignal{
			closed: make(chan struct{}),
		},
	}
	next := &testLambdaConnectorCloseSignal{closed: make(chan struct{})}
	connectorpkg.Register(context.Background(), server, previous, nil)
	server.RegisterService(&testLambdaExtraServiceDesc, previous)

	reloader := &lambdaConnectorReloader{
		server: server,
		current: &lambdaConnectorGeneration{
			version:   "old",
			connector: previous,
		},
		build: func(context.Context, string) (*lambdaConnectorGeneration, error) {
			return &lambdaConnectorGeneration{
				version:   "new",
				connector: next,
			}, nil
		},
	}

	if err := reloader.reloadIfNeeded(context.Background(), "new"); err == nil {
		t.Fatal("expected reload to fail")
	}
	assertConnectorClosed(t, next.closed)
}

func TestLambdaConnectorReloaderClosesPreviousGenerationWhenApplyLogLevelFails(t *testing.T) {
	server := c1_lambda_grpc.NewServer(nil)
	previous := &testLambdaConnectorCloseSignal{closed: make(chan struct{})}
	next := &testLambdaConnectorCloseSignal{closed: make(chan struct{})}
	connectorpkg.Register(context.Background(), server, previous, nil)

	reloader := &lambdaConnectorReloader{
		server: server,
		current: &lambdaConnectorGeneration{
			version:   "old",
			connector: previous,
		},
		build: func(context.Context, string) (*lambdaConnectorGeneration, error) {
			return &lambdaConnectorGeneration{
				version:   "new",
				connector: next,
				logging:   lambdaLogLevelConfig{level: "definitely-not-a-log-level"},
			}, nil
		},
	}

	if err := reloader.reloadIfNeeded(context.Background(), "new"); err == nil {
		t.Fatal("expected reload to fail")
	}
	if reloader.current == nil || reloader.current.connector != next {
		t.Fatalf("expected next generation to be installed, got %+v", reloader.current)
	}
	assertConnectorClosed(t, previous.closed)
	select {
	case <-next.closed:
		t.Fatal("next generation should remain live after log-level failure")
	default:
	}
}

func TestLambdaConnectorReloaderClosesNextGenerationWhenNoServicesReplaced(t *testing.T) {
	server := c1_lambda_grpc.NewServer(nil)
	next := &testLambdaConnectorCloseSignal{closed: make(chan struct{})}
	reloader := &lambdaConnectorReloader{
		server: server,
		current: &lambdaConnectorGeneration{
			version:   "old",
			connector: &testLambdaConnectorServer{},
		},
		build: func(context.Context, string) (*lambdaConnectorGeneration, error) {
			return &lambdaConnectorGeneration{
				version:   "new",
				connector: next,
			}, nil
		},
	}

	if err := reloader.reloadIfNeeded(context.Background(), "new"); err == nil {
		t.Fatal("expected reload to fail")
	}
	assertConnectorClosed(t, next.closed)
}

func assertConnectorClosed(t *testing.T, closed <-chan struct{}) {
	t.Helper()

	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("expected abandoned connector generation to close")
	}
}

func TestEffectiveLambdaConfigSyncResourceTypeIDs(t *testing.T) {
	t.Parallel()

	base := viper.New()
	base.Set("sync-resource-types", []string{"fallback"})
	connectorConfig := map[string]any{
		"sync-resource-types": []any{"user", "group"},
	}

	effectiveConfig := effectiveLambdaConfig(base, connectorConfig)

	got := effectiveConfig.GetStringSlice("sync-resource-types")
	want := []string{"user", "group"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sync-resource-types = %#v, want %#v", got, want)
	}
}
