package connectorrunner

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/conductorone/baton-sdk/internal/connector"
	wrapperv1 "github.com/conductorone/baton-sdk/pb/c1/connector_wrapper/v1"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type rollbackConnector struct {
	types.ConnectorServer
	mode, dir string
}

func (*rollbackConnector) GetMetadata(context.Context, *v2.ConnectorServiceGetMetadataRequest) (*v2.ConnectorServiceGetMetadataResponse, error) {
	return v2.ConnectorServiceGetMetadataResponse_builder{Metadata: v2.ConnectorMetadata_builder{DisplayName: "rollback fixture", Description: "service mode verification"}.Build()}.Build(), nil
}
func (*rollbackConnector) Validate(context.Context, *v2.ConnectorServiceValidateRequest) (*v2.ConnectorServiceValidateResponse, error) {
	return &v2.ConnectorServiceValidateResponse{}, nil
}
func (*rollbackConnector) Cleanup(context.Context, *v2.ConnectorServiceCleanupRequest) (*v2.ConnectorServiceCleanupResponse, error) {
	return &v2.ConnectorServiceCleanupResponse{}, nil
}
func (*rollbackConnector) ListResourceTypes(context.Context, *v2.ResourceTypesServiceListResourceTypesRequest) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	return v2.ResourceTypesServiceListResourceTypesResponse_builder{List: []*v2.ResourceType{v2.ResourceType_builder{Id: "group", DisplayName: "Groups"}.Build()}}.Build(), nil
}
func (c *rollbackConnector) ListResources(ctx context.Context, req *v2.ResourcesServiceListResourcesRequest) (*v2.ResourcesServiceListResourcesResponse, error) {
	id, next := "first", "next"
	if req.GetPageToken() != "" {
		if req.GetPageToken() != "next" {
			return nil, errors.New("unexpected page token")
		}
		if c.mode == "crash" {
			root, err := os.OpenRoot(c.dir)
			if err != nil {
				return nil, err
			}
			err = root.WriteFile("rollback-ready", []byte("second resource request"), 0600)
			_ = root.Close()
			if err != nil {
				return nil, err
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}
		if c.mode == "error" {
			return nil, status.Error(codes.PermissionDenied, "fixture collection failure")
		}
		id, next = "second", ""
	}
	return v2.ResourcesServiceListResourcesResponse_builder{List: []*v2.Resource{v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "group", Resource: id}.Build(), DisplayName: id,
	}.Build()}, NextPageToken: next}.Build(), nil
}

func TestLedgerServiceRollbackChild(t *testing.T) {
	dir := os.Getenv("BATON_ROLLBACK_DIR")
	if dir == "" {
		t.Skip("subprocess fixture for service-mode rollback")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	server := &rollbackConnector{dir: filepath.Clean(dir), mode: os.Getenv("BATON_ROLLBACK_MODE")}
	if os.Args[len(os.Args)-1] == "_connector-service" {
		scanner := bufio.NewScanner(os.Stdin)
		require.True(t, scanner.Scan())
		data, err := base64.StdEncoding.DecodeString(scanner.Text())
		require.NoError(t, err)
		config := &wrapperv1.ServerConfig{}
		require.NoError(t, proto.Unmarshal(data, config))
		require.NoError(t, config.ValidateAll())
		go func() { _, _ = io.Copy(io.Discard, os.Stdin); os.Exit(0) }()
		wrapper, err := connector.NewWrapper(ctx, server)
		require.NoError(t, err)
		require.NoError(t, wrapper.Run(ctx, config))
		return
	}
	opts := []Option{
		WithClientCredentials("rollback@localhost/test", os.Getenv("BATON_ROLLBACK_SECRET")),
		WithTempDir(dir), WithStorageEngine(c1zstore.EnginePebble), WithWorkerCount(1), WithTaskConcurrency(1),
	}
	if os.Getenv("BATON_ROLLBACK_KEEP") == "true" {
		opts = append(opts, WithKeepPreviousSyncC1Z(), WithKeepPreviousSyncC1ZRuntimeOptIn())
	}
	runner, err := NewConnectorRunner(ctx, server, opts...)
	require.NoError(t, err)
	defer func() { require.NoError(t, runner.Close(context.Background())) }()
	require.False(t, runner.oneShot)
	require.NoError(t, runner.Run(ctx))
}
