package connector

import (
	"context"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"

	connectorwrapperV1 "github.com/conductorone/baton-sdk/pb/c1/connector_wrapper/v1"
	"github.com/conductorone/baton-sdk/pkg/profiling"
)

// profileService implements the ProfileService gRPC service.
type profileService struct {
	connectorwrapperV1.UnimplementedProfileServiceServer
	profiler *profiling.Profiler
}

// FlushProfiles writes pending profile data to disk.
func (ps *profileService) FlushProfiles(ctx context.Context, req *connectorwrapperV1.FlushProfilesRequest) (*connectorwrapperV1.FlushProfilesResponse, error) {
	l := ctxzap.Extract(ctx)
	l.Info("FlushProfiles RPC called, stopping profiling and writing profiles")

	if ps.profiler == nil {
		return &connectorwrapperV1.FlushProfilesResponse{
			Success: true,
		}, nil
	}

	// Stop CPU profiling to flush data
	if err := ps.profiler.Stop(ctx); err != nil {
		// nolint:nilerr // This should be nil, we're returning the error to the client
		return &connectorwrapperV1.FlushProfilesResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	// Write memory profile
	if err := ps.profiler.WriteMemProfile(ctx); err != nil {
		// nolint:nilerr // This should be nil, we're returning the error to the client
		return &connectorwrapperV1.FlushProfilesResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &connectorwrapperV1.FlushProfilesResponse{
		Success: true,
	}, nil
}
