package connector

import (
	"context"

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
	if ps.profiler == nil {
		return &connectorwrapperV1.FlushProfilesResponse{
			Success: true,
		}, nil
	}

	// Stop CPU profiling to flush data
	if err := ps.profiler.Stop(ctx); err != nil {
		return &connectorwrapperV1.FlushProfilesResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	// Write memory profile
	if err := ps.profiler.WriteMemProfile(ctx); err != nil {
		return &connectorwrapperV1.FlushProfilesResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &connectorwrapperV1.FlushProfilesResponse{
		Success: true,
	}, nil
}
