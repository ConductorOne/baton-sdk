package connector

import (
	"context"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type noopProvisioner struct{}

func (n *noopProvisioner) Grant(ctx context.Context, req *v2.GrantManagerServiceGrantRequest) (*v2.GrantManagerServiceGrantResponse, error) {
	return nil, status.Error(codes.FailedPrecondition, "provisioning is not enabled")
}

func (n *noopProvisioner) Revoke(ctx context.Context, req *v2.GrantManagerServiceRevokeRequest) (*v2.GrantManagerServiceRevokeResponse, error) {
	return nil, status.Error(codes.FailedPrecondition, "provisioning is not enabled")
}
