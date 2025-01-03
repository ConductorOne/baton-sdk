package connector

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

var _ v2.GrantManagerServiceServer = (*noopProvisioner)(nil)
var _ v2.ResourceManagerServiceServer = (*noopProvisioner)(nil)
var _ v2.AccountManagerServiceServer = (*noopProvisioner)(nil)
var _ v2.CredentialManagerServiceServer = (*noopProvisioner)(nil)

type noopProvisioner struct{}

func (n *noopProvisioner) Grant(ctx context.Context, req *v2.GrantManagerServiceGrantRequest) (*v2.GrantManagerServiceGrantResponse, error) {
	return nil, status.Error(codes.FailedPrecondition, "provisioning is not enabled")
}

func (n *noopProvisioner) Revoke(ctx context.Context, req *v2.GrantManagerServiceRevokeRequest) (*v2.GrantManagerServiceRevokeResponse, error) {
	return nil, status.Error(codes.FailedPrecondition, "provisioning is not enabled")
}

func (n *noopProvisioner) CreateResource(ctx context.Context, request *v2.CreateResourceRequest) (*v2.CreateResourceResponse, error) {
	return nil, status.Error(codes.FailedPrecondition, "provisioning is not enabled")
}

func (n *noopProvisioner) DeleteResource(ctx context.Context, request *v2.DeleteResourceRequest) (*v2.DeleteResourceResponse, error) {
	return nil, status.Error(codes.FailedPrecondition, "provisioning is not enabled")
}

func (n *noopProvisioner) RotateCredential(ctx context.Context, request *v2.RotateCredentialRequest) (*v2.RotateCredentialResponse, error) {
	return nil, status.Error(codes.FailedPrecondition, "provisioning is not enabled")
}

func (n *noopProvisioner) CreateAccount(ctx context.Context, request *v2.CreateAccountRequest) (*v2.CreateAccountResponse, error) {
	return nil, status.Error(codes.FailedPrecondition, "provisioning is not enabled")
}
