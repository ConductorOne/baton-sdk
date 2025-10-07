package connectorbuilder

import (
	"context"
	"fmt"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/retry"
	"github.com/conductorone/baton-sdk/pkg/types/tasks"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

// ResourceProvisioner extends ResourceSyncer to add capabilities for granting and revoking access.
//
// Note: ResourceProvisionerV2 is preferred for new connectors as it provides
// enhanced grant capabilities.
//
// Implementing this interface indicates the connector supports provisioning operations
// for the associated resource type.
type ResourceProvisioner interface {
	ResourceSyncer
	ResourceType(ctx context.Context) *v2.ResourceType
	Grant(ctx context.Context, resource *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error)
	Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error)
}

// ResourceProvisionerV2 extends ResourceSyncer to add capabilities for granting and revoking access
// with enhanced functionality compared to ResourceProvisioner.
//
// This is the recommended interface for implementing provisioning operations in new connectors.
// It differs from ResourceProvisioner by returning a list of grants from the Grant method.
type ResourceProvisionerV2 interface {
	ResourceSyncer
	ResourceType(ctx context.Context) *v2.ResourceType
	Grant(ctx context.Context, resource *v2.Resource, entitlement *v2.Entitlement) ([]*v2.Grant, annotations.Annotations, error)
	Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error)
}

func (b *builderImpl) Grant(ctx context.Context, request *v2.GrantManagerServiceGrantRequest) (*v2.GrantManagerServiceGrantResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.Grant")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.GrantType
	l := ctxzap.Extract(ctx)

	rt := request.Entitlement.Resource.Id.ResourceType

	retryer := retry.NewRetryer(ctx, retry.RetryConfig{
		MaxAttempts:  3,
		InitialDelay: 15 * time.Second,
		MaxDelay:     60 * time.Second,
	})

	var grantFunc func(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) ([]*v2.Grant, annotations.Annotations, error)
	provisioner, ok := b.resourceProvisioners[rt]
	if ok {
		grantFunc = func(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) ([]*v2.Grant, annotations.Annotations, error) {
			annos, err := provisioner.Grant(ctx, principal, entitlement)
			if err != nil {
				return nil, annos, err
			}
			return nil, annos, nil
		}
	}
	provisionerV2, ok := b.resourceProvisionersV2[rt]
	if ok {
		grantFunc = provisionerV2.Grant
	}

	if grantFunc == nil {
		l.Error("error: resource type does not have provisioner configured", zap.String("resource_type", rt))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: resource type does not have provisioner configured")
	}

	for {
		grants, annos, err := grantFunc(ctx, request.Principal, request.Entitlement)
		if err == nil {
			b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
			return &v2.GrantManagerServiceGrantResponse{Annotations: annos, Grants: grants}, nil
		}
		if retryer.ShouldWaitAndRetry(ctx, err) {
			continue
		}
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("grant failed: %w", err)
	}
}

func (b *builderImpl) Revoke(ctx context.Context, request *v2.GrantManagerServiceRevokeRequest) (*v2.GrantManagerServiceRevokeResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.Revoke")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.RevokeType

	l := ctxzap.Extract(ctx)

	rt := request.Grant.Entitlement.Resource.Id.ResourceType

	retryer := retry.NewRetryer(ctx, retry.RetryConfig{
		MaxAttempts:  3,
		InitialDelay: 15 * time.Second,
		MaxDelay:     60 * time.Second,
	})

	var revokeFunc func(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error)
	provisioner, ok := b.resourceProvisioners[rt]
	if ok {
		revokeFunc = provisioner.Revoke
	}
	provisionerV2, ok := b.resourceProvisionersV2[rt]
	if ok {
		revokeFunc = provisionerV2.Revoke
	}

	if revokeFunc == nil {
		l.Error("error: resource type does not have provisioner configured", zap.String("resource_type", rt))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: resource type does not have provisioner configured")
	}

	for {
		annos, err := revokeFunc(ctx, request.Grant)
		if err == nil {
			b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
			return &v2.GrantManagerServiceRevokeResponse{Annotations: annos}, nil
		}
		if retryer.ShouldWaitAndRetry(ctx, err) {
			continue
		}
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("revoke failed: %w", err)
	}
}
