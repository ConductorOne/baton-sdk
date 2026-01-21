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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	ResourceProvisionerLimited
}

type ResourceProvisionerLimited interface {
	RevokeProvisioner
	GrantProvisioner
}

type RevokeProvisioner interface {
	Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error)
}

type GrantProvisioner interface {
	Grant(ctx context.Context, resource *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error)
}

// ResourceProvisionerV2 extends ResourceSyncer to add capabilities for granting and revoking access
// with enhanced functionality compared to ResourceProvisioner.
//
// This is the recommended interface for implementing provisioning operations in new connectors.
// It differs from ResourceProvisioner by returning a list of grants from the Grant method.
type ResourceProvisionerV2 interface {
	ResourceSyncer
	ResourceProvisionerV2Limited
}

type ResourceProvisionerV2Limited interface {
	RevokeProvisioner
	GrantProvisionerV2
}

type GrantProvisionerV2 interface {
	Grant(ctx context.Context, resource *v2.Resource, entitlement *v2.Entitlement) ([]*v2.Grant, annotations.Annotations, error)
}

func (b *builder) Grant(ctx context.Context, request *v2.GrantManagerServiceGrantRequest) (*v2.GrantManagerServiceGrantResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.Grant")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.GrantType
	l := ctxzap.Extract(ctx)

	rt := request.GetEntitlement().GetResource().GetId().GetResourceType()

	provisioner, ok := b.resourceProvisioners[rt]

	if !ok {
		l.Error("error: resource type does not have provisioner configured", zap.String("resource_type", rt))
		err := status.Errorf(codes.Unimplemented, "resource type %s does not have provisioner configured", rt)
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}

	retryer := retry.NewRetryer(ctx, retry.RetryConfig{
		MaxAttempts:  3,
		InitialDelay: 15 * time.Second,
		MaxDelay:     60 * time.Second,
	})

	for {
		grants, annos, err := provisioner.Grant(ctx, request.GetPrincipal(), request.GetEntitlement())
		if err == nil {
			b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
			return v2.GrantManagerServiceGrantResponse_builder{Annotations: annos, Grants: grants}.Build(), nil
		}
		if retryer.ShouldWaitAndRetry(ctx, err) {
			continue
		}
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("grant failed: %w", err)
	}
}

func (b *builder) Revoke(ctx context.Context, request *v2.GrantManagerServiceRevokeRequest) (*v2.GrantManagerServiceRevokeResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.Revoke")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.RevokeType

	l := ctxzap.Extract(ctx)

	rt := request.GetGrant().GetEntitlement().GetResource().GetId().GetResourceType()

	var revokeProvisioner RevokeProvisioner
	provisioner, ok := b.resourceProvisioners[rt]
	if ok {
		revokeProvisioner = provisioner
	}

	if revokeProvisioner == nil {
		l.Error("error: resource type does not have provisioner configured", zap.String("resource_type", rt))
		err := status.Errorf(codes.Unimplemented, "resource type %s does not have provisioner configured", rt)
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}

	retryer := retry.NewRetryer(ctx, retry.RetryConfig{
		MaxAttempts:  3,
		InitialDelay: 15 * time.Second,
		MaxDelay:     60 * time.Second,
	})

	for {
		annos, err := revokeProvisioner.Revoke(ctx, request.GetGrant())
		if err == nil {
			b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
			return v2.GrantManagerServiceRevokeResponse_builder{Annotations: annos}.Build(), nil
		}
		if retryer.ShouldWaitAndRetry(ctx, err) {
			continue
		}
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("revoke failed: %w", err)
	}
}

func newResourceProvisionerV1to2(p ResourceProvisionerLimited) ResourceProvisionerV2Limited {
	return &resourceProvisionerV1to2{
		ResourceProvisionerLimited: p,
	}
}

type resourceProvisionerV1to2 struct {
	ResourceProvisionerLimited
}

func (r *resourceProvisionerV1to2) Grant(ctx context.Context, resource *v2.Resource, entitlement *v2.Entitlement) ([]*v2.Grant, annotations.Annotations, error) {
	annos, err := r.ResourceProvisionerLimited.Grant(ctx, resource, entitlement)
	if err != nil {
		return nil, annos, err
	}
	return nil, annos, nil
}

func (b *builder) addProvisioner(_ context.Context, typeId string, in interface{}) error {
	if provisioner, ok := in.(ResourceProvisionerLimited); ok {
		if _, ok := b.resourceProvisioners[typeId]; ok {
			return fmt.Errorf("error: duplicate resource type found for resource provisioner %s", typeId)
		}
		b.resourceProvisioners[typeId] = newResourceProvisionerV1to2(provisioner)
	}
	if provisioner, ok := in.(ResourceProvisionerV2Limited); ok {
		if _, ok := b.resourceProvisioners[typeId]; ok {
			return fmt.Errorf("error: duplicate resource type found for resource provisioner v2 %s", typeId)
		}
		b.resourceProvisioners[typeId] = provisioner
	}
	return nil
}
