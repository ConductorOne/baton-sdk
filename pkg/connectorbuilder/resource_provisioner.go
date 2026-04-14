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
	ResourceSyncerV2
	ResourceProvisionerV2Limited
}

type ResourceProvisionerV2Limited interface {
	RevokeProvisioner
	GrantProvisionerV2
}

type GrantProvisionerV2 interface {
	Grant(ctx context.Context, resource *v2.Resource, entitlement *v2.Entitlement) ([]*v2.Grant, annotations.Annotations, error)
}

// SwapProvisioner extends a resource syncer to add support for atomic
// entitlement transitions within an exclusion group.
//
// Connectors should implement this interface when the downstream system
// supports atomic role changes (e.g., "set user role" APIs that implicitly
// revoke the previous role).
//
// If a connector does not implement SwapProvisioner, the SDK falls back to
// sequential Revoke + Grant. The fallback is not atomic — there is a brief
// window where the principal holds neither entitlement.
type SwapProvisioner interface {
	Swap(
		ctx context.Context,
		currentGrant *v2.Grant,
		newEntitlement *v2.Entitlement,
	) ([]*v2.Grant, annotations.Annotations, error)
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
	if swapper, ok := in.(SwapProvisioner); ok {
		b.swapProvisioners[typeId] = swapper
	}
	return nil
}

func (b *builder) Swap(ctx context.Context, request *v2.SwapServiceSwapRequest) (*v2.SwapServiceSwapResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.Swap")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.SwapType
	l := ctxzap.Extract(ctx)

	revokeRT := request.GetRevokeGrant().GetEntitlement().GetResource().GetId().GetResourceType()
	grantRT := request.GetGrantEntitlement().GetResource().GetId().GetResourceType()

	if revokeRT != grantRT {
		err := status.Errorf(codes.InvalidArgument,
			"swap: revoke resource type %s does not match grant resource type %s",
			revokeRT, grantRT)
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}

	principal := request.GetRevokeGrant().GetPrincipal()

	// Try native swap first
	if swapper, ok := b.swapProvisioners[grantRT]; ok {
		retryer := retry.NewRetryer(ctx, retry.RetryConfig{
			MaxAttempts:  3,
			InitialDelay: 15 * time.Second,
			MaxDelay:     60 * time.Second,
		})
		for {
			grants, annos, err := swapper.Swap(ctx, request.GetRevokeGrant(), request.GetGrantEntitlement())
			if err == nil {
				b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
				return v2.SwapServiceSwapResponse_builder{Annotations: annos, Grants: grants}.Build(), nil
			}
			if retryer.ShouldWaitAndRetry(ctx, err) {
				continue
			}
			b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
			return nil, fmt.Errorf("swap failed: %w", err)
		}
	}

	// Fallback: sequential Revoke then Grant
	l.Info("swap: connector does not implement SwapProvisioner, falling back to Revoke+Grant",
		zap.String("resource_type", grantRT))

	provisioner, ok := b.resourceProvisioners[grantRT]
	if !ok {
		err := status.Errorf(codes.Unimplemented,
			"resource type %s does not have provisioner configured", grantRT)
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}

	// Step 1: Revoke the existing grant (with retry)
	revokeRetryer := retry.NewRetryer(ctx, retry.RetryConfig{
		MaxAttempts:  3,
		InitialDelay: 15 * time.Second,
		MaxDelay:     60 * time.Second,
	})
	for {
		_, err := provisioner.Revoke(ctx, request.GetRevokeGrant())
		if err == nil {
			break
		}
		if revokeRetryer.ShouldWaitAndRetry(ctx, err) {
			continue
		}
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("swap revoke step failed: %w", err)
	}

	// Step 2: Grant the new entitlement (with retry)
	grantRetryer := retry.NewRetryer(ctx, retry.RetryConfig{
		MaxAttempts:  3,
		InitialDelay: 15 * time.Second,
		MaxDelay:     60 * time.Second,
	})
	for {
		grants, annos, err := provisioner.Grant(ctx, principal, request.GetGrantEntitlement())
		if err == nil {
			b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
			return v2.SwapServiceSwapResponse_builder{Annotations: annos, Grants: grants}.Build(), nil
		}
		if grantRetryer.ShouldWaitAndRetry(ctx, err) {
			continue
		}

		// Grant failed after retries. Build a rich error with SwapPartialFailure
		// detail so ConductorOne can decide on compensating actions.
		l.Error("swap: revoke succeeded but grant failed — principal may have no entitlement",
			zap.String("revoked_grant", request.GetRevokeGrant().GetId()),
			zap.String("failed_entitlement", request.GetGrantEntitlement().GetId()),
			zap.Error(err),
		)

		partialFailure := v2.SwapPartialFailure_builder{
			RevokedGrant:           request.GetRevokeGrant(),
			FailedGrantEntitlement: request.GetGrantEntitlement(),
		}.Build()

		revokedAnnos := annotations.Annotations(request.GetRevokeGrant().GetEntitlement().GetAnnotations())
		eg := &v2.EntitlementExclusionGroup{}
		if found, pickErr := revokedAnnos.Pick(eg); found && pickErr == nil {
			partialFailure.SetExclusionGroupId(eg.GetExclusionGroupId())
		}

		st := status.New(codes.Internal, "swap grant step failed (revoke already completed)")
		stWithDetails, detailErr := st.WithDetails(partialFailure)
		if detailErr != nil {
			b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), st.Err())
			return nil, st.Err()
		}

		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), stWithDetails.Err())
		return nil, stWithDetails.Err()
	}
}
