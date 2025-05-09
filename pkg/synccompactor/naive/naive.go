package naive

import (
	"context"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func NewNaiveCompactor(base *dotc1z.C1File, applied *dotc1z.C1File, dest *dotc1z.C1File) *Compactor {
	return &Compactor{
		base:    base,
		applied: applied,
		dest:    dest,
	}
}

type Compactor struct {
	base    *dotc1z.C1File
	applied *dotc1z.C1File
	dest    *dotc1z.C1File
}

func (n *Compactor) Compact(ctx context.Context) error {
	if err := n.processResourceTypes(ctx); err != nil {
		return err
	}
	if err := n.processResources(ctx); err != nil {
		return err
	}
	if err := n.processEntitlements(ctx); err != nil {
		return err
	}
	if err := n.processGrants(ctx); err != nil {
		return err
	}
	return nil
}

func naiveCompact[T proto.Message, REQ listRequest, RESP listResponse[T]](
	ctx context.Context,
	base listFunc[T, REQ, RESP],
	applied listFunc[T, REQ, RESP],
	save func(context.Context, ...T) error,
) error {
	var t T
	l := ctxzap.Extract(ctx)
	l.Info("naive compaction: compacting objects", zap.String("object_type", string(t.ProtoReflect().Descriptor().FullName())))
	// List all objects from the base file and save them in the destination file
	if err := listAllObjects(ctx, base, func(items []T) (bool, error) {
		if err := save(ctx, items...); err != nil {
			return false, err
		}
		return true, nil
	}); err != nil {
		return err
	}

	// Then list all objects from the applied file and save them in the destination file, overwriting ones with the same external_id
	if err := listAllObjects(ctx, applied, func(items []T) (bool, error) {
		if err := save(ctx, items...); err != nil {
			return false, err
		}
		return true, nil
	}); err != nil {
		return err
	}

	return nil
}

func (n *Compactor) processResourceTypes(ctx context.Context) error {
	return naiveCompact(ctx, n.base.ListResourceTypes, n.applied.ListResourceTypes, n.dest.PutResourceTypesIfNewer)
}

func (n *Compactor) processResources(ctx context.Context) error {
	return naiveCompact(ctx, n.base.ListResources, n.applied.ListResources, n.dest.PutResourcesIfNewer)
}

func (n *Compactor) processGrants(ctx context.Context) error {
	return naiveCompact(ctx, n.base.ListGrants, n.applied.ListGrants, n.dest.PutGrantsIfNewer)
}

func (n *Compactor) processEntitlements(ctx context.Context) error {
	return naiveCompact(ctx, n.base.ListEntitlements, n.applied.ListEntitlements, n.dest.PutEntitlementsIfNewer)
}
