package resourcelookup

import (
	"context"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/types"
)

type ResourceFinder struct {
	lookupToken string
	connector   types.ConnectorClient
}

func (p *ResourceFinder) Run(ctx context.Context) error {
	l := ctxzap.Extract(ctx)
	resource, err := p.connector.LookupResource(ctx, &v2.ResourceLookupServiceLookupResourceRequest{
		LookupToken: p.lookupToken,
		Annotations: nil,
	})
	if err != nil {
		l.Error("Failed to lookup resource", zap.Error(err))
		return err
	}

	l.Debug("Resource lookup result", zap.Any("resource", resource), zap.Any("annotations", resource.GetAnnotations()))

	return nil
}

func (p *ResourceFinder) Close(ctx context.Context) error {
	return nil
}

func NewResourceLookupper(c types.ConnectorClient, lookupToken string) *ResourceFinder {
	return &ResourceFinder{
		connector:   c,
		lookupToken: lookupToken,
	}
}
