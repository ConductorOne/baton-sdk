package connectorbuilder

import (
	"context"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/types"
)

// ResourceSyncer2Wrapper wraps a ResourceSyncer2 to implement ResourceSyncer interface.
// It extracts the session store from the context and passes it to the underlying ResourceSyncer2 methods.
type ResourceSyncer2Wrapper struct {
	syncer2      ResourceSyncer2
	sessionStore types.SessionStore
}

// NewResourceSyncer2Wrapper creates a new wrapper that converts ResourceSyncer2 to ResourceSyncer.
func NewResourceSyncer2Wrapper(syncer2 ResourceSyncer2, sessionStore types.SessionStore) *ResourceSyncer2Wrapper {
	return &ResourceSyncer2Wrapper{
		syncer2:      syncer2,
		sessionStore: sessionStore,
	}
}

// ResourceType returns the resource type from the underlying ResourceSyncer2.
func (w *ResourceSyncer2Wrapper) ResourceType(ctx context.Context) *v2.ResourceType {
	return w.syncer2.ResourceType(ctx)
}

// List wraps the ResourceSyncer2.List method by extracting the session store from context.
func (w *ResourceSyncer2Wrapper) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	return w.syncer2.List(ctx, parentResourceID, pToken, types.ResourceSyncerOptions{
		Session: w.sessionStore,
	})
}

// Entitlements wraps the ResourceSyncer2.Entitlements method by extracting the session store from context.
func (w *ResourceSyncer2Wrapper) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return w.syncer2.Entitlements(ctx, resource, pToken, types.ResourceSyncerOptions{
		Session: w.sessionStore,
	})
}

// Grants wraps the ResourceSyncer2.Grants method by extracting the session store from context.
func (w *ResourceSyncer2Wrapper) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return w.syncer2.Grants(ctx, resource, pToken, types.ResourceSyncerOptions{
		Session: w.sessionStore,
	})
}

// ConnectorBuilder2Wrapper wraps a ConnectorBuilder2 to implement ConnectorBuilder interface.
// It converts ResourceSyncer2 instances to ResourceSyncer by wrapping them with ResourceSyncer2Wrapper.
type ConnectorBuilder2Wrapper struct {
	builder2     ConnectorBuilder2
	sessionStore types.SessionStore
}

// NewConnectorBuilder2Wrapper creates a new wrapper that converts ConnectorBuilder2 to ConnectorBuilder.
func NewConnectorBuilder2Wrapper(builder2 ConnectorBuilder2, sessionStore types.SessionStore) *ConnectorBuilder2Wrapper {
	return &ConnectorBuilder2Wrapper{
		builder2:     builder2,
		sessionStore: sessionStore,
	}
}

// Metadata returns the metadata from the underlying ConnectorBuilder2.
func (w *ConnectorBuilder2Wrapper) Metadata(ctx context.Context) (*v2.ConnectorMetadata, error) {
	return w.builder2.Metadata(ctx)
}

// Validate validates the connector using the underlying ConnectorBuilder2.
func (w *ConnectorBuilder2Wrapper) Validate(ctx context.Context) (annotations.Annotations, error) {
	return w.builder2.Validate(ctx)
}

// ResourceSyncers converts ResourceSyncer2 instances to ResourceSyncer by wrapping them.
func (w *ConnectorBuilder2Wrapper) ResourceSyncers(ctx context.Context) []ResourceSyncer {
	syncers2 := w.builder2.ResourceSyncers(ctx)
	syncers := make([]ResourceSyncer, len(syncers2))

	for i, syncer2 := range syncers2 {
		syncers[i] = NewResourceSyncer2Wrapper(syncer2, w.sessionStore)
	}

	return syncers
}
