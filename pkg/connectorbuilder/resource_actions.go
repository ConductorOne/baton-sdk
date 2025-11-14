package connectorbuilder

import (
	"context"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"google.golang.org/protobuf/types/known/structpb"
)

// ResourceActionHandler is the function signature for handling resource actions.
// It receives the resource ID and action arguments, and returns the response,
// annotations, and any error.
type ResourceActionHandler func(
	ctx context.Context,
	resourceID *v2.ResourceId,
	args *structpb.Struct,
) (*structpb.Struct, annotations.Annotations, error)

// ResourceActionProvider is an interface that resource builders can implement
// to provide resource-scoped actions for their resource type.
type ResourceActionProvider interface {
	// ResourceActions returns the schemas and handlers for all resource actions
	// supported by this resource type.
	ResourceActions(ctx context.Context) ([]*v2.ResourceActionSchema, map[string]ResourceActionHandler, error)
}
