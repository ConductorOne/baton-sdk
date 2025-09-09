package sync

import (
	"context"
	"reflect"

	connectorV2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/types"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"
)

// syncIDClientWrapper wraps a ConnectorClient to add syncID to annotations in requests,
// to be used by the Session Manager.
type syncIDClientWrapper struct {
	types.ConnectorClient // Embed the original client
	syncID                string
}

// ensure syncIDClientWrapper implements types.ConnectorClient.
var _ types.ConnectorClient = (*syncIDClientWrapper)(nil)

// requestWithAnnotations is an interface that all request types implement.
type requestWithAnnotations interface {
	GetAnnotations() []*anypb.Any
}

// addSyncIDToRequest adds syncID to the annotations of a request.
func (w *syncIDClientWrapper) addSyncIDToRequest(req requestWithAnnotations) {
	if req == nil || w.syncID == "" {
		return
	}

	// Get current annotations
	currentAnnotations := req.GetAnnotations()
	if currentAnnotations == nil {
		currentAnnotations = []*anypb.Any{}
	}

	// Create ActiveSync annotation
	activeSync := &connectorV2.ActiveSync{
		Id: w.syncID,
	}

	// Add the ActiveSync annotation
	annos := annotations.Annotations(currentAnnotations)
	annos.Update(activeSync)

	// Use reflection to set the Annotations field since the interface only provides a getter
	reqValue := reflect.ValueOf(req)
	if reqValue.Kind() == reflect.Ptr {
		reqValue = reqValue.Elem()
	}

	annotationsField := reqValue.FieldByName("Annotations")
	if annotationsField.IsValid() {
		annotationsField.Set(reflect.ValueOf([]*anypb.Any(annos)))
	}
}

// Only override methods that have requests with annotations.
func (w *syncIDClientWrapper) ListResourceTypes(
	ctx context.Context,
	in *connectorV2.ResourceTypesServiceListResourceTypesRequest,
	opts ...grpc.CallOption,
) (*connectorV2.ResourceTypesServiceListResourceTypesResponse, error) {
	w.addSyncIDToRequest(in)
	return w.ConnectorClient.ListResourceTypes(ctx, in, opts...)
}

func (w *syncIDClientWrapper) ListResources(
	ctx context.Context,
	in *connectorV2.ResourcesServiceListResourcesRequest,
	opts ...grpc.CallOption,
) (*connectorV2.ResourcesServiceListResourcesResponse, error) {
	w.addSyncIDToRequest(in)
	return w.ConnectorClient.ListResources(ctx, in, opts...)
}

func (w *syncIDClientWrapper) GetResource(
	ctx context.Context,
	in *connectorV2.ResourceGetterServiceGetResourceRequest,
	opts ...grpc.CallOption,
) (*connectorV2.ResourceGetterServiceGetResourceResponse, error) {
	w.addSyncIDToRequest(in)
	return w.ConnectorClient.GetResource(ctx, in, opts...)
}

func (w *syncIDClientWrapper) ListEntitlements(
	ctx context.Context,
	in *connectorV2.EntitlementsServiceListEntitlementsRequest,
	opts ...grpc.CallOption,
) (*connectorV2.EntitlementsServiceListEntitlementsResponse, error) {
	w.addSyncIDToRequest(in)
	return w.ConnectorClient.ListEntitlements(ctx, in, opts...)
}

func (w *syncIDClientWrapper) ListGrants(
	ctx context.Context,
	in *connectorV2.GrantsServiceListGrantsRequest,
	opts ...grpc.CallOption,
) (*connectorV2.GrantsServiceListGrantsResponse, error) {
	w.addSyncIDToRequest(in)
	return w.ConnectorClient.ListGrants(ctx, in, opts...)
}

func (w *syncIDClientWrapper) Grant(
	ctx context.Context,
	in *connectorV2.GrantManagerServiceGrantRequest,
	opts ...grpc.CallOption,
) (*connectorV2.GrantManagerServiceGrantResponse, error) {
	w.addSyncIDToRequest(in)
	return w.ConnectorClient.Grant(ctx, in, opts...)
}

func (w *syncIDClientWrapper) Revoke(
	ctx context.Context,
	in *connectorV2.GrantManagerServiceRevokeRequest,
	opts ...grpc.CallOption,
) (*connectorV2.GrantManagerServiceRevokeResponse, error) {
	w.addSyncIDToRequest(in)
	return w.ConnectorClient.Revoke(ctx, in, opts...)
}

func (w *syncIDClientWrapper) ListEventFeeds(
	ctx context.Context,
	in *connectorV2.ListEventFeedsRequest,
	opts ...grpc.CallOption,
) (*connectorV2.ListEventFeedsResponse, error) {
	w.addSyncIDToRequest(in)
	return w.ConnectorClient.ListEventFeeds(ctx, in, opts...)
}

func (w *syncIDClientWrapper) ListEvents(
	ctx context.Context,
	in *connectorV2.ListEventsRequest,
	opts ...grpc.CallOption,
) (*connectorV2.ListEventsResponse, error) {
	w.addSyncIDToRequest(in)
	return w.ConnectorClient.ListEvents(ctx, in, opts...)
}

func (w *syncIDClientWrapper) ListActionSchemas(
	ctx context.Context,
	in *connectorV2.ListActionSchemasRequest,
	opts ...grpc.CallOption,
) (*connectorV2.ListActionSchemasResponse, error) {
	w.addSyncIDToRequest(in)
	return w.ConnectorClient.ListActionSchemas(ctx, in, opts...)
}

func (w *syncIDClientWrapper) GetActionSchema(
	ctx context.Context,
	in *connectorV2.GetActionSchemaRequest,
	opts ...grpc.CallOption,
) (*connectorV2.GetActionSchemaResponse, error) {
	w.addSyncIDToRequest(in)
	return w.ConnectorClient.GetActionSchema(ctx, in, opts...)
}

func (w *syncIDClientWrapper) InvokeAction(
	ctx context.Context,
	in *connectorV2.InvokeActionRequest,
	opts ...grpc.CallOption,
) (*connectorV2.InvokeActionResponse, error) {
	w.addSyncIDToRequest(in)
	return w.ConnectorClient.InvokeAction(ctx, in, opts...)
}

func (w *syncIDClientWrapper) GetActionStatus(
	ctx context.Context,
	in *connectorV2.GetActionStatusRequest,
	opts ...grpc.CallOption,
) (*connectorV2.GetActionStatusResponse, error) {
	w.addSyncIDToRequest(in)
	return w.ConnectorClient.GetActionStatus(ctx, in, opts...)
}

func (w *syncIDClientWrapper) CreateTicket(
	ctx context.Context,
	in *connectorV2.TicketsServiceCreateTicketRequest,
	opts ...grpc.CallOption,
) (*connectorV2.TicketsServiceCreateTicketResponse, error) {
	w.addSyncIDToRequest(in)
	return w.ConnectorClient.CreateTicket(ctx, in, opts...)
}

func (w *syncIDClientWrapper) GetTicket(
	ctx context.Context,
	in *connectorV2.TicketsServiceGetTicketRequest,
	opts ...grpc.CallOption,
) (*connectorV2.TicketsServiceGetTicketResponse, error) {
	w.addSyncIDToRequest(in)
	return w.ConnectorClient.GetTicket(ctx, in, opts...)
}

func (w *syncIDClientWrapper) GetTicketSchema(
	ctx context.Context,
	in *connectorV2.TicketsServiceGetTicketSchemaRequest,
	opts ...grpc.CallOption,
) (*connectorV2.TicketsServiceGetTicketSchemaResponse, error) {
	w.addSyncIDToRequest(in)
	return w.ConnectorClient.GetTicketSchema(ctx, in, opts...)
}

func (w *syncIDClientWrapper) ListTicketSchemas(
	ctx context.Context,
	in *connectorV2.TicketsServiceListTicketSchemasRequest,
	opts ...grpc.CallOption,
) (*connectorV2.TicketsServiceListTicketSchemasResponse, error) {
	w.addSyncIDToRequest(in)
	return w.ConnectorClient.ListTicketSchemas(ctx, in, opts...)
}

func (w *syncIDClientWrapper) Cleanup(
	ctx context.Context,
	in *connectorV2.ConnectorServiceCleanupRequest,
	opts ...grpc.CallOption,
) (*connectorV2.ConnectorServiceCleanupResponse, error) {
	w.addSyncIDToRequest(in)
	return w.ConnectorClient.Cleanup(ctx, in, opts...)
}
